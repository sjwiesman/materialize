// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL diagnostic conversion for the LSP server.
//!
//! Provides two tiers of diagnostics:
//!
//! - **Per-keystroke diagnostics** ([`diagnose()`]) — Resolves psql-style
//!   variables before parsing. Unresolved variables produce positioned
//!   diagnostics (ERROR or WARNING depending on the warn pragma). The resolved
//!   SQL is then parsed with [`mz_sql_parser::parser::parse_statements()`] and
//!   any parse error positions are mapped back to original-text offsets via
//!   [`resolved_to_original`]. The [`Rope`] is always built from the original
//!   text (what the editor shows), since all emitted byte offsets are in
//!   original-text space.
//!
//! - **On-save validation errors** ([`validation_diagnostics()`]) — Converts
//!   project-level [`ValidationError`]s into LSP diagnostics grouped by file.
//!   When an error carries a byte offset (most statement-level errors), the
//!   diagnostic is positioned at the correct line/column. File-level errors
//!   (e.g., missing CREATE statement) fall back to `(0, 0)`.

use crate::fs::FileSystem;
use crate::project::compiler::typecheck::{
    ObjectTypeCheckError, ObjectTypeCheckErrorKind, TypeCheckError,
};
use crate::project::error::ValidationError;
use crate::project::syntax::variables::{resolve_variables, resolved_to_original};
use mz_sql::catalog::CatalogError;
use mz_sql::plan::PlanError;
use ropey::Rope;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tower_lsp::lsp_types::{Diagnostic, DiagnosticSeverity, Position, Range};

/// Parse `text` as SQL and return diagnostics for any parse errors and variable issues.
///
/// Resolves psql-style variables before parsing. Unresolved variables produce
/// diagnostics at their position in the original text. Parse errors from the
/// resolved SQL are mapped back to original-text positions via the substitution log.
///
/// # Arguments
/// * `text` — The original SQL source text (as the editor shows it).
/// * `rope` — A [`Rope`] built from the same `text`, used for byte-offset to
///   line/column conversion.
/// * `variables` — Variable definitions from the project config.
/// * `profile_name` — Active profile name (if any), shown in undefined-variable
///   messages. When `None`, the diagnostic suggests setting a profile.
///
/// # Returns
/// A (possibly empty) vec of LSP diagnostics. Variable diagnostics are
/// `WARNING` if the file has the warn pragma, `ERROR` otherwise. Parse errors
/// are always `ERROR`.
pub fn diagnose(
    text: &str,
    rope: &Rope,
    variables: &BTreeMap<String, String>,
    profile_name: Option<&str>,
) -> Vec<Diagnostic> {
    if text.trim().is_empty() {
        return Vec::new();
    }

    let resolved = resolve_variables(text, variables);
    let mut diags = Vec::new();

    // Unresolved variable diagnostics — positioned in original text.
    let var_severity = if resolved.has_warn_pragma {
        DiagnosticSeverity::WARNING
    } else {
        DiagnosticSeverity::ERROR
    };
    for uv in &resolved.unresolved {
        let position =
            offset_to_position(uv.byte_offset, rope).unwrap_or_else(|| Position::new(0, 0));
        let end_position = offset_to_position(uv.byte_offset + uv.byte_len, rope)
            .unwrap_or_else(|| Position::new(0, 0));
        let message = match profile_name {
            Some(name) => format!(
                "undefined variable ':{}'  — define in [profiles.{}.variables] in project.toml",
                uv.name, name
            ),
            None => format!(
                "undefined variable ':{}'  — no profile is selected; run `mz-deploy profile set <name>` and define in [profiles.<name>.variables] in project.toml",
                uv.name
            ),
        };
        diags.push(Diagnostic {
            range: Range::new(position, end_position),
            severity: Some(var_severity),
            source: Some("mz-deploy".to_string()),
            message,
            ..Default::default()
        });
    }

    // Parse the resolved SQL; map errors back to original-text positions.
    if let Err(e) = mz_sql_parser::parser::parse_statements(&resolved.sql) {
        let original_offset = resolved_to_original(e.error.pos, &resolved.substitutions);
        let position =
            offset_to_position(original_offset, rope).unwrap_or_else(|| Position::new(0, 0));
        diags.push(Diagnostic {
            range: Range::new(position, position),
            severity: Some(DiagnosticSeverity::ERROR),
            source: Some("mz-deploy".to_string()),
            message: e.error.message.clone(),
            ..Default::default()
        });
    }

    diags
}

/// Convert [`ValidationError`]s into LSP diagnostics grouped by file path.
///
/// When an error carries a `byte_offset`, the file is read and a [`Rope`] is
/// built so the offset can be converted to a precise line/column position.
/// Errors without an offset (file-level) fall back to `(0, 0)`.
///
/// Returns an empty map when `errors` is empty.
pub(crate) fn validation_diagnostics(
    fs: &FileSystem,
    errors: &[ValidationError],
) -> BTreeMap<PathBuf, Vec<Diagnostic>> {
    let mut map: BTreeMap<PathBuf, Vec<Diagnostic>> = BTreeMap::new();
    // Cache ropes per file so we only read each file once.
    let mut rope_cache: BTreeMap<PathBuf, Option<Rope>> = BTreeMap::new();
    let zero = Position::new(0, 0);

    for error in errors {
        let position = if let Some(offset) = error.context.byte_offset {
            let rope = rope_cache
                .entry(error.context.file.clone())
                .or_insert_with(|| {
                    fs.read_to_string(&error.context.file)
                        .ok()
                        .map(|s| Rope::from_str(&s))
                });
            rope.as_ref()
                .and_then(|r| offset_to_position(offset, r))
                .unwrap_or(zero)
        } else {
            zero
        };

        map.entry(error.context.file.clone())
            .or_default()
            .push(Diagnostic {
                range: Range::new(position, position),
                severity: Some(DiagnosticSeverity::ERROR),
                source: Some("mz-deploy".to_string()),
                message: error.kind.message(),
                ..Default::default()
            });
    }

    map
}

/// Convert a [`TypeCheckError`] into LSP diagnostics grouped by file path.
///
/// Per-object errors (`TypeCheckFailed`, `Multiple`) are positioned by
/// inspecting the underlying error's structured information:
///
/// - `ParserStatementError` → byte offset from `error.error.pos`.
/// - `PlanError::UnknownColumn`, `AmbiguousColumn`, `UnknownFunction`,
///   `UnknownOperator`, etc. → first whole-word match of the offending
///   identifier in the on-disk source file.
/// - `CatalogError::UnknownItem`, `UnknownDatabase`, etc. → likewise.
/// - Variants without a locatable identifier fall back to `(0, 0)`.
///
/// The on-disk source file is read once per file and cached for the call.
/// If the read fails, all diagnostics for that file fall back to `(0, 0)`.
///
/// Non-object variants (`DatabaseSetupError`, `SortError`,
/// `TypesCacheWriteFailed`) have no per-file context and return an empty map;
/// callers should log them to the client message stream instead.
pub(crate) fn typecheck_diagnostics(
    fs: &FileSystem,
    error: &TypeCheckError,
) -> BTreeMap<PathBuf, Vec<Diagnostic>> {
    let errors: &[ObjectTypeCheckError] = match error {
        TypeCheckError::TypeCheckFailed(e) => std::slice::from_ref(e),
        TypeCheckError::Multiple(errs) => errs.as_slice(),
        TypeCheckError::DatabaseSetupError(_)
        | TypeCheckError::SortError(_)
        | TypeCheckError::TypesCacheWriteFailed(_) => &[],
    };

    let mut map: BTreeMap<PathBuf, Vec<Diagnostic>> = BTreeMap::new();
    let mut source_cache: BTreeMap<PathBuf, Option<(String, Rope)>> = BTreeMap::new();

    for e in errors {
        let entry = source_cache
            .entry(e.file_path.clone())
            .or_insert_with(|| read_source(fs, &e.file_path));
        let range = locate_in_source(&e.kind, entry.as_ref());

        let mut message = e.error_message();
        if let Some(detail) = e.detail() {
            message.push_str("\ndetail: ");
            message.push_str(&detail);
        }
        if let Some(hint) = e.hint() {
            message.push_str("\nhint: ");
            message.push_str(&hint);
        }

        map.entry(e.file_path.clone())
            .or_default()
            .push(Diagnostic {
                range,
                severity: Some(DiagnosticSeverity::ERROR),
                source: Some("mz-deploy".to_string()),
                message,
                ..Default::default()
            });
    }

    map
}

fn read_source(fs: &FileSystem, path: &Path) -> Option<(String, Rope)> {
    let text = fs.read_to_string(path).ok()?;
    let rope = Rope::from_str(&text);
    Some((text, rope))
}

fn locate_in_source(kind: &ObjectTypeCheckErrorKind, source: Option<&(String, Rope)>) -> Range {
    let zero = Position::new(0, 0);
    let Some((text, rope)) = source else {
        return Range::new(zero, zero);
    };
    let Some((start, end)) = locate_kind(kind, text) else {
        return Range::new(zero, zero);
    };
    let start_pos = offset_to_position(start, rope).unwrap_or(zero);
    let end_pos = offset_to_position(end, rope).unwrap_or(start_pos);
    Range::new(start_pos, end_pos)
}

fn locate_kind(kind: &ObjectTypeCheckErrorKind, source: &str) -> Option<(usize, usize)> {
    match kind {
        ObjectTypeCheckErrorKind::Parser(e) => {
            let pos = e.error.pos;
            Some((pos, pos))
        }
        ObjectTypeCheckErrorKind::Plan(e) => locate_plan(e, source),
        ObjectTypeCheckErrorKind::Catalog(e) => locate_catalog(e, source),
        ObjectTypeCheckErrorKind::Internal(_) => None,
    }
}

fn locate_plan(e: &PlanError, source: &str) -> Option<(usize, usize)> {
    use PlanError::*;
    match e {
        UnknownColumn { column, .. }
        | UngroupedColumn { column, .. }
        | UnknownColumnInUsingClause { column, .. }
        | AmbiguousColumnInUsingClause { column, .. }
        | WrongJoinTypeForLateralColumn { column, .. } => find_identifier(source, column.as_str()),
        AmbiguousColumn(column) => find_identifier(source, column.as_str()),
        AmbiguousTable(name) => find_identifier(source, name.item.as_str()),
        UnknownFunction { name, .. }
        | IndistinctFunction { name, .. }
        | UnknownOperator { name, .. }
        | IndistinctOperator { name, .. } => find_identifier(source, last_component(name)),
        Parser(p) => Some((p.pos, p.pos)),
        ParserStatement(p) => Some((p.error.pos, p.error.pos)),
        Catalog(c) => locate_catalog(c, source),
        _ => None,
    }
}

fn locate_catalog(e: &CatalogError, source: &str) -> Option<(usize, usize)> {
    use CatalogError::*;
    match e {
        UnknownDatabase(name)
        | UnknownSchema(name)
        | UnknownRole(name)
        | UnknownCluster(name)
        | UnknownClusterReplica(name)
        | UnknownConnection(name)
        | UnknownNetworkPolicy(name)
        | UnknownItem(name) => find_identifier(source, last_component(name)),
        UnknownFunction { name, .. } | UnknownType { name, .. } => {
            find_identifier(source, last_component(name))
        }
        _ => None,
    }
}

/// Strip qualifying prefixes from a dotted identifier, returning the final
/// component. `schema.table` → `table`; `t` → `t`.
fn last_component(s: &str) -> &str {
    s.rsplit_once('.').map(|(_, last)| last).unwrap_or(s)
}

/// Find the first whole-word occurrence of `name` in `source`, returning the
/// `[start, end)` byte range of the match. "Whole word" means the bytes
/// adjacent to the match are not identifier characters (`[A-Za-z0-9_]`).
fn find_identifier(source: &str, name: &str) -> Option<(usize, usize)> {
    if name.is_empty() {
        return None;
    }
    let bytes = source.as_bytes();
    let needle = name.as_bytes();
    if needle.len() > bytes.len() {
        return None;
    }
    for start in 0..=(bytes.len() - needle.len()) {
        if &bytes[start..start + needle.len()] != needle {
            continue;
        }
        let before_ok = start == 0 || !is_ident_byte(bytes[start - 1]);
        let end = start + needle.len();
        let after_ok = end == bytes.len() || !is_ident_byte(bytes[end]);
        if before_ok && after_ok {
            return Some((start, end));
        }
    }
    None
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Convert a byte offset to an LSP [`Position`] (line, column) using a [`Rope`].
pub(crate) fn offset_to_position(offset: usize, rope: &Rope) -> Option<Position> {
    let line = rope.try_char_to_line(offset).ok()?;
    let first_char_of_line = rope.try_line_to_char(line).ok()?;
    let column = offset - first_char_of_line;

    let line_u32 = line.try_into().ok()?;
    let column_u32 = column.try_into().ok()?;

    Some(Position::new(line_u32, column_u32))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_sql_produces_no_diagnostics() {
        let text = "CREATE VIEW foo AS SELECT 1;";
        let rope = Rope::from_str(text);
        assert!(diagnose(text, &rope, &BTreeMap::new(), None).is_empty());
    }

    #[test]
    fn syntax_error_produces_diagnostic_at_correct_position() {
        let text = "CREATE VIEW foo AS SELECTT 1;";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &BTreeMap::new(), None);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].severity, Some(DiagnosticSeverity::ERROR));
        // Error should be on line 0 (first line)
        assert_eq!(diags[0].range.start.line, 0);
    }

    #[test]
    fn multiline_error_position() {
        let text = "CREATE VIEW foo AS\nSELECT 1;\nCREATE VIEW bar AS SELECTT 2;";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &BTreeMap::new(), None);
        assert_eq!(diags.len(), 1);
        // Error should be on line 2 (third line, zero-indexed)
        assert_eq!(diags[0].range.start.line, 2);
    }

    #[test]
    fn empty_file_produces_no_diagnostics() {
        let text = "";
        let rope = Rope::from_str(text);
        assert!(diagnose(text, &rope, &BTreeMap::new(), None).is_empty());
    }

    #[test]
    fn whitespace_only_file_produces_no_diagnostics() {
        let text = "   \n  \n  ";
        let rope = Rope::from_str(text);
        assert!(diagnose(text, &rope, &BTreeMap::new(), None).is_empty());
    }

    // --- Variable-aware diagnose tests ---

    fn vars(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn resolved_variable_no_diagnostics() {
        let text = "CREATE MATERIALIZED VIEW mv IN CLUSTER quickstart AS SELECT 1";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &BTreeMap::new(), None);
        assert!(diags.is_empty());
    }

    #[test]
    fn resolved_variable_produces_clean_parse() {
        let v = vars(&[("cluster", "quickstart")]);
        let text = "CREATE MATERIALIZED VIEW mv IN CLUSTER :cluster AS SELECT 1";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &v, None);
        assert!(diags.is_empty());
    }

    #[test]
    fn unresolved_variable_produces_error() {
        let text = "CREATE MATERIALIZED VIEW mv IN CLUSTER :cluster AS SELECT 1";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &BTreeMap::new(), None);
        // Should have at least the variable error
        let var_diags: Vec<_> = diags
            .iter()
            .filter(|d| d.message.contains("undefined variable"))
            .collect();
        assert_eq!(var_diags.len(), 1);
        assert_eq!(var_diags[0].severity, Some(DiagnosticSeverity::ERROR));
        // `:cluster` starts at byte 39
        assert!(var_diags[0].message.contains(":cluster"));
    }

    #[test]
    fn unresolved_variable_with_pragma_produces_warning() {
        let text = "-- PRAGMA WARN_ON_MISSING_VARIABLES;\nCREATE MATERIALIZED VIEW mv IN CLUSTER :cluster AS SELECT 1";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &BTreeMap::new(), None);
        let var_diags: Vec<_> = diags
            .iter()
            .filter(|d| d.message.contains("undefined variable"))
            .collect();
        assert_eq!(var_diags.len(), 1);
        assert_eq!(var_diags[0].severity, Some(DiagnosticSeverity::WARNING));
    }

    #[test]
    fn parse_error_maps_back_to_original_position() {
        // After resolving :x → "ab", the parse error in resolved text
        // should map back to the original text position.
        let v = vars(&[("x", "ab")]);
        // "CREATE VIEW :x AS SELECTT 1" → "CREATE VIEW ab AS SELECTT 1"
        let text = "CREATE VIEW :x AS SELECTT 1";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &v, None);
        // Should have exactly one parse error diagnostic.
        let parse_diags: Vec<_> = diags
            .iter()
            .filter(|d| !d.message.contains("undefined variable"))
            .collect();
        assert_eq!(parse_diags.len(), 1);
        assert_eq!(parse_diags[0].severity, Some(DiagnosticSeverity::ERROR));
        assert_eq!(parse_diags[0].range.start.line, 0);
    }

    #[test]
    fn no_variables_unchanged_behavior() {
        let text = "CREATE VIEW foo AS SELECT 1;";
        let rope = Rope::from_str(text);
        assert!(diagnose(text, &rope, &BTreeMap::new(), None).is_empty());
    }

    // --- typecheck_diagnostics tests ---

    use crate::project::ir::object_id::ObjectId;
    use mz_repr::ColumnName;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn obj_err_internal(file: &str, msg: &str) -> ObjectTypeCheckError {
        obj_err_with_kind(
            PathBuf::from(file),
            ObjectTypeCheckErrorKind::Internal(msg.to_string()),
        )
    }

    fn obj_err_with_kind(file: PathBuf, kind: ObjectTypeCheckErrorKind) -> ObjectTypeCheckError {
        ObjectTypeCheckError {
            object_id: "db.schema.obj".parse::<ObjectId>().unwrap(),
            file_path: file,
            sql_statement: String::new(),
            kind,
        }
    }

    /// Write `text` to `path` and return `path`.
    fn write_fixture(path: PathBuf, text: &str) -> PathBuf {
        std::fs::write(&path, text).unwrap();
        path
    }

    #[test]
    fn typecheck_single_error_groups_by_file() {
        // Use a path that doesn't exist on disk → source read fails → range is (0,0).
        let err =
            TypeCheckError::TypeCheckFailed(obj_err_internal("/nonexistent/proj/a.sql", "boom"));
        let map = typecheck_diagnostics(&FileSystem::new(), &err);
        assert_eq!(map.len(), 1);
        let diags = map.get(&PathBuf::from("/nonexistent/proj/a.sql")).unwrap();
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].severity, Some(DiagnosticSeverity::ERROR));
        assert_eq!(diags[0].source.as_deref(), Some("mz-deploy"));
        assert_eq!(diags[0].message, "boom");
        assert_eq!(diags[0].range.start, Position::new(0, 0));
    }

    #[test]
    fn typecheck_multiple_errors_grouped_by_file() {
        let err = TypeCheckError::Multiple(vec![
            obj_err_internal("/nonexistent/a.sql", "first"),
            obj_err_internal("/nonexistent/b.sql", "second"),
            obj_err_internal("/nonexistent/a.sql", "third"),
        ]);
        let map = typecheck_diagnostics(&FileSystem::new(), &err);
        assert_eq!(map.len(), 2);
        assert_eq!(
            map.get(&PathBuf::from("/nonexistent/a.sql")).unwrap().len(),
            2
        );
        assert_eq!(
            map.get(&PathBuf::from("/nonexistent/b.sql")).unwrap().len(),
            1
        );
    }

    #[test]
    fn typecheck_non_object_variants_return_empty() {
        let err = TypeCheckError::DatabaseSetupError("oops".into());
        assert!(typecheck_diagnostics(&FileSystem::new(), &err).is_empty());
    }

    #[test]
    fn unknown_column_underlines_column() {
        let dir = tempdir().unwrap();
        let path = write_fixture(
            dir.path().join("v.sql"),
            "CREATE VIEW v AS SELECT bogus FROM t",
        );
        let kind = ObjectTypeCheckErrorKind::Plan(Arc::new(PlanError::UnknownColumn {
            table: None,
            column: ColumnName::from("bogus"),
            similar: Box::new([]),
        }));
        let err = TypeCheckError::TypeCheckFailed(obj_err_with_kind(path.clone(), kind));
        let map = typecheck_diagnostics(&FileSystem::new(), &err);
        let diag = &map.get(&path).unwrap()[0];
        // 'bogus' starts at byte 24, ends at 29.
        assert_eq!(diag.range.start, Position::new(0, 24));
        assert_eq!(diag.range.end, Position::new(0, 29));
    }

    #[test]
    fn ambiguous_column_underlines_column() {
        let dir = tempdir().unwrap();
        let path = write_fixture(dir.path().join("v.sql"), "SELECT shared FROM a, b");
        let kind = ObjectTypeCheckErrorKind::Plan(Arc::new(PlanError::AmbiguousColumn(
            ColumnName::from("shared"),
        )));
        let err = TypeCheckError::TypeCheckFailed(obj_err_with_kind(path.clone(), kind));
        let map = typecheck_diagnostics(&FileSystem::new(), &err);
        let diag = &map.get(&path).unwrap()[0];
        // 'shared' starts at byte 7.
        assert_eq!(diag.range.start, Position::new(0, 7));
        assert_eq!(diag.range.end, Position::new(0, 13));
    }

    #[test]
    fn unknown_function_underlines_function() {
        let dir = tempdir().unwrap();
        let path = write_fixture(
            dir.path().join("v.sql"),
            "CREATE VIEW v AS SELECT bogus_fn(1)",
        );
        let kind = ObjectTypeCheckErrorKind::Plan(Arc::new(PlanError::UnknownFunction {
            name: "bogus_fn".to_string(),
            arg_types: vec!["int4".to_string()],
        }));
        let err = TypeCheckError::TypeCheckFailed(obj_err_with_kind(path.clone(), kind));
        let map = typecheck_diagnostics(&FileSystem::new(), &err);
        let diag = &map.get(&path).unwrap()[0];
        // 'bogus_fn' starts at byte 24.
        assert_eq!(diag.range.start, Position::new(0, 24));
        assert_eq!(diag.range.end, Position::new(0, 32));
    }

    #[test]
    fn catalog_unknown_item_underlines_name() {
        let dir = tempdir().unwrap();
        let path = write_fixture(
            dir.path().join("v.sql"),
            "CREATE VIEW v AS SELECT * FROM bogus_table",
        );
        let kind =
            ObjectTypeCheckErrorKind::Catalog(CatalogError::UnknownItem("bogus_table".to_string()));
        let err = TypeCheckError::TypeCheckFailed(obj_err_with_kind(path.clone(), kind));
        let map = typecheck_diagnostics(&FileSystem::new(), &err);
        let diag = &map.get(&path).unwrap()[0];
        // 'bogus_table' starts at byte 31.
        assert_eq!(diag.range.start, Position::new(0, 31));
        assert_eq!(diag.range.end, Position::new(0, 42));
    }

    #[test]
    fn parser_error_uses_byte_offset() {
        let dir = tempdir().unwrap();
        let sql = "CREATE VIEW v AS SELECTT 1";
        let path = write_fixture(dir.path().join("v.sql"), sql);
        // Parse a deliberately broken statement to get a real
        // ParserStatementError carrying a non-zero byte offset.
        let parser_err = mz_sql_parser::parser::parse_statements(sql).unwrap_err();
        let expected_pos = parser_err.error.pos as u32;
        let kind = ObjectTypeCheckErrorKind::Parser(parser_err);
        let err = TypeCheckError::TypeCheckFailed(obj_err_with_kind(path.clone(), kind));
        let map = typecheck_diagnostics(&FileSystem::new(), &err);
        let diag = &map.get(&path).unwrap()[0];
        // The diagnostic should land at the parser's reported byte offset.
        // Whatever offset the parser chose, it must be > 0 (the error is
        // not at the start of the input) and reflected in the diagnostic.
        assert!(
            expected_pos > 0,
            "parser pos should be > 0, got {expected_pos}"
        );
        assert_eq!(diag.range.start, Position::new(0, expected_pos));
    }

    #[test]
    fn unhandled_variant_falls_back_to_zero() {
        let dir = tempdir().unwrap();
        let path = write_fixture(dir.path().join("v.sql"), "CREATE VIEW v AS SELECT 1");
        let kind = ObjectTypeCheckErrorKind::Plan(Arc::new(PlanError::Unstructured(
            "something happened".to_string(),
        )));
        let err = TypeCheckError::TypeCheckFailed(obj_err_with_kind(path.clone(), kind));
        let map = typecheck_diagnostics(&FileSystem::new(), &err);
        let diag = &map.get(&path).unwrap()[0];
        assert_eq!(diag.range.start, Position::new(0, 0));
        assert_eq!(diag.range.end, Position::new(0, 0));
    }

    #[test]
    fn identifier_not_in_source_falls_back_to_zero() {
        let dir = tempdir().unwrap();
        let path = write_fixture(dir.path().join("v.sql"), "CREATE VIEW v AS SELECT 1");
        let kind = ObjectTypeCheckErrorKind::Plan(Arc::new(PlanError::UnknownColumn {
            table: None,
            column: ColumnName::from("not_in_source"),
            similar: Box::new([]),
        }));
        let err = TypeCheckError::TypeCheckFailed(obj_err_with_kind(path.clone(), kind));
        let map = typecheck_diagnostics(&FileSystem::new(), &err);
        let diag = &map.get(&path).unwrap()[0];
        assert_eq!(diag.range.start, Position::new(0, 0));
    }

    #[test]
    fn whole_word_match_skips_substrings() {
        let dir = tempdir().unwrap();
        // 'customer_id' contains 'id' as a substring. Looking for 'id' should
        // skip 'customer_id' and find the standalone 'id'.
        let path = write_fixture(
            dir.path().join("v.sql"),
            "CREATE VIEW v AS SELECT customer_id, id FROM t",
        );
        let kind = ObjectTypeCheckErrorKind::Plan(Arc::new(PlanError::UnknownColumn {
            table: None,
            column: ColumnName::from("id"),
            similar: Box::new([]),
        }));
        let err = TypeCheckError::TypeCheckFailed(obj_err_with_kind(path.clone(), kind));
        let map = typecheck_diagnostics(&FileSystem::new(), &err);
        let diag = &map.get(&path).unwrap()[0];
        // 'id' as a standalone identifier is at byte 37 (after 'customer_id, ').
        assert_eq!(diag.range.start, Position::new(0, 37));
        assert_eq!(diag.range.end, Position::new(0, 39));
    }

    #[test]
    fn last_component_strips_qualifier() {
        assert_eq!(last_component("foo"), "foo");
        assert_eq!(last_component("schema.table"), "table");
        assert_eq!(last_component("db.schema.table"), "table");
    }

    #[test]
    fn message_includes_hint_from_catalog_error() {
        let dir = tempdir().unwrap();
        let path = write_fixture(dir.path().join("v.sql"), "SELECT bogus_fn() FROM t");
        let kind = ObjectTypeCheckErrorKind::Catalog(CatalogError::UnknownFunction {
            name: "bogus_fn".to_string(),
            alternative: Some("real_fn".to_string()),
        });
        let err = TypeCheckError::TypeCheckFailed(obj_err_with_kind(path.clone(), kind));
        let map = typecheck_diagnostics(&FileSystem::new(), &err);
        let diag = &map.get(&path).unwrap()[0];
        assert!(
            diag.message.contains("hint: Try using real_fn"),
            "expected hint in message, got: {:?}",
            diag.message
        );
    }
}
