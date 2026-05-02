// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Rich CLI rendering for [`PositionalDiagnostic`]s.
//!
//! [`render`] turns one diagnostic into a styled [`annotate_snippets`] string.
//! [`to_positional`] inspects a [`CliError`] and pulls out any positional
//! diagnostics it carries so `display_error` can render rustc-quality output:
//! caret under the offending token, file/line origin, plain `help:` footers,
//! and `did you mean` patches that show the suggested replacement inline.
//!
//! Errors that don't carry source positions (configuration errors, network
//! failures, etc.) return an empty `Vec`; the caller falls back to the plain
//! [`std::fmt::Display`] path.

use crate::cli::CliError;
use crate::diagnostics::{PositionalDiagnostic, Replacement, Severity, Suggestion};
use crate::log::color_enabled;
use crate::project::compiler::typecheck::{
    ObjectTypeCheckError, ObjectTypeCheckErrorKind, TypeCheckError,
};
use crate::project::error::{ParseError, ProjectError, ValidationError, ValidationErrors};
use annotate_snippets::{AnnotationKind, Group, Level, Patch, Renderer, Snippet, Title};
use mz_repr::ColumnName;
use mz_sql::catalog::CatalogError;
use mz_sql::names::PartialItemName;
use mz_sql::plan::PlanError;

/// Render a single [`PositionalDiagnostic`] to a styled string.
///
/// Includes the primary annotated snippet, plain footers, and any
/// structured replacement suggestions as inline `did you mean` patches.
pub(crate) fn render(pd: &PositionalDiagnostic) -> String {
    let level = match pd.severity {
        Severity::Error => Level::ERROR,
        Severity::Warning => Level::WARNING,
    };
    let origin = origin_string(&pd.file);

    let mut groups: Vec<Group<'_>> = Vec::new();
    let primary_title: Title<'_> = level.primary_title(pd.message.as_str());
    let primary_group = if pd.source.is_empty() {
        Group::with_title(primary_title)
    } else {
        primary_title.element(
            Snippet::source(&pd.source)
                .path(origin.as_str())
                .annotation(AnnotationKind::Primary.span(clamped_range(pd))),
        )
    };
    groups.push(primary_group);

    for footer in &pd.footers {
        groups.push(Group::with_title(
            Level::HELP.secondary_title(footer.as_str()),
        ));
    }

    for s in &pd.suggestions {
        if s.alternatives.is_empty() {
            continue;
        }
        let mut group = Group::with_title(Level::HELP.secondary_title(s.label.as_str()));
        for alt in &s.alternatives {
            group = group.element(Snippet::source(&pd.source).path(origin.as_str()).patch(
                Patch::new(clamp(&pd.source, &alt.byte_range), alt.replacement.as_str()),
            ));
        }
        groups.push(group);
    }

    let renderer = if color_enabled() {
        Renderer::styled()
    } else {
        Renderer::plain()
    };
    renderer.render(&groups[..]).to_string()
}

/// Render `path` as a snippet origin, dropping redundant `./` components
/// so paths like `././models/foo.sql` print as `models/foo.sql`.
fn origin_string(path: &std::path::Path) -> String {
    let trimmed: std::path::PathBuf = path
        .components()
        .filter(|c| !matches!(c, std::path::Component::CurDir))
        .collect();
    if trimmed.as_os_str().is_empty() {
        path.display().to_string()
    } else {
        trimmed.display().to_string()
    }
}

/// Clamp the byte range to `[0, source.len()]` so an out-of-bounds offset
/// (e.g. a parser pos past EOF) doesn't panic inside annotate-snippets.
fn clamped_range(pd: &PositionalDiagnostic) -> std::ops::Range<usize> {
    clamp(&pd.source, &pd.byte_range)
}

fn clamp(source: &str, range: &std::ops::Range<usize>) -> std::ops::Range<usize> {
    let len = source.len();
    let start = range.start.min(len);
    let end = range.end.min(len).max(start);
    start..end
}

/// Extract any positional diagnostics carried by `error`.
///
/// Returns an empty `Vec` for errors that don't reference SQL source — those
/// fall back to plain [`std::fmt::Display`] rendering at the call site.
pub(crate) fn to_positional(error: &CliError) -> Vec<PositionalDiagnostic> {
    match error {
        CliError::Project(ProjectError::Parse(pe)) => parse_to_positional(pe),
        CliError::Project(ProjectError::Validation(ves)) => validation_to_positional(ves),
        CliError::TypeCheckFailed(tce) => typecheck_to_positional(tce),
        _ => Vec::new(),
    }
}

fn parse_to_positional(error: &ParseError) -> Vec<PositionalDiagnostic> {
    match error {
        ParseError::SqlParseFailed { path, sql, source } => vec![PositionalDiagnostic {
            severity: Severity::Error,
            file: path.clone(),
            source: sql.clone(),
            byte_range: source.error.pos..source.error.pos,
            message: source.error.message.clone(),
            footers: Vec::new(),
            suggestions: Vec::new(),
        }],
        ParseError::StatementsParseFailed { .. } | ParseError::UnresolvedVariables(_) => Vec::new(),
    }
}

fn validation_to_positional(errors: &ValidationErrors) -> Vec<PositionalDiagnostic> {
    errors
        .errors
        .iter()
        .map(validation_error_to_positional)
        .collect()
}

fn validation_error_to_positional(error: &ValidationError) -> PositionalDiagnostic {
    let message = error.kind.message();
    let footers: Vec<String> = error.kind.help().into_iter().collect();
    let file = error.context.file.clone();

    if let (Some(offset), Ok(source)) = (error.context.byte_offset, std::fs::read_to_string(&file))
    {
        return PositionalDiagnostic {
            severity: Severity::Error,
            file,
            source,
            byte_range: offset..offset,
            message,
            footers,
            suggestions: Vec::new(),
        };
    }

    PositionalDiagnostic {
        severity: Severity::Error,
        file,
        source: error.context.sql_statement.clone().unwrap_or_default(),
        byte_range: 0..0,
        message,
        footers,
        suggestions: Vec::new(),
    }
}

fn typecheck_to_positional(error: &TypeCheckError) -> Vec<PositionalDiagnostic> {
    let errors: Vec<&ObjectTypeCheckError> = match error {
        TypeCheckError::Multiple(es) => es.iter().collect(),
        TypeCheckError::DatabaseSetupError(_)
        | TypeCheckError::SortError(_)
        | TypeCheckError::TypesCacheWriteFailed(_) => return Vec::new(),
    };

    errors
        .iter()
        .map(|e| object_typecheck_to_positional(e))
        .collect()
}

fn object_typecheck_to_positional(error: &ObjectTypeCheckError) -> PositionalDiagnostic {
    let source = std::fs::read_to_string(&error.file_path).unwrap_or_default();
    let primary_range = crate::diagnostics::locate_typecheck(&error.kind, &source).unwrap_or(0..0);

    let (message, footers, suggestions) = format_kind(&error.kind, &source, &primary_range);

    let mut full_message = message;
    if let Some(detail) = error.detail() {
        full_message.push_str("\ndetail: ");
        full_message.push_str(&detail);
    }

    PositionalDiagnostic {
        severity: Severity::Error,
        file: error.file_path.clone(),
        source,
        byte_range: primary_range,
        message: full_message,
        footers,
        suggestions,
    }
}

/// Build the (message, footers, suggestions) triple for one typecheck kind.
///
/// Variants that carry alternatives (`UnknownColumn::similar`,
/// `UnknownFunction::alternative`, `UnknownType::alternative`) are formatted
/// directly so we control identifier quoting and can emit structured
/// patches. Other variants fall back to `Display` + the upstream `hint()`.
fn format_kind(
    kind: &ObjectTypeCheckErrorKind,
    source: &str,
    primary_range: &std::ops::Range<usize>,
) -> (String, Vec<String>, Vec<Suggestion>) {
    match kind {
        ObjectTypeCheckErrorKind::Plan(e) => format_plan(e, source, primary_range),
        ObjectTypeCheckErrorKind::Catalog(e) => format_catalog(e, source, primary_range),
        ObjectTypeCheckErrorKind::Parser(e) => (e.to_string(), Vec::new(), Vec::new()),
        ObjectTypeCheckErrorKind::Internal(msg) => (msg.clone(), Vec::new(), Vec::new()),
    }
}

fn format_plan(
    e: &PlanError,
    source: &str,
    primary_range: &std::ops::Range<usize>,
) -> (String, Vec<String>, Vec<Suggestion>) {
    if let PlanError::UnknownColumn {
        table,
        column,
        similar,
    } = e
    {
        let qualified = column_display(table.as_ref(), column);
        let message = format!("column {qualified} does not exist");
        if similar.is_empty() {
            return (message, Vec::new(), Vec::new());
        }
        let span = locate_replacement(source, primary_range, column.as_str());
        let label = match similar.as_ref() {
            [single] => format!("did you mean `{}`?", column_display(table.as_ref(), single)),
            _ => "did you mean one of these?".to_string(),
        };
        let alternatives = similar
            .iter()
            .map(|alt| Replacement {
                byte_range: span.clone(),
                replacement: alt.as_str().to_string(),
            })
            .collect();
        return (
            message,
            Vec::new(),
            vec![Suggestion {
                label,
                alternatives,
            }],
        );
    }
    fallback_plan(e)
}

fn fallback_plan(e: &PlanError) -> (String, Vec<String>, Vec<Suggestion>) {
    let footers = e.hint().into_iter().collect();
    (e.to_string(), footers, Vec::new())
}

fn format_catalog(
    e: &CatalogError,
    source: &str,
    primary_range: &std::ops::Range<usize>,
) -> (String, Vec<String>, Vec<Suggestion>) {
    match e {
        CatalogError::UnknownFunction {
            name,
            alternative: Some(alt),
        } => {
            let message = format!("function {name} does not exist");
            let suggestion = Suggestion {
                label: format!("did you mean `{alt}`?"),
                alternatives: vec![Replacement {
                    byte_range: locate_replacement(source, primary_range, last_component(name)),
                    replacement: alt.clone(),
                }],
            };
            (message, Vec::new(), vec![suggestion])
        }
        other => fallback_catalog(other),
    }
}

fn fallback_catalog(e: &CatalogError) -> (String, Vec<String>, Vec<Suggestion>) {
    let footers = e.hint().into_iter().collect();
    (e.to_string(), footers, Vec::new())
}

/// Format `table.column` as a dotted PostgreSQL reference (relation +
/// column). Each component is rendered as its raw identifier — no outer
/// quotes — so a reader interprets the dot as a separator rather than as
/// part of a single quoted identifier.
fn column_display(table: Option<&PartialItemName>, column: &ColumnName) -> String {
    match table {
        Some(t) => format!("{}.{}", t.item, column),
        None => column.as_str().to_string(),
    }
}

/// Strip the qualifying prefix from a dotted name so the patch replaces
/// just the trailing component (the name the user typed).
fn last_component(s: &str) -> &str {
    s.rsplit_once('.').map(|(_, last)| last).unwrap_or(s)
}

/// Find the byte range of `needle` to replace.
///
/// Prefer the primary annotation range when its content matches `needle`;
/// otherwise fall back to a whole-word search of the source so the patch
/// still lands somewhere reasonable for variants whose locator returned a
/// less specific span.
fn locate_replacement(
    source: &str,
    primary_range: &std::ops::Range<usize>,
    needle: &str,
) -> std::ops::Range<usize> {
    let in_bounds = primary_range.end <= source.len() && primary_range.start <= primary_range.end;
    if in_bounds && &source[primary_range.clone()] == needle {
        return primary_range.clone();
    }
    crate::diagnostics::find_identifier(source, needle).unwrap_or_else(|| primary_range.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn pd(source: &str, range: std::ops::Range<usize>, message: &str) -> PositionalDiagnostic {
        PositionalDiagnostic {
            severity: Severity::Error,
            file: PathBuf::from("test.sql"),
            source: source.to_string(),
            byte_range: range,
            message: message.to_string(),
            footers: Vec::new(),
            suggestions: Vec::new(),
        }
    }

    #[test]
    fn render_includes_message_and_origin() {
        let out = render(&pd("SELECT bogus", 7..12, "unknown column"));
        assert!(out.contains("unknown column"));
        assert!(out.contains("test.sql"));
    }

    #[test]
    fn render_message_only_when_source_empty() {
        let out = render(&pd("", 0..0, "missing CREATE statement"));
        assert!(out.contains("missing CREATE statement"));
        // No snippet block → no origin pointer.
        assert!(!out.contains("test.sql"));
    }

    #[test]
    fn render_with_footer() {
        let mut diag = pd("SELECT 1", 7..8, "type mismatch");
        diag.footers.push("convert with CAST".to_string());
        let out = render(&diag);
        assert!(out.contains("type mismatch"));
        assert!(out.contains("convert with CAST"));
    }

    #[test]
    fn clamped_range_caps_at_source_len() {
        let diag = pd("abc", 100..200, "out of range");
        assert_eq!(clamped_range(&diag), 3..3);
    }

    #[test]
    fn clamped_range_preserves_in_bounds() {
        let diag = pd("abcdef", 1..4, "ok");
        assert_eq!(clamped_range(&diag), 1..4);
    }

    #[test]
    fn origin_string_strips_curdir() {
        assert_eq!(
            origin_string(std::path::Path::new("././models/app/foo.sql")),
            "models/app/foo.sql"
        );
    }

    #[test]
    fn origin_string_preserves_absolute() {
        assert_eq!(
            origin_string(std::path::Path::new("/abs/models/foo.sql")),
            "/abs/models/foo.sql"
        );
    }

    #[test]
    fn origin_string_preserves_bare_curdir() {
        assert_eq!(origin_string(std::path::Path::new(".")), ".");
    }

    #[test]
    fn last_component_strips_qualifier() {
        assert_eq!(last_component("foo"), "foo");
        assert_eq!(last_component("schema.table"), "table");
    }

    #[test]
    fn locate_replacement_prefers_primary_range_when_matches() {
        // primary_range covers "emails"; needle is "emails" — use the primary.
        let r = locate_replacement("SELECT emails FROM t", &(7..13), "emails");
        assert_eq!(r, 7..13);
    }

    #[test]
    fn locate_replacement_falls_back_to_search() {
        // primary_range points elsewhere; use whole-word search.
        let r = locate_replacement("SELECT emails FROM t", &(0..0), "emails");
        assert_eq!(r, 7..13);
    }
}
