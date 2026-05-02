# LSP Quick Fixes for "Did You Mean" Suggestions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface "did you mean" suggestions as LSP code actions of kind
`quickfix` so editors can offer one-click replacements for typo'd identifiers,
in the same shape rust-analyzer uses. Three tiers of coverage:

- **Tier 1** — pass through structured suggestions the typechecker already
  produces: `PlanError::UnknownColumn { similar }` and
  `CatalogError::UnknownFunction { alternative }`.
- **Tier 2** — LSP-side fuzzy match against `ProjectCache` for variants
  upstream doesn't suggest for: `CatalogError::UnknownItem`,
  `UnknownSchema`, `UnknownDatabase`, and `UnknownCluster`.
- **Tier 3** — validation errors with a known-correct value: when an object's
  declared name/schema/database disagrees with what its file path implies
  (`ObjectNameMismatch`, `SchemaMismatch`, `DatabaseMismatch`,
  `ClusterNameMismatch`, `RoleNameMismatch`, `NetworkPolicyNameMismatch`),
  the error already carries `expected: String`. Surface the concrete
  suggestion in the hint text and as a quick fix that rewrites the declared
  identifier to the expected one.

**Architecture:** The CLI's `format_kind` family in `src/cli/render.rs` already
turns a typecheck error into `(message, footers, suggestions)` where each
`Suggestion` carries replacement byte-ranges. That logic moves into
`src/diagnostics.rs` so both the CLI and the LSP can call it. A new
`src/lsp/code_action.rs` module owns (a) the JSON payload that rides on
`Diagnostic.data` to round-trip suggestions through the client, (b) the pure
builder that turns `CodeActionParams` back into `CodeAction`s, and (c) Tier 2
fuzzy enrichment that walks the project cache for candidate names and runs
Damerau-Levenshtein distance to pick close matches. The LSP backend gains a
`code_action` handler, advertises the capability, and threads its
`ProjectCache` into `typecheck_diagnostics` so the enrichment can run.

**Tech Stack:** Rust, `tower-lsp` 0.20, `lsp-types`, `serde`, `serde_json`,
`ropey`, `strsim` 0.11 (workspace), existing `mz_sql::plan::PlanError` /
`mz_sql::catalog::CatalogError`, in-tree `ProjectCache`.

---

## File Structure

- **Modify** `src/mz-deploy/src/diagnostics.rs` — gains the `format_typecheck_kind` family (moved from `cli/render.rs`) plus its helpers (`format_plan`, `format_catalog`, `column_display`, `last_component`, `locate_replacement`, `fallback_plan`, `fallback_catalog`). Also gains `locate_validation` and `format_validation_kind` for Tier 3 (mirror of the typecheck pair, scoped to *Mismatch variants). Existing locator helpers stay put.
- **Modify** `src/mz-deploy/src/cli/render.rs` — drops the moved helpers and their tests; calls `crate::diagnostics::format_typecheck_kind` from `object_typecheck_to_positional` and `crate::diagnostics::format_validation_kind` from `validation_error_to_positional`.
- **Modify** `src/mz-deploy/src/project/error/validation.rs` — `help()` for the *Mismatch variants names the concrete `expected` value instead of generic guidance.
- **Create** `src/mz-deploy/src/lsp/code_action.rs` — defines the `QuickFixData` JSON payload (with LSP `Range`s, not byte offsets), the helper that converts a `Vec<Suggestion>` + rope into `QuickFixData`, the pure code-action builder that consumes `CodeActionParams` and emits `Vec<CodeActionOrCommand>`, and the Tier 2 `did_you_mean` / `Candidates` / `fuzzy_suggestions` / `harvest_candidates` family. Has its own unit tests.
- **Modify** `src/mz-deploy/src/lsp/diagnostics.rs` — `typecheck_diagnostics` calls `format_typecheck_kind` for the message body, falls back to `code_action::fuzzy_suggestions` when upstream gave no suggestions and a `Candidates` set is available, and attaches the `QuickFixData` JSON to `Diagnostic.data`. Builds the LSP message string as `<msg>\ndetail: ...\nhint: <footer>` so editors that ignore code actions still see the textual hint.
- **Modify** `src/mz-deploy/src/lsp/server.rs` — adds `code_action_provider: Some(CodeActionProviderCapability::Simple(true))` to the initialize response, an `async fn code_action(...)` handler that delegates to the new module, and threads the `ProjectCache` (via `harvest_candidates`) into the `typecheck_diagnostics` call inside `maybe_rebuild`.
- **Modify** `src/mz-deploy/src/lsp.rs` — adds `mod code_action;` next to the other LSP submodule declarations.
- **Modify** `src/mz-deploy/Cargo.toml` — adds `strsim = { workspace = true }` to `[dependencies]`.

---

## Task 1: Move `format_typecheck_kind` and helpers into `src/diagnostics.rs`

This is a pure refactor. No behavior change, no new types. Done first so the
rest of the work has a shared API to call.

**Files:**
- Modify: `src/mz-deploy/src/diagnostics.rs`
- Modify: `src/mz-deploy/src/cli/render.rs`

- [ ] **Step 1: Append the relocated functions to `src/diagnostics.rs`**

Below the existing `find_identifier` helper at the bottom of the file, paste
exactly the following block (the bodies match the current `cli/render.rs`
implementations verbatim — the only changes are visibility, the import paths
they reach for, and that `format_typecheck_kind` is now `pub(crate)`):

```rust
use mz_repr::ColumnName;
use mz_sql::names::PartialItemName;

use crate::project::compiler::typecheck::ObjectTypeCheckErrorKind;

/// Build the (message, footers, suggestions) triple for one typecheck kind.
///
/// Variants that carry alternatives (`UnknownColumn::similar`,
/// `UnknownFunction::alternative`, `UnknownType::alternative`) are formatted
/// directly so we control identifier quoting and can emit structured
/// patches. Other variants fall back to `Display` + the upstream `hint()`.
pub(crate) fn format_typecheck_kind(
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
    find_identifier(source, needle).unwrap_or_else(|| primary_range.clone())
}
```

- [ ] **Step 2: Move the matching tests into `src/diagnostics.rs`**

`src/diagnostics.rs` already has a `#[cfg(test)] mod tests`. Append these
test cases inside it (these are the relocated tests from `cli/render.rs` —
keep names and bodies identical so any reference in commit history still
matches):

```rust
    #[test]
    fn last_component_strips_qualifier() {
        assert_eq!(last_component("foo"), "foo");
        assert_eq!(last_component("schema.table"), "table");
    }

    #[test]
    fn locate_replacement_prefers_primary_range_when_matches() {
        let r = locate_replacement("SELECT emails FROM t", &(7..13), "emails");
        assert_eq!(r, 7..13);
    }

    #[test]
    fn locate_replacement_falls_back_to_search() {
        let r = locate_replacement("SELECT emails FROM t", &(0..0), "emails");
        assert_eq!(r, 7..13);
    }
```

If `src/diagnostics.rs` doesn't already have a `tests` module, scaffold one:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // (tests above go here)
}
```

- [ ] **Step 3: Delete the moved code from `src/cli/render.rs`**

In `src/mz-deploy/src/cli/render.rs`, delete the now-duplicated definitions:
`format_kind`, `format_plan`, `fallback_plan`, `format_catalog`,
`fallback_catalog`, `column_display`, `last_component`, `locate_replacement`.
Also delete the matching test cases (`last_component_strips_qualifier`,
`locate_replacement_prefers_primary_range_when_matches`,
`locate_replacement_falls_back_to_search`) — they live in `diagnostics.rs`
now.

The `use` statements no longer needed after the deletions: `mz_repr::ColumnName`,
`mz_sql::names::PartialItemName`, `mz_sql::plan::PlanError`,
`mz_sql::catalog::CatalogError`, and the `ObjectTypeCheckErrorKind` import.
Keep the `Replacement` and `Suggestion` imports — `to_positional` still
threads those types out via `PositionalDiagnostic`.

- [ ] **Step 4: Point `cli/render.rs` at the shared API**

In `object_typecheck_to_positional`, change the call site from the local
`format_kind` to `crate::diagnostics::format_typecheck_kind`. The call
signature is identical, so this is a one-token rename:

```rust
let (message, footers, suggestions) =
    crate::diagnostics::format_typecheck_kind(&error.kind, &source, &primary_range);
```

- [ ] **Step 5: Verify the refactor compiles and tests still pass**

Run: `cargo test -p mz-deploy --lib diagnostics`
Expected: `last_component_strips_qualifier` and the two `locate_replacement_*`
tests pass under their new module.

Run: `cargo test -p mz-deploy --lib cli::render`
Expected: the remaining render tests still pass.

Run: `cargo build -p mz-deploy`
Expected: clean build.

- [ ] **Step 6: Commit**

```bash
git add src/mz-deploy/src/diagnostics.rs src/mz-deploy/src/cli/render.rs
git commit -m "mz-deploy: lift format_typecheck_kind into shared diagnostics module"
```

---

## Task 2: Define the `QuickFixData` payload and conversion helper

The data that rides on `Diagnostic.data` so the `code_action` handler can
rebuild a `WorkspaceEdit` without re-running the typecheck. Uses LSP `Range`
(line/col) — not byte offsets — so the handler doesn't need access to the
file source.

**Files:**
- Create: `src/mz-deploy/src/lsp/code_action.rs`
- Modify: `src/mz-deploy/src/lsp.rs`

- [ ] **Step 1: Register the new submodule**

In `src/mz-deploy/src/lsp.rs`, add `mod code_action;` alongside the existing
declarations (alphabetical order — between `mod code_lens;` and
`mod completion;`).

- [ ] **Step 2: Write the failing test for the conversion helper**

Create `src/mz-deploy/src/lsp/code_action.rs` with this initial scaffolding
plus a failing test:

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! LSP code-action support: serializable suggestion payload + builder.

use crate::diagnostics::Suggestion;
use ropey::Rope;
use serde::{Deserialize, Serialize};
use tower_lsp::lsp_types::Range;

/// JSON payload riding on `Diagnostic.data` so the `code_action` handler
/// can rebuild a `WorkspaceEdit` without re-running the typecheck.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct QuickFixData {
    pub suggestions: Vec<SuggestionData>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SuggestionData {
    pub label: String,
    pub alternatives: Vec<ReplacementData>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ReplacementData {
    pub range: Range,
    pub new_text: String,
}

/// Convert the byte-range-flavored [`Suggestion`]s produced by the diagnostics
/// formatter into LSP-shaped [`SuggestionData`] using `rope` to map byte
/// offsets to line/column. Returns `None` when `suggestions` is empty so the
/// caller can leave `Diagnostic.data` unset.
pub(crate) fn suggestions_to_data(
    suggestions: &[Suggestion],
    rope: &Rope,
) -> Option<QuickFixData> {
    if suggestions.is_empty() {
        return None;
    }
    let suggestions = suggestions
        .iter()
        .map(|s| SuggestionData {
            label: s.label.clone(),
            alternatives: s
                .alternatives
                .iter()
                .map(|alt| ReplacementData {
                    range: byte_range_to_lsp(alt.byte_range.clone(), rope),
                    new_text: alt.replacement.clone(),
                })
                .collect(),
        })
        .collect();
    Some(QuickFixData { suggestions })
}

fn byte_range_to_lsp(range: std::ops::Range<usize>, rope: &Rope) -> Range {
    use crate::lsp::diagnostics::offset_to_position;
    use tower_lsp::lsp_types::Position;
    let zero = Position::new(0, 0);
    let start = offset_to_position(range.start, rope).unwrap_or(zero);
    let end = offset_to_position(range.end, rope).unwrap_or(start);
    Range::new(start, end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::Replacement;
    use tower_lsp::lsp_types::Position;

    #[test]
    fn suggestions_to_data_empty_returns_none() {
        let rope = Rope::from_str("SELECT 1");
        assert!(suggestions_to_data(&[], &rope).is_none());
    }

    #[test]
    fn suggestions_to_data_maps_byte_range_to_line_col() {
        let source = "SELECT custoser_name FROM users";
        let rope = Rope::from_str(source);
        let suggestion = Suggestion {
            label: "did you mean `customer_name`?".to_string(),
            alternatives: vec![Replacement {
                byte_range: 7..20,
                replacement: "customer_name".to_string(),
            }],
        };
        let data = suggestions_to_data(&[suggestion], &rope).expect("non-empty");
        assert_eq!(data.suggestions.len(), 1);
        let alt = &data.suggestions[0].alternatives[0];
        assert_eq!(alt.range.start, Position::new(0, 7));
        assert_eq!(alt.range.end, Position::new(0, 20));
        assert_eq!(alt.new_text, "customer_name");
    }
}
```

`offset_to_position` is currently `pub(crate)` in `lsp/diagnostics.rs` — verify
the visibility on its declaration. If it's only `pub(super)`, widen it to
`pub(crate)` so `code_action` can reach it.

- [ ] **Step 3: Run the new tests to verify they pass**

Run: `cargo test -p mz-deploy --lib lsp::code_action`
Expected: `suggestions_to_data_empty_returns_none` and
`suggestions_to_data_maps_byte_range_to_line_col` both PASS.

- [ ] **Step 4: Commit**

```bash
git add src/mz-deploy/src/lsp.rs src/mz-deploy/src/lsp/code_action.rs
git commit -m "mz-deploy: add QuickFixData payload and rope→LSP range helper"
```

---

## Task 3: Attach `QuickFixData` to typecheck diagnostics

Now that the helper exists, plumb suggestions through `typecheck_diagnostics`.
Continue keeping the human-readable `hint:` line in the message so editors
without code-action support still see it.

**Files:**
- Modify: `src/mz-deploy/src/lsp/diagnostics.rs`

- [ ] **Step 1: Write a failing test**

In the existing `tests` module of `src/mz-deploy/src/lsp/diagnostics.rs`,
add the following:

```rust
    #[test]
    fn typecheck_unknown_column_attaches_quickfix_data() {
        use crate::lsp::code_action::QuickFixData;
        use crate::project::compiler::typecheck::{
            ObjectId, ObjectTypeCheckError, ObjectTypeCheckErrorKind,
        };
        use mz_repr::ColumnName;
        use mz_sql::plan::PlanError;
        use std::sync::Arc;

        let source = "SELECT custoser_name FROM users";
        let path = std::env::temp_dir().join("typecheck_qf_test.sql");
        std::fs::write(&path, source).unwrap();

        let plan_err = PlanError::UnknownColumn {
            table: None,
            column: ColumnName::from("custoser_name"),
            similar: Box::new([ColumnName::from("customer_name")]),
        };
        let err = ObjectTypeCheckError {
            object_id: ObjectId::test_only("v"),
            file_path: path.clone(),
            kind: ObjectTypeCheckErrorKind::Plan(Arc::new(plan_err)),
        };
        let tc = TypeCheckError::Multiple(vec![err]);

        let fs = FileSystem::default();
        let map = typecheck_diagnostics(&fs, &tc);
        let diags = map.get(&path).expect("diags for file");
        assert_eq!(diags.len(), 1);

        let data = diags[0]
            .data
            .as_ref()
            .expect("Diagnostic.data should be set when suggestions exist");
        let qf: QuickFixData = serde_json::from_value(data.clone()).expect("decodes");
        assert_eq!(qf.suggestions.len(), 1);
        assert_eq!(qf.suggestions[0].alternatives.len(), 1);
        assert_eq!(qf.suggestions[0].alternatives[0].new_text, "customer_name");
        assert!(diags[0].message.contains("column custoser_name does not exist"));
        let _ = std::fs::remove_file(&path);
    }
```

`ObjectId::test_only` is the existing helper used by other typecheck tests
(see `src/project/compiler/typecheck/executor.rs` for the pattern). If the
constructor name differs in the current tree, mirror what
`fake_typecheck_error` in that file uses.

`FileSystem::default()` may not exist; check the type — if it's
`FileSystem::with_overlay(BTreeMap::new())` instead, use that.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p mz-deploy --lib lsp::diagnostics::tests::typecheck_unknown_column_attaches_quickfix_data`
Expected: FAIL — `Diagnostic.data` is currently never set, so the unwrap on
`diags[0].data` panics.

- [ ] **Step 3: Update `typecheck_diagnostics` to call `format_typecheck_kind` and attach data**

Replace the body of the `for e in errors` loop in
`src/mz-deploy/src/lsp/diagnostics.rs::typecheck_diagnostics` with the
following:

```rust
    for e in errors {
        let entry = source_cache
            .entry(e.file_path.clone())
            .or_insert_with(|| read_source(fs, &e.file_path));

        let diag = match entry.as_ref() {
            Some((source, rope)) => {
                let byte_range = locate_typecheck(&e.kind, source).unwrap_or(0..0);
                let (body, footers, suggestions) =
                    crate::diagnostics::format_typecheck_kind(&e.kind, source, &byte_range);

                let mut message = body;
                if let Some(detail) = e.detail() {
                    message.push_str("\ndetail: ");
                    message.push_str(&detail);
                }
                for footer in &footers {
                    message.push_str("\nhint: ");
                    message.push_str(footer);
                }

                let pd = PositionalDiagnostic {
                    severity: Severity::Error,
                    file: e.file_path.clone(),
                    source: source.clone(),
                    byte_range,
                    message,
                    footers,
                    suggestions: suggestions.clone(),
                };
                let mut diag = to_lsp(&pd, rope);
                if let Some(qf) =
                    crate::lsp::code_action::suggestions_to_data(&suggestions, rope)
                {
                    diag.data = Some(serde_json::to_value(qf).expect("serializable"));
                }
                diag
            }
            None => {
                // No source available — fall back to the upstream Display +
                // detail + hint, with no quick-fix data.
                let mut message = e.error_message();
                if let Some(detail) = e.detail() {
                    message.push_str("\ndetail: ");
                    message.push_str(&detail);
                }
                if let Some(hint) = e.hint() {
                    message.push_str("\nhint: ");
                    message.push_str(&hint);
                }
                Diagnostic {
                    range: Range::new(zero, zero),
                    severity: Some(DiagnosticSeverity::ERROR),
                    source: Some("mz-deploy".to_string()),
                    message,
                    ..Default::default()
                }
            }
        };

        map.entry(e.file_path.clone()).or_default().push(diag);
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p mz-deploy --lib lsp::diagnostics::tests::typecheck_unknown_column_attaches_quickfix_data`
Expected: PASS.

- [ ] **Step 5: Run the full LSP diagnostics test set as a regression check**

Run: `cargo test -p mz-deploy --lib lsp::diagnostics`
Expected: all green; previously-existing parse-level tests still pass.

- [ ] **Step 6: Commit**

```bash
git add src/mz-deploy/src/lsp/diagnostics.rs
git commit -m "mz-deploy: attach QuickFixData to typecheck diagnostics"
```

---

## Task 4: Pure code-action builder

The builder turns a `CodeActionParams` into a `Vec<CodeActionOrCommand>`. It
walks `params.context.diagnostics`, decodes `QuickFixData` from each one's
`data`, and emits one `CodeAction` (kind `quickfix`) per alternative. Marks
the action `is_preferred = true` when the suggestion is the unique best
choice (i.e. exactly one alternative under one suggestion). This is what
rust-analyzer does for typo suggestions.

**Files:**
- Modify: `src/mz-deploy/src/lsp/code_action.rs`

- [ ] **Step 1: Write a failing test for the builder**

Append to the `tests` module in `src/mz-deploy/src/lsp/code_action.rs`:

```rust
    use tower_lsp::lsp_types::{
        CodeActionContext, CodeActionKind, CodeActionOrCommand, CodeActionParams, Diagnostic,
        DiagnosticSeverity, PartialResultParams, TextDocumentIdentifier, Url, WorkDoneProgressParams,
    };

    fn lsp_range(sl: u32, sc: u32, el: u32, ec: u32) -> Range {
        Range::new(Position::new(sl, sc), Position::new(el, ec))
    }

    fn diag_with_quickfix(qf: QuickFixData) -> Diagnostic {
        Diagnostic {
            range: lsp_range(0, 7, 0, 20),
            severity: Some(DiagnosticSeverity::ERROR),
            source: Some("mz-deploy".to_string()),
            message: "column custoser_name does not exist".to_string(),
            data: Some(serde_json::to_value(qf).unwrap()),
            ..Default::default()
        }
    }

    fn params_with(uri: Url, diag: Diagnostic) -> CodeActionParams {
        CodeActionParams {
            text_document: TextDocumentIdentifier { uri },
            range: diag.range,
            context: CodeActionContext {
                diagnostics: vec![diag],
                only: None,
                trigger_kind: None,
            },
            work_done_progress_params: WorkDoneProgressParams::default(),
            partial_result_params: PartialResultParams::default(),
        }
    }

    #[test]
    fn builder_emits_one_action_per_alternative() {
        let uri = Url::parse("file:///tmp/v.sql").unwrap();
        let qf = QuickFixData {
            suggestions: vec![SuggestionData {
                label: "did you mean one of these?".to_string(),
                alternatives: vec![
                    ReplacementData {
                        range: lsp_range(0, 7, 0, 20),
                        new_text: "customer_name".to_string(),
                    },
                    ReplacementData {
                        range: lsp_range(0, 7, 0, 20),
                        new_text: "customer_id".to_string(),
                    },
                ],
            }],
        };
        let params = params_with(uri.clone(), diag_with_quickfix(qf));
        let actions = build_code_actions(&params);
        assert_eq!(actions.len(), 2);
        for action in &actions {
            let CodeActionOrCommand::CodeAction(ca) = action else {
                panic!("expected CodeAction, got {:?}", action);
            };
            assert_eq!(ca.kind.as_ref(), Some(&CodeActionKind::QUICKFIX));
            assert_eq!(ca.is_preferred, Some(false));
            let edits = ca
                .edit
                .as_ref()
                .and_then(|w| w.changes.as_ref())
                .and_then(|c| c.get(&uri))
                .expect("edit for file");
            assert_eq!(edits.len(), 1);
        }
    }

    #[test]
    fn builder_marks_single_alternative_preferred() {
        let uri = Url::parse("file:///tmp/v.sql").unwrap();
        let qf = QuickFixData {
            suggestions: vec![SuggestionData {
                label: "did you mean `customer_name`?".to_string(),
                alternatives: vec![ReplacementData {
                    range: lsp_range(0, 7, 0, 20),
                    new_text: "customer_name".to_string(),
                }],
            }],
        };
        let params = params_with(uri, diag_with_quickfix(qf));
        let actions = build_code_actions(&params);
        assert_eq!(actions.len(), 1);
        let CodeActionOrCommand::CodeAction(ca) = &actions[0] else {
            panic!("expected CodeAction");
        };
        assert_eq!(ca.is_preferred, Some(true));
        assert!(ca.title.contains("customer_name"));
    }

    #[test]
    fn builder_skips_diagnostics_without_quickfix_data() {
        let uri = Url::parse("file:///tmp/v.sql").unwrap();
        let diag = Diagnostic {
            range: lsp_range(0, 7, 0, 20),
            severity: Some(DiagnosticSeverity::ERROR),
            source: Some("mz-deploy".to_string()),
            message: "boring parse error".to_string(),
            data: None,
            ..Default::default()
        };
        let params = params_with(uri, diag);
        assert!(build_code_actions(&params).is_empty());
    }
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `cargo test -p mz-deploy --lib lsp::code_action::tests::builder`
Expected: FAIL — `build_code_actions` is not defined yet.

- [ ] **Step 3: Implement `build_code_actions`**

Above the `#[cfg(test)]` line in `src/mz-deploy/src/lsp/code_action.rs`,
add:

```rust
use std::collections::HashMap;
use tower_lsp::lsp_types::{
    CodeAction, CodeActionKind, CodeActionOrCommand, CodeActionParams, Diagnostic, TextEdit, Url,
    WorkspaceEdit,
};

/// Build the list of quick-fix code actions for a `textDocument/codeAction`
/// request. Inspects each diagnostic's `data` field for [`QuickFixData`] and
/// emits one [`CodeAction`] per alternative.
pub(crate) fn build_code_actions(params: &CodeActionParams) -> Vec<CodeActionOrCommand> {
    let uri = &params.text_document.uri;
    let mut actions = Vec::new();
    for diag in &params.context.diagnostics {
        let Some(data) = diag.data.as_ref() else {
            continue;
        };
        let Ok(qf) = serde_json::from_value::<QuickFixData>(data.clone()) else {
            continue;
        };
        let total_alternatives: usize = qf
            .suggestions
            .iter()
            .map(|s| s.alternatives.len())
            .sum();
        let unique_best = total_alternatives == 1;
        for suggestion in qf.suggestions {
            for alt in suggestion.alternatives {
                actions.push(CodeActionOrCommand::CodeAction(action_for_alt(
                    uri,
                    diag.clone(),
                    alt,
                    unique_best,
                )));
            }
        }
    }
    actions
}

fn action_for_alt(
    uri: &Url,
    diag: Diagnostic,
    alt: ReplacementData,
    is_preferred: bool,
) -> CodeAction {
    let title = format!("Replace with `{}`", alt.new_text);
    let edit = TextEdit {
        range: alt.range,
        new_text: alt.new_text,
    };
    let mut changes = HashMap::new();
    changes.insert(uri.clone(), vec![edit]);
    CodeAction {
        title,
        kind: Some(CodeActionKind::QUICKFIX),
        diagnostics: Some(vec![diag]),
        edit: Some(WorkspaceEdit {
            changes: Some(changes),
            document_changes: None,
            change_annotations: None,
        }),
        is_preferred: Some(is_preferred),
        ..Default::default()
    }
}
```

- [ ] **Step 4: Run the new tests to verify they pass**

Run: `cargo test -p mz-deploy --lib lsp::code_action`
Expected: all four tests in the module PASS.

- [ ] **Step 5: Commit**

```bash
git add src/mz-deploy/src/lsp/code_action.rs
git commit -m "mz-deploy: add pure code-action builder for did-you-mean quick fixes"
```

---

## Task 5: Wire the handler into the LSP server

Hook the builder into the `LanguageServer` impl and advertise the capability.

**Files:**
- Modify: `src/mz-deploy/src/lsp/server.rs`

- [ ] **Step 1: Add the capability to the initialize response**

Locate the `ServerCapabilities { ... }` block inside `initialize` (around
line 520 of `server.rs`). Add `code_action_provider` next to
`code_lens_provider`:

```rust
                code_action_provider: Some(CodeActionProviderCapability::Simple(true)),
```

- [ ] **Step 2: Add the `code_action` handler**

Inside `impl LanguageServer for Backend`, alongside the other handlers (place
between `code_lens` and `semantic_tokens_full` to mirror the
`ServerCapabilities` ordering), add:

```rust
    async fn code_action(
        &self,
        params: CodeActionParams,
    ) -> Result<Option<CodeActionResponse>> {
        let actions = code_action::build_code_actions(&params);
        if actions.is_empty() {
            Ok(None)
        } else {
            Ok(Some(actions))
        }
    }
```

- [ ] **Step 3: Add the `code_action` import**

In the `use crate::lsp::{ ... };` block at the top of `server.rs`, add
`code_action` to the alphabetized list (between `code_lens` and `completion`):

```rust
use crate::lsp::{
    code_action, code_lens, completion, diagnostics, document_symbol, goto_definition, hover,
    references, semantic_tokens, toml_diagnostics, workspace_symbol,
};
```

- [ ] **Step 4: Verify it builds**

Run: `cargo build -p mz-deploy`
Expected: clean build.

- [ ] **Step 5: Run the full mz-deploy test suite to confirm nothing regressed**

Run: `cargo test -p mz-deploy --lib`
Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/mz-deploy/src/lsp/server.rs
git commit -m "mz-deploy: serve textDocument/codeAction with did-you-mean quick fixes"
```

---

## Task 6: End-to-end smoke test against a real LSP backend

Verify the round-trip: a typecheck error with a known-similar column produces
a diagnostic carrying `QuickFixData`, and a follow-up `code_action` call on
that diagnostic returns a `CodeAction` with the expected `WorkspaceEdit`.

This goes in the existing tests module of `server.rs` so it sits alongside
`concurrent_publish_diagnostics_do_not_deadlock`. Doesn't require booting
an LSP service over stdio — call into the backend directly.

**Files:**
- Modify: `src/mz-deploy/src/lsp/server.rs`

- [ ] **Step 1: Write the failing test**

Append to the `#[cfg(test)] mod tests` at the bottom of `server.rs`:

```rust
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn code_action_returns_quickfix_for_unknown_column_diagnostic() {
        use crate::lsp::code_action::QuickFixData;
        use std::sync::Mutex as StdMutex;
        use tower_lsp::lsp_types::{
            CodeActionContext, CodeActionKind, CodeActionOrCommand, CodeActionParams, Diagnostic,
            DiagnosticSeverity, PartialResultParams, Position, Range, TextDocumentIdentifier,
            WorkDoneProgressParams,
        };

        let captured_client: Arc<StdMutex<Option<Client>>> = Arc::new(StdMutex::new(None));
        let captured_client_clone = Arc::clone(&captured_client);
        let (_service, _socket) = tower_lsp::LspService::new(move |client| {
            *captured_client_clone.lock().unwrap() = Some(client.clone());
            Backend::new_with_root(client, std::env::temp_dir())
        });
        let client = captured_client.lock().unwrap().take().unwrap();
        let backend = Backend::new_with_root(client, std::env::temp_dir());

        let uri = Url::from_file_path(std::env::temp_dir().join("qf.sql")).unwrap();
        let qf = QuickFixData {
            suggestions: vec![crate::lsp::code_action::SuggestionData {
                label: "did you mean `customer_name`?".to_string(),
                alternatives: vec![crate::lsp::code_action::ReplacementData {
                    range: Range::new(Position::new(0, 7), Position::new(0, 20)),
                    new_text: "customer_name".to_string(),
                }],
            }],
        };
        let diag = Diagnostic {
            range: Range::new(Position::new(0, 7), Position::new(0, 20)),
            severity: Some(DiagnosticSeverity::ERROR),
            source: Some("mz-deploy".to_string()),
            message: "column custoser_name does not exist".to_string(),
            data: Some(serde_json::to_value(qf).unwrap()),
            ..Default::default()
        };

        let params = CodeActionParams {
            text_document: TextDocumentIdentifier { uri: uri.clone() },
            range: diag.range,
            context: CodeActionContext {
                diagnostics: vec![diag],
                only: None,
                trigger_kind: None,
            },
            work_done_progress_params: WorkDoneProgressParams::default(),
            partial_result_params: PartialResultParams::default(),
        };

        let response = backend.code_action(params).await.unwrap();
        let actions = response.expect("code_action should return Some");
        assert_eq!(actions.len(), 1);
        let CodeActionOrCommand::CodeAction(ca) = &actions[0] else {
            panic!("expected CodeAction");
        };
        assert_eq!(ca.kind.as_ref(), Some(&CodeActionKind::QUICKFIX));
        assert_eq!(ca.is_preferred, Some(true));
        let edits = ca
            .edit
            .as_ref()
            .unwrap()
            .changes
            .as_ref()
            .unwrap()
            .get(&uri)
            .unwrap();
        assert_eq!(edits[0].new_text, "customer_name");
    }
```

- [ ] **Step 2: Run the test**

Run: `cargo test -p mz-deploy --lib lsp::server::tests::code_action_returns_quickfix_for_unknown_column_diagnostic`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/mz-deploy/src/lsp/server.rs
git commit -m "mz-deploy: end-to-end test for LSP code_action quick-fix path"
```

---

## Task 7: Add `did_you_mean` fuzzy-match helper

Tier 2's foundation. Adds `strsim` to `mz-deploy`'s deps and exposes a
`did_you_mean(needle, candidates) -> Vec<String>` returning the up-to-three
closest matches by Damerau-Levenshtein distance, sorted best-first.

**Files:**
- Modify: `src/mz-deploy/Cargo.toml`
- Modify: `src/mz-deploy/src/lsp/code_action.rs`

- [ ] **Step 1: Add `strsim` to mz-deploy dependencies**

In `src/mz-deploy/Cargo.toml`, add inside `[dependencies]` (alphabetical
position is between `serde_json` and `tempfile`-adjacent — slot it next to
`serde_json` for proximity):

```toml
strsim = { workspace = true }
```

Verify the workspace declaration already lists it: line 488 of the root
`Cargo.toml` has `strsim = "0.11.1"`. No root edit required.

- [ ] **Step 2: Write failing tests**

Append to the `#[cfg(test)] mod tests` block in
`src/mz-deploy/src/lsp/code_action.rs`:

```rust
    #[test]
    fn did_you_mean_returns_empty_for_no_close_match() {
        let candidates = ["customer_name", "customer_id", "shipping_address"];
        let out = did_you_mean("xyz", candidates.iter().map(|s| s.to_string()));
        assert!(out.is_empty(), "expected no matches, got {:?}", out);
    }

    #[test]
    fn did_you_mean_returns_exact_match_first() {
        let candidates = ["customer_name", "customer_id"];
        let out = did_you_mean("customer_name", candidates.iter().map(|s| s.to_string()));
        assert_eq!(out, vec!["customer_name".to_string()]);
    }

    #[test]
    fn did_you_mean_handles_transposition() {
        // Damerau-Levenshtein treats one transposition as distance 1.
        let candidates = ["customer_name"];
        let out = did_you_mean("cusotmer_name", candidates.iter().map(|s| s.to_string()));
        assert_eq!(out, vec!["customer_name".to_string()]);
    }

    #[test]
    fn did_you_mean_returns_top_three_sorted_by_distance() {
        let candidates = [
            "customer_name",
            "customer_id",
            "customers",
            "shipping_address",
            "products",
        ];
        let out = did_you_mean("custoser_name", candidates.iter().map(|s| s.to_string()));
        assert_eq!(out.len(), 3, "should cap at 3, got {:?}", out);
        // Best match (distance 1) comes first.
        assert_eq!(out[0], "customer_name");
    }

    #[test]
    fn did_you_mean_skips_empty_candidates() {
        let candidates: Vec<String> = Vec::new();
        let out = did_you_mean("anything", candidates);
        assert!(out.is_empty());
    }
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p mz-deploy --lib lsp::code_action::tests::did_you_mean`
Expected: FAIL — `did_you_mean` is not defined yet.

- [ ] **Step 4: Implement `did_you_mean`**

Above the `#[cfg(test)]` line in `src/mz-deploy/src/lsp/code_action.rs`,
add:

```rust
/// Maximum number of suggestions returned by [`did_you_mean`].
const MAX_DID_YOU_MEAN: usize = 3;

/// Return up to [`MAX_DID_YOU_MEAN`] closest names from `candidates` to
/// `needle`, sorted by Damerau-Levenshtein distance ascending. Names whose
/// distance exceeds `max(2, needle.len() / 3)` are filtered out so unrelated
/// matches don't surface as quick fixes.
pub(crate) fn did_you_mean<I, S>(needle: &str, candidates: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let threshold = std::cmp::max(2, needle.len() / 3);
    let mut scored: Vec<(usize, String)> = candidates
        .into_iter()
        .map(|c| {
            let s = c.into();
            let d = strsim::damerau_levenshtein(needle, &s);
            (d, s)
        })
        .filter(|(d, _)| *d <= threshold)
        .collect();
    scored.sort_by_key(|(d, _)| *d);
    scored.truncate(MAX_DID_YOU_MEAN);
    scored.into_iter().map(|(_, s)| s).collect()
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p mz-deploy --lib lsp::code_action::tests::did_you_mean`
Expected: all five PASS.

- [ ] **Step 6: Commit**

```bash
git add src/mz-deploy/Cargo.toml src/mz-deploy/src/lsp/code_action.rs
git commit -m "mz-deploy: add did_you_mean Damerau-Levenshtein helper"
```

---

## Task 8: Build candidate pool and fuzzy-suggest by error kind

Adds the `Candidates` struct that holds the per-kind name pools, the pure
`fuzzy_suggestions` function that maps a typecheck `kind` to a
`Vec<Suggestion>`, and the `harvest_candidates` adapter that walks a
`ProjectCache` to fill `Candidates`.

**Files:**
- Modify: `src/mz-deploy/src/lsp/code_action.rs`

- [ ] **Step 1: Write failing tests for the pure layer**

Append to the `tests` module:

```rust
    use crate::project::compiler::typecheck::ObjectTypeCheckErrorKind;
    use mz_sql::catalog::CatalogError;
    use std::sync::Arc;

    fn cands(items: &[&str], schemas: &[&str], databases: &[&str], clusters: &[&str]) -> Candidates {
        Candidates {
            items: items.iter().map(|s| s.to_string()).collect(),
            schemas: schemas.iter().map(|s| s.to_string()).collect(),
            databases: databases.iter().map(|s| s.to_string()).collect(),
            clusters: clusters.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn fuzzy_suggestions_for_unknown_item_uses_items_pool() {
        let source = "SELECT * FROM cusotmers";
        let primary = 14..23; // "cusotmers"
        let kind = ObjectTypeCheckErrorKind::Catalog(
            CatalogError::UnknownItem("cusotmers".to_string()),
        );
        let c = cands(&["customers", "products"], &[], &[], &[]);
        let out = fuzzy_suggestions(&kind, source, &primary, &c);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].alternatives.len(), 1);
        assert_eq!(out[0].alternatives[0].replacement, "customers");
        assert_eq!(out[0].alternatives[0].byte_range, 14..23);
    }

    #[test]
    fn fuzzy_suggestions_for_unknown_schema_uses_schemas_pool() {
        let source = "SELECT * FROM publik.t";
        let primary = 14..20; // "publik"
        let kind = ObjectTypeCheckErrorKind::Catalog(
            CatalogError::UnknownSchema("publik".to_string()),
        );
        let c = cands(&[], &["public", "private"], &[], &[]);
        let out = fuzzy_suggestions(&kind, source, &primary, &c);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].alternatives[0].replacement, "public");
    }

    #[test]
    fn fuzzy_suggestions_for_unknown_cluster_uses_clusters_pool() {
        let source = "CREATE VIEW v IN CLUSTER quikstart AS SELECT 1";
        let primary = 25..34; // "quikstart"
        let kind = ObjectTypeCheckErrorKind::Catalog(
            CatalogError::UnknownCluster("quikstart".to_string()),
        );
        let c = cands(&[], &[], &[], &["quickstart", "compute"]);
        let out = fuzzy_suggestions(&kind, source, &primary, &c);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].alternatives[0].replacement, "quickstart");
    }

    #[test]
    fn fuzzy_suggestions_for_kind_without_matches_returns_empty() {
        let source = "SELECT 1";
        let primary = 0..0;
        let kind = ObjectTypeCheckErrorKind::Catalog(
            CatalogError::UnknownItem("zzzzzzz".to_string()),
        );
        let c = cands(&["customers"], &[], &[], &[]);
        let out = fuzzy_suggestions(&kind, source, &primary, &c);
        assert!(out.is_empty());
    }

    #[test]
    fn fuzzy_suggestions_for_unhandled_kind_returns_empty() {
        let source = "SELECT 1";
        let primary = 0..0;
        let kind = ObjectTypeCheckErrorKind::Internal("whatever".to_string());
        let c = cands(&["customers"], &["public"], &["materialize"], &["quickstart"]);
        let out = fuzzy_suggestions(&kind, source, &primary, &c);
        assert!(out.is_empty());
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p mz-deploy --lib lsp::code_action::tests::fuzzy_suggestions`
Expected: FAIL — `Candidates` and `fuzzy_suggestions` are not defined.

- [ ] **Step 3: Implement `Candidates` and `fuzzy_suggestions`**

Above the `#[cfg(test)]` line in `src/mz-deploy/src/lsp/code_action.rs`,
add:

```rust
use crate::diagnostics::{Replacement, Suggestion, find_identifier};
use crate::project::compiler::typecheck::ObjectTypeCheckErrorKind;
use mz_sql::catalog::CatalogError;

/// Per-kind candidate name pools harvested from the project cache. Empty
/// vectors are valid — they just mean no fuzzy suggestions for that kind.
#[derive(Debug, Default, Clone)]
pub(crate) struct Candidates {
    pub items: Vec<String>,
    pub schemas: Vec<String>,
    pub databases: Vec<String>,
    pub clusters: Vec<String>,
}

/// LSP-side enrichment: for `Catalog::Unknown{Item,Schema,Database,Cluster}`,
/// fuzzy-match the offending name against the corresponding pool and return
/// one [`Suggestion`] containing the closest alternatives. Returns an empty
/// vec for variants we don't enrich (everything else, including
/// `UnknownColumn`/`UnknownFunction` whose suggestions come from upstream).
pub(crate) fn fuzzy_suggestions(
    kind: &ObjectTypeCheckErrorKind,
    source: &str,
    primary_range: &std::ops::Range<usize>,
    candidates: &Candidates,
) -> Vec<Suggestion> {
    let (needle, pool): (&str, &[String]) = match kind {
        ObjectTypeCheckErrorKind::Catalog(CatalogError::UnknownItem(name)) => {
            (last_component(name), &candidates.items)
        }
        ObjectTypeCheckErrorKind::Catalog(CatalogError::UnknownSchema(name)) => {
            (last_component(name), &candidates.schemas)
        }
        ObjectTypeCheckErrorKind::Catalog(CatalogError::UnknownDatabase(name)) => {
            (last_component(name), &candidates.databases)
        }
        ObjectTypeCheckErrorKind::Catalog(CatalogError::UnknownCluster(name)) => {
            (name.as_str(), &candidates.clusters)
        }
        _ => return Vec::new(),
    };

    let matches = did_you_mean(needle, pool.iter().cloned());
    if matches.is_empty() {
        return Vec::new();
    }

    let span = locate_replacement_for_needle(source, primary_range, needle);
    let label = match matches.as_slice() {
        [single] => format!("did you mean `{single}`?"),
        _ => "did you mean one of these?".to_string(),
    };
    let alternatives = matches
        .into_iter()
        .map(|alt| Replacement {
            byte_range: span.clone(),
            replacement: alt,
        })
        .collect();
    vec![Suggestion {
        label,
        alternatives,
    }]
}

/// Strip the qualifying prefix from a dotted name so we match on the
/// trailing component (e.g. `db.schema.tbl` → `tbl`).
fn last_component(s: &str) -> &str {
    s.rsplit_once('.').map(|(_, last)| last).unwrap_or(s)
}

/// Mirror of `crate::diagnostics::locate_replacement` but inlined here so
/// the function isn't widened to `pub(crate)` for one caller. Same
/// behavior: prefer the primary range when its content matches `needle`,
/// otherwise fall back to a whole-word search.
fn locate_replacement_for_needle(
    source: &str,
    primary_range: &std::ops::Range<usize>,
    needle: &str,
) -> std::ops::Range<usize> {
    let in_bounds = primary_range.end <= source.len() && primary_range.start <= primary_range.end;
    if in_bounds && &source[primary_range.clone()] == needle {
        return primary_range.clone();
    }
    find_identifier(source, needle).unwrap_or_else(|| primary_range.clone())
}
```

`find_identifier` is already `pub(crate)` in `src/diagnostics.rs` (it's used
by `locate_plan` etc.) — no visibility change needed.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p mz-deploy --lib lsp::code_action::tests::fuzzy_suggestions`
Expected: all five PASS.

- [ ] **Step 5: Add `harvest_candidates` for the impure layer**

Append to `src/mz-deploy/src/lsp/code_action.rs` (above `#[cfg(test)]`):

```rust
use crate::project_cache::ProjectCache;

/// Build a [`Candidates`] set from the project cache: every project item
/// name into `items`, every schema into `schemas`, every database into
/// `databases`, and the unique non-empty cluster names referenced by
/// project objects into `clusters`. Returns an empty `Candidates` when
/// `cache` is `None`.
pub(crate) fn harvest_candidates(cache: Option<&ProjectCache>) -> Candidates {
    let Some(cache) = cache else {
        return Candidates::default();
    };
    let dbs = cache.list_databases_with_objects();
    let mut databases = Vec::with_capacity(dbs.len());
    let mut schemas: Vec<String> = Vec::new();
    for db in &dbs {
        databases.push(db.name.clone());
        for s in &db.schemas {
            schemas.push(s.name.clone());
        }
    }
    schemas.sort();
    schemas.dedup();

    let summaries = cache.list_objects();
    let mut items: Vec<String> = summaries.iter().map(|s| s.name.clone()).collect();
    items.sort();
    items.dedup();

    let mut clusters: Vec<String> = summaries
        .iter()
        .filter_map(|s| s.cluster.clone())
        .collect();
    clusters.sort();
    clusters.dedup();

    Candidates {
        items,
        schemas,
        databases,
        clusters,
    }
}
```

Verify by reading `src/mz-deploy/src/project_cache.rs` that
`list_databases_with_objects() -> Vec<CachedDatabase>` exposes `.name` and
`.schemas: Vec<CachedSchema>` (with `CachedSchema { name: String, ... }`),
and that `list_objects() -> Vec<CachedObjectSummary>` exposes `name: String`
and `cluster: Option<String>`. If the field names differ, mirror them.

- [ ] **Step 6: Add a smoke test for `harvest_candidates(None)`**

```rust
    #[test]
    fn harvest_candidates_none_returns_default() {
        let c = harvest_candidates(None);
        assert!(c.items.is_empty());
        assert!(c.schemas.is_empty());
        assert!(c.databases.is_empty());
        assert!(c.clusters.is_empty());
    }
```

A test against a real `ProjectCache` would require building a SQLite
fixture, which is heavy. Trust the manual smoke test in Task 10 to exercise
the populated path.

- [ ] **Step 7: Run all tests**

Run: `cargo test -p mz-deploy --lib lsp::code_action`
Expected: every test in the module passes.

- [ ] **Step 8: Commit**

```bash
git add src/mz-deploy/src/lsp/code_action.rs
git commit -m "mz-deploy: fuzzy quick-fix suggestions for unknown item/schema/db/cluster"
```

---

## Task 9: Plumb `Candidates` through `typecheck_diagnostics`

Wire the Tier 2 enrichment into the diagnostic pipeline. `typecheck_diagnostics`
gets a new `&Candidates` argument and consults `fuzzy_suggestions` whenever
`format_typecheck_kind` returned no upstream suggestions. `maybe_rebuild`
calls `harvest_candidates(self.project_cache.lock().await.as_ref())` once
before invoking `typecheck_diagnostics`.

**Files:**
- Modify: `src/mz-deploy/src/lsp/diagnostics.rs`
- Modify: `src/mz-deploy/src/lsp/server.rs`

- [ ] **Step 1: Write a failing test**

In `src/mz-deploy/src/lsp/diagnostics.rs::tests`, add:

```rust
    #[test]
    fn typecheck_unknown_item_attaches_fuzzy_quickfix_data() {
        use crate::lsp::code_action::{Candidates, QuickFixData};
        use crate::project::compiler::typecheck::{
            ObjectId, ObjectTypeCheckError, ObjectTypeCheckErrorKind,
        };
        use mz_sql::catalog::CatalogError;

        let source = "SELECT * FROM cusotmers";
        let path = std::env::temp_dir().join("typecheck_fuzzy_test.sql");
        std::fs::write(&path, source).unwrap();

        let err = ObjectTypeCheckError {
            object_id: ObjectId::test_only("v"),
            file_path: path.clone(),
            kind: ObjectTypeCheckErrorKind::Catalog(
                CatalogError::UnknownItem("cusotmers".to_string()),
            ),
        };
        let tc = TypeCheckError::Multiple(vec![err]);

        let fs = FileSystem::with_overlay(BTreeMap::new());
        let candidates = Candidates {
            items: vec!["customers".to_string()],
            ..Default::default()
        };
        let map = typecheck_diagnostics(&fs, &tc, &candidates);
        let diags = map.get(&path).expect("diags for file");
        assert_eq!(diags.len(), 1);

        let data = diags[0].data.as_ref().expect("Diagnostic.data should be set");
        let qf: QuickFixData = serde_json::from_value(data.clone()).expect("decodes");
        assert_eq!(qf.suggestions.len(), 1);
        assert_eq!(qf.suggestions[0].alternatives.len(), 1);
        assert_eq!(qf.suggestions[0].alternatives[0].new_text, "customers");
        let _ = std::fs::remove_file(&path);
    }
```

(Match `ObjectId::test_only` to whatever the existing pattern in
`executor.rs` uses — same hedge as Task 3.)

- [ ] **Step 2: Run the test to confirm it fails for the right reason**

Run: `cargo test -p mz-deploy --lib lsp::diagnostics::tests::typecheck_unknown_item_attaches_fuzzy_quickfix_data`
Expected: FAIL — `typecheck_diagnostics` currently takes two args, not three.

- [ ] **Step 3: Widen `typecheck_diagnostics`'s signature**

In `src/mz-deploy/src/lsp/diagnostics.rs`, change the function signature to:

```rust
pub(crate) fn typecheck_diagnostics(
    fs: &FileSystem,
    error: &TypeCheckError,
    candidates: &crate::lsp::code_action::Candidates,
) -> BTreeMap<PathBuf, Vec<Diagnostic>> {
```

And, inside the per-error loop, after `format_typecheck_kind` returns a
`(body, footers, suggestions)` triple, replace empty `suggestions` with
fuzzy ones when available:

```rust
                let (body, footers, mut suggestions) =
                    crate::diagnostics::format_typecheck_kind(&e.kind, source, &byte_range);
                if suggestions.is_empty() {
                    suggestions = crate::lsp::code_action::fuzzy_suggestions(
                        &e.kind,
                        source,
                        &byte_range,
                        candidates,
                    );
                }
```

The rest of the body (message assembly, `to_lsp`, `data` attachment) is
unchanged.

- [ ] **Step 4: Update the existing Task 3 test that calls `typecheck_diagnostics`**

`typecheck_unknown_column_attaches_quickfix_data` (added in Task 3) needs the
new third argument. Add `let candidates = Candidates::default();` and pass
`&candidates` — `UnknownColumn` already gets its suggestion from the upstream
`similar` field, so the empty `Candidates` is fine.

- [ ] **Step 5: Update `maybe_rebuild` to pass candidates**

In `src/mz-deploy/src/lsp/server.rs::maybe_rebuild`, just before the
`typecheck_diagnostics` call, snapshot the cache and harvest candidates:

```rust
                let candidates = {
                    let guard = self.project_cache.lock().await;
                    crate::lsp::code_action::harvest_candidates(guard.as_ref())
                };
                let tc_diags = diagnostics::typecheck_diagnostics(&fs, &tc_err, &candidates);
```

(Replace the existing `diagnostics::typecheck_diagnostics(&fs, &tc_err)`
call.)

The cache lock is later re-acquired inside the same function for the
"open the long-lived ProjectCache" block, but the two acquisitions don't
nest — the inner block above drops `guard` at the closing brace before
control returns to the rest of `maybe_rebuild`, so no deadlock is possible
on `tokio::sync::Mutex` (which is not reentrant).

A consequence of the existing ordering: on the very first rebuild after
a server start, `self.project_cache` is `None` (lazy-opened only at the
end of `maybe_rebuild`), so the harvest yields empty `Candidates` and
Tier 2 quick fixes don't appear. From the second rebuild onward (or any
rebuild where the SQLite file existed before the LSP started), Tier 2
suggestions are available. This degrades cleanly — Tier 1 still works
on the first rebuild, and the manual smoke test (Task 10) flags this
explicitly.

- [ ] **Step 6: Run the new + the original Tier 1 tests**

Run: `cargo test -p mz-deploy --lib lsp::diagnostics::tests`
Expected: all PASS, including `typecheck_unknown_item_attaches_fuzzy_quickfix_data`
and the (updated) `typecheck_unknown_column_attaches_quickfix_data`.

- [ ] **Step 7: Verify the full crate still builds and tests pass**

Run: `cargo test -p mz-deploy --lib`
Expected: green.

- [ ] **Step 8: Commit**

```bash
git add src/mz-deploy/src/lsp/diagnostics.rs src/mz-deploy/src/lsp/server.rs
git commit -m "mz-deploy: thread harvested catalog candidates into typecheck diagnostics"
```

---

## Task 10: Concretize *Mismatch hint text

Surface the `expected` value directly in `help()` so users see the right
answer in the message itself, even in editors that don't request code
actions and in the CLI.

**Files:**
- Modify: `src/mz-deploy/src/project/error/validation.rs`

- [ ] **Step 1: Search for tests that pin the current help strings**

Run: `grep -rn "must match the .sql file name\|must match the directory" src/mz-deploy/`
Expected output (from the source itself, plus any test fixtures):

```
src/mz-deploy/src/project/error/validation.rs:917:                Some("the object name in your CREATE statement must match the .sql file name".to_string())
src/mz-deploy/src/project/error/validation.rs:920:                Some("the schema in your qualified object name must match the directory name".to_string())
src/mz-deploy/src/project/error/validation.rs:923:                Some("the database in your qualified object name must match the directory name".to_string())
src/mz-deploy/src/project/error/validation.rs:1028:                Some("the cluster name in your CREATE CLUSTER statement must match the .sql file name".to_string())
src/mz-deploy/src/project/error/validation.rs:1046:                Some("the role name in your CREATE ROLE statement must match the .sql file name".to_string())
src/mz-deploy/src/project/error/validation.rs:1067:                Some("the network policy name in your CREATE NETWORK POLICY statement must match the .sql file name".to_string())
```

If grep also turns up matches in test files (`*_test.rs`, `tests/`, or
mzcompose snapshots), note them — they'll need to track the new wording in
Step 3.

- [ ] **Step 2: Update `help()` for the six *Mismatch variants**

In `src/mz-deploy/src/project/error/validation.rs::help`, replace the six
generic strings with concrete ones that name the `expected` value:

```rust
            Self::ObjectNameMismatch { expected, .. } => {
                Some(format!("rename to '{expected}' to match the file name"))
            }
            Self::SchemaMismatch { expected, .. } => {
                Some(format!("qualify with '{expected}' to match the directory"))
            }
            Self::DatabaseMismatch { expected, .. } => {
                Some(format!("qualify with '{expected}' to match the directory"))
            }
```

…and similarly:

```rust
            Self::ClusterNameMismatch { expected, .. } => {
                Some(format!("rename the cluster to '{expected}' to match the file name"))
            }
            Self::RoleNameMismatch { expected, .. } => {
                Some(format!("rename the role to '{expected}' to match the file name"))
            }
            Self::NetworkPolicyNameMismatch { expected, .. } => {
                Some(format!("rename the policy to '{expected}' to match the file name"))
            }
```

The bindings now require destructuring `expected` from the enum, which is
already a field on every one of these variants (verify by reading lines
143–147 and 272/293/310 of `validation.rs`).

- [ ] **Step 3: Update any tests that pinned the old wording**

Run: `cargo test -p mz-deploy --lib`
Expected: any pre-existing tests that asserted on the exact help string
will fail. Update them to match the new concrete phrasing. If grep in Step
1 turned up no test references, this step is a no-op.

- [ ] **Step 4: Re-run tests to confirm green**

Run: `cargo test -p mz-deploy --lib`
Expected: green.

- [ ] **Step 5: Commit**

```bash
git add src/mz-deploy/src/project/error/validation.rs
git commit -m "mz-deploy: name expected value in name/schema/database mismatch hints"
```

---

## Task 11: Add `locate_validation` and `format_validation_kind`

Mirror of `locate_typecheck` / `format_typecheck_kind` for the *Mismatch
variants. Both shared helpers live in `src/diagnostics.rs` so the CLI and
LSP paths stay symmetric.

**Files:**
- Modify: `src/mz-deploy/src/diagnostics.rs`

- [ ] **Step 1: Make the helper from Task 1 reachable from validation code**

In Task 1 we moved `locate_replacement` and `last_component` into
`src/diagnostics.rs` as private helpers. The Tier 3 code below reuses
`locate_replacement`; nothing else changes. Verify the function is still
present in the file before proceeding.

- [ ] **Step 2: Add `find_identifier_after`**

`find_identifier` searches from byte 0; for validation we want to start
from the offending statement's offset so we don't match an earlier
unrelated occurrence of the name. Append the helper to `src/diagnostics.rs`
just below `find_identifier`:

```rust
/// Same as [`find_identifier`] but starts the search at `start_byte`.
/// Returns absolute byte ranges into `source`.
pub(crate) fn find_identifier_after(
    source: &str,
    name: &str,
    start_byte: usize,
) -> Option<Range<usize>> {
    let slice = source.get(start_byte..)?;
    let local = find_identifier(slice, name)?;
    Some((start_byte + local.start)..(start_byte + local.end))
}
```

- [ ] **Step 3: Write failing tests for the new helpers**

Append to the `tests` mod in `src/diagnostics.rs`:

```rust
    #[test]
    fn find_identifier_after_skips_earlier_occurrence() {
        let source = "CREATE TABLE foo (...);\nCREATE VIEW v AS SELECT * FROM foo;";
        let r = find_identifier_after(source, "foo", 24).unwrap();
        // Match should be the second `foo`, not the first.
        assert!(r.start > 24);
        assert_eq!(&source[r.clone()], "foo");
    }

    #[test]
    fn locate_validation_object_name_mismatch_finds_declared_token() {
        use crate::project::error::ValidationErrorKind;
        let source = "CREATE TABLE customers (id INT);";
        let kind = ValidationErrorKind::ObjectNameMismatch {
            declared: "customers".to_string(),
            expected: "users".to_string(),
        };
        let r = locate_validation(&kind, source, Some(0)).unwrap();
        assert_eq!(&source[r], "customers");
    }

    #[test]
    fn format_validation_kind_object_name_mismatch_yields_rename_suggestion() {
        use crate::project::error::ValidationErrorKind;
        let source = "CREATE TABLE customers (id INT);";
        let kind = ValidationErrorKind::ObjectNameMismatch {
            declared: "customers".to_string(),
            expected: "users".to_string(),
        };
        let primary = locate_validation(&kind, source, Some(0)).unwrap();
        let (msg, footers, suggestions) = format_validation_kind(&kind, source, &primary);
        assert!(msg.contains("declared 'customers'"));
        assert!(msg.contains("expected 'users'"));
        assert!(footers.iter().any(|f| f.contains("rename to 'users'")));
        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].alternatives.len(), 1);
        assert_eq!(suggestions[0].alternatives[0].replacement, "users");
        assert_eq!(&source[suggestions[0].alternatives[0].byte_range.clone()], "customers");
    }

    #[test]
    fn format_validation_kind_unhandled_returns_no_suggestions() {
        use crate::project::error::ValidationErrorKind;
        let kind = ValidationErrorKind::NoMainStatement {
            object_name: "x".to_string(),
        };
        let (_msg, _footers, sugg) = format_validation_kind(&kind, "", &(0..0));
        assert!(sugg.is_empty());
    }
```

`ValidationErrorKind` is already exposed from `src/project/error.rs` as a
`pub(crate)` re-export — verify with `grep -n 'ValidationErrorKind' src/project/error.rs`.
If not visible from `src/diagnostics.rs`, expose it with
`pub(crate) use validation::ValidationErrorKind;` in `src/project/error.rs`
(it's already there, line 41).

- [ ] **Step 4: Run tests to verify they fail**

Run: `cargo test -p mz-deploy --lib diagnostics::tests::locate_validation diagnostics::tests::format_validation_kind diagnostics::tests::find_identifier_after`
Expected: FAIL — `locate_validation` and `format_validation_kind` aren't
defined yet.

- [ ] **Step 5: Implement `locate_validation` and `format_validation_kind`**

Append to `src/mz-deploy/src/diagnostics.rs`:

```rust
use crate::project::error::ValidationErrorKind;

/// Locate the byte range of the declared identifier in a *Mismatch
/// validation error. Returns `None` for variants that don't carry a
/// `declared` name we can rewrite.
pub(crate) fn locate_validation(
    kind: &ValidationErrorKind,
    source: &str,
    statement_offset: Option<usize>,
) -> Option<Range<usize>> {
    let needle = mismatch_declared(kind)?;
    find_identifier_after(source, needle, statement_offset.unwrap_or(0))
}

/// Build the (message, footers, suggestions) triple for a validation kind.
///
/// For *Mismatch variants the suggestion is a single replacement that
/// rewrites the declared identifier to the expected one. Other variants
/// surface only the message and any upstream `help()` text.
pub(crate) fn format_validation_kind(
    kind: &ValidationErrorKind,
    source: &str,
    primary_range: &Range<usize>,
) -> (String, Vec<String>, Vec<Suggestion>) {
    let message = kind.message();
    let footers: Vec<String> = kind.help().into_iter().collect();
    let suggestions = mismatch_suggestion(kind, source, primary_range);
    (message, footers, suggestions)
}

/// `Some(declared)` if `kind` is one of the rewritable *Mismatch variants.
fn mismatch_declared(kind: &ValidationErrorKind) -> Option<&str> {
    use ValidationErrorKind::*;
    match kind {
        ObjectNameMismatch { declared, .. }
        | SchemaMismatch { declared, .. }
        | DatabaseMismatch { declared, .. }
        | ClusterNameMismatch { declared, .. }
        | RoleNameMismatch { declared, .. }
        | NetworkPolicyNameMismatch { declared, .. } => Some(declared.as_str()),
        _ => None,
    }
}

fn mismatch_expected(kind: &ValidationErrorKind) -> Option<&str> {
    use ValidationErrorKind::*;
    match kind {
        ObjectNameMismatch { expected, .. }
        | SchemaMismatch { expected, .. }
        | DatabaseMismatch { expected, .. }
        | ClusterNameMismatch { expected, .. }
        | RoleNameMismatch { expected, .. }
        | NetworkPolicyNameMismatch { expected, .. } => Some(expected.as_str()),
        _ => None,
    }
}

fn mismatch_suggestion(
    kind: &ValidationErrorKind,
    source: &str,
    primary_range: &Range<usize>,
) -> Vec<Suggestion> {
    let (Some(declared), Some(expected)) = (mismatch_declared(kind), mismatch_expected(kind))
    else {
        return Vec::new();
    };
    let span = locate_replacement(source, primary_range, declared);
    vec![Suggestion {
        label: format!("rename to `{expected}`"),
        alternatives: vec![Replacement {
            byte_range: span,
            replacement: expected.to_string(),
        }],
    }]
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p mz-deploy --lib diagnostics::tests`
Expected: all PASS, including the four new ones.

- [ ] **Step 7: Commit**

```bash
git add src/mz-deploy/src/diagnostics.rs
git commit -m "mz-deploy: add locate_validation and format_validation_kind"
```

---

## Task 12: Wire validation suggestions through CLI and LSP

Both rendering paths use the new shared helpers. The CLI gains rich
`did you mean`-style patches in its terminal output; the LSP attaches
`QuickFixData` to the validation diagnostic's `data` field.

**Files:**
- Modify: `src/mz-deploy/src/cli/render.rs`
- Modify: `src/mz-deploy/src/lsp/diagnostics.rs`

- [ ] **Step 1: Update CLI `validation_error_to_positional`**

In `src/mz-deploy/src/cli/render.rs::validation_error_to_positional`,
replace the body so it uses `locate_validation` + `format_validation_kind`
when source is readable:

```rust
fn validation_error_to_positional(error: &ValidationError) -> PositionalDiagnostic {
    let file = error.context.file.clone();

    if let Ok(source) = std::fs::read_to_string(&file) {
        let primary_range = crate::diagnostics::locate_validation(
            &error.kind,
            &source,
            error.context.byte_offset,
        )
        .unwrap_or_else(|| {
            let off = error.context.byte_offset.unwrap_or(0);
            off..off
        });
        let (message, footers, suggestions) =
            crate::diagnostics::format_validation_kind(&error.kind, &source, &primary_range);
        return PositionalDiagnostic {
            severity: Severity::Error,
            file,
            source,
            byte_range: primary_range,
            message,
            footers,
            suggestions,
        };
    }

    PositionalDiagnostic {
        severity: Severity::Error,
        file,
        source: error.context.sql_statement.clone().unwrap_or_default(),
        byte_range: 0..0,
        message: error.kind.message(),
        footers: error.kind.help().into_iter().collect(),
        suggestions: Vec::new(),
    }
}
```

(The `error.kind.message()` and `error.kind.help()` now flow through
`format_validation_kind` for the success path; the fallback path mirrors
the previous behavior verbatim.)

- [ ] **Step 2: Update LSP `validation_diagnostics`**

In `src/mz-deploy/src/lsp/diagnostics.rs::validation_diagnostics`, replace
the per-error block to call the shared formatter and attach quick-fix data
when suggestions exist:

```rust
    for error in errors {
        let entry = source_cache
            .entry(error.context.file.clone())
            .or_insert_with(|| read_source(fs, &error.context.file));

        let diag = match (entry.as_ref(), error.context.byte_offset) {
            (Some((source, rope)), Some(_)) => {
                let primary_range = crate::diagnostics::locate_validation(
                    &error.kind,
                    source,
                    error.context.byte_offset,
                )
                .unwrap_or_else(|| {
                    let off = error.context.byte_offset.unwrap_or(0);
                    off..off
                });
                let (body, footers, suggestions) =
                    crate::diagnostics::format_validation_kind(&error.kind, source, &primary_range);

                let mut message = body;
                for footer in &footers {
                    message.push_str("\nhint: ");
                    message.push_str(footer);
                }

                let pd = PositionalDiagnostic {
                    severity: Severity::Error,
                    file: error.context.file.clone(),
                    source: source.clone(),
                    byte_range: primary_range,
                    message,
                    footers,
                    suggestions: suggestions.clone(),
                };
                let mut diag = to_lsp(&pd, rope);
                if let Some(qf) =
                    crate::lsp::code_action::suggestions_to_data(&suggestions, rope)
                {
                    diag.data = Some(serde_json::to_value(qf).expect("serializable"));
                }
                diag
            }
            _ => Diagnostic {
                range: Range::new(zero, zero),
                severity: Some(DiagnosticSeverity::ERROR),
                source: Some("mz-deploy".to_string()),
                message: error.kind.message(),
                ..Default::default()
            },
        };

        map.entry(error.context.file.clone())
            .or_default()
            .push(diag);
    }
```

The fallback arm (no readable source or no offset) intentionally keeps
the lean shape it had before — there's no useful position to attach a
fix to.

- [ ] **Step 3: Write failing test for the LSP path**

In `src/mz-deploy/src/lsp/diagnostics.rs::tests`, add:

```rust
    #[test]
    fn validation_object_name_mismatch_attaches_quickfix_data() {
        use crate::lsp::code_action::QuickFixData;
        use crate::project::error::{ErrorContext, ValidationError, ValidationErrorKind};

        let source = "CREATE TABLE customers (id INT);";
        let path = std::env::temp_dir().join("validation_qf_test.sql");
        std::fs::write(&path, source).unwrap();

        let err = ValidationError {
            kind: ValidationErrorKind::ObjectNameMismatch {
                declared: "customers".to_string(),
                expected: "users".to_string(),
            },
            context: ErrorContext {
                file: path.clone(),
                sql_statement: Some(source.to_string()),
                byte_offset: Some(0),
            },
        };

        let fs = FileSystem::with_overlay(BTreeMap::new());
        let map = validation_diagnostics(&fs, &[err]);
        let diags = map.get(&path).expect("diags for file");
        assert_eq!(diags.len(), 1);

        let data = diags[0].data.as_ref().expect("Diagnostic.data should be set");
        let qf: QuickFixData = serde_json::from_value(data.clone()).expect("decodes");
        assert_eq!(qf.suggestions[0].alternatives[0].new_text, "users");
        let _ = std::fs::remove_file(&path);
    }
```

(The test reads from disk because `validation_diagnostics` calls
`read_source` directly. `FileSystem` is just a passthrough here — passing
the empty overlay keeps the FS implementation reading from the real
filesystem at `path`.)

- [ ] **Step 4: Run new + existing tests**

Run: `cargo test -p mz-deploy --lib lsp::diagnostics`
Expected: all PASS, including the new
`validation_object_name_mismatch_attaches_quickfix_data` and the existing
typecheck-side tests.

- [ ] **Step 5: Build the full crate as a regression check**

Run: `cargo test -p mz-deploy --lib`
Expected: green across the crate.

- [ ] **Step 6: Commit**

```bash
git add src/mz-deploy/src/cli/render.rs src/mz-deploy/src/lsp/diagnostics.rs
git commit -m "mz-deploy: surface validation rename suggestions in CLI and LSP quick fixes"
```

---

## Task 13: Manual verification in a real editor (optional but recommended)

The unit tests prove the data round-trips, but only a real client confirms
that the editor actually surfaces a one-click "Replace with `customer_name`"
button.

- [ ] **Step 1: Build the LSP binary**

Run: `cargo build -p mz-deploy`
Expected: clean build, `target/debug/mz-deploy` exists.

- [ ] **Step 2: Exercise Tier 1 — typo'd column**

Use the development VSCode extension (the project has one — it shells out
to the `mz-deploy` CLI). Open a `.sql` file in a project where you can
write a known-bad column reference, e.g.
`SELECT custoser_name FROM customers` against a `customers` table whose
column is `customer_name`. Save to trigger typecheck.

The diagnostic should read `column custoser_name does not exist`. Opening
the lightbulb on the underlined token should offer
`Replace with \`customer_name\``. Selecting it should rewrite the token.

- [ ] **Step 3: Exercise Tier 2 — typo'd table/schema/cluster**

In a project with at least one customers table, write
`SELECT * FROM cusotmers`. Save to trigger typecheck.

The diagnostic should read `unknown catalog item 'cusotmers'`. The
lightbulb should offer `Replace with \`customers\`` (sourced from the Tier
2 fuzzy match against `ProjectCache.list_objects()`).

Repeat with a typo'd schema (`SELECT * FROM publik.t`) and a typo'd cluster
(`CREATE VIEW v IN CLUSTER quikstart AS ...`) if the project has the
relevant catalog state.

- [ ] **Step 4: Exercise Tier 3 — wrong object name in file**

In a file at `materialize/public/users.sql`, write a CREATE statement
with a deliberately mismatched name:

```sql
CREATE TABLE customers (id INT);
```

Save. The diagnostic should read
`object name mismatch: declared 'customers', expected 'users'` with the
hint `rename to 'users' to match the file name`. The lightbulb on the
underlined `customers` token should offer `rename to \`users\``. Selecting
it rewrites the token in place to match the file name.

Repeat with a wrong schema qualifier (`CREATE TABLE wrong_schema.users (...)`)
in `materialize/public/users.sql` and a wrong database qualifier
(`CREATE TABLE wrong_db.public.users (...)`) to confirm `SchemaMismatch`
and `DatabaseMismatch` quick fixes work.

- [ ] **Step 5: Diagnose if quick fixes do not appear**

If a Tier 1 action is missing: check the LSP log for `code_action`
requests and verify the client is round-tripping `Diagnostic.data` (some
clients drop unknown fields).

If a Tier 2 action is missing: confirm the project has been built at
least once so the SQLite cache exists. The LSP opens the cache lazily on
first rebuild — without a cache file, `harvest_candidates` returns the
default empty `Candidates` and no Tier 2 suggestions are produced.

If a Tier 3 action is missing: confirm `error.context.byte_offset` is
populated by reading the LSP log for the diagnostic — `validation_diagnostics`
only attaches `Diagnostic.data` when both the source file is readable and
the offset is `Some`.

- [ ] **Step 6: No commit needed** — manual smoke test only.

---

## Self-Review Checklist

Verified against the original idea:

- ✅ Tier 1 column typos: Task 3 attaches `QuickFixData` to typecheck diagnostics derived from `PlanError::UnknownColumn`; Tasks 4–5 surface them as `CodeAction`s.
- ✅ Tier 1 function typos: same path covers `CatalogError::UnknownFunction { alternative: Some(_) }` (formatter handles it; data plumbing is identical).
- ✅ Tier 2 item typos (`SELECT * FROM cusotmers`): Task 8 fuzzy-matches `CatalogError::UnknownItem` against `cache.list_objects()`; Task 9 plumbs the candidate set through.
- ✅ Tier 2 schema/database/cluster typos: same `Candidates` mechanism with the appropriate pool per variant.
- ✅ Tier 3 declared/expected mismatches: Task 10 names the `expected` value in the hint, Task 11 adds the locator + formatter, Task 12 wires both rendering paths and adds the LSP unit test. Covers `ObjectNameMismatch`, `SchemaMismatch`, `DatabaseMismatch`, `ClusterNameMismatch`, `RoleNameMismatch`, `NetworkPolicyNameMismatch`.
- ✅ "Did you mean" wording is preserved in the LSP message itself for editors that don't request code actions (Tasks 3, 12 both append `\nhint: <footer>`).
- ✅ rust-analyzer parity: kind = `quickfix`, one action per alternative, `is_preferred = true` only when the suggestion is unambiguous, `diagnostics` field populated on each action so the editor knows which problem it fixes.
- ✅ Pure logic separated from I/O: `did_you_mean`, `fuzzy_suggestions`, `format_validation_kind`, and `format_typecheck_kind` are all pure; `harvest_candidates` is the only impure adapter and lives behind a single function call.
- ✅ Builder is pure and easy to test: `build_code_actions(&CodeActionParams) -> Vec<CodeActionOrCommand>` has no I/O.
- ✅ Tier 2 degrades gracefully when the project hasn't been built: `harvest_candidates(None)` returns empty `Candidates`, `fuzzy_suggestions` returns no matches, the diagnostic still publishes with just its message.
- ✅ Tier 3 degrades gracefully when source is unreadable or `byte_offset` is `None`: `validation_diagnostics` falls back to a position-less diagnostic with no quick-fix data.
- ✅ No placeholders. Every step has the actual code or command.
- ✅ Type names consistent across tasks: `QuickFixData`, `SuggestionData`, `ReplacementData`, `build_code_actions`, `suggestions_to_data`, `did_you_mean`, `Candidates`, `fuzzy_suggestions`, `harvest_candidates`, `format_typecheck_kind`, `format_validation_kind`, `locate_validation`, `find_identifier_after`.
- ✅ Tier 2 variants explicitly out of scope (no reliable candidate pool in `ProjectCache`): `UnknownConnection`, `UnknownNetworkPolicy`, `UnknownRole`, `UnknownClusterReplica`, `UnknownClusterReplicaSize`, `UnknownType`. The plan can be extended for these when a candidate source becomes available.
- ✅ Tier 3 *Mismatch variants explicitly out of scope (less common, share the same plumbing if desired): `*ReferenceMismatch` (Index/Grant/Comment/ColumnComment), `*TargetMismatch` (Cluster/Role/NetworkPolicy/SchemaMod/DatabaseMod), `AlterDefaultPrivileges{Database,Schema}Mismatch`. They have the same `{ declared, expected }`-shaped data and would slot into `mismatch_declared`/`mismatch_expected` if needed later.
