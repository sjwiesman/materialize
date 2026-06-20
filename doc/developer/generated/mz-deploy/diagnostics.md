---
source: src/mz-deploy/src/diagnostics.rs
revision: a647094cc4
---

# mz_deploy::diagnostics

Source-positioned diagnostics decoupled from any output format, shared by the LSP
server and the CLI. `PositionalDiagnostic` is the neutral intermediate: a
`Severity`, file path, owned source string, half-open byte range, message,
`footers` (plain help lines), and `suggestions`. A `Suggestion` groups
interchangeable `Replacement`s (a byte range plus replacement text) under one help
label, rendering as a rustc-style "did you mean" block. The LSP wraps a
`PositionalDiagnostic` into `tower_lsp::Diagnostic`; the CLI wraps it into an
`annotate_snippets` snippet.

Locator helpers derive byte ranges from `mz_sql` and validation errors that carry
only an identifier name. `locate_typecheck` dispatches on `ObjectTypeCheckErrorKind`
to `locate_plan` (handling `PlanError` column/function variants and wrapped parser
errors) and `locate_catalog` (handling `CatalogError` unknown-object variants),
returning parser byte offsets directly and `None` for `Internal`. `find_identifier`
finds the first whole-word occurrence of a name (adjacent bytes must not be
identifier characters); `find_identifier_after` does the same starting at an
offset; `last_component` strips dotted qualifiers; `locate_validation` finds the
declared identifier of a `*Mismatch` validation error.

Formatters turn an error kind into a `(message, footers, suggestions)` triple —
footers carry class-level advice ("why"), suggestions carry mechanical edits
("what"). `format_typecheck_kind` formats `UnknownColumn` (with `similar`
alternatives) and `UnknownFunction` (with `alternative`) directly so it controls
quoting and emits structured patches, falling back to `Display` + the upstream
`hint()` otherwise. `format_validation_kind` emits a rename suggestion for
`*Mismatch` variants (`mismatch_pair`, `mismatch_suggestion`). `locate_replacement`
chooses the byte range to patch, preferring the primary annotation when it matches
the needle and otherwise searching the source. `column_display` renders a
`table.column` reference.
