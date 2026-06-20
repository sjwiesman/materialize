---
source: src/mz-deploy/src/lsp/diagnostics.rs
revision: 673fdb9d44
---

# mz_deploy::lsp::diagnostics

Emits `tower_lsp::lsp_types::Diagnostic`s for the LSP server across three tiers,
each building from the shared locator/formatter helpers in
`crate::diagnostics` and converting byte offsets to line/column via a `Rope`.

`diagnose` is the per-keystroke path. It resolves psql-style variables before
parsing (`resolve_variables`); unresolved variables become positioned diagnostics
(`WARNING` under the warn pragma, `ERROR` otherwise) whose messages point at the
project profile. The resolved SQL is parsed with
`mz_sql_parser::parser::parse_statements`, and any parse-error offset is mapped
back to original-text space via `resolved_to_original`. `parse_positional` builds
the intermediate `PositionalDiagnostic`s, which `to_lsp` converts.

`validation_diagnostics` converts project `ValidationError`s into per-file LSP
diagnostics. When an error carries a byte offset it reads and caches the source,
locates the precise range (`locate_validation`), formats the kind
(`format_validation_kind`), and attaches quick-fix data; file-level errors fall
back to `(0, 0)`.

`typecheck_diagnostics` handles `TypeCheckError`. Per-object errors
(`TypeCheckError::Multiple`) are positioned by inspecting the kind via
`locate_typecheck` and formatted via `format_typecheck_kind`; when the formatter
produces no suggestions it falls back to `code_action::fuzzy_suggestions` against
the harvested `Candidates`. Non-object variants (`DatabaseSetupError`,
`SortError`, `TypesCacheWriteFailed`) return an empty map. Missing source files
fall back to the upstream Display message.

Shared helpers: `build_error_diagnostic` constructs an error-severity diagnostic
for a byte range; `append_detail_and_hints` appends `detail:`/`hint:` lines for
editors that don't render code actions; `attach_quickfix_data` serializes
suggestions onto `Diagnostic.data`. `offset_to_position` and `position_to_offset`
convert between byte offsets and LSP positions using UTF-16 column counting.
