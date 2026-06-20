---
source: src/mz-deploy/src/lsp/code_action.rs
revision: a647094cc4
---

# mz_deploy::lsp::code_action

Backs the `textDocument/codeAction` flow, producing quick-fix actions that
rewrite identifiers an editor flags. The module covers four concerns.

A JSON sidecar carries suggestion data from a diagnostic to a later code-action
request. `QuickFixData` (a list of `SuggestionData`, each with a label and a vec
of `ReplacementData` of `{range, new_text}`) is serialized onto `Diagnostic.data`
by `suggestions_to_data`, which maps the byte-range-flavored `Suggestion`s from
the diagnostics formatter into LSP line/column ranges via a `Rope` (helper
`byte_range_to_lsp`). It returns `None` for empty input so the caller can leave
`data` unset.

`build_code_actions` is the pure builder: it reads `QuickFixData` off each
diagnostic in a `CodeActionParams` and emits one `CodeAction` per alternative
(`action_for_alt`), titled `Replace with \`<text>\``, kind `QUICKFIX`, carrying a
`WorkspaceEdit`. When a diagnostic has exactly one alternative it is marked
`is_preferred`.

The third concern is LSP-side fuzzy enrichment for catalog errors the
typechecker doesn't suggest replacements for. `harvest_candidates` builds a
`Candidates` set (deduped item, schema, database, and cluster name pools) from a
`ProjectCache`. `fuzzy_suggestions` matches the offending name from a
`CatalogError::Unknown{Item,Schema,Database,Cluster}` against the corresponding
pool and returns a `Suggestion`; other kinds (including `UnknownColumn` /
`UnknownFunction`, whose suggestions come from upstream) yield nothing.
`did_you_mean` ranks candidates by Damerau-Levenshtein distance (via `strsim`),
filtering out names beyond `max(2, needle.len() / 3)` and capping the result at
`MAX_DID_YOU_MEAN` (3).
