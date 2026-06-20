---
source: src/mz-deploy/src/lsp.rs
revision: a647094cc4
---

# mz_deploy::lsp

A tower-lsp Language Server for the `.sql` files in an mz-deploy project,
running over stdio. The server reuses the project compiler's IR and typecheck
cache: a `Backend` holds the per-file `Rope` documents (updated on every
change), the `graph::Project` model and `ProjectCache` (rebuilt fully on save
via `project::plan_sync()`), the active profile name, and the project root.
Parse-error diagnostics run per file on every keystroke; the heavier project
model rebuilds on save.

The module re-exports `run`, the entrypoint.

## Submodules

- **`run`** — Process entrypoint: constructs the `Backend` and serves LSP over
  stdio.
- **`server`** — Defines `Backend`, the tower-lsp `LanguageServer`
  implementation that holds server state and routes each request.
- **`completion`** — Keyword and object-name completions, with qualification
  adapting to the dot-qualified prefix already typed.
- **`hover`** — Output schema (columns, types, nullability) for a referenced
  object, drawn from the build artifacts merged with `types.lock`.
- **`goto_definition`** — Resolves the identifier at the cursor to the `.sql`
  file that defines it.
- **`references`** — Reverse-dependency lookup: which project objects depend on
  a given identifier.
- **`document_symbol`** — Structural outline of a file (the main CREATE as
  root, with indexes, grants, comments, and unit tests as children).
- **`workspace_symbol`** — Project-wide fuzzy search across object names.
- **`code_lens`** — "Run Test" lenses above `EXECUTE UNIT TEST` and "Explain"
  lenses above materialized views and named indexes.
- **`code_action`** — Quick-fix and refactor actions offered for diagnostics
  and statements.
- **`semantic_tokens`** — Lexer-driven syntax coloring for the full Materialize
  keyword set.
- **`diagnostics`** — Converts parse errors into LSP diagnostics with correct
  line/column positions.
- **`functions`** — Built-in function metadata used by completions and hovers.
- **`symbol_kind`** — Maps project object kinds to LSP `SymbolKind` values.
