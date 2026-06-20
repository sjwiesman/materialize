---
source: src/mz-deploy/src/lsp/workspace_symbol.rs
revision: 673fdb9d44
---

# mz_deploy::lsp::workspace_symbol

Workspace-wide symbol search powering "Go to Symbol in Workspace". `workspace_symbols`
iterates every object in the `ProjectCache`: an empty query returns all objects, a
non-empty query matches by case-insensitive substring on the fully-qualified name
(`database.schema.object`). Each match becomes a `SymbolInformation` with the
object name, a `SymbolKind` from `object_kind_to_symbol_kind` (shared
`symbol_kind` module), a `Location` at the start of the source file, and a
container name of `database.schema` for grouping. External dependencies are
excluded because they have no file path.
