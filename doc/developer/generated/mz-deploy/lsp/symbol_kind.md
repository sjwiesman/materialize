---
source: src/mz-deploy/src/lsp/symbol_kind.rs
revision: a647094cc4
---

# mz_deploy::lsp::symbol_kind

Shared mapping from the project's `ObjectKind` to an LSP `SymbolKind`, used by
both `document_symbol` and `workspace_symbol` so the outline and symbol search
present consistent icons. `object_kind_to_symbol_kind` maps each kind to its
closest LSP analog: Table → `STRUCT`, View → `FUNCTION`, MaterializedView →
`CLASS`, Source → `INTERFACE`, Sink → `MODULE`, Secret → `CONSTANT`, Connection →
`NAMESPACE`.
