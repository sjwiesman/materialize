---
source: src/mz-deploy/src/lsp/document_symbol.rs
revision: 673fdb9d44
---

# mz_deploy::lsp::document_symbol

Provides the structural outline (`textDocument/documentSymbol`) of a `.sql` file
for the editor's outline view and breadcrumbs. `document_symbols` resolves the
file's default database/schema and object name from the URI and project root,
looks the object up in the `ProjectCache`, and returns an empty vec when the file
doesn't correspond to a known project object.

The result is a single root `DocumentSymbol` for the main CREATE statement —
named by its fully-qualified name, detailed with the object kind, and mapped to an
LSP `SymbolKind` via `object_kind_to_symbol_kind` (from the shared `symbol_kind`
module). Its children, built by `child_symbol`, are the supporting statements:
indexes (`KEY`), grants (`EVENT`, labeled `GRANT <privilege> TO <grantee>`),
comments (`STRING`, distinguishing `COMMENT ON COLUMN`), and unit tests
(`METHOD`). The root spans the whole document; children use zero-width ranges
because per-statement byte offsets are not tracked in the typed IR.
