---
source: src/mz-deploy/src/lsp/run.rs
revision: a647094cc4
---

# mz_deploy::lsp::run

LSP server entry point. `run` is the async function called from the `lsp`
subcommand: it wires `tokio` stdin/stdout into a `tower_lsp` `LspService` built
around `Backend::new_with_root` (seeded with the project root), then serves the
language server over stdio until the client disconnects.
