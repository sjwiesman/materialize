---
source: src/mz-deploy/src/lsp/server.rs
revision: 673fdb9d44
---

# mz_deploy::lsp::server

The LSP backend and its `tower_lsp::LanguageServer` implementation. `Backend` is
a cheap-to-clone handle around `Arc<BackendInner>` (cloned to capture state in
spawned tokio tasks, with field access via `Deref`). `BackendInner` holds session
state: the client handle, open documents as `Rope`s, a lazily-opened
`ProjectCache`, separate parse and project diagnostic maps, the project root, and
cached settings/variables/profile name. Construction is `new_with_root`.

`load_settings` reads `project.toml` and the active profile's variables (resolving
the profile via `read_mzprofile`, exposed here as `resolve_lsp_profile_name`),
defaulting silently when configuration is absent. `try_open_project_cache` opens a
read-only SQLite `ProjectCache`, returning `None` when the DB file is missing.

Diagnostics come from two sources that must be merged because LSP
`publishDiagnostics` is full-replacement per URI. `publish_diagnostics` refreshes
the per-keystroke parse diagnostics for one document (via `diagnostics::diagnose`,
skipping `.toml` files) and stores its rope; `publish_merged` emits the union of
parse and project diagnostics for a URI. `build_overlay` snapshots open documents
into a `FileSystem` overlay so unsaved edits feed the compiler;
`snapshot_at_position` captures document text, byte offset, and the
identifier chain at a cursor.

Rebuilds are debounced and serialized. `schedule_rebuild_after_idle` bumps a
monotonic `edit_version` and spawns a task that calls `maybe_rebuild` only if no
newer keystroke arrived within `IDLE_REBUILD_DEBOUNCE`. `maybe_rebuild` takes
`rebuild_lock`, runs `project::plan_sync` against the overlay, derives validation
diagnostics, runs `run_typecheck` on success (merging typecheck diagnostics with
fuzzy candidates harvested from the cache), lazily opens the `ProjectCache` once
its DB exists, and — guarding against buffer changes during the rebuild via
`edit_version`/`rebuilt_through` — republishes the union of old and new project
diagnostic URIs so stale entries clear while fresh ones appear.

The `LanguageServer` impl declares server capabilities in `initialize`
(full-sync, completion, definition, references, document/workspace symbols, hover,
code lens, code action, and semantic tokens with the `semantic_tokens` legend),
triggers rebuilds on `initialized`/`did_change`/`did_save`/`did_close`/
`did_change_watched_files`, and dispatches each request to the corresponding
`lsp` submodule (`goto_definition`, `references`, `document_symbol`,
`workspace_symbol`, `hover`, `completion`, `code_lens`, `code_action`,
`semantic_tokens`).
