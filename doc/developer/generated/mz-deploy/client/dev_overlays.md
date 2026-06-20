---
source: src/mz-deploy/src/client/dev_overlays.rs
revision: a647094cc4
---

# mz_deploy::client::dev_overlays

Read/write helpers for the `_mz_deploy.tables.dev_overlays` manifest, implemented
on `DevOverlaysClient` and called by `cli::commands::dev` to drop and rebuild
per-developer overlay databases.

`list_overlays` returns the overlay database names recorded for a given
profile + project pair, ordered by name. `insert_overlay` records a newly created
overlay database (with `created_at = now()`). `delete_overlays` removes all
overlay records for a profile + project pair.
