---
source: src/mz-deploy/src/cli/commands/apply_objects.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::apply_objects

Shared helper used by the per-type apply commands to reconcile the
grants and comments attached to a single database object.

The one public function, `reconcile_grants_and_comments(client, executor,
obj_id, typed_obj, grant_kind)`, calls `grants::reconcile` with the object's
`grants` and the supplied `GrantObjectKind`, then executes each of the object's
`comments` statements through the executor. `apply_secrets`, `apply_connections`,
`apply_sources`, and `apply_tables` all call it after creating or altering an
object so grant and comment reconciliation is implemented once rather than
duplicated per phase.
