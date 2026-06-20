---
source: src/mz-deploy/src/cli/commands/promote.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::promote

Implements `mz-deploy promote`, which cuts a staged deployment over to
production with an atomic `ALTER ... SWAP` and then runs resumable post-swap
work. The entry point `run(settings, deploy_id, force, dry_run)` connects, runs
the `setup` checks, requires the deployer role, validates the deployment is
staged-and-not-promoted, loads the persisted `ApplyState`, generates a
`DeploymentPlan`, and — unless `dry_run` — executes the swap, the post-swap
steps, and apply-state cleanup before reporting success.

`DeploymentPlan` is the single structure holding everything needed to display
and execute the cutover (deploy id, `ApplyState`, staging suffix, staging schemas
and clusters, pending sink statements, replacement-MV records, and dependent
sinks). It is built once by `generate_deployment_plan`, which for a `PostSwap`
resume skips conflict checks and resource gathering, otherwise calls
`gather_resources_and_check_conflicts`; it also loads pending statements,
replacement MVs, and (for display) the sinks depending on the production schemas.
The plan implements both `serde::Serialize` (for `--json`) and `Display`, the
latter rendering colored sections for schema swaps, cluster swaps, sinks to
create, replacement MVs, sinks to repoint, and old resources to drop, with a
resume note for `PreSwap`/`PostSwap` states.

`gather_resources_and_check_conflicts` checks `check_deployment_conflicts`
(erroring with `CliError::DeploymentConflict` unless `force`, in which case it
warns and continues), then reads the deployment's schema records and, using the
production snapshot, decides which schemas participate in the swap: `Sinks`
schemas are skipped, `Replacement` schemas already-Replacement in production are
skipped (they use APPLY REPLACEMENT instead) while first-time Replacement schemas
are included, and `Objects` schemas always swap. It batch-checks which staging
schemas and clusters actually exist and returns the participating sets plus the
`_<deploy_id>` suffix.

`execute_swap_phase` branches on `ApplyState`: `NotStarted` creates the apply-
state schemas then runs `execute_atomic_swap`; `PreSwap` resumes directly into
the swap; `PostSwap` does nothing. `execute_atomic_swap` runs a single
`BEGIN`/`COMMIT` transaction issuing `ALTER SCHEMA ... SWAP WITH` for each
schema, `ALTER CLUSTER ... SWAP WITH` for each cluster, and finally an
`ALTER SCHEMA` swap of the `apply_<id>_pre`/`apply_<id>_post` state schemas that
atomically records the swap as complete; any failure rolls back.

`run_post_swap_steps` runs `execute_pending_sinks` (creating not-yet-existing
sinks from `pending_statements`, creating their sink-only schemas first, and
marking each executed), `apply_replacement_mvs` (issuing `ALTER MATERIALIZED VIEW
... APPLY REPLACEMENT ...` then dropping the now-empty replacement schemas),
and, when schemas were swapped, `repoint_dependent_sinks` (re-querying post-swap
and issuing `ALTER SINK ... SET FROM` to point sinks at the new production
objects, erroring with `CliError::SinkRepointFailed` if a target is missing). It
then marks the deployment promoted via `update_promoted_at` and best-effort drops
the old (now suffixed) schemas and clusters via `drop_old_resources`. Finally
`cleanup_apply_state` deletes the apply-state schemas, pending statements,
replacement-MV records, and cluster records.

`strip_staging_suffix` recovers a production name by removing the staging suffix
exactly once (via `str::strip_suffix`, not `trim_end_matches`) so production
names that legitimately end in the suffix are not mangled; a unit-test module
covers the once-only stripping and the no-match passthrough.
