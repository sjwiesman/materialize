---
source: src/mz-deploy/src/cli/commands/stage.rs
revision: 673fdb9d44
---

# mz_deploy::cli::commands::stage

Implements `mz-deploy stage`, the first step of the blue/green lifecycle. It
deploys the project's views, materialized views, and indexes into renamed
"staging" schemas and clusters (suffixed `_<stage_name>`) alongside production,
recording metadata so `promote` can later swap staging into production atomically.

`run(settings, stage_name, allow_dirty, no_rollback, dry_run)` is the entry
point. It enforces a clean git tree unless `allow_dirty`, derives `stage_name`
(explicit, else the short git SHA, else `executor::generate_random_env_name`),
compiles the project, connects, and runs the `setup` verify/validate/require-
deployer checks. It then calls `analyze_project_changes`; if that returns `None`
(no changes) it exits. Otherwise it validates, records metadata (unless dry-run),
and either prints a `StagePlan` (dry-run) or calls
`create_resources_with_rollback`, finishing with a `StageResult` printed via
`log::output` and the deploy id via `log::print_deploy_id`.

`analyze_project_changes` rejects a stage name that already has a deployment,
builds a snapshot from the planned project, loads the production snapshot, and
diffs them into a `ChangeSet` (or treats an empty production as a full deploy). It
calls `validate_no_new_objects_in_existing_stable_schemas` (new replacement
objects may not be added to a schema that already holds production objects),
selects objects with `select_stage_objects`, partitions them with
`partition_objects`, validates table dependencies, and collects required
resources with `collect_stage_resources`. The result is a `StageAnalysis`
holding `objects`, `sinks`, `replacement_mvs`, `schema_set`, and `cluster_set`.

`partition_objects` splits the selected objects into a `PartitionedObjects`:
tables/sources/secrets/connections are counted as skipped (handled by `apply`),
sinks are deferred to apply, changed materialized views in the replacement set go
to `replacement_mvs`, and everything else is kept for staging.
`collect_stage_resources` derives the schema and cluster sets purely from the
staged objects.

`validate_project_for_stage` runs all preflight validations (project,
cluster-isolation, privileges, schema and cluster ownership, sink-connection
existence) before any writes. `record_stage_metadata` writes a staging
`DeploymentSnapshot` (tagging each schema with `DeploymentKind::Objects`,
`Sinks`, or `Replacement`), and persists deferred work: `PendingStatement` rows
for sinks (fully qualified via `NormalizingVisitor::fully_qualifying`) and
`ReplacementMvRecord` rows for replacement MVs.

`create_resources_with_rollback` is the execution orchestrator. It runs
`create_databases_and_schemas` (project databases plus suffixed staging schemas
and the production swap targets), `create_staging_clusters` (records cluster
mappings first so abort/rollback can clean up, then clones each production
cluster's managed/unmanaged config into a suffixed staging cluster), and
`deploy_objects_to_staging`. On error it rolls back via
`rollback_staging_resources` unless `dry_run` or `no_rollback` is set; rollback
is best-effort, using `best_effort_fetch`/`best_effort_delete` to continue past
individual failures.

`deploy_objects_to_staging` deploys external indexes (with cluster names
rewritten for staging), then each regular object and each replacement MV via
`deploy_single_object`. That helper normalizes each statement through a staging
`NormalizingVisitor` — suffixing names, dependencies, and clusters, while leaving
references to objects in the replacement set unsuffixed so they point at
production — applies a caller `transform` (used to set `replacement_for` on
replacement MVs via `CREATE REPLACEMENT MATERIALIZED VIEW ... FOR`), and then
deploys the object's indexes, grants, and comments with their references
normalized.

Serializable view types (`StageResult`, `StagePlan` and its `StagePlanSchema` /
`StagePlanCluster` / `StagePlanObject` members) back human and `--json` output.
A unit-test module covers `partition_objects` and `collect_stage_resources`
across full and incremental deployments with mixed object types.
