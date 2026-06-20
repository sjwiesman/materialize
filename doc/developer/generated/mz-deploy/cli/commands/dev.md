---
source: src/mz-deploy/src/cli/commands/dev.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::dev

Implements `mz-deploy dev`, which builds throwaway per-developer overlay
databases from the dirty subset of the project's views and materialized views.
Overlay databases follow the `<base_db>__<profile>` convention (`overlay_db_name`),
every overlay materialized view and index is rewritten to run on one
user-supplied target cluster, and the overlay is dropped and rebuilt on every
invocation. The command requires the `materialize_developer` role plus `CREATEDB`.

`run(settings, cluster, down, dry_run)` resolves the active profile and project
name, compiles the project, connects, and runs the `setup`
verify/validate/require-developer checks plus a `require_createdb` check against a
sample overlay database name. When `down` is set it runs only `drop_phase` and
exits. Otherwise it requires the `cluster` argument (clap guarantees it is
present when `down` is false) and calls `refuse_if_targets_production_cluster`,
which errors with `CliError::DevTargetsProductionCluster` if the target cluster
hosts a promoted deployment.

It then computes the dirty set: it builds a snapshot from the planned project,
loads the production snapshot, and (treating empty production as a full overlay)
diffs them into a `ChangeSet` to pick `objects_to_deploy`. Only `CreateView` and
`CreateMaterializedView` objects are kept as overlay objects; tables, sources,
and sinks are skipped. The schemas of those objects form the dirty set.
`print_plan` lists the dirty schemas and resulting overlay databases. On
`dry_run` it returns after printing; otherwise it runs `drop_phase` and, if the
dirty set is non-empty, `create_phase`.

`drop_phase` drops every overlay database recorded in the dev-overlays manifest
for this `(profile, project)` pair, deletes those manifest rows, and then sweeps
any in-project `<base_db>__<profile>` databases not in the manifest (left by a
catalog restore or interrupted run); `drop_database` issues `DROP DATABASE IF
EXISTS ... CASCADE`. `create_phase` creates each overlay database (inserting a
manifest row immediately so a crash mid-run is still recoverable), creates the
overlay schemas, and emits each object in dependency order: statements are
rewritten through an overlay `NormalizingVisitor` (rebasing names and
dependencies into the overlay databases and rewriting every `IN CLUSTER` clause
to the target cluster), with the object's indexes rewritten and executed the
same way.
