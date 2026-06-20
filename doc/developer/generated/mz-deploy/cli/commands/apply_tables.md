---
source: src/mz-deploy/src/cli/commands/apply_tables.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::apply_tables

Reconciles table objects, creating those that do not yet exist. `matches`
accepts both `Statement::CreateTable` and `Statement::CreateTableFromSource`.
`PHASE_NAME` is `"tables"` and `GRANT_KIND` is `GrantObjectKind::Table`.

`plan` gathers the project's table objects, returns an empty result when none
exist, sorts them with `get_sorted_objects_filtered`, and batch-checks existence
via `check_catalog_objects_exist` against the table catalog table, preparing
schemas for not-yet-existing tables through `apply_plan.prepare_schemas`. Existing
tables only run `apply_objects::reconcile_grants_and_comments`
(`ObjectAction::UpToDate`); new tables execute the `CREATE TABLE` statement, then
each of the object's indexes, then reconcile grants/comments
(`ObjectAction::Created`).

Newly created `CREATE TABLE FROM SOURCE` objects carry a `transaction_group` set
to the source name (from the statement's `source`); other objects have `None`.
After building all `ObjectResult`s, the phase sorts them by `transaction_group`,
which places ungrouped objects first and clusters source-derived tables together
by source while preserving topological order within each group.

`run(settings, dry_run)` compiles and connects via
`compile_apply_project_and_connect`, adds the planned phase to an `ApplyPlan`,
and when not a dry run executes the plan and then calls `lock::run` to refresh
the types lock.
