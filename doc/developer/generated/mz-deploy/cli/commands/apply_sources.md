---
source: src/mz-deploy/src/cli/commands/apply_sources.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::apply_sources

Reconciles `CREATE SOURCE` objects, creating those that do not yet exist in the
database. `PHASE_NAME` is `"sources"` and `GRANT_KIND` is
`GrantObjectKind::Source`. `matches` filters for `Statement::CreateSource`.

`plan` collects the project's source objects, returns an empty result when there
are none, sorts them in dependency order with `get_sorted_objects_filtered`, and
batch-checks existence via `check_catalog_objects_exist` against the source
catalog table. It calls `apply_plan.prepare_schemas` for the schemas of
not-yet-existing sources. For each object: if it already exists, the source body
is left untouched and only `apply_objects::reconcile_grants_and_comments` runs,
yielding `ObjectAction::UpToDate`; if it is new, the `CREATE SOURCE` statement is
executed, each of the object's indexes is executed, grants/comments are
reconciled, and the action is `ObjectAction::Created`. Each iteration produces an
`ObjectResult` carrying the executor's collected statements.

`run(settings, dry_run)` compiles and connects via
`compile_apply_project_and_connect`, adds the planned phase to an `ApplyPlan`,
and executes it unless `dry_run` is set, returning the plan.
