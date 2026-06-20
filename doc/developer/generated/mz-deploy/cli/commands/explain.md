---
source: src/mz-deploy/src/cli/commands/explain.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::explain

Implements the `explain` command, which shows the `EXPLAIN` plan for a
materialized view or a named index by staging the target and its dependencies in
an ephemeral Materialize Docker container.

The target string is `database.schema.object` (a materialized view) or
`database.schema.object#index_name` (a specific index), parsed by `parse_target`
into an `ExplainTarget`. `run` compiles the project (optionally over an overlay
filesystem of unsaved buffers), locates the object, validates it with
`validate_target` (which also returns the target cluster), loads column schemas
via `load_types_and_cache` (types cache then `types.lock`), starts the container
with `DockerRuntime`, builds the staging plan, creates a uniquely named
`_mz_explain_<timestamp>` schema, executes the plan, and always drops that schema
with `CASCADE` afterward.

`plan_staging` / `plan_dep` walk the dependency graph and classify each
dependency into a `StagingAction`: dependencies with indexes on the target's
cluster become a `StubTable` plus `CreateIndex` actions; materialized views,
tables, and external dependencies become `StubTable` only; plain views recurse
and become `CreateView`. `get_columns_for_stub` sources column schemas from the
caches or from a `CREATE TABLE` AST. `execute_explain` creates the database and
schema, applies the actions, creates the target via `create_target`, runs the
`EXPLAIN` (built by `build_explain_sql`), and post-processes the output —
stripping the temporary schema prefix and rewriting the target cluster name.
SQL generation rewrites names and `IN CLUSTER` clauses to `quickstart` via
`NormalizingVisitor::explain`.
