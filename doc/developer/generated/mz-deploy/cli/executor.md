---
source: src/mz-deploy/src/cli/executor.rs
revision: a647094cc4
---

# mz_deploy::cli::executor

Shared apply machinery for running deployment SQL, used by the `apply` family of
subcommands.

`DeploymentExecutor` wraps a `Client` and a dry-run flag plus a `RefCell`
statement log. Its constructors are `new` (executes), `new_dry_run` (only
records SQL for planning), and `with_dry_run`. `execute_sql` either runs a
statement or pushes it to the log; `take_statements` drains the log per object.
Higher-level methods build on this: `execute_object` runs a compiled
`DatabaseObject`'s main statement, indexes, grants, and comments;
`prepare_databases_and_schemas` issues `CREATE DATABASE/SCHEMA IF NOT EXISTS`
and applies `ModStatement` setup filtered by a schema set, rewriting schema
references via `normalize::rewrite_schema_names` when a staging suffix is given.
`ensure_database`, `ensure_schema`, `create_cluster`, and
`record_deployment_clusters` delegate to provisioning/deployment sub-clients in
real mode and log placeholders in dry-run mode.

`ApplyPlan` accumulates an apply across phases. It holds deduplicated
`setup_statements`, ordered `ApplyResult` phases, and a set of already-prepared
schemas. `prepare_schemas` runs setup for newly-seen schemas; `add_phase`
appends a phase; `execute` runs setup first, then groups per-object SQL into
`ExecutionBatch`es by `transaction_group` key — batches with a group are wrapped
in `BEGIN`/`COMMIT` with `ROLLBACK` on error, and redacted secret statements run
before visible ones with their text replaced by a redaction marker in error
reports. `ObjectResult` (with `ObjectAction`: `Created`/`Altered`/`UpToDate`/
`Skipped`) and `ApplyResult` carry per-object/per-phase results and implement
`Display` for cargo-style colored output and `Serialize` for JSON.

Free functions: `collect_deployment_metadata` gathers the current user and git
commit for provenance; `connect_apply_client` connects and requires the
`materialize_deployer` role; `compile_apply_project_and_connect` compiles
without typechecking and connects; `generate_random_env_name` produces a
7-character hex id from a SHA-256 of the current nanosecond timestamp.
