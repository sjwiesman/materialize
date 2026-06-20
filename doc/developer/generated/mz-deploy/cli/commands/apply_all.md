---
source: src/mz-deploy/src/cli/commands/apply_all.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::apply_all

Orchestrates the full infrastructure-reconciliation pass run by `mz-deploy
apply`. The single public function `run(settings, skip_secrets, dry_run)`
plans every apply phase in dependency order against one shared client, then
executes the combined plan unless `dry_run` is set.

`run` first calls `executor::compile_apply_project_and_connect` to compile the
project and open a connection, then builds an `ApplyPlan` and a dry-run
`DeploymentExecutor`. Phases are added in a fixed order chosen for referential
integrity: clusters, roles, network policies, then secrets (skipped when
`skip_secrets` is true), connections, sources, and tables. Clusters must exist
before materialized views reference them; connections before sources and sinks
use them; sources before `CREATE TABLE FROM SOURCE`. The infrastructure phases
(`clusters::plan`, `roles::plan`, `apply_network_policies::plan`) take only the
settings/client/executor, while the database-object phases (`apply_secrets`,
`apply_connections`, `apply_sources`, `apply_tables`) also receive the compiled
project and a mutable `&mut ApplyPlan` so they can deduplicate the schemas they
need to create.

Each phase's planning is idempotent — already-existing objects are detected and
skipped — so a partial failure leaves earlier phases intact and re-running
`apply` resumes from the failed phase onward. When not a dry run, `run` calls
`plan.execute(&client)` to apply all phases and then `lock::run` to refresh the
types lock. It returns the assembled `ApplyPlan`.
