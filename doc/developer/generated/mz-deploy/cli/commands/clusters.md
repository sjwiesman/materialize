---
source: src/mz-deploy/src/cli/commands/clusters.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::clusters

Implements the `clusters apply` command, which converges live cluster state to
match the project's cluster definitions.

`run` connects an apply client via `connect_apply_client`, builds a dry-run
`DeploymentExecutor`, accumulates a single phase from `plan`, and executes it
against the database unless `dry_run` is set. `plan` loads `ClusterDefinition`s
with `project::clusters::load_clusters` (scoped by profile name and suffix and
variable substitutions) and returns an `ApplyResult` with phase `"clusters"`;
an empty definition set short-circuits to an empty result.

`plan_cluster` reconciles one cluster. It queries the live cluster via
`client.introspection().get_cluster`. If absent, it runs the definition's
`create_stmt` and records `ObjectAction::Created`. If present, it compares the
desired size and replication factor (extracted from the create statement with
`extract_size` / `extract_replication_factor`) against the live values; on
drift it emits an `ALTER CLUSTER ... SET (SIZE, REPLICATION FACTOR)`
(`ObjectAction::Altered`, defaulting size to `25cc` and replication factor to
`1` when unspecified), otherwise `ObjectAction::UpToDate`. It then reconciles
grants through `grants::reconcile_named_object` with
`GrantNamedObjectKind::Cluster` and replays any `COMMENT` statements. Statements
are collected from the executor via `take_statements` into the returned
`ObjectResult`.
