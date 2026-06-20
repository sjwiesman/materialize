---
source: src/mz-deploy/src/client.rs
revision: a647094cc4
---

# mz_deploy::client

The database client layer. All interaction with a live Materialize region flows
through this module. The `Client` type (defined in `connection`) wraps a
`tokio_postgres` connection and exposes scoped sub-clients that group related
operations. The module also holds small SQL utilities — `quote_identifier`
(double-quotes and escapes an identifier), `sql_placeholders` (builds
`$1, $2, …`), and `staging_suffix_like_pattern` (builds a `LIKE` pattern
matching the `_<deploy_id>` staging suffix, escaping LIKE metacharacters so the
separating underscore matches literally) — plus the `SERVER_CLUSTER_NAME`
constant (`_mz_deploy_server`) that `setup` creates and every connection pins
to.

## Submodules

- **`connection`** — Defines `Client` and `DevOverlaysClient`, manages the
  `tokio_postgres` connection and TLS, and provides the connection-string and
  sslmode helpers (`build_options_string`, `default_sslmode`, `is_loopback_host`).
- **`deployment_ops`** — Reads and writes the `_mz_deploy` deployment metadata
  and drives the blue/green deployment lifecycle: staging, hydration
  monitoring, cutover, abort, and the crash-recovery state machine. Exposes
  `ClusterDeploymentStatus`, `ClusterStatusContext`, `HydrationStatusUpdate`,
  `FailureReason`, and `DEFAULT_ALLOWED_LAG_SECS`.
- **`introspection`** — Read-only catalog queries: schema/cluster/object
  existence checks, dependency lookups, and batch metadata retrieval. Exposes
  `DependentSink`.
- **`provisioning`** — DDL operations that create or alter databases, schemas,
  and clusters to match the project definition.
- **`validation`** — Pre-deployment validation that the target environment
  matches expected state before changes are applied.
- **`type_info`** — `SHOW COLUMNS` queries used to generate and refresh the
  `types.lock` data-contract file.
- **`dev_overlays`** — Support for the `dev` command's per-developer overlay
  databases.
- **`models`** — Data structures shared across the sub-clients (deployment
  records, cluster configs, conflict records, grants, and related types).
- **`errors`** — Error types: `ConnectionError` for transport/query failures
  and `DatabaseValidationError` for semantic mismatches, plus
  `format_relative_path`.
