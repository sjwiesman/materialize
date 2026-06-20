---
source: src/mz-deploy/src/client/validation.rs
revision: a647094cc4
---

# mz_deploy::client::validation

Pre-deployment environment validation, implemented on `ValidationClient` with
the real work in `pub(crate)` `*_impl` functions. Every check runs before any DDL
and surfaces failures as `DatabaseValidationError`. Catalog lookups use FQN
`IN (...)` queries batched in chunks of `LOOKUP_BATCH_SIZE` (1000).

The shared lookup helpers are `query_existing_names`,
`query_existing_schema_pairs`, and `query_existing_object_ids` (parameterized by
the `CatalogLookup` enum selecting `mz_objects` / `mz_sources` / `mz_tables` /
`mz_connections`), plus `query_sources_by_cluster` which maps clusters to the
user sources running on them.

`validate_project_impl` is the umbrella check: it derives external prerequisites
via `collect_external_dependencies`, then runs `find_missing_databases`,
`find_missing_schemas`, `find_missing_clusters`, and
`find_missing_external_dependencies`, attaching missing deps to source files via
`build_object_paths` / `build_compilation_errors`, and aggregates everything into
`DatabaseValidationError::Multiple`. The remaining `*_impl` functions cover the
other checks: `validate_cluster_isolation_impl` (sources/sinks must not share a
cluster with MVs/indexes, delegating to the project's own isolation rule),
`validate_privileges_impl` (superuser short-circuit, else USAGE on databases and
CREATECLUSTER on system), `validate_sources_exist_impl` (sources referenced by
`CREATE TABLE FROM SOURCE`), `validate_sink_connections_exist_impl` (Kafka and
Iceberg connections referenced by sinks), `validate_schema_ownership_impl` and
`validate_cluster_ownership_impl` (current role owns the production schemas /
clusters to be swapped), and `validate_table_dependencies_impl` (project tables
depended on by objects being deployed already exist).
