---
source: src/mz-deploy/src/client/introspection.rs
revision: a647094cc4
---

# mz_deploy::client::introspection

Read-only catalog introspection backing `IntrospectionClient`. These functions
query `mz_catalog` / `mz_internal` to inspect the live environment and a few
issue `DROP ... CASCADE` for staging cleanup; the trailing impl block exposes
them as methods.

Existence and lookup: `schema_exists`, `cluster_exists`, `network_policy_exists`,
`role_exists`, `check_connection_exists`, `object_exists`, `get_cluster`,
`list_clusters`, `get_current_user`, `get_role_members`, `get_role_parameters`.
Batch existence checks (`check_schemas_exist`, `check_clusters_exist`,
`check_objects_exist`, and the catalog-table-parameterized
`check_catalog_objects_exist` with its `check_tables_exist` /
`check_sources_exist` / `check_secrets_exist` / `check_connections_exist` /
`check_sinks_exist` wrappers) build FQN `IN (...)` queries via `sql_placeholders`
and map results back to the input ids/tuples. `get_cluster_config` assembles a
`ClusterConfig` — size/replication for managed clusters, replicas for unmanaged,
plus privilege grants for both.

Grant queries reuse `mz_aclexplode` and filter out system roles and the owner's
implicit privileges: `get_named_object_grants` (clusters, network policies) and
`get_database_object_grants` (schema-qualified objects). Default-privilege
queries that find `ALTER DEFAULT PRIVILEGES` grants to protect from revocation
are `get_default_privilege_grants_for_named_object` (and its cluster /
network-policy wrappers) and `get_default_privilege_grants_for_database_object`.
`get_connection_create_sql` returns canonical non-redacted SQL via
`SHOW CREATE CONNECTION`.

Staging discovery and teardown: `get_staging_schemas` / `get_staging_clusters`
match the `_<deploy_id>` suffix with escaped `LIKE`; `drop_schema_objects` and
`drop_objects` drop matching objects with the keyword chosen by the private
`mz_type_to_drop_keyword`; `drop_staging_schemas` and `drop_staging_clusters`
drop named staging artifacts. `find_sinks_depending_on_schemas` returns
`DependentSink` rows identifying sinks whose upstream object lives in a schema
about to be dropped, used to repoint them before a CASCADE drop.
