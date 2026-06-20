---
source: src/mz-deploy/src/client/provisioning.rs
revision: a647094cc4
---

# mz_deploy::client::provisioning

Idempotent DDL provisioning, implemented on `ProvisioningClient`. These methods
issue `CREATE ... IF NOT EXISTS` and `ALTER` statements to make the target
region's databases, schemas, and clusters match the project, and run before
object-level deployment. Callers are responsible for the referential ordering
databases → schemas → clusters.

`create_database` and `create_schema` issue idempotent creates, mapping failures
to `DatabaseCreationFailed` / `SchemaCreationFailed`. `create_cluster` creates a
managed cluster with the given `ClusterOptions`, translating an "already exists"
error into `ClusterAlreadyExists`. `create_cluster_with_config` materializes a
captured `ClusterConfig`: for managed clusters it delegates to `create_cluster`;
for unmanaged it creates the cluster with empty replicas then issues per-replica
`CREATE CLUSTER REPLICA`; in both cases it then replays the captured privilege
grants. `alter_cluster` applies size and replication-factor changes to an
existing managed cluster and, unlike the create methods, always runs the
statement (which may be a no-op).
