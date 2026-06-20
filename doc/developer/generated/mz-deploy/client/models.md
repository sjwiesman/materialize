---
source: src/mz-deploy/src/client/models.rs
revision: a647094cc4
---

# mz_deploy::client::models

Domain models giving a type-safe interface over raw catalog rows and
`_mz_deploy` metadata records.

Deployment classification: `DeploymentKind` (`Tables`, `Objects`, `Sinks`,
`Replacement`) and `DeploymentMode` (currently only `Stage`), both with `Display`
and `FromStr` for round-tripping the `kind` / `mode` columns.

Cluster types: `Cluster` (id, name, optional size and replication factor),
`ClusterOptions` (size + replication factor, with `from_cluster` deriving them
from a `Cluster`), `ClusterReplica` (name, size, optional availability zone),
`ObjectGrant` (grantee + privilege type), and `ClusterConfig` — a `Managed` /
`Unmanaged` enum capturing everything needed to clone a cluster, with a `grants`
accessor.

Metadata records: `SchemaDeploymentRecord` and `DeploymentObjectRecord` mirror
the `deployments` and `objects` tables; `DeploymentMetadata`,
`DeploymentDetails`, `StagingDeployment`, and `DeploymentHistoryEntry` are the
shapes returned by the various deployment query functions for validation and the
describe/list/history commands; `ConflictRecord` describes a schema updated after
a deployment started; `ProductionClusterRecord` flags a cluster hosting a
promoted deployment (used by the `dev` guard). `ApplyState`
(`NotStarted`/`PreSwap`/`PostSwap`) encodes resumable apply progress.
`ReplacementMvRecord` and `PendingStatement` back the `replacement_mvs` and
`pending_statements` tables for post-swap work. Several of these derive
`serde::Serialize` for JSON output.
