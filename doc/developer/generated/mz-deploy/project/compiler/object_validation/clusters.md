---
source: src/mz-deploy/src/project/compiler/object_validation/clusters.rs
revision: a647094cc4
---

# mz_deploy::project::compiler::object_validation::clusters

Enforces that objects requiring a cluster declare one with an `IN CLUSTER`
clause, so deployment is deterministic rather than relying on implicit cluster
selection. Each `pub(super)` function takes the object's `FullyQualifiedName`,
the relevant statement(s), byte offsets for diagnostics, and an error
accumulator.

`validate_index_clusters` checks each `CreateIndexStatement` in the file:
a missing `in_cluster` yields `IndexMissingCluster` (naming the index or
`<unnamed>`), and a present one is run through `validate_cluster_name` from the
`identifiers` module to confirm the cluster name is a legal identifier.
`validate_mv_cluster`, `validate_sink_cluster`, and `validate_source_cluster`
apply the same missing-versus-name-check pattern to
`CreateMaterializedView`, `CreateSink`, and `CreateSource` statements,
emitting `MaterializedViewMissingCluster`, `SinkMissingCluster`, and
`SourceMissingCluster` respectively. `validate_source_cluster` additionally
rejects sources that carry `external_references` with a
`SourceExternalReferences` error.
