---
source: src/mz-deploy/src/project/analysis/changeset/base_facts.rs
revision: a647094cc4
---

# mz_deploy::project::analysis::changeset::base_facts

Extracts relational base facts from a planned [`graph::Project`] for the Datalog
fixed-point computation in [`super::datalog`]. Each fact corresponds to an
extensional Datalog relation consumed by the propagation rules.

`BaseFacts` is the output struct. It holds six relations as parallel vectors and
sets: `object_in_schema` (`ObjectInSchema(object, database, schema)`),
`depends_on` (`DependsOn(child, parent)` taken from the project dependency
graph), `stmt_uses_cluster` (the cluster named in an object's `IN CLUSTER`
clause), `index_uses_cluster` (clusters used by `CREATE INDEX` statements on an
object, keyed additionally by index name), `is_sink` (objects backed by
`Statement::CreateSink`), and `is_replacement` (objects living in a schema listed
in `project.replacement_schemas`). Sinks and replacement objects are tracked
separately because the propagation rules treat them specially — sinks do not
dirty their clusters or schemas, and replacement MVs do not propagate dirtiness
to dependents or schemas.

`extract_base_facts` walks the project's `database > schema > object` hierarchy,
emitting facts per object. Cluster usage for each object's main statement is
obtained by calling [`extract_dependencies`](crate::project::analysis::deps) and
discarding the dependency set, keeping only the clusters. Index clusters come
directly from each `CreateIndexStatement`'s `in_cluster` field, with unnamed
indexes labeled `"unnamed_index"`. The `ClusterBoundary` helper relation
mentioned in the rules is not stored here; it is materialized downstream in the
evaluator from the union of statement and index cluster usage. Throughout,
`verbose!` emits colored progress lines (extracted sink/replacement facts and a
closing summary count) when verbose output is enabled.
