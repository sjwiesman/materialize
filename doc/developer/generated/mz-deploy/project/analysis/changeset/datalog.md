---
source: src/mz-deploy/src/project/analysis/changeset/datalog.rs
revision: a647094cc4
---

# mz_deploy::project::analysis::changeset::datalog

Datalog-style fixed-point engine that computes which objects, clusters, and
schemas are dirty (require redeployment) given a seed set of changed statements.
It implements the blast-radius propagation rules over the project's dependency
graph and cluster/schema membership.

`FactIndexes` precomputes lookup maps from the [`BaseFacts`](super::base_facts):
`stmt_to_clusters`, `index_to_clusters`, `dependents` (the reverse of
`depends_on`, parent to children), `object_to_schema`, `schema_to_objects`, and
the `cluster_boundary` — the set of all clusters referenced by any statement or
index, materializing the `ClusterBoundary` relation. `DirtyState` holds the three
mutable working sets (`dirty_stmts`, `dirty_clusters`, `dirty_schemas`) carried
across iterations and is seeded from the changed statements. `PendingFacts`
collects newly derived facts within one rule group before they are merged into
`DirtyState`, keeping each iteration's derivations consistent.

`Evaluator` drives the loop. `run` repeatedly applies five `derive_*` rule
groups in a fixed order until no set grows, signalling the fixed point.
`derive_cluster_dirtiness` dirties clusters used by *changed* (not merely dirty)
non-sink statements, via both `stmt_to_clusters` and `index_to_clusters`,
restricted to the cluster boundary. `derive_stmt_dirtiness_from_clusters` dirties
statements that use a dirty cluster. `derive_stmt_dependency_dirtiness`
propagates dirtiness downstream along `dependents`, skipping changed replacement
MVs (which use the in-place `CREATE REPLACEMENT` protocol and must not fan out).
`derive_schema_dirtiness` dirties the schema of each dirty statement, excluding
sinks and replacement objects. `derive_stmt_dirtiness_from_schemas` pulls every
non-replacement object in a dirty schema into the dirty set, giving schema-level
atomicity. Each helper is annotated with the Datalog rule it encodes and emits
`verbose!` traces of rule firings.

`compute_dirty_datalog` is the module entry point: it logs the start via
[`super::logging`], constructs the evaluator from the changed statements, base
facts, and changed replacements, runs to convergence, logs the final results,
and returns the three dirty sets.
