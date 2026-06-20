---
source: src/mz-deploy/src/project/analysis/topology.rs
revision: a647094cc4
---

# mz_deploy::project::analysis::topology

Extends [`graph::Project`](crate::project::ir::graph) with traversal, topological
ordering, and accessor methods over the `database > schema > object` hierarchy and
the flat dependency graph.

`topological_sort` performs a DFS over `dependency_graph`, returning object IDs
in deployment-safe order where every dependency precedes its dependents. External
dependencies are excluded from the traversal (they are not deployable), and a
back edge into an in-progress node yields `DependencyError::CircularDependency`.
The recursive `visit` helper tracks `visited` and `in_progress` sets to detect
cycles and accumulate the sorted output. `get_sorted_objects` pairs the sorted
IDs with their compiled objects via `find_typed_object`, and
`get_sorted_objects_filtered` does the same while keeping only IDs present in a
supplied filter set, preserving topological order within the subset.

Accessor methods flatten the hierarchy: `iter_objects` yields every
`DatabaseObject`, `find_object` locates one by `ObjectId`, `get_tables` returns
`CREATE TABLE` and `CREATE TABLE ... FROM SOURCE` objects, and
`get_tables_from_source` returns only the latter (whose columns must be queried
from the live server because they depend on the external source).
`build_reverse_dependency_graph` inverts `dependency_graph` to map each object to
the set of objects that depend on it, used for incremental downstream analysis.

`iter_mod_statements` returns module-level statements (from database.sql /
schema.sql files) in execution order — all database-level statements first, then
schema-level — so database setup precedes schema setup before object creation. It
strips the `SET api = ...` directive (matched case-insensitively on the variable
name), which is a mz-deploy directive rather than SQL to send to Materialize.
`validate_cluster_isolation` delegates to
[`super::graph_validation`](crate::project::analysis::graph_validation).
