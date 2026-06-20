---
source: src/mz-deploy/src/project/ir/graph.rs
revision: a647094cc4
---

# mz_deploy::project::ir::graph

The dependency-aware project graph — the final output of compilation (the result
of [`compile_sync`](crate::project::compiler)). It combines the
`database > schema > object` hierarchy with a flat dependency graph.

`Project` holds `databases` (the hierarchy used for iteration, ordering, and
module-statement execution), `dependency_graph` (a flat adjacency list, `ObjectId`
to its set of dependencies, used for topological sort and change propagation),
`external_dependencies` (objects referenced but not defined in the project, which
appear only as dependency-set values and are excluded from sorts and deployment),
`cluster_dependencies` (clusters referenced by indexes and MVs),
`replacement_schemas` (derived from `SET api = stable`), `tests` (unit tests keyed
by the object they test), and `compile_dirty` (objects that were cache misses this
run, driving incremental typechecking). The module invariant is that every
`ObjectId` reachable through `databases` has a `dependency_graph` entry.

`DatabaseObject` pairs an `ObjectId`, its validated
[`compiled::DatabaseObject`](crate::project::ir::compiled), and its set of
dependencies. `Schema` and `Database` carry names, children, and optional
module-level statements; `Schema` additionally carries a `schema_type`.
`SchemaType` is `Storage`, `Compute`, or `Empty`, segregating schemas by object
type to prevent accidental recreation; it implements `Display` and `FromStr`.
`ModStatement<'a>` is a borrowed module-level statement tagged with its execution
context — `Database` (database name) or `Schema` (database and schema names) —
used to run database.sql / schema.sql setup statements in order.
