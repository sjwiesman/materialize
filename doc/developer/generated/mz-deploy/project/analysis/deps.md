---
source: src/mz-deploy/src/project/analysis/deps.rs
revision: 673fdb9d44
---

# mz_deploy::project::analysis::deps

Converts a [`compiled::Project`](crate::project::ir::compiled) into the
dependency-aware [`graph::Project`](crate::project::ir::graph) by walking the SQL
AST of every object to extract object and cluster dependencies and to classify
schema types.

The `From<compiled::Project> for Project` impl drives assembly in three phases:
*collect* (flatten all compiled objects into `TypedObjectTask`s and gather the
set of defined object IDs for external-reference detection), *process* (extract
dependencies, clusters, and unit tests per object — a CPU-bound step parallelized
with rayon, producing `ProcessedObject`s), and *reassemble* (merge results into
the flat `dependency_graph`, the `external_dependencies` set, the
`cluster_dependencies` set, the test list, and the hierarchical
`database > schema > object` structure). `determine_schema_type` classifies each
schema as `Storage`, `Compute`, or `Empty` by inspecting its first object, relying
on the compiled-project invariant that a schema cannot mix storage and compute
objects.

Query-level traversal is delegated to mz-sql-parser's auto-generated `Visit`
trait. `DependencyVisitor` overrides `visit_table_factor` to record table
references as `ObjectId` dependencies and `visit_query` to manage CTE scope via
[`CteScope`](crate::project::resolve::cte_scope). CTE handling mirrors
Materialize's name resolver: simple CTEs see only earlier siblings while their
own body is visited (so a simple CTE shadowing a catalog object still depends on
that object when it references it), and mutually-recursive blocks push all names
up front. CTE names are excluded from the dependency set.

`extract_dependencies` is the public statement-level dispatcher (also used by the
changeset module for cluster analysis). It visits only the relevant subtree per
statement type and returns `(object_dependencies, cluster_dependencies)`,
collecting `IN CLUSTER` clusters for MVs/sinks/sources, the source dependency of
a table-from-source, the shard dependency of a sink, and connection/option
dependencies for sources and connections. Helpers `extract_source_connection_dep`,
`extract_connection_option_deps`, and the recursive `extract_with_option_value_deps`
pull connection, secret, SSH-tunnel, and AWS PrivateLink references out of
option values. `extract_external_indexes` returns indexes installed on a cluster
different from their object's cluster (all indexes for non-MV objects).

`validate_dependencies` cross-references dependencies declared in `project.toml`
against discovered external references, returning a `DependencyValidation` with
`undeclared` (discovered but not declared — a hard error) and `unused` (declared
but never referenced — a warning). Tests cover the validation set-difference
cases and the simple-CTE shadowing rule.
