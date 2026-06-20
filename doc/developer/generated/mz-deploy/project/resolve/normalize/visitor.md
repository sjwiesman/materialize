---
source: src/mz-deploy/src/project/resolve/normalize/visitor.rs
revision: a647094cc4
---

# mz_deploy::project::resolve::normalize::visitor

`NormalizingVisitor<T: NameTransformer>` traverses a SQL AST and rewrites object
names using a pluggable transformer strategy. Query-level recursion is delegated
to mz-sql-parser's auto-generated `VisitMut`; the visitor overrides only
`visit_query_mut` (CTE scoping) and `visit_table_factor_mut` (name transformation
plus implicit aliasing), letting the default traversal handle expressions, set
operations, and the like.

It owns the `transformer` and a `CteScope`. `visit_query_mut` handles CTE
visibility to match Materialize's resolver: for a simple `WITH` block it pushes an
empty scope, visits each CTE body before inserting that CTE's own name (so a
simple CTE can reference a shadowed catalog object inside its body), then visits
the main query body, order-by, limit, and offset with all names in scope; a
mutually-recursive block pushes all names up front via the default traversal.
`visit_table_factor_mut` captures the original (last-part) table name as an
implicit alias before transforming, unless the reference is a CTE, so that
qualified column references survive transformation. The scope is consulted in
`normalize_raw_item_name`, which skips transforming any unqualified single
identifier that names a CTE.

A family of `normalize_*` helpers apply the transformer to the various places
object references appear: `normalize_raw_item_name` /
`normalize_unresolved_item_name` (general names),
`normalize_unresolved_schema_name` (qualify bare schema names),
`normalize_sink_connection` / `normalize_source_connection` (Kafka, Postgres,
SQL Server, MySQL, Iceberg connection refs), `normalize_connection_options` /
`normalize_with_option_value` (secrets, item refs, AWS PrivateLink, Kafka broker
tunnels, nested sequences), `normalize_query` (views/MVs),
`normalize_index_references`, `normalize_index_clusters`,
`normalize_grant_references`, `normalize_comment_references`, and
`normalize_cluster_name` (the cluster-renaming helpers require `T:
ClusterTransformer`). Convenience constructors `fully_qualifying`,
`fully_qualifying_with_db_map`, `flattening`, `explain`, `overlay`, and `staging`
build a visitor wired to each concrete transformer.
