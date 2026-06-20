---
source: src/mz-deploy/src/project/compiler/typecheck/convert.rs
revision: a647094cc4
---

# mz_deploy::project::compiler::typecheck::convert

Pure conversions bridging the project's compiled AST, the parser AST that
`mz_sql::names::resolve` accepts, and the column-map representation stored in
the build artifact. The module holds no catalog state and does no I/O; it is
used by `bootstrap` (phase 1) and the per-task work closure (phase 2).

`create_stub_table_sql` renders a `CREATE TABLE` statement from an `ObjectId`
and a column map, quoting identifiers and appending `NOT NULL` for
non-nullable columns; this restores a cached dependency as a stub. The name is
qualified with a database when the `ObjectId` has one, otherwise just
schema.object.

`create_catalog_item_statement` (private) normalizes a compiled statement for
the in-memory catalog: it fully-qualifies names via a `NormalizingVisitor` and
strips properties irrelevant to typechecking — temporary flags, cluster
assignments on materialized views, and table constraints, with-options, and
foreign-key/check column options on tables. It handles `CreateView`,
`CreateMaterializedView`, and `CreateTable`, and returns `None` for statement
kinds (such as `CreateTableFromSource`) that produce no typecheckable item.
`create_catalog_item_sql` renders that to a SQL string, while
`create_catalog_item_ast` returns the parser AST directly, skipping the
render-and-reparse round trip.

`relation_desc_to_columns` converts a planner `RelationDesc` into the
`BTreeMap<String, ColumnType>` column map persisted in the cache, recording
each column's position and nullability. `sql_scalar_type_to_sql` (via
`sql_column_type_to_sql`) maps every `SqlScalarType` to its SQL type-name
string, handling parameterized types (numeric scale, timestamp precision,
char/varchar length) and composite types (arrays, lists, maps, ranges,
records).
