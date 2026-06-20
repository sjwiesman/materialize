---
source: src/mz-deploy/src/client/type_info.rs
revision: a647094cc4
---

# mz_deploy::client::type_info

Column-schema introspection for the data-contract (`lock`) and type-checking
systems, implemented on `TypeInfoClient`.

`query_types_for_objects` resolves the column schema, object kind, and comments
for a list of objects plus a list of source tables in a single catalog query. It
encodes the input `(database, schema, object)` triples as one `jsonb` array
parameter expanded via `jsonb_array_elements`, joins `mz_columns`, `mz_objects`,
`mz_schemas`, `mz_databases`, and `mz_internal.mz_comments`, and returns per-
object metadata as a `jsonb` text blob deserialized on the client into the
private `CatalogObjectInfo` / `CatalogColumnInfo` structs. The result is a
`(Types, Vec<ObjectId>)` pair where the second element lists input objects not
found in the catalog (surfaced by `lock` as `DeclaredDependenciesMissing`).
Objects passed as source tables are always recorded as `ObjectKind::Table`
regardless of their catalog type, and objects without columns appear with an
empty column map.
