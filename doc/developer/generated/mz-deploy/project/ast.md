---
source: src/mz-deploy/src/project/ast.rs
revision: a647094cc4
---

# mz_deploy::project::ast

AST-adjacent vocabulary types shared across source inputs, compiled objects, and
dependency analysis without carrying validation status (which keeps these types
free of circular dependencies).

`Statement` is an enum wrapping the accepted primary CREATE statement variants of
Materialize's SQL dialect — `CreateSink`, `CreateView`, `CreateMaterializedView`,
`CreateTable`, `CreateTableFromSource`, `CreateSource`, `CreateSecret`,
`CreateConnection` — each holding the parsed `Raw`-state AST node. `kind` maps a
variant to its `ObjectKind` (tables and table-from-source both map to `Table`).
`into_parser_statement` converts back to the parser's `Statement<Raw>` without
re-parsing, and `ident` extracts the declared object name as a `DatabaseIdent`.
`Display` formats the wrapped node.

`DatabaseIdent` is a structured object identifier supporting partial
qualification: `database` and `schema` are optional, `object` is required. It is
built `From<UnresolvedItemName>` (1/2/3 parts map to object / schema.object /
database.schema.object). `matches` does flexible matching where an identifier with
fewer qualifier levels matches one with more, as long as the present parts agree:
object names must match exactly, and any specified schema or database must match
the other's when that part is also present (missing qualifiers act as wildcards).

`Cluster` is a typed, name-only reference to a non-namespaced Materialize cluster
(referenced via `IN CLUSTER`), with `new`, a generic `From<AsRef<str>>`, and
`Display`, and deriving the ordering/hashing traits needed to use it as a set key.
