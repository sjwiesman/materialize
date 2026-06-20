---
source: src/mz-deploy/src/types.rs
revision: a647094cc4
---

# mz_deploy::types

The `types.lock` data-contract system. When a project references objects it does
not own (e.g. tables from an upstream ingestion pipeline), this module records
their column schemas so dependent views can be type-checked offline. It owns the
`types.lock` format and shared type/schema utilities; incremental runtime
typechecking itself lives in `crate::project::compiler::typecheck`.

`ObjectKind` enumerates the object kinds recorded in a contract entry (Table,
View, MaterializedView, Source, Sink, Secret, Connection) with kebab-case serde,
a `FromStr`/`from_db_str` parse pair, an `as_str` (serde form) and a `Display`
(human form). `BUILD_DIR` names the build-artifact directory (`target`).

`Types` is the in-memory representation: a version, a map from `ObjectId`
(fully-qualified `database.schema.object`) to a column map, a parallel `kinds`
map, and an optional object-level `comments` map. `ColumnType` is a single
column's SQL `type`, `nullable` flag, original `position`, and optional
`COMMENT ON COLUMN` text. `Types::get_table`, `get_kind`, and `write_types_lock`
read and persist the contract; `load_types_lock` reads and parses `types.lock`
from a directory.

On-disk serialization uses a separate `TypesLock` shape with per-kind sections
(`[[table]]`, `[[view]]`, etc.) and inline column tables. `From<&Types>` /
`From<TypesLock>` convert between the in-memory and on-disk forms (sorting columns
by position, rederiving positions on load), `all_objects`/`vec_for_kind` route
objects by kind, and `write_toml` hand-formats the file with a generated-file
header and TOML-escaped values (`escape_toml_string`). `TypesError` covers the
read/write/parse/directory failure modes.
