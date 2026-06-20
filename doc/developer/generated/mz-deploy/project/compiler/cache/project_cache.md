---
source: src/mz-deploy/src/project/compiler/cache/project_cache.rs
revision: 673fdb9d44
---

# mz_deploy::project::compiler::cache::project_cache

The read-only face of the compiler cache, used by consumers such as the LSP,
`test`, and `explain` that need compiled project metadata without driving a
compile. `ProjectCache` wraps a read-only, no-mutex `rusqlite::Connection` to
the build artifact database. `open` derives the database path from the
profile/suffix/variables and returns `Ok(None)` when the file does not exist
(the project has never been compiled). Because the cache is advisory, queries
return `None` or empty collections on missing data or query failure rather
than erroring; `query_vec` is the shared helper that prepares a statement,
maps rows, and silently yields an empty `Vec` if preparation or execution
fails.

The module defines plain data structs mirroring the project snapshot tables:
`CachedObject` (full metadata with comments, indexes, grants, aliases, and
infrastructure), `CachedObjectSummary` (lightweight — no SQL text or
sub-collections), `CachedDatabase`, `CachedSchema`, `CachedComment`,
`CachedIndex`, `CachedGrant`, `CachedInfrastructure`, `CachedProperty`, and
`CachedTest`.

Query methods read from the `typecheck_*` and `project_*` tables.
`get_columns` returns an object's column schema and `get_kind` its
`ObjectKind`, both from the typecheck tables. `get_column_names` batches a
single `IN (...)` query for many objects and returns lowercased column names
grouped by lowercased object key. `get_object` assembles a full `CachedObject`
by fetching the `project_objects` header then the comments, indexes, grants,
aliases, and infrastructure for that key; `get_object_by_path` resolves the
object key from a file path first. `list_objects` returns all summaries and
`list_databases_with_objects` returns the complete catalog of databases,
schemas, and fully-detailed objects. `list_external_dependencies`,
`get_dependencies`, and `get_dependents` (reverse lookup) read the dependency
tables; `get_tests` and `get_mod_statements` (ordered by position, matching on
a nullable schema) round out the surface. Private `query_*` helpers back the
sub-collection reads.
