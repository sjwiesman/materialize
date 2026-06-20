---
source: src/mz-deploy/src/project/compiler/cache/build_artifact.rs
revision: 673fdb9d44
---

# mz_deploy::project::compiler::cache::build_artifact

The writable persistence layer for the incremental compiler, scoped to one
profile namespace. `BuildArtifact` wraps a `rusqlite::Connection` to a SQLite
database (path derived from profile, optional suffix, and compile-time
variable bindings, so different profiles use isolated caches). `open` creates
the parent directory, opens the connection, applies performance pragmas (WAL
journaling, NORMAL synchronous, a 64 MiB page cache, 256 MiB mmap), and
reconciles the schema version: if the stored `meta.schema_version` does not
match `schema::SCHEMA_VERSION`, every table is dropped and recreated. All
cached state is advisory, so a version mismatch, missing entry, or corruption
is treated as a cache miss and rebuilt from source.

The database persists several categories of state. **File metadata**
(`file_state`) holds content hashes and source text keyed by path, with
freshness keyed on file size and mtime; `load_file_hashes` and
`load_file_contents` transparently refresh stale, missing, or NULL-contents
entries from disk, and bypass the disk-keyed cache entirely for overlay-covered
paths (where disk size+mtime are unchanged but the in-memory buffer differs).
**Object artifacts** (`object_state` plus per-fragment tables
`object_state_indexes`/`_grants`/`_comments`/`_tests`) store compiled object
payloads keyed by logical object id and a `fingerprint`;
`load_object_fingerprints` reads just the (key, fingerprint) pairs for fast
hit/miss detection during planning, while `load_object_artifacts`
materializes full payloads, and `upsert_object_rows` replaces each object's
fragment rows so row-count changes (e.g. a removed index) are reflected
exactly. **Typecheck artifacts** (`typecheck_objects`, `typecheck_columns`)
hold per-object kinds and column schemas via `upsert_typecheck_results` /
`load_typecheck_columns`. **External-type digests** are managed by
`load_external_type_digests` / `replace_external_type_digests`. **Project
snapshot** is the full compiled project graph written by `write_project`,
which incrementally rewrites per-object rows for `changed_keys ∪ deleted_keys`
across the `project_*` tables and rewrites small project-wide tables in full.
`prune_object_rows`, `prune_typecheck_results`, and `prune_rows` drop rows for
objects no longer in the project.

`CompiledObjectArtifact` is either `Skipped` (the object was excluded for the
active profile) or `Object(CompiledObjectArtifactData)`. The data variant
stores SQL text (statement, indexes, grants, comments, tests) rather than AST
nodes, since the AST types are not serializable; on cache hit the strings are
re-parsed. `ObjectStateRow` bundles an object key, fingerprint, and artifact
for upsert. `ObjectStateHeader` derives the `object_state` header columns from
an artifact.

`ProjectStatements` holds the prepared INSERT statements used by
`write_project` to populate the project snapshot tables, with helpers
`insert_object` (object row plus dependencies, comments, indexes, grants,
tests, infrastructure via `infrastructure::extract`, and the alias map),
`insert_infrastructure`, and `insert_mod_statements`. `statement_cluster`
extracts the unresolved `IN CLUSTER` name from a statement. The `AliasVisitor`
AST visitor walks `CreateView`/`CreateMaterializedView` query bodies to build
an alias → fully-qualified-name map via `extract_alias_map`, collecting direct
table references and their aliases (lowercased) while excluding CTE references
and skipping derived subqueries and table functions. `file_metadata_signature`
reads a path's size and mtime (saturating both to `i64`) for use as the
advisory freshness key.
