---
source: src/mz-deploy/src/project/compiler/cache.rs
revision: a647094cc4
---

# mz_deploy::project::compiler::cache

The SQLite-backed compiler cache, one database file per profile namespace.
`db_path` derives the location under `target/compiler/<namespace>/`, where the
namespace is a hash of the profile name, optional suffix, and variable bindings
— so different profiles, suffixes, or variable sets use isolated caches. The
module defines the shared `CacheError` vocabulary covering directory creation,
database open, database operation, and source-file read failures.

Two handles share this file location, schema version, and error vocabulary:

- **`build_artifact`** — `BuildArtifact`, the read/write handle used during
  compilation. It wraps a `rusqlite::Connection`, persists file metadata and
  content hashes, cached object artifacts, and typecheck results, and reuses
  them across invocations.
- **`project_cache`** — `ProjectCache`, the read-only handle used by downstream
  consumers (the LSP in particular) that need compiled project metadata without
  driving a compile. It opens the same database through a read-only, mutex-free
  connection.
- **`schema`** — the SQLite table DDL as `pub(super)` constants, including the
  `SCHEMA_VERSION` integer and the `DROP_SQL`/create statements the writer runs
  to rebuild the cache when the stored version no longer matches.
