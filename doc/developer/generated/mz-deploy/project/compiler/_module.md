---
source: src/mz-deploy/src/project/compiler.rs
revision: a647094cc4
---

# mz_deploy::project::compiler

The incremental project compiler — the canonical implementation of
`project::plan_sync`'s compile contract. The unit of incremental reuse is a
logical database object (`database.schema.object`), not the whole project.
Object-local work runs in parallel via rayon; cross-object validation is
deterministic and runs once all object results for the invocation are available.

`compile_sync` (and the stats-returning `compile_sync_with_stats`) drives the
pipeline:

1. **Discover** — `discover_project` walks the `models/` tree to find databases,
   schemas, objects, and mod files, producing a `Discovery` of descriptors plus
   a database-name map for profile-suffix rewriting.
2. **Plan** — fingerprint every object (a SHA-256 over its key, variables, and
   the path + content hash of each file variant) against the cached artifact
   store, partitioning objects into hits and misses.
3. **Compile misses** — parse, validate, and normalize each miss from source and
   persist a new artifact.
4. **Assemble** — `object_validation::assemble_project` combines database/schema
   metadata with validated objects into a [`compiled::Project`], applying
   cross-database and cluster name rewrites when a profile suffix is active.
5. **Build graph** — cross-object validation, dependency extraction, and
   topology produce the final [`graph::Project`].

All cached state is advisory: missing, corrupt, or schema-incompatible entries
are treated as misses and rebuilt. Cache namespaces are scoped per `(profile,
suffix, variables)` so caches never leak across configurations.

Child modules:

- **`cache`** — the SQLite-backed compiler cache: the writable `BuildArtifact`
  handle used during compilation and the read-only `ProjectCache` for the LSP,
  `test`, and `explain`.
- **`cache_io`** — small IO helpers shared across the cache layer, notably
  `hex_digest` for rendering digests as stable hex strings.
- **`mod_statements`** — validates the database- and schema-level mod files,
  restricting them to statements that target the database or schema they live
  under.
- **`object_validation`** — per-object validation and compiled-project assembly:
  statement classification, name/identifier checks, reference and cluster
  validation, profile-variant resolution, and schema-wide invariants.
- **`typecheck`** — incremental offline typecheck against a stub catalog running
  the real `mz_sql` planner, with schema-stability gating and a parallel DAG
  executor.
