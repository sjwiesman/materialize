# mz-deploy review: the project compiler

`src/mz-deploy/src/project/` is the largest subsystem (~24.7k insertions). It
is the offline half of the tool: no database connection, used by `compile`,
`test`, `stage`, and the LSP server alike. Reviewers of this area should read
this first; the crate-level doc comments cited below are the authoritative
statements of intent — review the code against them.

## Pipeline

```
discover → fingerprint → compile misses → assemble → build graph → typecheck
```

Entry points: `project::plan_sync` (`project.rs:281`, used by LSP) and
`project::plan` (`project.rs:88`, async wrapper). Orchestration lives in
`compile_sync_with_stats` (`compiler.rs:266`).

1. **Discovery** (`compiler.rs:500`): walks `models/<database>/<schema>/
   <object>.sql`, collecting per-object file variants (default `object.sql`
   plus profile overrides `object#profile.sql`) and database/schema-level
   "mod" files (grants/comments).
2. **Fingerprinting** (`compiler.rs:301`): content-hashes every variant in
   parallel (rayon) and classifies each object Hit/Miss against the SQLite
   cache. The fingerprint covers the object key, every variant's *absolute
   path* and content hash, and the compile-time variable map
   (`compiler.rs:38-62`).
3. **Compile misses** (`compiler.rs:378`): per object — psql-style `:var`
   substitution (`syntax/variables.rs`, 44 tests) *before* parsing with
   `mz_sql_parser`; per-object validation (`compiler/object_validation.rs:161`
   — exactly one main CREATE statement; names match file location; MVs,
   sinks, sources, and indexes must name a cluster); identifier
   normalization to fully-qualified `db.schema.object` form
   (`resolve/normalize/visitor.rs`).
4. **Assembly** (`object_validation::assemble_project`): groups objects by
   (db, schema), enforces schema-wide invariants — notably the
   storage/compute segregation rule (a schema may not mix tables/sources
   with views/MVs; `object_validation/schema_constraints.rs`) — and, when a
   profile suffix is active, rewrites cross-database and cluster references.
5. **Graph building** (`analysis/deps.rs:105`): walks each statement's AST
   (with CTE-scope tracking) to extract the dependency graph, validates
   external references against `project.toml` `dependencies`, and produces
   the final `ir::graph::Project`.
6. **Typecheck** (`compiler/typecheck.rs:72`, optional): plans every view/MV
   with the real `mz_sql` planner against an **in-memory hand-rolled
   catalog**, caching resulting column schemas in SQLite.

## The two pieces that deserve the most review time

### A. The embedded catalog (`compiler/typecheck/catalog.rs`, 2,520 lines)

This is a from-scratch implementation of `mz_sql::catalog::SessionCatalog` —
enough of Materialize's catalog (builtin types, system schemas, ID
allocation, item lookup) to let `mz_sql::plan::plan_statement` typecheck a
view without a server. The crate is honest about the trade-off:

> Validates objects against an in-memory catalog built from `mz-sql`,
> without requiring a running Materialize container. This backend is faster
> and more portable than the Docker backend, at the cost of lower fidelity
> (the in-memory catalog may not reproduce all Materialize behaviors).

Most trait methods are stubs or minimal implementations. Review questions:

- Which planner code paths can hit a stubbed method, and do the stubs fail
  loudly or silently produce wrong answers?
- Maintenance: every `SessionCatalog` trait change in `mz-sql` now has a
  second implementor in-tree. Who owns keeping it in sync? Is there a test
  that diffs its answers against a real `environmentd` (the Docker backend
  exists — is it exercised in CI)?
- Architectural alternative worth an explicit decision: reuse the real
  catalog (heavier deps) vs. accept this shim. This is the single biggest
  long-term-cost question in the PR.

Stated invariant (`catalog.rs:10-21`): a fresh catalog instance per object
validation, so no state leaks between objects. Verify nothing caches across
instances.

Incremental typecheck (`typecheck.rs:55-71`, `typecheck/executor.rs`):
3-phase — serial catalog bootstrap, parallel DAG execution with
schema-stability tracking (a node whose recomputed columns match its cached
columns does not dirty its dependents), persist to SQLite. The dirty-set
computation (`typecheck.rs:122-164`) is deliberately pessimistic; check
the under-approximation direction (missing a dirty object = stale types).

### B. Dirty propagation (`analysis/changeset.rs`, 1,269 lines + `changeset/datalog.rs`)

Determines what `stage` must redeploy. A hand-written Datalog-style
fixed-point over three mutually-recursive sets (the rules are spelled out in
the 47-line doc comment at `changeset.rs:10-56`):

- `DirtyStmt`: directly changed, OR uses a dirty cluster, OR depends on a
  dirty statement (**except** changed replacement MVs), OR lives in a dirty
  schema.
- `DirtyCluster`: a *directly changed* statement (not merely dirty, and not
  a sink) uses it.
- `DirtySchema`: contains a dirty statement that isn't a sink.

The asymmetries are load-bearing and easy to get wrong: index clusters do
not dirty their object; sinks never dirty schemas (they're created
post-swap); replacement MVs break downstream propagation. A wrong rule here
silently under- or over-deploys. Entry point:
`ChangeSet::from_deployment_snapshot_comparison` (`changeset.rs:90`); 17
inline tests exist — review whether they cover each rule's negative space
(e.g. "sink change must NOT dirty its schema").

## Build cache (`compiler/cache/`, ~2,900 lines, SQLite via rusqlite)

DB at `.mz-deploy/compiler_<profile>_<suffix>_<vars-hash>.db`. Tables:
`file_state` (path → size/mtime/hash/contents), `object_state` (fingerprint
+ compiled SQL strings), `typecheck_objects`/`typecheck_columns`,
`project_state` (whole-graph snapshot for LSP/explain). Schema-versioned;
all entries are advisory — corruption/missing rows degrade to recompile.

Stated correctness contract (`compiler.rs:64-80`), worth quoting because the
whole cache design hangs on it:

> A cache hit must therefore produce the same object facts that object-local
> parsing and validation would produce from source while skipping
> revalidation. … database- and schema-level mod statements are validated on
> every invocation; they are not cached independently … final dependency
> extraction operates on a complete compiled project assembled for the
> current invocation.

Notes for review:

- Artifacts are cached as **SQL strings** (AST isn't serializable) and
  re-parsed on hit. Verify pretty-print → re-parse round-trips exactly
  (statement-identity, not just semantic equivalence).
- Absolute paths in the fingerprint mean moving a checkout invalidates the
  cache — conservative, but consider whether relative paths were rejected
  for a reason.
- There is **no dependency-directed invalidation** at the compile layer (by
  design; the typecheck DAG handles schema stability). Confirm no consumer
  assumes otherwise.

## Other things reviewers should know

- **Byte-offset tracking** (`syntax/parser.rs:137-149`): statement source
  locations are recovered via pointer arithmetic on the assumption that the
  parser returns subslices of the input string. Documented and
  clippy-allowed, but it silently breaks if `mz_sql_parser` ever copies —
  diagnostics and LSP positions would drift. A test pinning this assumption
  would be cheap.
- **`.expect()` in normalization** (`resolve/normalize/normalize.rs:76`,
  `transformers.rs` ~lines 60–90): constructing suffixed identifiers with
  `Ident::new(..).expect(..)`. A profile suffix that produces an invalid
  identifier panics instead of erroring. Suffix validation upstream should
  be confirmed or these converted to errors.
- **Profile-variant semantics**: views/MVs may not have profile overrides;
  only the active variant is fully validated. Make sure the
  variant-consistency check (all variants same object kind) is enforced.
- **Zero-test modules**: `compiler/object_validation/` (955 lines across 5
  files — the main per-object validator) and `compiler/mod_statements.rs`
  (441 lines) have **no inline tests**. `analysis/deps.rs` (623 lines) has
  5. By contrast `syntax/variables.rs` (44), `analysis/changeset.rs` (17),
  and `resolve/normalize/tests.rs` (a 2,398-line test file) are well
  covered. Asking for tests on object_validation before merge is a
  reasonable review outcome.
