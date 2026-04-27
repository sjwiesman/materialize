# Parallel Full Typechecking via DAG Task Executor

## Background

Today, `project::compiler::typecheck` is incremental: a `plan()` step
fingerprints every typecheck-eligible object against a SQLite cache, partitions
into clean/dirty, and an `execute()` step validates only the dirty subset
against a single shared `LocalCatalog`, sequentially in topological order. A
`DirtyPropagator` cascades dirtiness to dependents whenever an object's output
columns change.

Typechecking has gotten fast enough that the incremental machinery is no longer
load-bearing. The cost — `TypecheckPlan`, `IncrementalState`, `DirtyPropagator`,
`plan_dep_creation`, fingerprint comparison, cached/recomputed merging — is
significant code surface for diminishing wins.

This design replaces incremental typechecking with a full typecheck on every
invocation, parallelized via an explicit DAG task executor. Validated columns
continue to be persisted to SQLite for the LSP and other downstream consumers.

## Goals

- Always typecheck every view and materialized view from scratch.
- Parallelize across rayon's thread pool with a DAG-aware ready-queue scheduler
  (no level-barrier wavefronts; tasks fire as soon as their dependencies
  complete).
- Continue to persist per-object validated columns to the existing SQLite
  tables, preserving the LSP read path unchanged.
- Substantially reduce the typecheck module's code volume and cognitive load.

## Non-goals

- No change to the on-disk schema consumed by `project_cache` (the LSP reader).
  Only writer-internal columns are touched.
- No change to the user-visible error format or messaging.
- No new dependencies. `rayon` and `crossbeam-channel` are already in the
  workspace.

## Public API

`typecheck.rs` exposes a single synchronous entrypoint that replaces the
`plan()` + `execute()` pair:

```rust
pub fn run(
    directory: &Path,
    profile: &str,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
    project: &Project,
    external_types: Types,
) -> Result<Types, TypeCheckError>
```

Returns the merged `Types` covering validated view/MV columns plus declared
columns for non-typechecked objects (tables, sources, sinks, etc.) plus
external `types.lock` entries — i.e. exactly the schema universe that
downstream callers like `validate_constraints_with_types` and the LSP need.

Three callers update accordingly:

- `cli/commands/compile.rs::typecheck_project` — replace plan/execute with one
  `run()` call. The `is_up_to_date()` "skip type check" branch goes away.
- `cli/commands/test.rs` — same.
- `lsp/server.rs::typecheck_project` — same; the current "no-op when plan is
  up to date" early return is gone. The LSP always typechecks on rebuild.

## Removed components

From `typecheck.rs`:

- `TypecheckPlan`, `IncrementalState`, `is_up_to_date()`.
- `DirtyPropagator` and the cascade-on-output-fingerprint-change logic.
- `plan()` itself.
- `plan_dep_creation()`, `plan_deps_dfs()`, `DepAction`, `DepContext` — replaced
  by direct "use upstream task results as stubs."
- `previous_artifacts` plumbing and the merge-cached-with-rechecked logic in
  `into_merged_cache` / `into_updated_artifacts`.

From `build_artifact.rs`:

- The `output_fingerprint` column on `typecheck_objects`. It was load-bearing
  only for the dirty propagator.
- `load_typecheck_artifacts()` and `TypecheckArtifactRow`. Only the planner
  reads them; once it's gone, no caller remains.
- `SCHEMA_VERSION` bumps from 5 → 6, wiping existing build-artifact databases
  on first run after the change. (Acceptable: caches are advisory.)

## Retained internals

- `compute_semantic_fingerprint()` and its helpers (`Sha256Hasher`,
  `fmt_sql_exprs`, `fmt_idents`) — still called once per successful node in
  phase 3 to populate the persisted `semantic_fingerprint` column.
- The `semantic_fingerprint` column on `typecheck_objects`. The writer continues
  to fill it with the same SHA-256 it computes today — useful for future
  audit/observability, even though the typecheck planner no longer reads it.
- `upsert_typecheck_results()` (signature unchanged minus the
  `output_fingerprint` argument) and `prune_typecheck_results()` (unchanged).
- `LocalCatalog` and all its trait impls, `create_or_replace_item()`,
  `create_stub_table()`, `relation_desc_to_columns()`, `bootstrap_namespaces()`,
  `type_hash()`.

## Module layout

```
src/project/compiler/typecheck.rs        # public run() entrypoint
src/project/compiler/typecheck/
  catalog.rs                             # LocalCatalog (existing, simplified
                                         #   to remove planner-specific glue)
  base.rs        (new)                   # build_base_catalog() — phase 1
  executor.rs    (new)                   # DAG ready-queue scheduler — phase 2
```

## Pipeline

### Phase 1 — build base catalog (serial)

```rust
fn build_base_catalog(
    project: &Project,
    external_types: &Types,
) -> Result<(Arc<LocalCatalog>, BTreeMap<ObjectId, ColumnMap>), TypeCheckError>
```

Steps, in order:

1. `LocalCatalog::new()` — seeds builtins (system schemas, types, functions).
2. `bootstrap_namespaces()` — every `(database, schema)` referenced by project
   objects or by `external_types`.
3. For each entry in `external_types.tables`: `create_stub_table()` against the
   base catalog.
4. For each non-typechecked project object (table, source, sink, secret,
   connection, table-from-source — i.e. everything where `requires_typecheck`
   is false): `create_or_replace_item()` from its compiled SQL.
5. After each successful `create_or_replace_item` in step 4, capture the
   resulting `RelationDesc` → `ColumnMap` into a `base_columns:
   BTreeMap<ObjectId, ColumnMap>`.
6. Wrap the final catalog in `Arc<LocalCatalog>` and return alongside
   `base_columns`.

Errors from step 4 are accumulated. If any are present at the end of phase 1,
`run()` returns `TypeCheckError::Multiple(...)` immediately without spawning
phase 2. Reasoning: a broken table definition makes downstream view errors
cascading "unknown table" noise that obscures the real cause.

### Phase 2 — DAG executor (parallel)

`executor::run()` accepts the topo-sorted list of view/MV nodes, the shared
`Arc<LocalCatalog>`, the project's dependency graph, and `base_columns`.

Per-node bookkeeping:

```rust
struct NodeBookkeeping<'a> {
    obj: &'a compiled::DatabaseObject,
    remaining_deps: AtomicUsize,
    result: OnceLock<Result<ColumnMap, NodeFailure>>,
    dependents: Vec<ObjectId>,           // forward edges
    direct_dep_ids: Vec<ObjectId>,       // deps that come from other nodes
                                         //   (not from base_columns)
}

enum NodeFailure {
    TypecheckError(ObjectTypeCheckError),
    BlockedBy(ObjectId),
}
```

`remaining_deps` counts only deps in the view/MV set; deps satisfied by
`base_columns` (tables/sources/external) are pre-resolved and not counted.

Channel and shared state:

- A single `crossbeam_channel::unbounded::<ObjectId>()` carries IDs of nodes
  whose dependencies are all complete and that are ready to run.
- An `AtomicUsize` tracks how many nodes have completed; workers exit when
  it equals total node count.
- `Arc<HashMap<ObjectId, Arc<NodeBookkeeping>>>` is shared across workers so
  any worker can look up any node's slot.

Scheduler:

1. Build `NodeBookkeeping` for every view/MV. For each, compute
   `direct_dep_ids` and the initial `remaining_deps` count by scanning
   `dependency_graph[node_id]` and partitioning against `base_columns`.
2. Build the forward `dependents` list for each node by inverting the relevant
   subset of the dependency graph.
3. Seed the channel with every node whose initial `remaining_deps` is zero.
4. Spawn `rayon::current_num_threads()` workers inside a `rayon::scope`.
   Each worker loops:
   - `rx.recv_timeout(SHORT)` — small timeout so workers periodically check
     the global completion count and exit even if the channel is briefly
     empty mid-run.
   - On receive: run the per-node task (below), set the node's `OnceLock`
     result, increment the completion counter, and for each dependent
     atomically `fetch_sub(1, AcqRel)` on its `remaining_deps`. If the
     decrement returned 1 (i.e. the new value is 0), push the dependent's
     ID onto the channel.
5. After `rayon::scope` returns, walk the topo-sorted node list, read each
   `OnceLock`, and produce `Vec<NodeOutcome>`.

Per-node task body:

1. For each `dep_id` in `direct_dep_ids`, read the dep's `OnceLock`. The DAG
   ordering plus the counter discipline guarantees the value is set by the
   time we run.
2. If any dep result is `Err(_)`: write
   `Done(Err(BlockedBy(first_failed_dep)))` into our `OnceLock`, log a verbose
   line, return without running validation. The completion counter and
   dependents-decrement still happen exactly once. Downstream nodes will see
   our `Err` and propagate `BlockedBy` further.
3. Otherwise: `let mut cat = (*base_catalog).clone();` (relies on the existing
   `Clone` derives on `LocalCatalog` and its sub-types).
4. For each `(dep_id, columns)` from step 1, call
   `cat.create_stub_table(dep_id, &columns)`.
5. `cat.create_or_replace_item(node_id, &sql)` → `RelationDesc` →
   `relation_desc_to_columns()`.
6. Write `Done(Ok(columns))` or `Done(Err(TypecheckError(e)))` into our
   `OnceLock`.

Failure-propagation note: a failed node still participates in the
counter-decrement protocol. Its dependents discover the upstream error when
they pull from the channel and read the dep's `OnceLock`. Result: every node
runs through the scheduler exactly once, and the entire DAG's outcome map is
populated when phase 2 ends.

Why ready-queue over condvar/scope: with mz-deploy's typical project shape
(wide-but-shallow DAGs, hundreds of objects, many independent leaves), the
ready-queue model never parks worker threads waiting for upstream results —
workers pop only runnable work. The condvar variant would have most workers
parked on `wait` while a few run, leaking thread time. ~70 lines of executor
vs ~30, but the model is also simpler under failure (no missed-notify
concerns).

### Phase 3 — persist and return (serial)

```rust
fn persist_outcomes(
    db: &mut BuildArtifact,
    base_columns: &BTreeMap<ObjectId, ColumnMap>,
    outcomes: &[NodeOutcome],
    external_types: &Types,
    all_view_mv_keys: &BTreeSet<String>,
) -> Result<Types, TypeCheckError>
```

1. For each `Ok(columns)` outcome, build a `TypecheckedObjectArtifact` row
   (`object_key`, `semantic_fingerprint` (still computed),
   `object_kind`, columns) and stage it for upsert.
2. `db.upsert_typecheck_results(rows)`.
3. `db.prune_typecheck_results(all_view_mv_keys)` — keep-set is *every*
   typecheck-eligible object currently in the project, not just successful
   ones. This deletes rows for objects removed from the project but preserves
   rows for objects that failed typecheck in this run (their last successful
   row stays — important for the LSP, which would otherwise lose schema for
   a temporarily broken object).
4. Build merged `Types` = `cached_view_mv_columns ∪ base_columns ∪
   external_types`. Object kinds populated from the artifact map and from
   `object_kind_for_stmt` over `base_columns` entries.
5. Aggregate `NodeFailure::TypecheckError(...)` outcomes from phase 2 into
   `TypeCheckError::Multiple(...)` and return as `Err` if any. `BlockedBy`
   outcomes do not contribute errors — only the original failure is reported,
   matching the "skip dependents" decision.

## SQLite schema change

```diff
 CREATE TABLE IF NOT EXISTS typecheck_objects (
     object_key TEXT PRIMARY KEY,
     semantic_fingerprint TEXT NOT NULL,
-    output_fingerprint TEXT NOT NULL,
     object_kind TEXT NOT NULL
 );
```

`SCHEMA_VERSION` bumps 5 → 6. `BuildArtifact::open_with_path` already wipes
all tables on version mismatch, so existing caches are dropped on first run.
The `project_cache` reader (used by the LSP) only selects `object_kind` from
this table and the columns from `typecheck_columns`, so the LSP read path is
unaffected.

## Error semantics summary

| Source | Outcome |
|---|---|
| Phase 1 table/source/etc. fails | All phase 1 errors aggregated, `run()` returns `TypeCheckError::Multiple`. Phase 2 is not entered. |
| Phase 2 view/MV typecheck fails | Stored as `NodeFailure::TypecheckError`. Old SQLite row preserved (not pruned, not updated). |
| Phase 2 view/MV blocked by upstream failure | Stored as `NodeFailure::BlockedBy(<id>)`. Verbose log line emitted. Old SQLite row preserved. No error contribution to the public result. |
| Object removed from project | Pruned from SQLite at end of phase 3. |
| Object added to project | First successful run populates its SQLite row. |

## Testing

Unit tests in `executor.rs`:

- Linear chain `a → b → c`: all three typecheck in order, each task runs once,
  final outcome map has 3 `Ok` entries.
- Diamond `a → {b, c} → d`: `b` and `c` runnable concurrently (verifiable with
  an injected barrier in a test backend); `d` waits for both; output columns
  from both stubbed before `d` validates.
- Independent leaves: 4 nodes with no deps run in parallel.
- Failure propagation: `a` fails → direct dependents recorded as
  `BlockedBy(a)`, transitive dependents recorded as `BlockedBy(<direct dep>)`.
- Failure isolation: `a` fails on one branch; a disjoint chain `x → y`
  succeeds and is persisted normally.
- Empty graph: zero view/MV nodes → executor returns immediately with empty
  outcomes.
- Counter race: a node with two deps where both finish before the dependent
  is enqueued — the counter must reach zero exactly once and the node must
  enter the queue exactly once.

Existing tests:

- The `incremental_compile_*` tests in `compiler.rs::tests` are deleted. They
  cover the object compiler's cache hit/miss behavior, which this design does
  not change, but they aren't pulling enough weight to keep around.
- All existing CLI / LSP / mzcompose integration tests remain untouched as
  regression coverage: the user-visible error format and SQLite read path are
  unchanged.

## Migration

Single PR. The new module structure replaces the old one in place; callers
update to the new `run()` signature in the same change. Existing build-artifact
databases are wiped automatically via the `SCHEMA_VERSION` bump.
