# Parallel Full Typecheck Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace incremental typechecking in `mz-deploy` with full typechecking on every invocation, parallelized via a ready-queue DAG executor; keep persisting per-object validated columns to SQLite for the LSP.

**Architecture:** Three phases inside a new `typecheck::run()` entrypoint: (1) build a base `LocalCatalog` seeded with builtins, namespaces, external types, and all non-typechecked objects; wrap in `Arc`; (2) spawn one rayon task per view/MV node, coordinated by an `AtomicUsize` per-node remaining-deps counter and a single `crossbeam_channel` of ready node IDs; each task clones the base catalog, stubs its dep results, validates; (3) persist successful columns to SQLite, preserving rows for failed/blocked objects.

**Tech Stack:** Rust, `rayon`, `crossbeam-channel`, `rusqlite`, `mz-sql` (in-memory catalog).

**Spec:** `docs/superpowers/specs/2026-04-27-parallel-full-typecheck-design.md`

---

## File Structure

**New files:**
- `src/mz-deploy/src/project/compiler/typecheck/executor.rs` — generic DAG ready-queue scheduler. Pure scheduling logic over a closure; no `LocalCatalog` references. Unit-testable with synthetic graphs.
- `src/mz-deploy/src/project/compiler/typecheck/base.rs` — `build_base_catalog()` for phase 1. Seeds builtins, bootstraps namespaces, registers external types, registers non-typechecked objects.

**Modified files:**
- `src/mz-deploy/src/project/compiler/typecheck.rs` — add `run()`, delete `plan()`/`execute()` and incremental machinery.
- `src/mz-deploy/src/project/compiler/typecheck/catalog.rs` — delete the `execute()` orchestrator and dep-planning helpers; keep `LocalCatalog` and SQL helpers.
- `src/mz-deploy/src/project/compiler/build_artifact.rs` — drop `output_fingerprint` column, bump `SCHEMA_VERSION` 5→6, delete `load_typecheck_artifacts` and `StoredTypecheckArtifact`, update `upsert_typecheck_results` signature.
- `src/mz-deploy/src/cli/commands/compile.rs` — switch caller from `plan`/`execute` to `run`.
- `src/mz-deploy/src/cli/commands/test.rs` — same.
- `src/mz-deploy/src/lsp/server.rs` — same.

---

## Task 1: Add executor module skeleton with public types

**Files:**
- Create: `src/mz-deploy/src/project/compiler/typecheck/executor.rs`
- Modify: `src/mz-deploy/src/project/compiler/typecheck.rs` (add `mod executor;` declaration)

- [ ] **Step 1: Write the failing test**

Create `src/mz-deploy/src/project/compiler/typecheck/executor.rs`:

```rust
//! Ready-queue DAG executor for parallel typechecking.
//!
//! Generic over a value type `T` produced by each node and a work closure that
//! validates one node given its direct dependencies' results. The scheduler
//! itself has no `LocalCatalog` knowledge — typecheck-specific work is supplied
//! by the caller.

use crate::project::compiler::typecheck::ObjectTypeCheckError;
use crate::project::ir::object_id::ObjectId;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Reason a node did not produce a successful result.
#[derive(Debug)]
pub(super) enum NodeFailure {
    /// The node's own validation failed.
    Failed(ObjectTypeCheckError),
    /// An upstream node (direct dependency) did not produce a successful result.
    Blocked(ObjectId),
}

/// Final outcome for one node after the DAG executor runs.
#[derive(Debug)]
pub(super) enum NodeOutcome<T> {
    Ok(Arc<T>),
    Err(NodeFailure),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_outcome_types_compile() {
        // Sanity: enums are public to the typecheck module and parameterize correctly.
        let _: NodeOutcome<i32> = NodeOutcome::Ok(Arc::new(7));
        let _: NodeOutcome<i32> = NodeOutcome::Err(NodeFailure::Blocked(ObjectId {
            database: "d".into(),
            schema: "s".into(),
            object: "o".into(),
        }));
    }
}
```

Add the module declaration to `src/mz-deploy/src/project/compiler/typecheck.rs` near the existing `mod catalog;` declaration:

```rust
mod catalog;
mod executor;
```

- [ ] **Step 2: Run test to verify it compiles**

Run: `cargo test -p mz-deploy --lib project::compiler::typecheck::executor:: -- --nocapture`
Expected: PASS (only the sanity test runs).

- [ ] **Step 3: Commit**

```bash
git add src/mz-deploy/src/project/compiler/typecheck/executor.rs \
        src/mz-deploy/src/project/compiler/typecheck.rs
git commit -m "feat(mz-deploy): add typecheck executor module skeleton"
```

---

## Task 2: Implement run() for the empty graph

**Files:**
- Modify: `src/mz-deploy/src/project/compiler/typecheck/executor.rs`

- [ ] **Step 1: Write the failing test**

Append to the `tests` module in `executor.rs`:

```rust
    #[test]
    fn empty_graph_returns_empty() {
        let outcomes = run::<i32, _>(
            Vec::<ObjectId>::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            |_id, _deps| -> Result<i32, ObjectTypeCheckError> {
                panic!("work closure must not be called for empty graph")
            },
        );
        assert!(outcomes.is_empty());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p mz-deploy --lib project::compiler::typecheck::executor::tests::empty_graph_returns_empty`
Expected: FAIL — `run` is not defined.

- [ ] **Step 3: Implement `run()` for the empty case**

Add to `executor.rs` (above the `tests` module):

```rust
/// Run the DAG executor over `nodes`.
///
/// `direct_deps` maps each node ID to the IDs of its direct dependencies that
/// are *also nodes* (deps satisfied by external column maps must be excluded
/// before calling this function).
///
/// `dependents` maps each node ID to the IDs of nodes that directly depend on
/// it (the inverse of `direct_deps`).
///
/// `work` is invoked per-node with the node's ID and a map of dep ID → dep
/// result; it returns either the node's produced value or a typecheck error.
pub(super) fn run<T, F>(
    nodes: Vec<ObjectId>,
    direct_deps: BTreeMap<ObjectId, Vec<ObjectId>>,
    dependents: BTreeMap<ObjectId, Vec<ObjectId>>,
    work: F,
) -> BTreeMap<ObjectId, NodeOutcome<T>>
where
    T: Send + Sync + 'static,
    F: Fn(&ObjectId, &BTreeMap<ObjectId, Arc<T>>) -> Result<T, ObjectTypeCheckError>
        + Send
        + Sync,
{
    if nodes.is_empty() {
        return BTreeMap::new();
    }
    // Real implementation arrives in the next task.
    let _ = (direct_deps, dependents, work);
    unreachable!("non-empty graph not yet implemented")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p mz-deploy --lib project::compiler::typecheck::executor::tests::empty_graph_returns_empty`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/mz-deploy/src/project/compiler/typecheck/executor.rs
git commit -m "feat(mz-deploy): add typecheck executor empty-graph case"
```

---

## Task 3: Implement bookkeeping construction and independent leaves

**Files:**
- Modify: `src/mz-deploy/src/project/compiler/typecheck/executor.rs`

- [ ] **Step 1: Write the failing test**

Append to `tests`:

```rust
    fn id(name: &str) -> ObjectId {
        ObjectId {
            database: "d".into(),
            schema: "s".into(),
            object: name.into(),
        }
    }

    #[test]
    fn independent_leaves_run_to_completion() {
        let nodes = vec![id("a"), id("b"), id("c"), id("d")];
        let direct_deps: BTreeMap<ObjectId, Vec<ObjectId>> = nodes
            .iter()
            .map(|id| (id.clone(), Vec::new()))
            .collect();
        let dependents: BTreeMap<ObjectId, Vec<ObjectId>> = nodes
            .iter()
            .map(|id| (id.clone(), Vec::new()))
            .collect();

        let outcomes = run::<String, _>(nodes.clone(), direct_deps, dependents, |id, _deps| {
            Ok(id.object.clone())
        });

        assert_eq!(outcomes.len(), 4);
        for id in &nodes {
            match outcomes.get(id) {
                Some(NodeOutcome::Ok(v)) => assert_eq!(v.as_ref(), &id.object),
                other => panic!("unexpected outcome for {id:?}: {other:?}"),
            }
        }
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p mz-deploy --lib project::compiler::typecheck::executor::tests::independent_leaves_run_to_completion`
Expected: FAIL with `unreachable!` panic.

- [ ] **Step 3: Implement bookkeeping and the worker loop**

Replace the body of `run()` in `executor.rs`:

```rust
use crossbeam_channel::{unbounded, RecvTimeoutError};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

struct NodeBookkeeping<T> {
    direct_deps: Vec<ObjectId>,
    dependents: Vec<ObjectId>,
    remaining_deps: AtomicUsize,
    result: OnceLock<Result<Arc<T>, NodeFailure>>,
}

pub(super) fn run<T, F>(
    nodes: Vec<ObjectId>,
    mut direct_deps: BTreeMap<ObjectId, Vec<ObjectId>>,
    mut dependents: BTreeMap<ObjectId, Vec<ObjectId>>,
    work: F,
) -> BTreeMap<ObjectId, NodeOutcome<T>>
where
    T: Send + Sync + 'static,
    F: Fn(&ObjectId, &BTreeMap<ObjectId, Arc<T>>) -> Result<T, ObjectTypeCheckError>
        + Send
        + Sync,
{
    if nodes.is_empty() {
        return BTreeMap::new();
    }

    // Build per-node bookkeeping in an Arc-wrapped HashMap so workers can
    // resolve dep slots and dependent slots through shared lookups.
    let bookkeeping: HashMap<ObjectId, Arc<NodeBookkeeping<T>>> = nodes
        .iter()
        .map(|node_id| {
            let deps = direct_deps.remove(node_id).unwrap_or_default();
            let deps_count = deps.len();
            let downstream = dependents.remove(node_id).unwrap_or_default();
            (
                node_id.clone(),
                Arc::new(NodeBookkeeping {
                    direct_deps: deps,
                    dependents: downstream,
                    remaining_deps: AtomicUsize::new(deps_count),
                    result: OnceLock::new(),
                }),
            )
        })
        .collect();
    let bookkeeping = Arc::new(bookkeeping);

    let total = bookkeeping.len();
    let completed = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = unbounded::<ObjectId>();

    // Seed the queue with nodes that already have zero remaining deps.
    for (node_id, bk) in bookkeeping.iter() {
        if bk.remaining_deps.load(Ordering::Acquire) == 0 {
            tx.send(node_id.clone()).expect("channel open");
        }
    }

    rayon::scope(|scope| {
        let worker_count = rayon::current_num_threads().max(1);
        for _ in 0..worker_count {
            let rx = rx.clone();
            let tx = tx.clone();
            let bookkeeping = Arc::clone(&bookkeeping);
            let completed = Arc::clone(&completed);
            let work = &work;
            scope.spawn(move |_| {
                worker_loop(rx, tx, bookkeeping, completed, total, work);
            });
        }
    });

    // Materialize outcomes in the caller's preferred order (insertion order of
    // `nodes`).
    let mut outcomes = BTreeMap::new();
    for node_id in nodes {
        let bk = bookkeeping
            .get(&node_id)
            .expect("bookkeeping entry exists for every node");
        let outcome = match bk
            .result
            .get()
            .expect("every node's result must be set before run() returns")
        {
            Ok(value) => NodeOutcome::Ok(Arc::clone(value)),
            Err(NodeFailure::Failed(err)) => {
                NodeOutcome::Err(NodeFailure::Failed(clone_typecheck_error(err)))
            }
            Err(NodeFailure::Blocked(id)) => NodeOutcome::Err(NodeFailure::Blocked(id.clone())),
        };
        outcomes.insert(node_id, outcome);
    }
    outcomes
}

fn worker_loop<T, F>(
    rx: crossbeam_channel::Receiver<ObjectId>,
    tx: crossbeam_channel::Sender<ObjectId>,
    bookkeeping: Arc<HashMap<ObjectId, Arc<NodeBookkeeping<T>>>>,
    completed: Arc<AtomicUsize>,
    total: usize,
    work: &F,
) where
    T: Send + Sync + 'static,
    F: Fn(&ObjectId, &BTreeMap<ObjectId, Arc<T>>) -> Result<T, ObjectTypeCheckError>
        + Send
        + Sync,
{
    while completed.load(Ordering::Acquire) < total {
        let node_id = match rx.recv_timeout(Duration::from_millis(10)) {
            Ok(id) => id,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        };
        let bk = bookkeeping
            .get(&node_id)
            .expect("scheduled node has a bookkeeping entry")
            .clone();

        // Gather direct dep results.
        let mut dep_results: BTreeMap<ObjectId, Arc<T>> = BTreeMap::new();
        let mut blocked_by: Option<ObjectId> = None;
        for dep_id in &bk.direct_deps {
            let dep_bk = bookkeeping
                .get(dep_id)
                .expect("dep has a bookkeeping entry");
            match dep_bk
                .result
                .get()
                .expect("dep result set before dependent runs")
            {
                Ok(value) => {
                    dep_results.insert(dep_id.clone(), Arc::clone(value));
                }
                Err(_) => {
                    blocked_by = Some(dep_id.clone());
                    break;
                }
            }
        }

        let outcome: Result<Arc<T>, NodeFailure> = if let Some(dep_id) = blocked_by {
            Err(NodeFailure::Blocked(dep_id))
        } else {
            match work(&node_id, &dep_results) {
                Ok(value) => Ok(Arc::new(value)),
                Err(err) => Err(NodeFailure::Failed(err)),
            }
        };

        bk.result
            .set(outcome)
            .ok()
            .expect("result slot is filled exactly once");
        completed.fetch_add(1, Ordering::AcqRel);

        for dependent_id in &bk.dependents {
            let dep_bk = bookkeeping
                .get(dependent_id)
                .expect("dependent has a bookkeeping entry");
            let prev = dep_bk.remaining_deps.fetch_sub(1, Ordering::AcqRel);
            debug_assert!(prev >= 1, "remaining_deps underflow for {dependent_id:?}");
            if prev == 1 {
                tx.send(dependent_id.clone()).expect("channel open");
            }
        }
    }
}

/// `ObjectTypeCheckError` does not implement `Clone`, but the executor needs to
/// re-emit the typecheck error in its outcome map for the original node. We
/// shallow-copy the public fields, which is enough for downstream rendering.
fn clone_typecheck_error(err: &ObjectTypeCheckError) -> ObjectTypeCheckError {
    ObjectTypeCheckError {
        object_id: err.object_id.clone(),
        file_path: err.file_path.clone(),
        sql_statement: err.sql_statement.clone(),
        error_message: err.error_message.clone(),
        detail: err.detail.clone(),
        hint: err.hint.clone(),
    }
}
```

The `crossbeam-channel` crate is already in the workspace via `crossbeam` (verify by inspecting `Cargo.toml`). If `crossbeam-channel` is not directly listed in `src/mz-deploy/Cargo.toml`, add it under `[dependencies]`:

```toml
crossbeam-channel = { workspace = true }
```

Confirm via `cargo check -p mz-deploy`.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p mz-deploy --lib project::compiler::typecheck::executor::tests::independent_leaves_run_to_completion`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/mz-deploy/src/project/compiler/typecheck/executor.rs \
        src/mz-deploy/Cargo.toml
git commit -m "feat(mz-deploy): implement typecheck executor scheduler core"
```

---

## Task 4: Linear chain test

**Files:**
- Modify: `src/mz-deploy/src/project/compiler/typecheck/executor.rs`

- [ ] **Step 1: Write the failing test**

Append to `tests`:

```rust
    #[test]
    fn linear_chain_threads_results() {
        // a -> b -> c
        let a = id("a");
        let b = id("b");
        let c = id("c");
        let nodes = vec![a.clone(), b.clone(), c.clone()];
        let direct_deps: BTreeMap<ObjectId, Vec<ObjectId>> = vec![
            (a.clone(), vec![]),
            (b.clone(), vec![a.clone()]),
            (c.clone(), vec![b.clone()]),
        ]
        .into_iter()
        .collect();
        let dependents: BTreeMap<ObjectId, Vec<ObjectId>> = vec![
            (a.clone(), vec![b.clone()]),
            (b.clone(), vec![c.clone()]),
            (c.clone(), vec![]),
        ]
        .into_iter()
        .collect();

        let outcomes = run::<u64, _>(nodes, direct_deps, dependents, |id, deps| {
            // Each node returns 1 + sum of dep values; chain produces 1, 2, 3.
            let upstream: u64 = deps.values().map(|v| **v).sum();
            Ok(1 + upstream)
        });

        let unwrap_ok = |id: &ObjectId| -> u64 {
            match outcomes.get(id).expect("outcome for id") {
                NodeOutcome::Ok(v) => **v,
                NodeOutcome::Err(e) => panic!("unexpected err for {id:?}: {e:?}"),
            }
        };
        assert_eq!(unwrap_ok(&a), 1);
        assert_eq!(unwrap_ok(&b), 2);
        assert_eq!(unwrap_ok(&c), 3);
    }
```

- [ ] **Step 2: Run test to verify it passes**

Run: `cargo test -p mz-deploy --lib project::compiler::typecheck::executor::tests::linear_chain_threads_results`
Expected: PASS — the existing implementation already supports linear chains.

- [ ] **Step 3: Commit**

```bash
git add src/mz-deploy/src/project/compiler/typecheck/executor.rs
git commit -m "test(mz-deploy): cover linear chain in typecheck executor"
```

---

## Task 5: Diamond test (concurrency check)

**Files:**
- Modify: `src/mz-deploy/src/project/compiler/typecheck/executor.rs`

- [ ] **Step 1: Write the failing test**

Append to `tests`:

```rust
    use std::sync::atomic::AtomicI32;
    use std::sync::Mutex;

    #[test]
    fn diamond_dispatches_b_and_c_in_parallel() {
        // a -> {b, c} -> d, where b and c park on a barrier so we can
        // verify both are in flight simultaneously.
        let a = id("a");
        let b = id("b");
        let c = id("c");
        let d = id("d");
        let nodes = vec![a.clone(), b.clone(), c.clone(), d.clone()];
        let direct_deps: BTreeMap<ObjectId, Vec<ObjectId>> = vec![
            (a.clone(), vec![]),
            (b.clone(), vec![a.clone()]),
            (c.clone(), vec![a.clone()]),
            (d.clone(), vec![b.clone(), c.clone()]),
        ]
        .into_iter()
        .collect();
        let dependents: BTreeMap<ObjectId, Vec<ObjectId>> = vec![
            (a.clone(), vec![b.clone(), c.clone()]),
            (b.clone(), vec![d.clone()]),
            (c.clone(), vec![d.clone()]),
            (d.clone(), vec![]),
        ]
        .into_iter()
        .collect();

        let inflight = Arc::new(AtomicI32::new(0));
        let max_inflight = Arc::new(AtomicI32::new(0));
        let observed_for_d = Arc::new(Mutex::new(Vec::<ObjectId>::new()));

        let inflight_w = Arc::clone(&inflight);
        let max_inflight_w = Arc::clone(&max_inflight);
        let observed_for_d_w = Arc::clone(&observed_for_d);

        let outcomes = run::<u64, _>(nodes, direct_deps, dependents, move |id, deps| {
            if id.object == "b" || id.object == "c" {
                let now = inflight_w.fetch_add(1, Ordering::AcqRel) + 1;
                max_inflight_w.fetch_max(now, Ordering::AcqRel);
                std::thread::sleep(Duration::from_millis(50));
                inflight_w.fetch_sub(1, Ordering::AcqRel);
            }
            if id.object == "d" {
                let mut keys: Vec<ObjectId> = deps.keys().cloned().collect();
                keys.sort();
                *observed_for_d_w.lock().unwrap() = keys;
            }
            Ok(1u64)
        });

        assert!(matches!(outcomes.get(&a), Some(NodeOutcome::Ok(_))));
        assert!(matches!(outcomes.get(&b), Some(NodeOutcome::Ok(_))));
        assert!(matches!(outcomes.get(&c), Some(NodeOutcome::Ok(_))));
        assert!(matches!(outcomes.get(&d), Some(NodeOutcome::Ok(_))));
        assert_eq!(
            max_inflight.load(Ordering::Acquire),
            2,
            "expected b and c to overlap in flight"
        );
        let observed = observed_for_d.lock().unwrap().clone();
        assert_eq!(observed, vec![b.clone(), c.clone()]);
    }
```

- [ ] **Step 2: Run test to verify it passes**

Run: `cargo test -p mz-deploy --lib project::compiler::typecheck::executor::tests::diamond_dispatches_b_and_c_in_parallel`
Expected: PASS. (Requires rayon thread pool ≥ 2; CI's pool is large enough.)

- [ ] **Step 3: Commit**

```bash
git add src/mz-deploy/src/project/compiler/typecheck/executor.rs
git commit -m "test(mz-deploy): cover diamond parallelism in typecheck executor"
```

---

## Task 6: Failure propagation and isolation

**Files:**
- Modify: `src/mz-deploy/src/project/compiler/typecheck/executor.rs`

- [ ] **Step 1: Write the failing test**

Append to `tests`:

```rust
    use std::path::PathBuf;

    fn fake_typecheck_error(id: &ObjectId, msg: &str) -> ObjectTypeCheckError {
        ObjectTypeCheckError {
            object_id: id.clone(),
            file_path: PathBuf::from("test"),
            sql_statement: String::new(),
            error_message: msg.into(),
            detail: None,
            hint: None,
        }
    }

    #[test]
    fn failure_propagates_to_dependents_and_isolates_other_branches() {
        // Failing branch:  a (FAIL) -> b -> c
        // Healthy branch:  x -> y
        let a = id("a");
        let b = id("b");
        let c = id("c");
        let x = id("x");
        let y = id("y");
        let nodes = vec![a.clone(), b.clone(), c.clone(), x.clone(), y.clone()];

        let direct_deps: BTreeMap<ObjectId, Vec<ObjectId>> = vec![
            (a.clone(), vec![]),
            (b.clone(), vec![a.clone()]),
            (c.clone(), vec![b.clone()]),
            (x.clone(), vec![]),
            (y.clone(), vec![x.clone()]),
        ]
        .into_iter()
        .collect();
        let dependents: BTreeMap<ObjectId, Vec<ObjectId>> = vec![
            (a.clone(), vec![b.clone()]),
            (b.clone(), vec![c.clone()]),
            (c.clone(), vec![]),
            (x.clone(), vec![y.clone()]),
            (y.clone(), vec![]),
        ]
        .into_iter()
        .collect();

        let a_for_closure = a.clone();
        let outcomes = run::<u32, _>(nodes, direct_deps, dependents, move |id, _deps| {
            if *id == a_for_closure {
                Err(fake_typecheck_error(id, "boom"))
            } else {
                Ok(1)
            }
        });

        match outcomes.get(&a).unwrap() {
            NodeOutcome::Err(NodeFailure::Failed(err)) => assert_eq!(err.error_message, "boom"),
            other => panic!("expected Failed for a, got {other:?}"),
        }
        match outcomes.get(&b).unwrap() {
            NodeOutcome::Err(NodeFailure::Blocked(blocker)) => assert_eq!(blocker, &a),
            other => panic!("expected Blocked(a) for b, got {other:?}"),
        }
        match outcomes.get(&c).unwrap() {
            NodeOutcome::Err(NodeFailure::Blocked(blocker)) => assert_eq!(blocker, &b),
            other => panic!("expected Blocked(b) for c, got {other:?}"),
        }
        assert!(matches!(outcomes.get(&x), Some(NodeOutcome::Ok(_))));
        assert!(matches!(outcomes.get(&y), Some(NodeOutcome::Ok(_))));
    }
```

- [ ] **Step 2: Run the test**

Run: `cargo test -p mz-deploy --lib project::compiler::typecheck::executor::tests::failure_propagates_to_dependents_and_isolates_other_branches`
Expected: PASS — failure handling is already implemented in Task 3.

- [ ] **Step 3: Commit**

```bash
git add src/mz-deploy/src/project/compiler/typecheck/executor.rs
git commit -m "test(mz-deploy): cover failure propagation in typecheck executor"
```

---

## Task 7: Add `base.rs` module — phase 1 catalog setup

**Files:**
- Create: `src/mz-deploy/src/project/compiler/typecheck/base.rs`
- Modify: `src/mz-deploy/src/project/compiler/typecheck.rs` (add `mod base;` declaration)
- Modify: `src/mz-deploy/src/project/compiler/typecheck/catalog.rs` (relax visibility on helpers used from `base.rs`)

The work in `base.rs` is plumbing of existing `LocalCatalog` operations; unit testing it standalone requires building a `Project`, which is heavy. Coverage comes from the existing CLI integration tests once `run()` is wired in Task 9.

- [ ] **Step 1: Promote helpers in `catalog.rs`**

In `src/mz-deploy/src/project/compiler/typecheck/catalog.rs`, change the visibility of the SQL helpers used by `base.rs`:

```rust
// at the function declarations near the bottom of the file, change from
// `fn` (private) to `pub(super) fn`.
pub(super) fn create_catalog_item_sql(stmt: &ProjectStatement, fqn: &FullyQualifiedName) -> Option<String> { ... }
pub(super) fn relation_desc_to_columns(desc: &RelationDesc) -> BTreeMap<String, ColumnType> { ... }
```

Also expose `CatalogRuntime::open`, `bootstrap_namespaces`, `create_stub_table`, and `create_or_replace_item` to `super`:

```rust
impl CatalogRuntime {
    pub(super) fn open() -> Result<Self, TypeCheckError> { ... }
    pub(super) fn bootstrap_namespaces(&mut self, project: &super::Project, external_types: &super::Types) { ... }
    pub(super) fn create_stub_table(
        &mut self,
        object_id: &ObjectId,
        columns: &BTreeMap<String, ColumnType>,
    ) -> Result<(), TypeCheckError> { ... }
    pub(super) fn create_or_replace_item(
        &mut self,
        object_id: &ObjectId,
        sql: &str,
    ) -> Result<RelationDesc, ObjectTypeCheckError> { ... }
}
```

`pub(super) struct CatalogRuntime` — promote the struct visibility too.

- [ ] **Step 2: Create `base.rs`**

Create `src/mz-deploy/src/project/compiler/typecheck/base.rs`:

```rust
//! Phase 1 of typechecking: build the base catalog.
//!
//! Seeds builtins (via [`CatalogRuntime::open`]), bootstraps namespaces,
//! registers external `types.lock` entries as stub tables, and registers all
//! non-typechecked project objects (tables, sources, sinks, secrets,
//! connections) from their compiled SQL. Returns the catalog wrapped in `Arc`
//! plus a map of column metadata for the registered non-typechecked objects.

use super::catalog::{
    create_catalog_item_sql, relation_desc_to_columns, CatalogRuntime,
};
use super::{
    requires_typecheck, ObjectTypeCheckError, TypeCheckError, TypeCheckErrors,
};
use crate::project::ir::compiled::FullyQualifiedName;
use crate::project::ir::graph::Project;
use crate::project::ir::object_id::ObjectId;
use crate::types::{ColumnType, Types};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Result of phase 1.
pub(super) struct BaseCatalog {
    pub(super) catalog: Arc<CatalogRuntime>,
    /// Column maps for objects that don't undergo typechecking but whose
    /// schemas downstream views depend on (tables, sources, etc., plus
    /// external `types.lock` entries).
    pub(super) base_columns: BTreeMap<ObjectId, BTreeMap<String, ColumnType>>,
}

/// Build the base catalog. Errors from registering non-typechecked objects are
/// accumulated; if any are present after this phase, the caller should abort
/// before running phase 2.
pub(super) fn build_base_catalog(
    project: &Project,
    external_types: &Types,
) -> Result<BaseCatalog, TypeCheckError> {
    let mut runtime = CatalogRuntime::open()?;
    runtime.bootstrap_namespaces(project, external_types);

    // Stub external types.lock entries.
    for (fqn, columns) in &external_types.tables {
        let Some(object_id) = parse_external_fqn(fqn) else {
            // Malformed FQN in types.lock; skip silently to match historic behavior
            // of bootstrap_namespaces.
            continue;
        };
        runtime.create_stub_table(&object_id, columns)?;
    }

    // Register every non-typechecked project object.
    let mut base_columns: BTreeMap<ObjectId, BTreeMap<String, ColumnType>> = BTreeMap::new();
    let mut errors: Vec<ObjectTypeCheckError> = Vec::new();
    for db_obj in project.iter_objects() {
        if requires_typecheck(&db_obj.stmt) {
            continue;
        }
        let object_id = ObjectId {
            database: db_obj.id.database.clone(),
            schema: db_obj.id.schema.clone(),
            object: db_obj.id.object.clone(),
        };
        let fqn: FullyQualifiedName = object_id.clone().into();
        let Some(sql) = create_catalog_item_sql(&db_obj.stmt, &fqn) else {
            continue;
        };
        match runtime.create_or_replace_item(&object_id, &sql) {
            Ok(desc) => {
                base_columns.insert(object_id, relation_desc_to_columns(&desc));
            }
            Err(err) => errors.push(err),
        }
    }

    if !errors.is_empty() {
        return Err(TypeCheckError::Multiple(TypeCheckErrors { errors }));
    }

    Ok(BaseCatalog {
        catalog: Arc::new(runtime),
        base_columns,
    })
}

fn parse_external_fqn(fqn: &str) -> Option<ObjectId> {
    let mut parts = fqn.splitn(3, '.');
    let database = parts.next()?.to_string();
    let schema = parts.next()?.to_string();
    let object = parts.next()?.to_string();
    Some(ObjectId {
        database,
        schema,
        object,
    })
}
```

- [ ] **Step 3: Add `mod base;` declaration**

In `src/mz-deploy/src/project/compiler/typecheck.rs`, add the new module:

```rust
mod base;
mod catalog;
mod executor;
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo check -p mz-deploy`
Expected: success (warnings about unused functions in `base.rs` are OK at this stage; the next task wires them up).

- [ ] **Step 5: Commit**

```bash
git add src/mz-deploy/src/project/compiler/typecheck/base.rs \
        src/mz-deploy/src/project/compiler/typecheck/catalog.rs \
        src/mz-deploy/src/project/compiler/typecheck.rs
git commit -m "feat(mz-deploy): add typecheck base-catalog module"
```

---

## Task 8: Make `CatalogRuntime` cloneable

**Files:**
- Modify: `src/mz-deploy/src/project/compiler/typecheck/catalog.rs`

The executor's per-task closure does `(*base_catalog).clone()` to obtain a private mutable catalog. `CatalogRuntime` currently derives `Debug` but not `Clone`. `LocalCatalog` already derives `Clone` (verify) — wrapping it in a derived `Clone` impl on `CatalogRuntime` makes the per-task copy trivial.

- [ ] **Step 1: Inspect `LocalCatalog` for `Clone`**

Open `src/mz-deploy/src/project/compiler/typecheck/catalog.rs` and locate the `LocalCatalog` struct (around line 860). All its fields are owned `BTreeMap`/`HashMap`/`Vec`/primitive types whose subtypes already derive `Clone`. Verify `#[derive(Debug, Clone)]` on `LocalCatalog`. If `Clone` is missing, add it. If any nested type lacks `Clone`, add it there too — they should already (see derives on `LocalDatabase`, `LocalSchema`, etc. around lines 510–700).

- [ ] **Step 2: Add `Clone` to `CatalogRuntime`**

In `catalog.rs`, change:

```rust
#[derive(Debug)]
pub(super) struct CatalogRuntime {
    catalog: LocalCatalog,
}
```

to:

```rust
#[derive(Debug, Clone)]
pub(super) struct CatalogRuntime {
    catalog: LocalCatalog,
}
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p mz-deploy`
Expected: success.

- [ ] **Step 4: Commit**

```bash
git add src/mz-deploy/src/project/compiler/typecheck/catalog.rs
git commit -m "feat(mz-deploy): make CatalogRuntime cloneable for parallel tasks"
```

---

## Task 9: Add `typecheck::run()` function

**Files:**
- Modify: `src/mz-deploy/src/project/compiler/typecheck.rs`

Add `run()` *alongside* the existing `plan()` and `execute()` so the file still compiles. Old code is removed in a later task once callers have switched.

- [ ] **Step 1: Write the failing test**

Append to `src/mz-deploy/src/project/compiler/typecheck.rs` (near the bottom, before any existing `tests` module if present, otherwise under a new `#[cfg(test)] mod tests`):

```rust
#[cfg(test)]
mod run_tests {
    use super::*;
    use crate::project::compiler::compile_sync;
    use std::collections::BTreeMap;
    use std::fs;
    use tempfile::tempdir;

    fn write_sql(root: &std::path::Path, rel: &str, sql: &str) {
        let path = root.join(rel);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, sql).unwrap();
    }

    #[test]
    fn run_typechecks_simple_view_and_persists_columns() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        write_sql(
            root,
            "models/materialize/public/t1.sql",
            "CREATE TABLE t1 (a int)",
        );
        write_sql(
            root,
            "models/materialize/public/v1.sql",
            "CREATE VIEW v1 AS SELECT a FROM materialize.public.t1",
        );

        let project = compile_sync(root, "default", None, &BTreeMap::new()).unwrap();
        let merged = run(root, "default", None, &BTreeMap::new(), &project, Types::default()).unwrap();

        // The view is in the merged Types map.
        assert!(merged.tables.contains_key("materialize.public.v1"));
        // The table is too (from base_columns).
        assert!(merged.tables.contains_key("materialize.public.t1"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p mz-deploy --lib project::compiler::typecheck::run_tests::run_typechecks_simple_view_and_persists_columns`
Expected: FAIL — `run` is not defined.

- [ ] **Step 3: Implement `run()`**

In `src/mz-deploy/src/project/compiler/typecheck.rs`, add the new function. Keep the existing `plan()`, `execute()`, and supporting types intact for now (they're removed in Task 13).

```rust
use base::BaseCatalog;
use catalog::{create_catalog_item_sql, relation_desc_to_columns};
use executor::{NodeFailure, NodeOutcome};
use std::sync::Arc;

/// Full-typecheck entrypoint. Replaces the old `plan()`/`execute()` pair.
///
/// Runs three phases:
///
/// 1. Build the base catalog (serial): seeds builtins, namespaces, external
///    types, and all non-typechecked project objects.
/// 2. Run the DAG executor (parallel): each view/MV is a node; tasks fire as
///    soon as their dependencies have produced column maps.
/// 3. Persist successful outcomes to SQLite. Failed and blocked objects keep
///    their last successful row in the cache.
///
/// Returns the merged `Types` covering newly-validated columns, base columns
/// (tables/sources/etc.), and external `types.lock` entries.
pub fn run(
    directory: &Path,
    profile: &str,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
    project: &Project,
    external_types: Types,
) -> Result<Types, TypeCheckError> {
    // Phase 1.
    let BaseCatalog {
        catalog: base_catalog,
        base_columns,
    } = base::build_base_catalog(project, &external_types)?;

    // Identify the typecheck-eligible nodes (views and materialized views).
    let sorted = project.get_sorted_objects()?;
    let mut node_ids: Vec<ObjectId> = Vec::new();
    let mut typed_objects: BTreeMap<ObjectId, &crate::project::ir::compiled::DatabaseObject> =
        BTreeMap::new();
    for (object_id, db_obj) in &sorted {
        if !requires_typecheck(&db_obj.stmt) {
            continue;
        }
        node_ids.push(object_id.clone());
        typed_objects.insert(object_id.clone(), *db_obj);
    }
    let node_set: BTreeSet<ObjectId> = node_ids.iter().cloned().collect();

    // Build direct_deps / dependents restricted to view/MV nodes (other deps
    // are pre-resolved through base_columns).
    let mut direct_deps: BTreeMap<ObjectId, Vec<ObjectId>> = BTreeMap::new();
    let mut dependents: BTreeMap<ObjectId, Vec<ObjectId>> = BTreeMap::new();
    for node_id in &node_ids {
        let deps = project
            .dependency_graph
            .get(node_id)
            .cloned()
            .unwrap_or_default();
        let node_only_deps: Vec<ObjectId> = deps
            .iter()
            .filter(|d| node_set.contains(d))
            .cloned()
            .collect();
        for dep_id in &node_only_deps {
            dependents
                .entry(dep_id.clone())
                .or_default()
                .push(node_id.clone());
        }
        direct_deps.insert(node_id.clone(), node_only_deps);
        dependents.entry(node_id.clone()).or_default(); // ensure key exists
    }

    // Phase 2.
    let typed_objects = Arc::new(typed_objects);
    let base_columns_arc = Arc::new(base_columns);
    let outcomes = {
        let typed_objects = Arc::clone(&typed_objects);
        let base_columns = Arc::clone(&base_columns_arc);
        let base_catalog = Arc::clone(&base_catalog);
        executor::run::<BTreeMap<String, ColumnType>, _>(
            node_ids.clone(),
            direct_deps,
            dependents,
            move |node_id, dep_results| {
                let db_obj = typed_objects
                    .get(node_id)
                    .expect("typed_object exists for every scheduled node");
                let mut runtime = (*base_catalog).clone();
                // Stub view/MV deps from upstream task results.
                for (dep_id, columns) in dep_results {
                    runtime
                        .create_stub_table(dep_id, columns.as_ref())
                        .map_err(|err| match err {
                            TypeCheckError::TypeCheckFailed(e) => e,
                            other => ObjectTypeCheckError {
                                object_id: dep_id.clone(),
                                file_path: db_obj.path.clone(),
                                sql_statement: String::new(),
                                error_message: format!("failed to stub dependency: {other}"),
                                detail: None,
                                hint: None,
                            },
                        })?;
                }
                // Stub base-column deps (tables, sources, externals) we depend on.
                if let Some(deps) = project.dependency_graph.get(node_id) {
                    for dep_id in deps {
                        if dep_results.contains_key(dep_id) {
                            continue;
                        }
                        let Some(columns) = base_columns.get(dep_id) else {
                            continue;
                        };
                        runtime
                            .create_stub_table(dep_id, columns)
                            .map_err(|err| match err {
                                TypeCheckError::TypeCheckFailed(e) => e,
                                other => ObjectTypeCheckError {
                                    object_id: dep_id.clone(),
                                    file_path: db_obj.path.clone(),
                                    sql_statement: String::new(),
                                    error_message: format!(
                                        "failed to stub base dependency: {other}"
                                    ),
                                    detail: None,
                                    hint: None,
                                },
                            })?;
                    }
                }
                let fqn: crate::project::ir::compiled::FullyQualifiedName =
                    node_id.clone().into();
                let sql = create_catalog_item_sql(&db_obj.stmt, &fqn).ok_or_else(|| {
                    ObjectTypeCheckError {
                        object_id: node_id.clone(),
                        file_path: db_obj.path.clone(),
                        sql_statement: String::new(),
                        error_message: "internal: failed to render catalog SQL".into(),
                        detail: None,
                        hint: None,
                    }
                })?;
                let desc = runtime.create_or_replace_item(node_id, &sql)?;
                Ok(relation_desc_to_columns(&desc))
            },
        )
    };

    // Phase 3.
    let mut errors: Vec<ObjectTypeCheckError> = Vec::new();
    let mut upsert_rows: Vec<(
        String,
        String,
        String,
        String,
        BTreeMap<String, ColumnType>,
    )> = Vec::new();
    let mut merged_tables: BTreeMap<String, BTreeMap<String, ColumnType>> = BTreeMap::new();
    let mut merged_kinds: BTreeMap<String, ObjectKind> = BTreeMap::new();

    // Seed merged maps from base_columns (tables/sources/etc.).
    for (id, columns) in base_columns_arc.iter() {
        let key = id.to_string();
        merged_tables.insert(key.clone(), columns.clone());
        if let Some(db_obj) = project.iter_objects().find(|o| {
            o.id.database == id.database
                && o.id.schema == id.schema
                && o.id.object == id.object
        }) {
            merged_kinds.insert(key, object_kind_for_stmt(&db_obj.stmt));
        }
    }
    // Seed from external types.lock so callers see the full surface.
    for (fqn, columns) in &external_types.tables {
        merged_tables.insert(fqn.clone(), columns.clone());
        if let Some(kind) = external_types.kinds.get(fqn) {
            merged_kinds.insert(fqn.clone(), *kind);
        }
    }

    for node_id in &node_ids {
        let Some(outcome) = outcomes.get(node_id) else {
            continue;
        };
        match outcome {
            NodeOutcome::Ok(columns) => {
                let db_obj = typed_objects
                    .get(node_id)
                    .expect("typed_object exists for outcome");
                let key = node_id.to_string();
                let kind = object_kind_for_stmt(&db_obj.stmt);
                let semantic_fingerprint = compute_semantic_fingerprint(db_obj);
                merged_tables.insert(key.clone(), columns.as_ref().clone());
                merged_kinds.insert(key.clone(), kind);
                upsert_rows.push((
                    key,
                    semantic_fingerprint,
                    String::new(), // output_fingerprint placeholder, removed in Task 14
                    kind.as_str().to_string(),
                    columns.as_ref().clone(),
                ));
            }
            NodeOutcome::Err(NodeFailure::Failed(err)) => {
                errors.push(ObjectTypeCheckError {
                    object_id: err.object_id.clone(),
                    file_path: err.file_path.clone(),
                    sql_statement: err.sql_statement.clone(),
                    error_message: err.error_message.clone(),
                    detail: err.detail.clone(),
                    hint: err.hint.clone(),
                });
            }
            NodeOutcome::Err(NodeFailure::Blocked(blocker)) => {
                verbose!(
                    "Skipping {}: blocked by upstream error in {}",
                    node_id,
                    blocker
                );
            }
        }
    }

    // Persist successful outcomes; preserve the last successful row for objects
    // that failed or were blocked in this run by *not* including them in the
    // upsert. The keep-set passed to prune is every typecheck-eligible object
    // currently in the project.
    let mut db = BuildArtifact::open(directory, profile, profile_suffix, variables)
        .map_err(TypesError::from)?;
    let row_refs: Vec<(String, String, String, String, &BTreeMap<String, ColumnType>)> =
        upsert_rows
            .iter()
            .map(|(k, sf, of, kind, cols)| (k.clone(), sf.clone(), of.clone(), kind.clone(), cols))
            .collect();
    db.upsert_typecheck_results(&row_refs)
        .map_err(TypesError::from)?;
    let keep: BTreeSet<String> = node_ids.iter().map(|id| id.to_string()).collect();
    db.prune_typecheck_results(&keep)
        .map_err(TypesError::from)?;

    if !errors.is_empty() {
        return Err(TypeCheckError::Multiple(TypeCheckErrors { errors }));
    }

    Ok(Types {
        version: 1,
        tables: merged_tables,
        kinds: merged_kinds,
        comments: BTreeMap::new(),
    })
}
```

- [ ] **Step 4: Run the test**

Run: `cargo test -p mz-deploy --lib project::compiler::typecheck::run_tests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/mz-deploy/src/project/compiler/typecheck.rs
git commit -m "feat(mz-deploy): add typecheck::run() with parallel DAG executor"
```

---

## Task 10: Switch `cli/commands/compile.rs` caller

**Files:**
- Modify: `src/mz-deploy/src/cli/commands/compile.rs`

- [ ] **Step 1: Replace the body of `typecheck_project`**

In `src/mz-deploy/src/cli/commands/compile.rs`, locate `fn typecheck_project` (around line 280). Replace the body that calls `typecheck::plan` and `typecheck::execute` with a single `typecheck::run` call:

```rust
fn typecheck_project(
    settings: &Settings,
    planned_project: &Project,
    show_progress: bool,
) -> Result<Option<Duration>, CliError> {
    let directory = &settings.directory;
    use crate::project::compiler::typecheck;

    if show_progress {
        progress::stage_start("Type checking");
    }
    let typecheck_start = Instant::now();

    let external_types = crate::types::load_types_lock(directory).unwrap_or_else(|_| {
        if show_progress {
            progress::info("No types.lock found, assuming no external dependencies");
            progress::info("See SET api = stable for more information");
        }
        crate::types::Types::default()
    });

    typecheck::run(
        directory,
        &settings.profile_name,
        settings.profile_suffix(),
        settings.variables(),
        planned_project,
        external_types,
    )
    .map_err(|e| CliError::Message(format!("type check failed: {}", e)))?;
    timing!("typecheck", typecheck_start.elapsed());

    Ok(Some(typecheck_start.elapsed()))
}
```

- [ ] **Step 2: Verify build and tests**

Run: `cargo check -p mz-deploy && cargo test -p mz-deploy --lib`
Expected: success. (Existing tests in compile.rs don't assert on the old plan/execute path.)

- [ ] **Step 3: Commit**

```bash
git add src/mz-deploy/src/cli/commands/compile.rs
git commit -m "refactor(mz-deploy): switch compile typecheck to parallel run()"
```

---

## Task 11: Switch `cli/commands/test.rs` caller

**Files:**
- Modify: `src/mz-deploy/src/cli/commands/test.rs`

- [ ] **Step 1: Replace the typecheck call**

Locate the typecheck block in `src/mz-deploy/src/cli/commands/test.rs` (around line 818):

```rust
    use crate::project::compiler::typecheck;

    let plan = typecheck::plan(
        ...
    )?;
    if plan.is_up_to_date() {
        ...
    }
    let result = typecheck::execute(
        ...
    );
```

Replace with:

```rust
    use crate::project::compiler::typecheck;

    typecheck::run(
        directory,
        &settings.profile_name,
        settings.profile_suffix(),
        settings.variables(),
        planned_project,
        external_types,
    )
    .map_err(|e| CliError::Message(format!("type check failed: {}", e)))?;
```

(Inspect surrounding code to preserve any progress reporting / timing wrappers; the equivalents from `compile.rs` apply here too.)

- [ ] **Step 2: Verify build and tests**

Run: `cargo check -p mz-deploy && cargo test -p mz-deploy --lib`
Expected: success.

- [ ] **Step 3: Commit**

```bash
git add src/mz-deploy/src/cli/commands/test.rs
git commit -m "refactor(mz-deploy): switch test command typecheck to parallel run()"
```

---

## Task 12: Switch `lsp/server.rs` caller

**Files:**
- Modify: `src/mz-deploy/src/lsp/server.rs`

- [ ] **Step 1: Replace the typecheck call**

Locate the LSP typecheck block (around line 326). Replace:

```rust
let types_lock = types::load_types_lock(&root).unwrap_or_default();
let plan = match project::compiler::typecheck::plan(
    &root,
    &profile,
    profile_suffix.as_deref(),
    &variables,
    &project,
    types_lock,
) {
    Ok(p) => p,
    Err(_) => return,
};

if plan.is_up_to_date() {
    return;
}

let _ = project::compiler::typecheck::execute(
    &project,
    &root,
    &profile,
    profile_suffix.as_deref(),
    &variables,
    plan,
);
```

with:

```rust
let types_lock = types::load_types_lock(&root).unwrap_or_default();
let _ = project::compiler::typecheck::run(
    &root,
    &profile,
    profile_suffix.as_deref(),
    &variables,
    &project,
    types_lock,
);
```

The `is_up_to_date` early return is intentionally gone — the LSP always typechecks on rebuild, per the design.

- [ ] **Step 2: Verify build**

Run: `cargo check -p mz-deploy`
Expected: success.

- [ ] **Step 3: Commit**

```bash
git add src/mz-deploy/src/lsp/server.rs
git commit -m "refactor(mz-deploy/lsp): switch typecheck to parallel run()"
```

---

## Task 13: Delete dead `typecheck.rs` code

**Files:**
- Modify: `src/mz-deploy/src/project/compiler/typecheck.rs`

- [ ] **Step 1: Verify no remaining callers**

Run: `grep -rn "typecheck::plan\|typecheck::execute\|TypecheckPlan\|IncrementalState\|DirtyPropagator" src/mz-deploy/src/`
Expected: matches only inside `typecheck.rs` itself (and possibly its module subtree). If other matches exist, fix the caller before continuing.

- [ ] **Step 2: Delete the dead types and functions**

In `src/mz-deploy/src/project/compiler/typecheck.rs`, remove:

- `pub(crate) struct TypecheckPlan` and its `impl`.
- `struct IncrementalState`.
- `struct CompletedState`.
- `struct DirtyPropagator` and its `impl`.
- `struct DepContext`.
- `enum DepAction` and its `impl`.
- `pub(crate) fn plan()`.
- `pub(crate) fn execute()`.
- `fn plan_dep_creation()`.
- `fn plan_deps_dfs()`.
- `fn write_typecheck_outputs()`.

Keep:

- `enum TypeCheckError` and its `Display`/`Error` impls.
- `struct ObjectTypeCheckError`, `struct TypeCheckErrors`, and their formatting.
- `fn requires_typecheck`, `fn object_kind_for_stmt`.
- `fn compute_semantic_fingerprint`, `struct Sha256Hasher`, `fn fmt_sql_exprs`, `fn fmt_idents` (still used by `run()` to populate `semantic_fingerprint`).
- The `run()` function added in Task 9.
- The `run_tests` module.

- [ ] **Step 3: Verify build and tests**

Run: `cargo check -p mz-deploy && cargo test -p mz-deploy --lib`
Expected: success. Some `unused` warnings on items inside `catalog.rs` that the deleted code referenced are expected — Task 14 cleans those up.

- [ ] **Step 4: Commit**

```bash
git add src/mz-deploy/src/project/compiler/typecheck.rs
git commit -m "refactor(mz-deploy): remove incremental typecheck machinery"
```

---

## Task 14: Delete dead `catalog.rs` code

**Files:**
- Modify: `src/mz-deploy/src/project/compiler/typecheck/catalog.rs`

- [ ] **Step 1: Identify dead items**

The catalog module's `pub(super) fn execute` orchestrator and its helpers `typecheck_incremental`, `ensure_dep_exists` (formerly used during incremental execution) are now unreachable from any caller. Confirm with:

Run: `grep -rn "typecheck_incremental\|ensure_dep_exists\|catalog::execute" src/mz-deploy/src/`
Expected: matches only inside `catalog.rs` itself.

- [ ] **Step 2: Delete the dead orchestrator and helpers**

Remove from `src/mz-deploy/src/project/compiler/typecheck/catalog.rs`:

- `pub(super) fn execute(...)` — the top-level incremental entrypoint.
- `fn typecheck_incremental(...)` — the sequential inner loop.
- `fn ensure_dep_exists(...)` — the dep-creation helper that walked `DepAction`s.

Keep `CatalogRuntime`, `LocalCatalog`, `LocalDatabase`, `LocalSchema`, `LocalRole`, `LocalCluster`, `LocalItem`, `LocalNetworkPolicy` (if present), the `bootstrap_namespaces`, `create_stub_table`, `create_or_replace_item`, `build_error` helpers, all trait impls (`SessionCatalog`, `ExprHumanizer`, `ConnectionResolver`), and the SQL helpers `create_stub_table_sql`, `create_catalog_item_sql`, `relation_desc_to_columns`.

- [ ] **Step 3: Verify build and tests**

Run: `cargo check -p mz-deploy && cargo test -p mz-deploy --lib`
Expected: success.

- [ ] **Step 4: Commit**

```bash
git add src/mz-deploy/src/project/compiler/typecheck/catalog.rs
git commit -m "refactor(mz-deploy): remove incremental typecheck orchestrator"
```

---

## Task 15: Drop `output_fingerprint` from SQLite schema

**Files:**
- Modify: `src/mz-deploy/src/project/compiler/build_artifact.rs`
- Modify: `src/mz-deploy/src/project/compiler/typecheck.rs`

- [ ] **Step 1: Update the schema**

In `src/mz-deploy/src/project/compiler/build_artifact.rs`:

1. Bump the `INSERT OR REPLACE INTO meta(key, value) VALUES ('schema_version', '5');` constant — change `'5'` to `'6'`.

2. In `create_schema()`, change the `typecheck_objects` definition:

```sql
CREATE TABLE IF NOT EXISTS typecheck_objects (
    object_key TEXT PRIMARY KEY,
    semantic_fingerprint TEXT NOT NULL,
    object_kind TEXT NOT NULL
);
```

3. The `if version != Some(SCHEMA_VERSION)` block already drops these tables on mismatch, so no migration logic is needed.

4. Update `SCHEMA_VERSION`:

```rust
const SCHEMA_VERSION: i64 = 6;
```

(Find and update wherever this constant is declared.)

- [ ] **Step 2: Update `upsert_typecheck_results` signature**

Change the function signature and body in `build_artifact.rs`:

```rust
pub(crate) fn upsert_typecheck_results(
    &mut self,
    rows: &[(
        String,
        String,
        String,
        &BTreeMap<String, ColumnType>,
    )],
) -> Result<(), BuildArtifactError> {
    // ...
    let mut upsert_obj = tx
        .prepare(
            "
            INSERT INTO typecheck_objects(object_key, semantic_fingerprint, object_kind)
            VALUES(?1, ?2, ?3)
            ON CONFLICT(object_key) DO UPDATE SET
                semantic_fingerprint = excluded.semantic_fingerprint,
                object_kind = excluded.object_kind
            ",
        )
        ...
    for (key, semantic_fp, kind, columns) in rows {
        upsert_obj.execute(params![key, semantic_fp, kind])
            ...
    }
}
```

Update the per-row `for` loop to drop `output_fp`. The rest of the body (column delete + insert) is unchanged.

- [ ] **Step 3: Update the `run()` callsite**

In `src/mz-deploy/src/project/compiler/typecheck.rs`, update the `upsert_rows` construction:

```rust
let mut upsert_rows: Vec<(
    String,
    String,
    String,
    BTreeMap<String, ColumnType>,
)> = Vec::new();

// ... in the Ok branch:
upsert_rows.push((
    key,
    semantic_fingerprint,
    kind.as_str().to_string(),
    columns.as_ref().clone(),
));

// ... and the row_refs construction:
let row_refs: Vec<(String, String, String, &BTreeMap<String, ColumnType>)> = upsert_rows
    .iter()
    .map(|(k, sf, kind, cols)| (k.clone(), sf.clone(), kind.clone(), cols))
    .collect();
db.upsert_typecheck_results(&row_refs)?;
```

- [ ] **Step 4: Verify build and tests**

Run: `cargo test -p mz-deploy --lib`
Expected: success. Existing build_artifact tests that use `upsert_typecheck_results` need their tuple shape updated — fix any such tests so they pass.

- [ ] **Step 5: Commit**

```bash
git add src/mz-deploy/src/project/compiler/build_artifact.rs \
        src/mz-deploy/src/project/compiler/typecheck.rs
git commit -m "refactor(mz-deploy): drop output_fingerprint from typecheck schema"
```

---

## Task 16: Delete `load_typecheck_artifacts` and `StoredTypecheckArtifact`

**Files:**
- Modify: `src/mz-deploy/src/project/compiler/build_artifact.rs`

- [ ] **Step 1: Verify no remaining callers**

Run: `grep -rn "load_typecheck_artifacts\|StoredTypecheckArtifact" src/mz-deploy/src/`
Expected: matches only inside `build_artifact.rs` itself.

- [ ] **Step 2: Delete the function and the type**

In `src/mz-deploy/src/project/compiler/build_artifact.rs`:

- Remove `pub(crate) fn load_typecheck_artifacts(...)` and its body.
- Remove `pub(crate) struct StoredTypecheckArtifact` and its derives.

- [ ] **Step 3: Verify build and tests**

Run: `cargo check -p mz-deploy && cargo test -p mz-deploy --lib`
Expected: success.

- [ ] **Step 4: Commit**

```bash
git add src/mz-deploy/src/project/compiler/build_artifact.rs
git commit -m "refactor(mz-deploy): remove unused typecheck artifact loader"
```

---

## Task 17: Delete incremental compile tests

**Files:**
- Modify: `src/mz-deploy/src/project/compiler.rs`

- [ ] **Step 1: Delete the two tests**

In `src/mz-deploy/src/project/compiler.rs`, locate the `tests` module (around line 1124) and delete:

- `fn incremental_compile_reuses_cached_objects()`
- `fn incremental_compile_invalidates_changed_object()`

Keep the `write_sql` helper if any remaining test uses it; otherwise delete it as well. If after these deletions the `tests` module has no contents, delete the entire module.

- [ ] **Step 2: Verify build and tests**

Run: `cargo test -p mz-deploy --lib project::compiler::`
Expected: success.

- [ ] **Step 3: Commit**

```bash
git add src/mz-deploy/src/project/compiler.rs
git commit -m "test(mz-deploy): drop incremental compile cache-stat tests"
```

---

## Task 18: Final verification

- [ ] **Step 1: Run the full test suite**

Run: `cargo test -p mz-deploy`
Expected: all tests pass.

- [ ] **Step 2: Lint**

Run: `bin/lint -p mz-deploy` and `cargo clippy -p mz-deploy --all-targets -- -D warnings`
Expected: no errors.

- [ ] **Step 3: Format**

Run: `bin/fmt`
Expected: no diffs.

- [ ] **Step 4: Smoke test against a real project**

If a sample mz-deploy project is available locally, run:

```bash
cd <mz-deploy-project>
cargo run -p mz-deploy -- compile
```

Expected: compile and typecheck succeed, no incremental cache hit/miss messages on the second run (since incremental typecheck is gone), but compile incrementality (object compiler) still reports cache hits.

- [ ] **Step 5: Commit anything pending**

```bash
git status
# If everything is clean, no further commits are needed.
```

---

## Self-Review

**Spec coverage:**

- Public API replaced with `run()`: Tasks 9–12.
- `is_up_to_date()` early return removed for LSP: Task 12.
- Incremental machinery removed: Tasks 13–14.
- `output_fingerprint` column dropped, `SCHEMA_VERSION` bumped 5→6: Task 15.
- `load_typecheck_artifacts`/`StoredTypecheckArtifact` removed: Task 16.
- `incremental_compile_*` tests deleted: Task 17.
- `semantic_fingerprint` column retained, populated by `run()`: Task 9.
- `upsert_typecheck_results` signature simplified: Task 15.
- `prune_typecheck_results` keep-set = all current eligible objects: Task 9.
- Failed/blocked outcomes do not overwrite cached rows: Task 9 (no upsert in those branches).
- Phase 1 base catalog with serial setup of namespaces, externals, non-typechecked objects: Task 7.
- Phase 1 errors abort before phase 2: Task 7 (`build_base_catalog` returns `Err` on accumulated errors).
- Per-task `Arc<CatalogRuntime>` clone: Tasks 8–9.
- Ready-queue executor with `AtomicUsize` counter and `crossbeam_channel`: Task 3.
- DAG executor unit tests for empty/leaves/chain/diamond/failure: Tasks 2–6.
- `BlockedBy` reporting via verbose log: Task 9.

**Placeholder scan:** No "TBD"/"TODO"/"similar to" placeholders. Every code step shows the exact code or exact replacement instructions.

**Type consistency:** `NodeBookkeeping`/`NodeOutcome`/`NodeFailure` defined in Task 1, used consistently in Tasks 3–6 and 9. `BaseCatalog`/`build_base_catalog` defined in Task 7, used in Task 9. `CatalogRuntime: Clone` in Task 8 enables the per-task clone in Task 9. `upsert_typecheck_results` signature change in Task 15 lines up with the placeholder construction in Task 9.
