//! Ready-queue DAG executor for parallel typechecking.
//!
//! Generic over a value type `T` produced by each node and a work closure that
//! validates one node given its direct dependencies' results. The scheduler
//! itself has no `LocalCatalog` knowledge — typecheck-specific work is supplied
//! by the caller.

use crate::project::compiler::typecheck::ObjectTypeCheckError;
use crate::project::ir::object_id::ObjectId;
use crossbeam_channel::{RecvTimeoutError, unbounded};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

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

struct NodeBookkeeping<T> {
    direct_deps: Vec<ObjectId>,
    dependents: Vec<ObjectId>,
    remaining_deps: AtomicUsize,
    result: OnceLock<Result<Arc<T>, NodeFailure>>,
}

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
    mut direct_deps: BTreeMap<ObjectId, Vec<ObjectId>>,
    mut dependents: BTreeMap<ObjectId, Vec<ObjectId>>,
    work: F,
) -> BTreeMap<ObjectId, NodeOutcome<T>>
where
    T: Send + Sync + 'static,
    F: Fn(&ObjectId, &BTreeMap<ObjectId, Arc<T>>) -> Result<T, ObjectTypeCheckError> + Send + Sync,
{
    if nodes.is_empty() {
        return BTreeMap::new();
    }

    // Build per-node bookkeeping in an Arc-wrapped map so workers can
    // resolve dep slots and dependent slots through shared lookups.
    let bookkeeping: BTreeMap<ObjectId, Arc<NodeBookkeeping<T>>> = nodes
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
        if bk.remaining_deps.load(Ordering::Relaxed) == 0 {
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
            Err(NodeFailure::Failed(err)) => NodeOutcome::Err(NodeFailure::Failed(err.clone())),
            Err(NodeFailure::Blocked(id)) => NodeOutcome::Err(NodeFailure::Blocked(id.clone())),
        };
        outcomes.insert(node_id, outcome);
    }
    outcomes
}

fn worker_loop<T, F>(
    rx: crossbeam_channel::Receiver<ObjectId>,
    tx: crossbeam_channel::Sender<ObjectId>,
    bookkeeping: Arc<BTreeMap<ObjectId, Arc<NodeBookkeeping<T>>>>,
    completed: Arc<AtomicUsize>,
    total: usize,
    work: &F,
) where
    T: Send + Sync + 'static,
    F: Fn(&ObjectId, &BTreeMap<ObjectId, Arc<T>>) -> Result<T, ObjectTypeCheckError> + Send + Sync,
{
    while completed.load(Ordering::Acquire) < total {
        let node_id = match rx.recv_timeout(Duration::from_millis(10)) {
            Ok(id) => id,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        };
        let bk = Arc::clone(
            bookkeeping
                .get(&node_id)
                .expect("scheduled node has a bookkeeping entry"),
        );

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

        if bk.result.set(outcome).is_err() {
            panic!(
                "result slot is filled exactly once. Each node is enqueued exactly once via the prev == 1 transition, so set() runs at most once per slot."
            );
        }

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
        // Bump after fan-out so any worker observing `completed == total`
        // has transitively observed every node's fan-out writes (Release/Acquire pair
        // with the loop predicate's load).
        completed.fetch_add(1, Ordering::Release);
    }
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
        let direct_deps: BTreeMap<ObjectId, Vec<ObjectId>> =
            nodes.iter().map(|id| (id.clone(), Vec::new())).collect();
        let dependents: BTreeMap<ObjectId, Vec<ObjectId>> =
            nodes.iter().map(|id| (id.clone(), Vec::new())).collect();

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

        let outcomes = run::<u64, _>(nodes, direct_deps, dependents, |_id, deps| {
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

    #[test]
    fn diamond_dispatches_b_and_c_in_parallel() {
        use std::sync::Mutex;
        use std::sync::atomic::AtomicI32;

        // a -> {b, c} -> d, where b and c park briefly so we can verify both
        // are in flight simultaneously.
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

    fn fake_typecheck_error(id: &ObjectId, msg: &str) -> ObjectTypeCheckError {
        ObjectTypeCheckError {
            object_id: id.clone(),
            file_path: std::path::PathBuf::from("test"),
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
}
