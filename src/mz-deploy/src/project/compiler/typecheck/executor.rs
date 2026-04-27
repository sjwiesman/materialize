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
}
