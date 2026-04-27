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
