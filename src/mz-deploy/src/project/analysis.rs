//! Analyses derived from compiled project state.
//!
//! This subsystem owns computations performed over the compiled project or its
//! dependency graph, including:
//!
//! - deployment snapshots
//! - dirty propagation and incremental deployment planning
//! - dependency extraction and topological traversal
//! - graph-wide deployment validations

pub(crate) mod changeset;
pub(crate) mod deployment_snapshot;
pub(crate) mod deps;
pub(crate) mod graph_validation;
pub(crate) mod topology;
