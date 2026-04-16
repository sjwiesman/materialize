//! Analyses derived from compiled project state.
//!
//! This subsystem owns computations performed over the compiled project or its
//! dependency graph, including:
//!
//! - deployment snapshots
//! - dirty propagation and incremental deployment planning
//! - dependency extraction and topological traversal
//! - graph-wide deployment validations

pub mod changeset;
pub mod deployment_snapshot;
pub mod deps;
pub mod graph_validation;
pub mod topology;
