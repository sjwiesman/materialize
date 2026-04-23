//! Validation helpers for compiled objects and project setup statements.
//!
//! These helpers implement the object-local and setup-statement rules enforced
//! by the compiler while building compiled project state.
//!
//! ## Validation Categories
//!
//! All validations run during [`super::object_validation`]'s object and project
//! assembly routines.
//! They are independent — each produces its own error list, and all errors
//! are aggregated before returning. No validation depends on the output of
//! another.
//!
//! ## Submodules
//!
//! - [`identifiers`]: Identifier format validation (naming rules, FQN matching)
//! - [`clusters`]: Cluster specification validation for indexes, MVs, sinks, sources
//! - [`references`]: Reference validation for indexes, grants, comments, and constraints
//! - [`constraints`]: Constraint enforcement validation (enforced constraints need `IN CLUSTER`)
//! - [`mod_statements`]: Database and schema mod file statement validation
//! - [`schema_constraints`]: Schema-level structural constraint validation

mod clusters;
mod constraints;
mod identifiers;
mod mod_statements;
mod references;
mod schema_constraints;

pub(super) use clusters::{
    validate_constraint_clusters, validate_index_clusters, validate_mv_cluster,
    validate_sink_cluster, validate_source_cluster,
};
pub(super) use constraints::validate_constraint_enforcement;
pub(crate) use constraints::{validate_constraint_columns, validate_constraint_fk_targets};
pub(super) use identifiers::{validate_fqn_identifiers, validate_ident};
pub(crate) use mod_statements::{validate_database_mod_statements, validate_schema_mod_statements};
pub(super) use references::{
    validate_comment_references, validate_constraint_references, validate_grant_references,
    validate_index_references,
};
pub(crate) use schema_constraints::validate_no_storage_and_computation_in_schema;
