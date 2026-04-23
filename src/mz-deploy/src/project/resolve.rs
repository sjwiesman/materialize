//! Name resolution and lowering transforms.
//!
//! This subsystem owns transformations that rewrite or interpret SQL names in
//! the context of project compilation:
//!
//! - CTE scope tracking
//! - identifier qualification and normalization
//! - lowering declarative constraints into deployable objects

pub(crate) mod constraint;
pub(crate) mod cte_scope;
pub(crate) mod normalize;
