//! Name resolution and lowering transforms.
//!
//! This subsystem owns transformations that rewrite or interpret SQL names in
//! the context of project compilation:
//!
//! - CTE scope tracking
//! - identifier qualification and normalization
//! - lowering declarative constraints into deployable objects

pub mod constraint;
pub mod cte_scope;
pub mod normalize;
