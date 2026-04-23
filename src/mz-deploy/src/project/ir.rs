//! Core semantic identifiers shared across the compiler.
//!
//! This subsystem owns IR components that are used broadly as stable semantic
//! building blocks. It contains identifiers, compiled project structures, and
//! dependency-aware graph structures whose meaning is independent of a specific
//! compiler subsystem.

pub(crate) mod compiled;
pub(crate) mod graph;
pub(crate) mod infrastructure;
pub(crate) mod object_id;
