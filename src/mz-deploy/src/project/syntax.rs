//! Source-facing compiler inputs.
//!
//! This subsystem owns behavior defined directly by project source files:
//!
//! - directory and file discovery
//! - profile-specific file variants
//! - parsed input structures
//! - variable substitution
//! - SQL parsing with source locations
//!
//! These modules describe how bytes on disk become structured compiler inputs.

pub(crate) mod input;
pub(crate) mod parser;
pub(crate) mod profile_files;
pub(crate) mod variables;
