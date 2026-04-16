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

pub mod input;
pub mod parser;
pub mod profile_files;
pub mod variables;
