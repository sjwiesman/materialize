//! Project compilation, graph assembly, and deployment analysis.
//!
//! This module defines the compile contract for a Materialize project rooted on
//! disk. The result of compilation is an [`ir::graph::Project`].
//!
//! Compilation has two behavioral layers:
//!
//! 1. **Object compilation** — each logical object is discovered from source
//!    files, parsed, validated, and normalized independently. These object-local
//!    results are the unit of parallelism and the unit of persistent cache reuse.
//! 2. **Graph assembly** — the current object set is assembled into a compiled
//!    project and then into a dependency-aware project graph, where cross-object
//!    constraints and deployment ordering are enforced.
//!
//! The project module is organized by compiler responsibility:
//!
//! - **`compiler`** — compile orchestration, object validation, incremental
//!   caching, and assembly
//! - **`syntax`** — source-file discovery, parsed input structures, parser
//!   integration, profile variants, and variable substitution
//! - **`resolve`** — name qualification, normalization, and lowering transforms
//! - **`analysis`** — dependency extraction, topology, deployment snapshots,
//!   dirty propagation, and graph-wide validations
//! - **`ir`** — semantic identifiers, compiled project IR, and dependency graph IR
//!
//! [`plan_sync()`] is the canonical synchronous compiler entrypoint. It uses the
//! incremental compiler in [`compiler`] to reuse persisted object artifacts
//! across invocations. [`plan()`] is an async wrapper that runs this compile
//! contract on a blocking thread pool.
//!
//! The sibling modules in `analysis/` operate on the assembled project graph to
//! answer deployment questions such as which objects changed, which downstream
//! objects must be restaged, and whether runtime cluster rules are satisfied.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

pub mod analysis;
pub mod ast;
pub mod clusters;
pub mod compiler;
pub mod error;
pub mod ir;
pub mod network_policies;
pub mod resolve;
pub mod roles;
pub mod syntax;

// Re-export commonly used types
pub use ir::graph::ModStatement;

/// A `(database_name, schema_name)` pair identifying a schema within a project.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize)]
pub struct SchemaQualifier {
    pub database: String,
    pub schema: String,
}

impl SchemaQualifier {
    pub fn new(database: String, schema: String) -> Self {
        Self { database, schema }
    }

    /// Collect the distinct `(database, schema)` pairs from a slice of objects.
    pub fn collect_from(objs: &[&ir::graph::DatabaseObject]) -> BTreeSet<Self> {
        objs.iter()
            .map(|obj| Self::new(obj.id.database.clone(), obj.id.schema.clone()))
            .collect()
    }
}

/// Async wrapper around [`plan_sync`] that runs the CPU-bound compiler on a
/// blocking thread pool.
pub async fn plan(
    root: PathBuf,
    profile: String,
    profile_suffix: Option<String>,
    variables: BTreeMap<String, String>,
) -> Result<ir::graph::Project, error::ProjectError> {
    mz_ore::task::spawn_blocking(
        || "project::plan",
        move || plan_sync(root, &profile, profile_suffix.as_deref(), &variables),
    )
    .await
}

/// Compile a project root into a planned deployment representation.
///
/// Behaviorally, this function:
///
/// - discovers project-owned objects and mod statements
/// - reuses any valid persisted object artifacts for the active compile context
/// - recompiles cache misses in parallel
/// - assembles the current typed project and lowers it into a planned project
///
/// The returned plan is defined by the project sources, the active profile
/// configuration, and the compile-time variable bindings. Cached artifacts may
/// accelerate evaluation, but they do not change the result.
pub fn plan_sync<P: AsRef<Path>>(
    root: P,
    profile: &str,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
) -> Result<ir::graph::Project, error::ProjectError> {
    compiler::compile_sync(root, profile, profile_suffix, variables)
}
