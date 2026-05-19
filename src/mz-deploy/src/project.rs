// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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

pub(crate) mod analysis;
pub(crate) mod ast;
pub(crate) mod clusters;
pub(crate) mod compiler;
pub(crate) mod error;
pub(crate) mod ir;
pub(crate) mod network_policies;
pub(crate) mod resolve;
pub(crate) mod roles;
pub(crate) mod syntax;

// Re-export commonly used types
pub(crate) use ir::graph::ModStatement;

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
            .map(|obj| {
                Self::new(
                    obj.id.expect_database().to_string(),
                    obj.id.schema().to_string(),
                )
            })
            .collect()
    }
}

/// Async wrapper around [`plan_sync`] that runs the CPU-bound compiler on a
/// blocking thread pool.
pub(crate) async fn plan(
    root: PathBuf,
    profile: Option<String>,
    profile_suffix: Option<String>,
    variables: BTreeMap<String, String>,
    fs: crate::fs::FileSystem,
) -> Result<ir::graph::Project, error::ProjectError> {
    mz_ore::task::spawn_blocking(
        || "project::plan",
        move || {
            plan_sync(
                &fs,
                root,
                profile.as_deref(),
                profile_suffix.as_deref(),
                &variables,
            )
        },
    )
    .await
}

#[cfg(test)]
mod plan_tests {
    use super::*;

    /// Overlay content replaces what's on disk: a project whose disk SQL would
    /// fail to parse compiles cleanly when an overlay provides valid SQL for
    /// the same file.
    #[test]
    fn plan_sync_uses_overlay_content() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("project.toml"),
            "[project]\nname = \"t\"\n",
        )
        .unwrap();
        let model_dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&model_dir).unwrap();
        let sql_path = model_dir.join("foo.sql");
        // Disk version is unparseable.
        std::fs::write(&sql_path, "THIS IS NOT VALID SQL").unwrap();

        // Without overlay, planning fails.
        let fs = crate::fs::FileSystem::new();
        assert!(
            plan_sync(&fs, root.path(), None, None, &Default::default()).is_err(),
            "disk-only plan should fail on unparseable SQL"
        );

        // With overlay supplying valid SQL for that path, planning succeeds.
        let mut overlay = BTreeMap::new();
        overlay.insert(sql_path, "CREATE VIEW foo AS SELECT 1 AS x;\n".to_string());
        let fs = crate::fs::FileSystem::with_overlay(overlay);
        let project = plan_sync(&fs, root.path(), None, None, &Default::default())
            .expect("overlay should supply parseable SQL");

        let id = ir::object_id::ObjectId::new(
            "mydb".to_string(),
            "public".to_string(),
            "foo".to_string(),
        );
        assert!(
            project.find_object(&id).is_some(),
            "overlay-defined view should be present in planned project"
        );
    }
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
pub(crate) fn plan_sync<P: AsRef<Path>>(
    fs: &crate::fs::FileSystem,
    root: P,
    profile: Option<&str>,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
) -> Result<ir::graph::Project, error::ProjectError> {
    compiler::compile_sync(fs, root, profile, profile_suffix, variables)
}
