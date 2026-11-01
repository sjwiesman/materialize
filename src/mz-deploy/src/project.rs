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
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
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

    /// With a profile_suffix active, an object whose CREATE statement qualifies
    /// itself with the unsuffixed directory name still validates: the user
    /// writes the canonical database name in their SQL and the suffix is
    /// applied during assembly. The compiled statement and its ObjectId both
    /// carry the suffixed database name.
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn plan_sync_with_profile_suffix_compiles_unsuffixed_qualified_name() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("project.toml"), "").unwrap();
        let schema_dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&schema_dir).unwrap();
        // User writes the unsuffixed directory-derived database name.
        std::fs::write(
            schema_dir.join("foo.sql"),
            "CREATE VIEW mydb.public.foo AS SELECT 1 AS x;\n",
        )
        .unwrap();

        let fs = crate::fs::FileSystem::new();
        let project = plan_sync(&fs, root.path(), None, Some("_dev"), &Default::default())
            .expect("unsuffixed qualified name should validate when suffix is active");

        let id = ir::object_id::ObjectId::new(
            "mydb_dev".to_string(),
            "public".to_string(),
            "foo".to_string(),
        );
        let obj = project
            .find_object(&id)
            .expect("planned project should expose object under suffixed database");
        let ident = obj.typed_object.stmt.ident();
        assert_eq!(
            ident
                .database
                .as_ref()
                .map(mz_sql_parser::ast::Ident::as_str),
            Some("mydb_dev"),
            "compiled CREATE statement should carry the suffixed database name",
        );
    }

    /// Cross-database references inside a view body that target another
    /// project-owned database get rewritten to the suffixed form alongside the
    /// owning database, so the resulting SQL is internally consistent.
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn plan_sync_with_profile_suffix_rewrites_cross_database_dependencies() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("project.toml"), "").unwrap();

        // Base table in db_a.
        let a_dir = root.path().join("models/db_a/public");
        std::fs::create_dir_all(&a_dir).unwrap();
        std::fs::write(
            a_dir.join("base.sql"),
            "CREATE TABLE db_a.public.base (id INT);\n",
        )
        .unwrap();

        // View in db_b that references db_a.public.base by its unsuffixed name.
        let b_dir = root.path().join("models/db_b/public");
        std::fs::create_dir_all(&b_dir).unwrap();
        std::fs::write(
            b_dir.join("derived.sql"),
            "CREATE VIEW db_b.public.derived AS SELECT id FROM db_a.public.base;\n",
        )
        .unwrap();

        let fs = crate::fs::FileSystem::new();
        let project = plan_sync(&fs, root.path(), None, Some("_dev"), &Default::default())
            .expect("cross-database references should compile under a profile suffix");

        let derived_id = ir::object_id::ObjectId::new(
            "db_b_dev".to_string(),
            "public".to_string(),
            "derived".to_string(),
        );
        let derived = project
            .find_object(&derived_id)
            .expect("derived view should appear under the suffixed database");

        // The dependency edge should target the *suffixed* db_a, not the
        // raw name the user wrote in the SELECT body.
        let base_id = ir::object_id::ObjectId::new(
            "db_a_dev".to_string(),
            "public".to_string(),
            "base".to_string(),
        );
        assert!(
            derived.dependencies.contains(&base_id),
            "cross-database dependency should be rewritten to the suffixed name; \
             got {:?}",
            derived.dependencies,
        );

        // The serialized statement body should mention the suffixed reference,
        // not the original one written by the user. (Catches regressions where
        // the AST rewrite skips the body or the statement's own name.)
        // The unsuffixed "db_a.public.base" cannot appear as a substring of
        // "db_a_dev.public.base" — the `_dev` interrupts the match — so a plain
        // `contains` check is enough to assert the original name is gone.
        let serialized = derived.typed_object.stmt.to_string();
        assert!(
            serialized.contains("db_a_dev.public.base"),
            "body should reference the suffixed db_a; got: {serialized}",
        );
        assert!(
            !serialized.contains("db_a.public.base"),
            "body should not retain the original db_a name; got: {serialized}",
        );
        assert!(
            serialized.contains("db_b_dev.public.derived"),
            "statement's own name should be suffixed; got: {serialized}",
        );
    }

    fn plan_and_typecheck(
        root: &Path,
        profile: Option<&str>,
        external_types: crate::types::Types,
    ) -> Result<(ir::graph::Project, compiler::typecheck::TypecheckStats), String> {
        let fs = crate::fs::FileSystem::new();
        let project = plan_sync(&fs, root, profile, None, &Default::default())
            .map_err(|err| err.to_string())?;
        let (_, stats) = compiler::typecheck::run(
            root,
            profile.unwrap_or(""),
            None,
            &Default::default(),
            &project,
            external_types,
        )
        .map_err(|err| err.to_string())?;
        Ok((project, stats))
    }

    /// Build an external-types map exposing a single-column relation under each id
    /// in `ids`, standing in for a `types.lock` entry.
    fn stub_types(ids: &[ir::object_id::ObjectId]) -> crate::types::Types {
        use crate::types::{ColumnType, Types};
        let mut types = Types::default();
        for id in ids {
            let mut columns = BTreeMap::new();
            columns.insert(
                "counter".to_string(),
                ColumnType {
                    r#type: "int8".to_string(),
                    nullable: false,
                    position: 0,
                    comment: None,
                },
            );
            types.tables.insert(id.clone(), columns);
        }
        types
    }

    /// A referencing object's cache entry must be invalidated when a new
    /// version of the object it references appears, even though the referencing
    /// file's own bytes never changed.
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn plan_sync_invalidates_cache_when_a_new_version_appears() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("project.toml"), "").unwrap();
        let raw_dir = root.path().join("models/mydb/raw");
        std::fs::create_dir_all(&raw_dir).unwrap();
        std::fs::write(
            raw_dir.join("events@1.sql"),
            "CREATE SOURCE events IN CLUSTER ingest FROM LOAD GENERATOR COUNTER;\n",
        )
        .unwrap();

        let public_dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&public_dir).unwrap();
        std::fs::write(
            public_dir.join("tracking.sql"),
            "CREATE VIEW tracking AS SELECT * FROM raw.events;\n",
        )
        .unwrap();

        // Both ids are stubbed up front so that one `Types` value covers both
        // compiles below.
        let external_types = stub_types(&[
            ir::object_id::ObjectId::new(
                "mydb".to_string(),
                "raw".to_string(),
                "events@1".to_string(),
            ),
            ir::object_id::ObjectId::new(
                "mydb".to_string(),
                "raw".to_string(),
                "events@2".to_string(),
            ),
        ]);

        // Only events@1 exists so far. This also warms the on-disk build
        // artifact cache under `root`.
        let (project, _stats) = plan_and_typecheck(root.path(), None, external_types.clone())
            .expect("project should compile and typecheck");
        let tracking = project
            .find_object(&ir::object_id::ObjectId::new(
                "mydb".to_string(),
                "public".to_string(),
                "tracking".to_string(),
            ))
            .expect("tracking view should be present");
        assert!(
            tracking
                .typed_object
                .stmt
                .to_string()
                .contains("\"events@1\""),
            "first compile should resolve to the only declared version"
        );

        // tracking.sql is not touched, so a fingerprint ignoring the version map
        // would report a cache hit and reuse the stale artifact.
        std::fs::write(
            raw_dir.join("events@2.sql"),
            "CREATE SOURCE events IN CLUSTER ingest FROM LOAD GENERATOR COUNTER;\n",
        )
        .unwrap();

        let (project, _stats) = plan_and_typecheck(root.path(), None, external_types)
            .expect("project should compile and typecheck against the same warm cache");
        let tracking = project
            .find_object(&ir::object_id::ObjectId::new(
                "mydb".to_string(),
                "public".to_string(),
                "tracking".to_string(),
            ))
            .expect("tracking view should be present");
        let tracking_sql = tracking.typed_object.stmt.to_string();
        assert!(
            tracking_sql.contains("\"events@2\""),
            "cache must be invalidated so the bare reference re-resolves to \
             the newest version, got: {tracking_sql}"
        );
    }

    /// A user-populated table classifies as `ObjectType::Table`, exactly like
    /// `CREATE TABLE ... FROM SOURCE`, so this is the case that pins the
    /// restriction to the statement variant rather than the object type.
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn plan_sync_rejects_versioned_plain_table() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("project.toml"), "").unwrap();
        let schema_dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&schema_dir).unwrap();
        std::fs::write(
            schema_dir.join("orders@1.sql"),
            "CREATE TABLE orders (id int);\n",
        )
        .unwrap();

        let err = plan_and_typecheck(root.path(), None, crate::types::Types::default())
            .expect_err("versioning a user-populated table should fail");
        assert!(
            err.contains("cannot be versioned"),
            "unexpected error: {err}"
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn plan_sync_rejects_versioned_view() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("project.toml"), "").unwrap();
        let schema_dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&schema_dir).unwrap();
        std::fs::write(
            schema_dir.join("order_ids@1.sql"),
            "CREATE VIEW order_ids AS SELECT 1 AS id;\n",
        )
        .unwrap();

        let err = plan_and_typecheck(root.path(), None, crate::types::Types::default())
            .expect_err("versioning a view should fail");
        assert!(
            err.contains("cannot be versioned"),
            "unexpected error: {err}"
        );
    }

    /// Every version gets its own project-cache row, keyed by its deployed name.
    /// The key is a primary key, so two versions keyed on the base name would
    /// evict each other.
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn plan_sync_caches_every_version_under_its_deployed_name() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("project.toml"), "").unwrap();
        let raw_dir = root.path().join("models/mydb/raw");
        std::fs::create_dir_all(&raw_dir).unwrap();
        for version in [1, 2] {
            std::fs::write(
                raw_dir.join(format!("events@{version}.sql")),
                "CREATE SOURCE events IN CLUSTER ingest FROM LOAD GENERATOR COUNTER;\n",
            )
            .unwrap();
        }

        plan_and_typecheck(root.path(), None, crate::types::Types::default())
            .expect("project should compile and typecheck");

        let cache = compiler::cache::ProjectCache::open(root.path(), "", None, &Default::default())
            .expect("cache should open")
            .expect("compiling should have written a cache");
        for version in [1, 2] {
            let id = ir::object_id::ObjectId::new(
                "mydb".to_string(),
                "raw".to_string(),
                format!("events@{version}"),
            );
            let cached = cache
                .get_object(&id)
                .unwrap_or_else(|| panic!("version {version} should be cached under {id}"));
            assert_eq!(cached.name, format!("events@{version}"));
        }
    }

    /// A versioned object's comment and privileges belong to that version, so
    /// its companion statements have to name the version and not the base
    /// name, which names no object at all.
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn plan_sync_points_companion_statements_at_the_versioned_name() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("project.toml"), "").unwrap();
        let raw_dir = root.path().join("models/mydb/raw");
        std::fs::create_dir_all(&raw_dir).unwrap();
        std::fs::write(
            raw_dir.join("events@1.sql"),
            "CREATE SOURCE events IN CLUSTER ingest FROM LOAD GENERATOR COUNTER;\n\
             COMMENT ON SOURCE events IS 'raw event stream';\n\
             GRANT SELECT ON TABLE events TO analyst;\n",
        )
        .unwrap();

        let (project, _stats) =
            plan_and_typecheck(root.path(), None, crate::types::Types::default())
                .expect("project should compile and typecheck");

        let object = project
            .find_object(&ir::object_id::ObjectId::new(
                "mydb".to_string(),
                "raw".to_string(),
                "events@1".to_string(),
            ))
            .expect("versioned source should be present");

        let comment = object.typed_object.comments[0].to_string();
        assert!(
            comment.contains("mydb.raw.\"events@1\""),
            "comment should target the deployed name, got: {comment}"
        );
        let grant = object.typed_object.grants[0].to_string();
        assert!(
            grant.contains("mydb.raw.\"events@1\""),
            "grant should target the deployed name, got: {grant}"
        );
    }

    /// A version declared only as an override for one profile does not exist
    /// under any other profile, so it must not be what a bare reference
    /// resolves to there.
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn plan_sync_ignores_a_version_declared_only_for_another_profile() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("project.toml"), "").unwrap();
        let raw_dir = root.path().join("models/mydb/raw");
        std::fs::create_dir_all(&raw_dir).unwrap();
        std::fs::write(
            raw_dir.join("events@1.sql"),
            "CREATE SOURCE events IN CLUSTER ingest FROM LOAD GENERATOR COUNTER;\n",
        )
        .unwrap();
        std::fs::write(
            raw_dir.join("events@2#dev.sql"),
            "CREATE SOURCE events IN CLUSTER ingest FROM LOAD GENERATOR COUNTER;\n",
        )
        .unwrap();

        let public_dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&public_dir).unwrap();
        std::fs::write(
            public_dir.join("tracking.sql"),
            "CREATE VIEW tracking AS SELECT * FROM raw.events;\n",
        )
        .unwrap();

        let tracking_id = ir::object_id::ObjectId::new(
            "mydb".to_string(),
            "public".to_string(),
            "tracking".to_string(),
        );
        let external_types = stub_types(&[
            ir::object_id::ObjectId::new(
                "mydb".to_string(),
                "raw".to_string(),
                "events@1".to_string(),
            ),
            ir::object_id::ObjectId::new(
                "mydb".to_string(),
                "raw".to_string(),
                "events@2".to_string(),
            ),
        ]);

        let (project, _stats) = plan_and_typecheck(root.path(), None, external_types.clone())
            .expect("a dev-only version must not break the default profile");
        let tracking_sql = project
            .find_object(&tracking_id)
            .expect("tracking view should be present")
            .typed_object
            .stmt
            .to_string();
        assert!(
            tracking_sql.contains("\"events@1\""),
            "the default profile should see only the version it declares, got: {tracking_sql}"
        );

        let (project, _stats) = plan_and_typecheck(root.path(), Some("dev"), external_types)
            .expect("project should compile and typecheck under the dev profile");
        let tracking_sql = project
            .find_object(&tracking_id)
            .expect("tracking view should be present")
            .typed_object
            .stmt
            .to_string();
        assert!(
            tracking_sql.contains("\"events@2\""),
            "the dev profile should see its own version, got: {tracking_sql}"
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn plan_sync_accepts_a_table_that_names_its_source_version() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("project.toml"), "").unwrap();
        let raw_dir = root.path().join("models/mydb/raw");
        std::fs::create_dir_all(&raw_dir).unwrap();
        std::fs::write(
            raw_dir.join("upstream@1.sql"),
            "CREATE SOURCE upstream IN CLUSTER ingest FROM LOAD GENERATOR COUNTER;\n",
        )
        .unwrap();
        std::fs::write(
            raw_dir.join("orders.sql"),
            "CREATE TABLE orders FROM SOURCE \"upstream@1\" (REFERENCE public.orders);\n",
        )
        .unwrap();

        let (project, _stats) =
            plan_and_typecheck(root.path(), None, crate::types::Types::default())
                .expect("a pinned source reference should compile and typecheck");
        let orders = project
            .find_object(&ir::object_id::ObjectId::new(
                "mydb".to_string(),
                "raw".to_string(),
                "orders".to_string(),
            ))
            .expect("table should be present");
        assert!(
            orders.dependencies.contains(&ir::object_id::ObjectId::new(
                "mydb".to_string(),
                "raw".to_string(),
                "upstream@1".to_string(),
            )),
            "table should depend on the version it names, got: {:?}",
            orders.dependencies
        );
    }

    /// Adopting a version suffix changes the name typecheck requires for the
    /// object, from the base name written in the file to the physical deployed
    /// name, so a `types.lock` entry keyed on the base name stops matching.
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn plan_sync_versioned_source_typecheck_fails_on_base_name_external_type() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("project.toml"), "").unwrap();
        let raw_dir = root.path().join("models/mydb/raw");
        std::fs::create_dir_all(&raw_dir).unwrap();
        std::fs::write(
            raw_dir.join("orders@1.sql"),
            "CREATE SOURCE orders IN CLUSTER ingest FROM LOAD GENERATOR COUNTER;\n",
        )
        .unwrap();

        let public_dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&public_dir).unwrap();
        std::fs::write(
            public_dir.join("downstream.sql"),
            "CREATE VIEW downstream AS SELECT * FROM raw.orders;\n",
        )
        .unwrap();

        // Keyed on the base name the file declares, not the physical name the
        // object deploys under.
        let external_types = stub_types(&[ir::object_id::ObjectId::new(
            "mydb".to_string(),
            "raw".to_string(),
            "orders".to_string(),
        )]);

        let err = plan_and_typecheck(root.path(), None, external_types)
            .expect_err("typecheck should fail: no catalog entry exists under the physical name");
        assert!(
            err.contains("unknown catalog item"),
            "unexpected error: {err}"
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
