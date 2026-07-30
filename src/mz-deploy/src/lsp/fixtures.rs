// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compiled project fixtures shared by the editor-feature tests.
//!
//! Every fixture runs the real compiler over real files and hands back the
//! [`ProjectCache`] it wrote, so the tests agree with what the compiler decided
//! rather than with a hand-built cache.

use crate::project::compiler::cache::ProjectCache;
use std::path::Path;
use tempfile::TempDir;

/// A project with two declared versions each of `mydb.raw.orders` and
/// `mydb.raw.customers`, one consumer spelling the reference bare, and one
/// pinning the older version.
///
/// ```text
/// models/mydb/raw/orders@1.sql     CREATE TABLE orders ...; COMMENT ON TABLE orders ...
/// models/mydb/raw/orders@2.sql     CREATE TABLE orders ...; COMMENT ON TABLE orders ...
/// models/mydb/raw/customers@1.sql  CREATE TABLE customers ...
/// models/mydb/raw/customers@2.sql  CREATE TABLE customers ...
/// models/mydb/core/report.sql      ... FROM raw.orders
/// models/mydb/core/pinned.sql      ... FROM raw."orders@1"
/// ```
pub(crate) fn versioned_project() -> (TempDir, ProjectCache) {
    let root = tempfile::tempdir().unwrap();
    write_versioned_models(root.path(), None);
    write_project_toml(root.path());
    compile(root.path(), None);
    let cache = open_cache(root.path(), "");
    (root, cache)
}

/// The same project, but with version 2 declared only as a `dev` override.
/// Compiled under `profile`, and the returned cache is that compilation's.
///
/// ```text
/// models/mydb/raw/orders@1.sql       CREATE TABLE orders FROM SOURCE ...
/// models/mydb/raw/orders@2#dev.sql   CREATE TABLE orders FROM SOURCE ...
/// models/mydb/core/report.sql        ... FROM raw.orders
/// ```
pub(crate) fn profile_versioned_project(profile: &str) -> (TempDir, ProjectCache) {
    let root = tempfile::tempdir().unwrap();
    write_versioned_models(root.path(), Some("dev"));
    write_project_toml(root.path());
    compile(root.path(), Some(profile));
    let cache = open_cache(root.path(), profile);
    (root, cache)
}

/// Write the model tree. `version_2_profile` tags the second version's file as
/// a profile override rather than a default.
fn write_versioned_models(root: &Path, version_2_profile: Option<&str>) {
    let public = root.join("models/mydb/public");
    std::fs::create_dir_all(&public).unwrap();
    std::fs::write(public.join("pgpass.sql"), "CREATE SECRET pgpass AS 'pw'").unwrap();
    std::fs::write(
        public.join("pg_conn.sql"),
        "CREATE CONNECTION pg_conn TO POSTGRES (\
         HOST 'postgres', DATABASE 'postgres', USER 'postgres', PASSWORD SECRET pgpass)",
    )
    .unwrap();

    let clusters = root.join("clusters");
    std::fs::create_dir_all(&clusters).unwrap();
    std::fs::write(
        clusters.join("ingest.sql"),
        "CREATE CLUSTER ingest SIZE = 'scale=1,workers=1';",
    )
    .unwrap();

    let raw = root.join("models/mydb/raw");
    std::fs::create_dir_all(&raw).unwrap();
    std::fs::write(
        raw.join("pg_source.sql"),
        "CREATE SOURCE pg_source IN CLUSTER ingest \
         FROM POSTGRES CONNECTION mydb.public.pg_conn (PUBLICATION 'mz_source')",
    )
    .unwrap();

    // The `COMMENT ON` names the table by its base name, exercising the
    // self-reference spelling.
    const ORDERS: &str = "CREATE TABLE orders FROM SOURCE mydb.raw.pg_source \
                          (REFERENCE public.orders);\n\
                          COMMENT ON TABLE orders IS 'Incoming orders';";
    std::fs::write(raw.join("orders@1.sql"), ORDERS).unwrap();
    let version_2 = match version_2_profile {
        Some(profile) => format!("orders@2#{profile}.sql"),
        None => "orders@2.sql".to_string(),
    };
    std::fs::write(raw.join(version_2), ORDERS).unwrap();

    // A second versioned object, so a reference from inside one version file to
    // a different versioned object can be told apart from a self-reference.
    const CUSTOMERS: &str =
        "CREATE TABLE customers FROM SOURCE mydb.raw.pg_source (REFERENCE public.customers);";
    std::fs::write(raw.join("customers@1.sql"), CUSTOMERS).unwrap();
    std::fs::write(raw.join("customers@2.sql"), CUSTOMERS).unwrap();

    // Storage and computation objects may not share a schema.
    let core = root.join("models/mydb/core");
    std::fs::create_dir_all(&core).unwrap();
    std::fs::write(
        core.join("report.sql"),
        "CREATE VIEW report AS SELECT * FROM raw.orders;",
    )
    .unwrap();
    if version_2_profile.is_none() {
        std::fs::write(
            core.join("pinned.sql"),
            "CREATE VIEW pinned AS SELECT * FROM raw.\"orders@1\";",
        )
        .unwrap();
    }
}

fn write_project_toml(root: &Path) {
    std::fs::write(root.join("project.toml"), "[project]\nname = \"test\"\n").unwrap();
}

fn compile(root: &Path, profile: Option<&str>) {
    crate::project::plan_sync(
        &crate::fs::FileSystem::new(),
        root,
        profile,
        None,
        &Default::default(),
    )
    .expect("project should compile");
}

fn open_cache(root: &Path, profile: &str) -> ProjectCache {
    ProjectCache::open(root, profile, None, &Default::default())
        .expect("cache should open")
        .expect("cache DB should exist")
}

/// The URI of a model file, for use as the "current file" of a request.
pub(crate) fn model_uri(root: &Path, relative: &str) -> tower_lsp::lsp_types::Url {
    tower_lsp::lsp_types::Url::from_file_path(root.join("models").join(relative)).unwrap()
}

/// The text of a model file, for locating a cursor offset in real source.
pub(crate) fn model_text(root: &Path, relative: &str) -> String {
    std::fs::read_to_string(root.join("models").join(relative)).unwrap()
}
