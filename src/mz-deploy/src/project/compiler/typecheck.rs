// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Runtime typechecking integrated with the project compiler.
//!
//! Validation runs against an `mz-deploy` in-memory catalog using `mz-sql`
//! directly (see [`catalog`]). See [`run`] for the algorithm.

use super::build_artifact::BuildArtifact;
use crate::project::ast::Statement;
use crate::project::ir::graph::Project;
use crate::project::ir::object_id::ObjectId;
use crate::types::{ColumnType, ObjectKind, Types, TypesError};
use crate::verbose;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

mod bootstrap;
mod catalog;
mod convert;
mod error;
mod executor;

pub(crate) use error::{ObjectTypeCheckError, ObjectTypeCheckErrorKind, TypeCheckError};

/// Counts of incremental typecheck behavior during a single `run` call.
///
/// `ran` and `skipped` partition the set of typecheck-eligible nodes:
/// `ran` was actually re-typechecked; `skipped` reused cached columns wholesale.
/// `schema_stable` and `schema_changed` further partition `ran` by whether the
/// fresh result matched the cached result (and therefore whether dependents
/// need to be invalidated).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TypecheckStats {
    pub ran: usize,
    pub skipped: usize,
    pub schema_stable: usize,
    pub schema_changed: usize,
}

/// Per-node value carried between work closures in the DAG executor.
///
/// `columns` is the validated (or cached) column schema. `schema_stable`
/// signals whether this node's output matches what the cache held; dependents
/// only need to re-typecheck when at least one dep is *not* schema-stable.
#[derive(Debug, Clone)]
struct NodeValue {
    columns: BTreeMap<String, ColumnType>,
    schema_stable: bool,
}

/// Full-typecheck entrypoint with incremental reuse.
///
/// Runs three phases:
///
/// 1. Build the base catalog (serial): seeds builtins, namespaces, external
///    types, and all non-typechecked project objects.
/// 2. Run the DAG executor (parallel): each view/MV is a node. A node either
///    re-typechecks (when its file or any upstream output changed) or returns
///    its cached column schema directly. Dependents only re-typecheck when at
///    least one upstream dep was schema-changed, which keeps a leaf edit that
///    doesn't change the leaf's output schema from cascading.
/// 3. Persist newly-validated columns to SQLite. Failed and blocked objects
///    keep their last successful row in the cache.
///
/// Returns the merged `Types` covering validated columns, base columns
/// (tables/sources/etc.), and external `types.lock` entries, plus stats
/// describing how much work the incremental layer skipped.
pub(crate) fn run(
    directory: &Path,
    profile: &str,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
    project: &Project,
    external_types: Types,
) -> Result<(Types, TypecheckStats), TypeCheckError> {
    let sorted = project.get_sorted_objects()?;
    let mut node_ids: Vec<ObjectId> = Vec::new();
    let mut typed_objects: BTreeMap<ObjectId, &crate::project::ir::compiled::DatabaseObject> =
        BTreeMap::new();
    for (object_id, db_obj) in &sorted {
        if !matches!(
            db_obj.stmt,
            Statement::CreateView(_) | Statement::CreateMaterializedView(_)
        ) {
            continue;
        }
        node_ids.push(object_id.clone());
        typed_objects.insert(object_id.clone(), *db_obj);
    }

    // Open the build artifact db now so we can use it for incremental reads
    // (cached columns, prior external-type digests) and the final upserts.
    let db_open_start = std::time::Instant::now();
    let mut db = BuildArtifact::open(directory, profile, profile_suffix, variables)
        .map_err(TypesError::from)?;
    crate::timing!("typecheck: open_db", db_open_start.elapsed());

    // Snapshot all cached typecheck columns up front. Reading inside the DAG
    // would require a Sync SQLite handle, which rusqlite::Connection isn't.
    // The map is keyed by `ObjectId.to_string()` (matches sqlite layout).
    let cache_load_start = std::time::Instant::now();
    let cached_columns_by_key: BTreeMap<String, BTreeMap<String, ColumnType>> =
        db.load_typecheck_columns().map_err(TypesError::from)?;
    crate::timing!("typecheck: load_cached_columns", cache_load_start.elapsed());

    // Diff per-external-table digests against the cached set. Any project
    // object whose `external_dependencies` intersects the changed set is added
    // to the dirty set on top of `project.compile_dirty`.
    let current_ext_digests = compute_external_digests(&external_types);
    let cached_ext_digests = db
        .load_external_type_digests()
        .map_err(TypesError::from)
        .unwrap_or_default();
    let changed_externals: BTreeSet<ObjectId> = current_ext_digests
        .iter()
        .filter(|(k, v)| cached_ext_digests.get(*k) != Some(*v))
        .filter_map(|(k, _)| k.parse().ok())
        .chain(
            cached_ext_digests
                .keys()
                .filter(|k| !current_ext_digests.contains_key(*k))
                .filter_map(|k| k.parse().ok()),
        )
        .collect();

    let initial_dirty: BTreeSet<ObjectId> = node_ids
        .iter()
        .filter(|id| {
            if project.compile_dirty.contains(id) {
                return true;
            }
            // External-dep change → object is dirty. Per-object external
            // deps are the intersection of the object's full dependency set
            // with the project-wide external_dependencies set.
            let Some(deps) = project.dependency_graph.get(id) else {
                return false;
            };
            deps.iter()
                .filter(|d| project.external_dependencies.contains(d))
                .any(|ext| changed_externals.contains(ext))
        })
        .cloned()
        .collect();

    let node_set: BTreeSet<ObjectId> = node_ids.iter().cloned().collect();
    let reverse_graph = project.build_reverse_dependency_graph();

    // `pessimistic_dirty` = views that might actually typecheck this run:
    // every input-dirty view, plus every transitive dependent of one (since
    // a schema change in an upstream cascades). Computed as a downstream-only
    // walk from `initial_dirty` through the reverse-dependency graph.
    let mut pessimistic_dirty: BTreeSet<ObjectId> = BTreeSet::new();
    {
        let mut stack: Vec<ObjectId> = initial_dirty.iter().cloned().collect();
        while let Some(id) = stack.pop() {
            if !pessimistic_dirty.insert(id.clone()) {
                continue;
            }
            if let Some(downs) = reverse_graph.get(&id) {
                for d in downs {
                    if node_set.contains(d) && !pessimistic_dirty.contains(d) {
                        stack.push(d.clone());
                    }
                }
            }
        }
    }

    // DAG = pessimistic_dirty + their *direct* view deps. We need those view
    // deps in the DAG so dirty consumers can pick up their (cached) columns
    // through `dep_results`. We do *not* recurse into transitive view deps:
    // a clean view in the DAG skip-returns its cached columns and never
    // typechecks, so it doesn't need its own upstream registered anywhere.
    let mut dag_nodes: BTreeSet<ObjectId> = pessimistic_dirty.clone();
    for id in &pessimistic_dirty {
        if let Some(deps) = project.dependency_graph.get(id) {
            for d in deps {
                if node_set.contains(d) {
                    dag_nodes.insert(d.clone());
                }
            }
        }
    }

    // Bootstrap only the *direct non-view* deps of nodes that may actually
    // typecheck. Nothing else needs to be in the base catalog — direct view
    // deps come through `dep_results`, transitive deps are unreached. On a
    // no-op build pessimistic_dirty is empty and bootstrap_set is empty.
    let mut bootstrap_set: BTreeSet<ObjectId> = BTreeSet::new();
    for id in &pessimistic_dirty {
        if let Some(deps) = project.dependency_graph.get(id) {
            for d in deps {
                if !node_set.contains(d) {
                    bootstrap_set.insert(d.clone());
                }
            }
        }
    }

    let bootstrap_start = std::time::Instant::now();
    let (base_catalog, base_columns) =
        bootstrap::bootstrap_catalog(project, &external_types, Some(&bootstrap_set))?;
    crate::timing!("typecheck: bootstrap_catalog", bootstrap_start.elapsed());

    // Build the DAG only over `dag_nodes`. Direct-dep edges are filtered to
    // node IDs actually present in the DAG (other deps are already stubbed
    // into the base catalog above).
    let dag_node_ids: Vec<ObjectId> = node_ids
        .iter()
        .filter(|id| dag_nodes.contains(id))
        .cloned()
        .collect();
    let mut direct_deps: BTreeMap<ObjectId, Vec<ObjectId>> = BTreeMap::new();
    let mut dependents: BTreeMap<ObjectId, Vec<ObjectId>> = BTreeMap::new();
    for node_id in &dag_node_ids {
        let node_deps = project
            .dependency_graph
            .get(node_id)
            .into_iter()
            .flatten()
            .filter(|d| dag_nodes.contains(d))
            .cloned()
            .collect();
        direct_deps.insert(node_id.clone(), node_deps);

        let node_dependents = reverse_graph
            .get(node_id)
            .into_iter()
            .flatten()
            .filter(|d| dag_nodes.contains(d))
            .cloned()
            .collect();
        dependents.insert(node_id.clone(), node_dependents);
    }

    let stats_counter = Arc::new(StatsCounter::default());

    let typed_objects = Arc::new(typed_objects);
    let cached_columns_by_key = Arc::new(cached_columns_by_key);
    let dag_start = std::time::Instant::now();
    let outcomes = {
        let typed_objects = Arc::clone(&typed_objects);
        let base_catalog = Arc::clone(&base_catalog);
        let initial_dirty = Arc::new(initial_dirty.clone());
        let cached_columns_by_key = Arc::clone(&cached_columns_by_key);
        let stats_counter = Arc::clone(&stats_counter);
        executor::run::<NodeValue, _>(
            dag_node_ids.clone(),
            direct_deps,
            dependents,
            move |node_id, dep_results| {
                let db_obj = typed_objects
                    .get(node_id)
                    .expect("typed_object exists for every scheduled node");

                let cached_columns = cached_columns_by_key.get(&node_id.to_string()).cloned();
                let any_dep_changed = dep_results.values().any(|v| !v.schema_stable);
                let must_typecheck =
                    initial_dirty.contains(node_id) || any_dep_changed || cached_columns.is_none();

                if !must_typecheck {
                    let columns = cached_columns.expect("must_typecheck guards None");
                    return Ok(NodeValue {
                        columns,
                        schema_stable: true,
                    });
                }

                let mut runtime = (*base_catalog).clone();
                for (dep_id, dep_value) in dep_results {
                    runtime
                        .create_stub_table(dep_id, &dep_value.columns)
                        .map_err(|err| {
                            ObjectTypeCheckError::internal(
                                dep_id.clone(),
                                db_obj.path.clone(),
                                format!("failed to stub dependency: {err}"),
                            )
                        })?;
                }
                let fqn: crate::project::ir::compiled::FullyQualifiedName = node_id.clone().into();
                let ast =
                    convert::create_catalog_item_ast(&db_obj.stmt, &fqn).ok_or_else(|| {
                        ObjectTypeCheckError::internal(
                            node_id.clone(),
                            db_obj.path.clone(),
                            "internal: failed to build catalog AST".into(),
                        )
                    })?;
                let desc = runtime.create_item_from_ast(node_id, ast)?;
                let columns = convert::relation_desc_to_columns(&desc);
                let schema_stable = cached_columns
                    .as_ref()
                    .map(|cached| cached == &columns)
                    .unwrap_or(false);
                stats_counter.bump_ran(schema_stable);
                Ok(NodeValue {
                    columns,
                    schema_stable,
                })
            },
        )
    };
    crate::timing!("typecheck: dag_executor", dag_start.elapsed());

    let merge_start = std::time::Instant::now();
    let mut errors: Vec<ObjectTypeCheckError> = Vec::new();
    let mut upsert_rows: Vec<(String, String, BTreeMap<String, ColumnType>)> = Vec::new();
    let mut merged_tables: BTreeMap<ObjectId, BTreeMap<String, ColumnType>> = BTreeMap::new();
    let mut merged_kinds: BTreeMap<ObjectId, ObjectKind> = BTreeMap::new();

    let project_objects_by_id: BTreeMap<&ObjectId, &crate::project::ir::compiled::DatabaseObject> =
        project
            .iter_objects()
            .map(|o| (&o.id, &o.typed_object))
            .collect();
    for (id, columns) in base_columns.iter() {
        merged_tables.insert(id.clone(), columns.clone());
        if let Some(typed_obj) = project_objects_by_id.get(id) {
            merged_kinds.insert(id.clone(), typed_obj.stmt.kind());
        }
    }
    for (id, columns) in &external_types.tables {
        merged_tables.insert(id.clone(), columns.clone());
        if let Some(kind) = external_types.kinds.get(id) {
            merged_kinds.insert(id.clone(), *kind);
        }
    }

    for node_id in &node_ids {
        let Some(outcome) = outcomes.get(node_id) else {
            continue;
        };
        match outcome {
            executor::NodeOutcome::Ok(value) => {
                let db_obj = typed_objects
                    .get(node_id)
                    .expect("typed_object exists for outcome");
                let kind = db_obj.stmt.kind();
                merged_tables.insert(node_id.clone(), value.columns.clone());
                merged_kinds.insert(node_id.clone(), kind);
                // Only persist nodes whose schema actually changed (or are
                // brand new). Skipped and schema-stable nodes already have a
                // matching row in the cache.
                if !value.schema_stable {
                    upsert_rows.push((
                        node_id.to_string(),
                        kind.as_str().to_string(),
                        value.columns.clone(),
                    ));
                }
            }
            executor::NodeOutcome::Failed(err) => {
                // The catalog only sees synthesized object names, not the
                // real source path, so it stamps a placeholder
                // `{db}/{schema}/{name}.sql`. Rewrite to the absolute
                // source path so downstream consumers (LSP diagnostics,
                // CLI output) can locate the actual file.
                let mut err = err.clone();
                if let Some(db_obj) = typed_objects.get(node_id) {
                    err.file_path = directory.join(&db_obj.path);
                }
                errors.push(err);
            }
            executor::NodeOutcome::Blocked(blocker) => {
                verbose!(
                    "Skipping {}: blocked by upstream error in {}",
                    node_id,
                    blocker
                );
            }
        }
    }

    crate::timing!("typecheck: merge_results", merge_start.elapsed());

    // Failed/blocked nodes keep their last successful row by being absent from
    // upsert_rows. The keep-set retains every typecheck-eligible object so
    // prune only deletes rows for objects no longer in the project.
    let persist_start = std::time::Instant::now();
    db.upsert_typecheck_results(&upsert_rows)
        .map_err(TypesError::from)?;
    let keep: BTreeSet<String> = node_ids.iter().map(|id| id.to_string()).collect();
    db.prune_typecheck_results(&keep)
        .map_err(TypesError::from)?;
    db.replace_external_type_digests(&current_ext_digests)
        .map_err(TypesError::from)?;
    crate::timing!("typecheck: persist", persist_start.elapsed());

    if !errors.is_empty() {
        return Err(TypeCheckError::Multiple(errors));
    }

    let mut stats = stats_counter.snapshot();
    stats.skipped = node_ids.len().saturating_sub(stats.ran);

    Ok((
        Types {
            version: 1,
            tables: merged_tables,
            kinds: merged_kinds,
            comments: BTreeMap::new(),
        },
        stats,
    ))
}

/// Atomic counter set used to aggregate per-node decisions across the parallel
/// DAG executor without locking. Snapshotted once after the DAG completes.
#[derive(Default)]
struct StatsCounter {
    ran: std::sync::atomic::AtomicUsize,
    schema_stable: std::sync::atomic::AtomicUsize,
    schema_changed: std::sync::atomic::AtomicUsize,
}

impl StatsCounter {
    fn bump_ran(&self, schema_stable: bool) {
        self.ran.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if schema_stable {
            self.schema_stable
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        } else {
            self.schema_changed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> TypecheckStats {
        use std::sync::atomic::Ordering::Relaxed;
        TypecheckStats {
            ran: self.ran.load(Relaxed),
            skipped: 0, // populated by the caller from node_ids.len() - ran
            schema_stable: self.schema_stable.load(Relaxed),
            schema_changed: self.schema_changed.load(Relaxed),
        }
    }
}

/// SHA-256 digest of a column map, deterministic across runs because the
/// underlying `BTreeMap` iterates in sorted key order.
fn digest_columns(cols: &BTreeMap<String, ColumnType>) -> String {
    let mut hasher = Sha256::new();
    for (name, t) in cols {
        hasher.update(name.as_bytes());
        hasher.update(b"\0");
        hasher.update(t.r#type.as_bytes());
        hasher.update(b"\0");
        hasher.update([u8::from(t.nullable)]);
        hasher.update(b"\0");
        hasher.update(u64::try_from(t.position).unwrap_or(u64::MAX).to_le_bytes());
        hasher.update(b"\0");
    }
    format!("{:x}", hasher.finalize())
}

/// Per-external-table digests keyed by `ObjectId.to_string()`.
fn compute_external_digests(external_types: &Types) -> BTreeMap<String, String> {
    external_types
        .tables
        .iter()
        .map(|(id, cols)| (id.to_string(), digest_columns(cols)))
        .collect()
}

#[cfg(test)]
mod run_tests {
    use super::*;
    use crate::project::compiler::compile_sync;
    use std::collections::BTreeMap;
    use std::fs;
    use tempfile::tempdir;

    fn write_sql(root: &Path, rel: &str, sql: &str) {
        let path = root.join(rel);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, sql).unwrap();
    }

    #[test]
    fn run_typechecks_simple_view_and_persists_columns() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        // Tables (storage) and views (computation) must be in separate schemas.
        write_sql(
            root,
            "models/materialize/storage/t1.sql",
            "CREATE TABLE t1 (a int)",
        );
        write_sql(
            root,
            "models/materialize/public/v1.sql",
            "CREATE VIEW v1 AS SELECT a FROM materialize.storage.t1",
        );

        let fs = crate::fs::FileSystem::new();
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let (merged, _stats) = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        )
        .unwrap();

        assert!(
            merged
                .tables
                .contains_key(&"materialize.public.v1".parse::<ObjectId>().unwrap())
        );
        assert!(
            merged
                .tables
                .contains_key(&"materialize.storage.t1".parse::<ObjectId>().unwrap())
        );
    }

    /// A second `run` after no source change should typecheck zero nodes.
    #[test]
    fn second_run_skips_all_nodes_when_nothing_changed() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        write_sql(
            root,
            "models/materialize/storage/t1.sql",
            "CREATE TABLE t1 (a int)",
        );
        write_sql(
            root,
            "models/materialize/public/v1.sql",
            "CREATE VIEW v1 AS SELECT a FROM materialize.storage.t1",
        );
        write_sql(
            root,
            "models/materialize/public/v2.sql",
            "CREATE VIEW v2 AS SELECT a FROM materialize.public.v1",
        );

        let fs = crate::fs::FileSystem::new();
        // First run: prime the cache.
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let (_, first) = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        )
        .unwrap();
        assert_eq!(first.ran, 2, "first run should typecheck v1 and v2");
        assert_eq!(first.skipped, 0);

        // Second run: nothing changed, both views should be skipped.
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let (_, second) = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        )
        .unwrap();
        assert_eq!(second.ran, 0, "second run should skip everything");
        assert_eq!(second.skipped, 2);
    }

    /// Editing a leaf view in a way that doesn't change its output schema
    /// should re-typecheck the leaf but skip its dependents.
    #[test]
    fn schema_stable_edit_does_not_dirty_dependents() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        write_sql(
            root,
            "models/materialize/storage/t1.sql",
            "CREATE TABLE t1 (a int)",
        );
        write_sql(
            root,
            "models/materialize/public/v1.sql",
            "CREATE VIEW v1 AS SELECT a FROM materialize.storage.t1",
        );
        write_sql(
            root,
            "models/materialize/public/v2.sql",
            "CREATE VIEW v2 AS SELECT a FROM materialize.public.v1",
        );

        let fs = crate::fs::FileSystem::new();
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let _ = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        )
        .unwrap();

        // Rewrite v1 in a way that produces the same column schema.
        write_sql(
            root,
            "models/materialize/public/v1.sql",
            "CREATE VIEW v1 AS SELECT a FROM (SELECT * FROM materialize.storage.t1)",
        );
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let (_, stats) = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        )
        .unwrap();

        assert_eq!(stats.ran, 1, "only v1 should re-typecheck");
        assert_eq!(stats.schema_stable, 1, "v1 output unchanged");
        assert_eq!(stats.schema_changed, 0);
        assert_eq!(stats.skipped, 1, "v2 should skip on stable upstream");
    }

    /// Changing one external table's schema only dirties objects that depend
    /// on that specific table. Unrelated objects keep their cached results.
    #[test]
    fn external_type_change_dirties_only_consumers() {
        use crate::types::ObjectKind;

        let temp = tempdir().unwrap();
        let root = temp.path();
        // v_ext_a depends on ext.public.t_a; v_ext_b depends on ext.public.t_b.
        // Both are in storage so the project itself has no internal deps to
        // muddy the test.
        write_sql(
            root,
            "models/materialize/public/v_ext_a.sql",
            "CREATE VIEW v_ext_a AS SELECT a FROM ext.public.t_a",
        );
        write_sql(
            root,
            "models/materialize/public/v_ext_b.sql",
            "CREATE VIEW v_ext_b AS SELECT a FROM ext.public.t_b",
        );

        let mk_types = |a_type: &str, b_type: &str| {
            let mut tables: BTreeMap<ObjectId, BTreeMap<String, ColumnType>> = BTreeMap::new();
            let mut kinds: BTreeMap<ObjectId, ObjectKind> = BTreeMap::new();
            let t_a: ObjectId = "ext.public.t_a".parse().unwrap();
            let t_b: ObjectId = "ext.public.t_b".parse().unwrap();
            tables.insert(
                t_a.clone(),
                BTreeMap::from([(
                    "a".to_string(),
                    ColumnType {
                        r#type: a_type.to_string(),
                        nullable: true,
                        position: 0,
                        comment: None,
                    },
                )]),
            );
            tables.insert(
                t_b.clone(),
                BTreeMap::from([(
                    "a".to_string(),
                    ColumnType {
                        r#type: b_type.to_string(),
                        nullable: true,
                        position: 0,
                        comment: None,
                    },
                )]),
            );
            kinds.insert(t_a, ObjectKind::Table);
            kinds.insert(t_b, ObjectKind::Table);
            Types {
                version: 1,
                tables,
                kinds,
                comments: BTreeMap::new(),
            }
        };

        let fs = crate::fs::FileSystem::new();
        // Prime.
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let _ = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            mk_types("integer", "integer"),
        )
        .unwrap();

        // Same externals → both views skip.
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let (_, stats) = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            mk_types("integer", "integer"),
        )
        .unwrap();
        assert_eq!(stats.skipped, 2, "no external change → both skip");
        assert_eq!(stats.ran, 0);

        // Change t_a's column type → only v_ext_a dirties.
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let (_, stats) = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            mk_types("text", "integer"),
        )
        .unwrap();
        assert_eq!(stats.ran, 1, "only v_ext_a should re-run");
        assert_eq!(stats.skipped, 1, "v_ext_b should skip");
    }

    /// A leaf edit that changes the output schema must cascade to dependents.
    #[test]
    fn schema_change_dirties_dependents() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        write_sql(
            root,
            "models/materialize/storage/t1.sql",
            "CREATE TABLE t1 (a int, b int)",
        );
        write_sql(
            root,
            "models/materialize/public/v1.sql",
            "CREATE VIEW v1 AS SELECT a FROM materialize.storage.t1",
        );
        write_sql(
            root,
            "models/materialize/public/v2.sql",
            "CREATE VIEW v2 AS SELECT * FROM materialize.public.v1",
        );

        let fs = crate::fs::FileSystem::new();
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let _ = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        )
        .unwrap();

        // Add a column to v1's projection — its schema changes.
        write_sql(
            root,
            "models/materialize/public/v1.sql",
            "CREATE VIEW v1 AS SELECT a, b FROM materialize.storage.t1",
        );
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let (_, stats) = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        )
        .unwrap();

        assert_eq!(stats.ran, 2, "v1 changed, v2 must re-run");
        assert_eq!(stats.schema_changed, 2);
        assert_eq!(stats.skipped, 0);
    }
}
