//! Runtime typechecking integrated with the project compiler.
//!
//! ## Algorithm
//!
//! Typechecking runs in three phases:
//!
//! 1. Build the base catalog (serial): seeds builtins, namespaces, external
//!    types, and all non-typechecked project objects.
//! 2. Run the DAG executor (parallel): each view/MV is a node; tasks fire as
//!    soon as their dependencies have produced column maps.
//! 3. Persist successful outcomes to SQLite.
//!
//! ## Backend
//!
//! Validation runs against an `mz-deploy` in-memory catalog using `mz-sql`
//! directly (see [`catalog`]).

use super::build_artifact::BuildArtifact;
use crate::project::ast::Statement;
use crate::project::ir::graph::Project;
use crate::project::ir::object_id::ObjectId;
use crate::types::{ColumnType, ObjectKind, Types, TypesError};
use crate::verbose;
use mz_sql_parser::ast::{Ident, Raw};
use owo_colors::OwoColorize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

mod base;
mod catalog;
mod executor;

/// Errors that can occur during runtime typechecking.
#[derive(Debug, Error)]
pub enum TypeCheckError {
    #[error(transparent)]
    TypeCheckFailed(#[from] ObjectTypeCheckError),

    #[error(transparent)]
    Multiple(#[from] TypeCheckErrors),

    #[error("database error during setup: {0}")]
    DatabaseSetupError(String),

    #[error("failed to get sorted objects: {0}")]
    SortError(#[from] crate::project::error::DependencyError),

    #[error("failed to write types cache: {0}")]
    TypesCacheWriteFailed(#[from] TypesError),
}

/// A single typecheck error for a specific object, rendered in rustc style.
#[derive(Debug, Clone)]
pub struct ObjectTypeCheckError {
    pub object_id: ObjectId,
    pub file_path: PathBuf,
    pub sql_statement: String,
    pub error_message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

impl fmt::Display for ObjectTypeCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let path_components: Vec<_> = self.file_path.components().collect();
        let len = path_components.len();
        let relative_path = if len >= 3 {
            format!(
                "{}/{}/{}",
                path_components[len - 3].as_os_str().to_string_lossy(),
                path_components[len - 2].as_os_str().to_string_lossy(),
                path_components[len - 1].as_os_str().to_string_lossy()
            )
        } else {
            self.file_path.display().to_string()
        };

        writeln!(f, "type check failed for '{}'", self.object_id)?;
        writeln!(f, " {} {}", "-->".bright_blue().bold(), relative_path)?;
        writeln!(f)?;

        let lines: Vec<_> = self.sql_statement.lines().collect();
        writeln!(f, "  {}", "|".bright_blue().bold())?;
        for (idx, line) in lines.iter().take(10).enumerate() {
            writeln!(f, "  {} {}", "|".bright_blue().bold(), line)?;
            if idx == 9 && lines.len() > 10 {
                writeln!(
                    f,
                    "  {} ... ({} more lines)",
                    "|".bright_blue().bold(),
                    lines.len() - 10
                )?;
                break;
            }
        }
        writeln!(f, "  {}", "|".bright_blue().bold())?;
        writeln!(f)?;
        writeln!(f, "  {}", self.error_message)?;

        if let Some(ref detail) = self.detail {
            writeln!(f, "  {}: {}", "detail".bright_cyan().bold(), detail)?;
        }
        if let Some(ref hint) = self.hint {
            writeln!(
                f,
                "  {} {}",
                "=".bright_blue().bold(),
                format!("hint: {}", hint).bold()
            )?;
        }

        Ok(())
    }
}

impl std::error::Error for ObjectTypeCheckError {}

/// Collection of typecheck errors, rendered as a numbered error summary.
#[derive(Debug)]
pub struct TypeCheckErrors {
    pub errors: Vec<ObjectTypeCheckError>,
}

impl fmt::Display for TypeCheckErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (idx, error) in self.errors.iter().enumerate() {
            if idx > 0 {
                writeln!(f)?;
            }
            write!(f, "{}", error)?;
        }

        writeln!(f)?;
        writeln!(
            f,
            "could not type check due to {} previous error{}",
            self.errors.len(),
            if self.errors.len() == 1 { "" } else { "s" }
        )?;

        Ok(())
    }
}

impl std::error::Error for TypeCheckErrors {}

/// Full-typecheck entrypoint.
///
/// Runs three phases:
///
/// 1. Build the base catalog (serial): seeds builtins, namespaces, external
///    types, and all non-typechecked project objects.
/// 2. Run the DAG executor (parallel): each view/MV is a node; tasks fire as
///    soon as their dependencies have produced column maps.
/// 3. Persist successful outcomes to SQLite. Failed and blocked objects keep
///    their last successful row in the cache.
///
/// Returns the merged `Types` covering newly-validated columns, base columns
/// (tables/sources/etc.), and external `types.lock` entries.
pub(crate) fn run(
    directory: &Path,
    profile: &str,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
    project: &Project,
    external_types: Types,
) -> Result<Types, TypeCheckError> {
    // Phase 1.
    let base::BaseCatalog {
        catalog: base_catalog,
        base_columns,
    } = base::build_base_catalog(project, &external_types)?;

    // Identify the typecheck-eligible nodes (views and materialized views).
    let sorted = project.get_sorted_objects()?;
    let mut node_ids: Vec<ObjectId> = Vec::new();
    let mut typed_objects: BTreeMap<ObjectId, &crate::project::ir::compiled::DatabaseObject> =
        BTreeMap::new();
    for (object_id, db_obj) in &sorted {
        if !requires_typecheck(&db_obj.stmt) {
            continue;
        }
        node_ids.push(object_id.clone());
        typed_objects.insert(object_id.clone(), *db_obj);
    }
    let node_set: BTreeSet<ObjectId> = node_ids.iter().cloned().collect();

    // Build direct_deps / dependents restricted to view/MV nodes.
    let mut direct_deps: BTreeMap<ObjectId, Vec<ObjectId>> = BTreeMap::new();
    let mut dependents: BTreeMap<ObjectId, Vec<ObjectId>> = BTreeMap::new();
    for node_id in &node_ids {
        let deps = project
            .dependency_graph
            .get(node_id)
            .cloned()
            .unwrap_or_default();
        let node_only_deps: Vec<ObjectId> = deps
            .iter()
            .filter(|d| node_set.contains(d))
            .cloned()
            .collect();
        for dep_id in &node_only_deps {
            dependents
                .entry(dep_id.clone())
                .or_default()
                .push(node_id.clone());
        }
        direct_deps.insert(node_id.clone(), node_only_deps);
        dependents.entry(node_id.clone()).or_default(); // ensure key exists
    }

    // Phase 2.
    let typed_objects = Arc::new(typed_objects);
    let base_columns_arc = Arc::new(base_columns);
    let outcomes = {
        let typed_objects = Arc::clone(&typed_objects);
        let base_catalog = Arc::clone(&base_catalog);
        executor::run::<BTreeMap<String, ColumnType>, _>(
            node_ids.clone(),
            direct_deps,
            dependents,
            move |node_id, dep_results| {
                let db_obj = typed_objects
                    .get(node_id)
                    .expect("typed_object exists for every scheduled node");
                let mut runtime = (*base_catalog).clone();
                // Stub view/MV deps from upstream task results.
                for (dep_id, columns) in dep_results {
                    runtime
                        .create_stub_table(dep_id, columns.as_ref())
                        .map_err(|err| match err {
                            TypeCheckError::TypeCheckFailed(e) => e,
                            other => ObjectTypeCheckError {
                                object_id: dep_id.clone(),
                                file_path: db_obj.path.clone(),
                                sql_statement: String::new(),
                                error_message: format!("failed to stub dependency: {other}"),
                                detail: None,
                                hint: None,
                            },
                        })?;
                }
                // Base-column deps (tables, sources, externals) are already
                // present in the cloned base catalog — no need to re-stub them.
                let fqn: crate::project::ir::compiled::FullyQualifiedName = node_id.clone().into();
                let sql =
                    catalog::create_catalog_item_sql(&db_obj.stmt, &fqn).ok_or_else(|| {
                        ObjectTypeCheckError {
                            object_id: node_id.clone(),
                            file_path: db_obj.path.clone(),
                            sql_statement: String::new(),
                            error_message: "internal: failed to render catalog SQL".into(),
                            detail: None,
                            hint: None,
                        }
                    })?;
                let desc = runtime.create_or_replace_item(node_id, &sql)?;
                Ok(catalog::relation_desc_to_columns(&desc))
            },
        )
    };

    // Phase 3.
    let mut errors: Vec<ObjectTypeCheckError> = Vec::new();
    let mut upsert_rows: Vec<(String, String, String, BTreeMap<String, ColumnType>)> = Vec::new();
    let mut merged_tables: BTreeMap<String, BTreeMap<String, ColumnType>> = BTreeMap::new();
    let mut merged_kinds: BTreeMap<String, ObjectKind> = BTreeMap::new();

    // Seed merged maps from base_columns (tables/sources/etc.).
    for (id, columns) in base_columns_arc.iter() {
        let key = id.to_string();
        merged_tables.insert(key.clone(), columns.clone());
        if let Some(db_obj) = project.iter_objects().find(|o| o.id == *id) {
            merged_kinds.insert(key, object_kind_for_stmt(&db_obj.typed_object.stmt));
        }
    }
    // Seed from external types.lock so callers see the full surface.
    for (fqn, columns) in &external_types.tables {
        merged_tables.insert(fqn.clone(), columns.clone());
        if let Some(kind) = external_types.kinds.get(fqn) {
            merged_kinds.insert(fqn.clone(), *kind);
        }
    }

    for node_id in &node_ids {
        let Some(outcome) = outcomes.get(node_id) else {
            continue;
        };
        match outcome {
            executor::NodeOutcome::Ok(columns) => {
                let db_obj = typed_objects
                    .get(node_id)
                    .expect("typed_object exists for outcome");
                let key = node_id.to_string();
                let kind = object_kind_for_stmt(&db_obj.stmt);
                let semantic_fingerprint = compute_semantic_fingerprint(db_obj);
                merged_tables.insert(key.clone(), columns.as_ref().clone());
                merged_kinds.insert(key.clone(), kind);
                upsert_rows.push((
                    key,
                    semantic_fingerprint,
                    kind.as_str().to_string(),
                    columns.as_ref().clone(),
                ));
            }
            executor::NodeOutcome::Err(executor::NodeFailure::Failed(err)) => {
                errors.push(err.clone());
            }
            executor::NodeOutcome::Err(executor::NodeFailure::Blocked(blocker)) => {
                verbose!(
                    "Skipping {}: blocked by upstream error in {}",
                    node_id,
                    blocker
                );
            }
        }
    }

    // Persist successful outcomes; preserve the last successful row for objects
    // that failed or were blocked in this run by *not* including them in the
    // upsert. The keep-set passed to prune is every typecheck-eligible object
    // currently in the project.
    let mut db = BuildArtifact::open(directory, profile, profile_suffix, variables)
        .map_err(TypesError::from)?;
    let row_refs: Vec<(String, String, String, &BTreeMap<String, ColumnType>)> = upsert_rows
        .iter()
        .map(|(k, sf, kind, cols)| (k.clone(), sf.clone(), kind.clone(), cols))
        .collect();
    db.upsert_typecheck_results(&row_refs)
        .map_err(TypesError::from)?;
    let keep: BTreeSet<String> = node_ids.iter().map(|id| id.to_string()).collect();
    db.prune_typecheck_results(&keep)
        .map_err(TypesError::from)?;

    if !errors.is_empty() {
        return Err(TypeCheckError::Multiple(TypeCheckErrors { errors }));
    }

    Ok(Types {
        version: 1,
        tables: merged_tables,
        kinds: merged_kinds,
        comments: BTreeMap::new(),
    })
}

pub(super) fn requires_typecheck(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::CreateView(_) | Statement::CreateMaterializedView(_)
    )
}

fn object_kind_for_stmt(stmt: &Statement) -> ObjectKind {
    match stmt {
        Statement::CreateView(_) => ObjectKind::View,
        Statement::CreateMaterializedView(_) => ObjectKind::MaterializedView,
        Statement::CreateTable(_) | Statement::CreateTableFromSource(_) => ObjectKind::Table,
        Statement::CreateSource(_) => ObjectKind::Source,
        Statement::CreateSink(_) => ObjectKind::Sink,
        Statement::CreateSecret(_) => ObjectKind::Secret,
        Statement::CreateConnection(_) => ObjectKind::Connection,
    }
}

struct Sha256Hasher {
    digest: Sha256,
}

impl Sha256Hasher {
    fn new() -> Self {
        Self {
            digest: Sha256::new(),
        }
    }

    fn finalize(self) -> String {
        format!("sha256:{:x}", self.digest.finalize())
    }
}

impl Hasher for Sha256Hasher {
    fn write(&mut self, bytes: &[u8]) {
        self.digest.update(bytes);
    }

    fn finish(&self) -> u64 {
        panic!("Sha256Hasher::finish() should not be called")
    }
}

/// Compute the semantic fingerprint for a compiled database object.
///
/// The fingerprint is a SHA-256 hash of:
/// 1. The compiled SQL statement
/// 2. All indexes, sorted by (cluster, on_name, name, key_parts)
/// 3. All constraints, sorted by (kind, on_name, name, columns)
///
/// Sorting ensures the fingerprint is deterministic regardless of declaration
/// order. Any change to the statement, its indexes, or its constraints
/// produces a different fingerprint, marking the object dirty.
fn compute_semantic_fingerprint(db_obj: &crate::project::ir::compiled::DatabaseObject) -> String {
    let mut hasher = Sha256Hasher::new();
    db_obj.stmt.hash(&mut hasher);

    let mut indexes = db_obj.indexes.clone();
    indexes.sort_by(|a, b| {
        a.in_cluster
            .cmp(&b.in_cluster)
            .then(a.on_name.cmp(&b.on_name))
            .then(a.name.cmp(&b.name))
            .then_with(|| {
                fmt_sql_exprs(a.key_parts.as_deref()).cmp(&fmt_sql_exprs(b.key_parts.as_deref()))
            })
    });
    for index in &indexes {
        index.hash(&mut hasher);
    }

    let mut constraints = db_obj.constraints.clone();
    constraints.sort_by(|a, b| {
        a.kind
            .to_string()
            .cmp(&b.kind.to_string())
            .then(a.on_name.cmp(&b.on_name))
            .then(a.name.cmp(&b.name))
            .then(fmt_idents(&a.columns).cmp(&fmt_idents(&b.columns)))
    });
    for constraint in &constraints {
        constraint.hash(&mut hasher);
    }

    hasher.finalize()
}

fn fmt_sql_exprs(exprs: Option<&[mz_sql_parser::ast::Expr<Raw>]>) -> String {
    exprs
        .unwrap_or(&[])
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn fmt_idents(idents: &[Ident]) -> String {
    idents
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
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

        let project = compile_sync(root, "default", None, &BTreeMap::new()).unwrap();
        let merged = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        )
        .unwrap();

        // The view is in the merged Types map.
        assert!(merged.tables.contains_key("materialize.public.v1"));
        // The table is too (from base_columns).
        assert!(merged.tables.contains_key("materialize.storage.t1"));
    }
}
