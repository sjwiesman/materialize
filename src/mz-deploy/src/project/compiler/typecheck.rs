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
use owo_colors::OwoColorize;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
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

    #[error("{}", format_multiple(.0))]
    Multiple(Vec<ObjectTypeCheckError>),

    #[error("database error during setup: {0}")]
    DatabaseSetupError(String),

    #[error("failed to get sorted objects: {0}")]
    SortError(#[from] crate::project::error::DependencyError),

    #[error("failed to write types cache: {0}")]
    TypesCacheWriteFailed(#[from] TypesError),
}

fn format_multiple(errors: &[ObjectTypeCheckError]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    for (idx, error) in errors.iter().enumerate() {
        if idx > 0 {
            out.push('\n');
        }
        let _ = write!(&mut out, "{}", error);
    }
    let _ = writeln!(&mut out);
    let _ = writeln!(
        &mut out,
        "could not type check due to {} previous error{}",
        errors.len(),
        if errors.len() == 1 { "" } else { "s" }
    );
    out
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

impl ObjectTypeCheckError {
    /// Build an internal-error variant with no SQL snippet, detail, or hint.
    fn internal(object_id: ObjectId, file_path: PathBuf, error_message: String) -> Self {
        Self {
            object_id,
            file_path,
            sql_statement: String::new(),
            error_message,
            detail: None,
            hint: None,
        }
    }
}

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
    let (base_catalog, base_columns) = base::build_base_catalog(project, &external_types)?;

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
    let node_set: BTreeSet<ObjectId> = node_ids.iter().cloned().collect();

    let reverse_graph = project.build_reverse_dependency_graph();
    let mut direct_deps: BTreeMap<ObjectId, Vec<ObjectId>> = BTreeMap::new();
    let mut dependents: BTreeMap<ObjectId, Vec<ObjectId>> = BTreeMap::new();
    for node_id in &node_ids {
        let node_deps = project
            .dependency_graph
            .get(node_id)
            .into_iter()
            .flatten()
            .filter(|d| node_set.contains(d))
            .cloned()
            .collect();
        direct_deps.insert(node_id.clone(), node_deps);

        let node_dependents = reverse_graph
            .get(node_id)
            .into_iter()
            .flatten()
            .filter(|d| node_set.contains(d))
            .cloned()
            .collect();
        dependents.insert(node_id.clone(), node_dependents);
    }

    let typed_objects = Arc::new(typed_objects);
    let base_columns_arc = Arc::new(base_columns);
    let outcomes = {
        let typed_objects = Arc::clone(&typed_objects);
        let base_catalog = Arc::clone(&base_catalog);
        executor::run::<mz_repr::RelationDesc, _>(
            node_ids.clone(),
            direct_deps,
            dependents,
            move |node_id, dep_results| {
                let db_obj = typed_objects
                    .get(node_id)
                    .expect("typed_object exists for every scheduled node");
                let mut runtime = (*base_catalog).clone();
                for (dep_id, desc) in dep_results {
                    runtime
                        .insert_stub_table_with_desc(dep_id, (**desc).clone())
                        .map_err(|err| match err {
                            TypeCheckError::TypeCheckFailed(e) => e,
                            other => ObjectTypeCheckError::internal(
                                dep_id.clone(),
                                db_obj.path.clone(),
                                format!("failed to stub dependency: {other}"),
                            ),
                        })?;
                }
                let fqn: crate::project::ir::compiled::FullyQualifiedName = node_id.clone().into();
                let ast =
                    catalog::create_catalog_item_ast(&db_obj.stmt, &fqn).ok_or_else(|| {
                        ObjectTypeCheckError::internal(
                            node_id.clone(),
                            db_obj.path.clone(),
                            "internal: failed to build catalog AST".into(),
                        )
                    })?;
                runtime.create_or_replace_item_from_ast(node_id, ast)
            },
        )
    };

    let mut errors: Vec<ObjectTypeCheckError> = Vec::new();
    let mut upsert_rows: Vec<(String, String, BTreeMap<String, ColumnType>)> = Vec::new();
    let mut merged_tables: BTreeMap<ObjectId, BTreeMap<String, ColumnType>> = BTreeMap::new();
    let mut merged_kinds: BTreeMap<ObjectId, ObjectKind> = BTreeMap::new();

    let project_objects_by_id: BTreeMap<&ObjectId, &crate::project::ir::compiled::DatabaseObject> =
        project
            .iter_objects()
            .map(|o| (&o.id, &o.typed_object))
            .collect();
    for (id, columns) in base_columns_arc.iter() {
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
            executor::NodeOutcome::Ok(desc) => {
                let db_obj = typed_objects
                    .get(node_id)
                    .expect("typed_object exists for outcome");
                let kind = db_obj.stmt.kind();
                let columns = catalog::relation_desc_to_columns(desc);
                merged_tables.insert(node_id.clone(), columns.clone());
                merged_kinds.insert(node_id.clone(), kind);
                upsert_rows.push((node_id.to_string(), kind.as_str().to_string(), columns));
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

    // Failed/blocked nodes keep their last successful row by being absent from
    // upsert_rows. The keep-set retains every typecheck-eligible object so
    // prune only deletes rows for objects no longer in the project.
    let mut db = BuildArtifact::open(directory, profile, profile_suffix, variables)
        .map_err(TypesError::from)?;
    db.upsert_typecheck_results(&upsert_rows)
        .map_err(TypesError::from)?;
    let keep: BTreeSet<String> = node_ids.iter().map(|id| id.to_string()).collect();
    db.prune_typecheck_results(&keep)
        .map_err(TypesError::from)?;

    if !errors.is_empty() {
        return Err(TypeCheckError::Multiple(errors));
    }

    Ok(Types {
        version: 1,
        tables: merged_tables,
        kinds: merged_kinds,
        comments: BTreeMap::new(),
    })
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
}
