//! Incremental runtime typechecking integrated with the project compiler.
//!
//! This module is the compiler-owned contract for incremental typechecking.
//! Typecheck state is persisted in the build artifact database for the active
//! profile, and planning is driven directly by compiled objects.
//!
//! ## Incremental Algorithm
//!
//! Typechecking runs in two phases:
//!
//! ```text
//! plan()    →  TypecheckPlan { dirty set, cached artifacts }
//! execute() →  TypecheckPlan { state: None, merged types }
//! ```
//!
//! [`plan()`] loads cached artifacts from the build artifact database and compares
//! each object's current semantic fingerprint against the stored one. Objects
//! whose fingerprint changed (or that have no cached artifact) are marked
//! dirty. [`execute()`] dispatches dirty objects to a backend for validation.
//!
//! The unit of persisted typecheck state is a single logical object. Each
//! artifact records:
//!
//! - **semantic fingerprint** — Derived from the object's definition
//!   (statement, indexes, constraints). A change indicates the object needs
//!   re-validation.
//! - **output fingerprint** — Derived from the validated output columns. A
//!   change indicates downstream dependents must also be re-validated.
//! - **columns** — The validated column names, types, and nullability.
//!   Enables constructing dependencies without re-validating them.
//!
//! ## Dirty Propagation
//!
//! [`DirtyPropagator`] is the functional core of incremental validation. When
//! a dirty object is validated, [`report_columns()`](DirtyPropagator::report_columns)
//! compares its new output fingerprint against the previous one. If the output
//! changed, all reverse dependents are marked dirty, causing cascading
//! re-validation in topological order.
//!
//! ## Dependency Stubbing
//!
//! Before validating a dirty object, its dependencies must exist in the
//! backend. [`plan_dep_creation()`] decides how to create each dependency:
//!
//! - **`StubExternal`** — dependency is an external object with a registered
//!   schema in the data contract; provide it using those pre-defined columns.
//! - **`StubInternal`** — dependency is a clean internal object with cached
//!   validation artifacts; provide it using the cached column metadata.
//! - **`CreateFromAst`** — dependency has no cached artifacts (e.g., a newly
//!   added object); validate its transitive dependencies first, then validate
//!   it from its compiled definition.
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
use crate::{timing, verbose};
use mz_sql_parser::ast::{Ident, Raw};
use owo_colors::OwoColorize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use thiserror::Error;

mod catalog;

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
#[derive(Debug)]
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

/// The compiler's incremental typecheck plan for the current invocation.
///
/// When `state` is `None` the plan is up-to-date: all objects matched their
/// cached artifacts and no runtime validation is needed. When `state` is
/// `Some`, the contained [`IncrementalState`] carries the dirty set and
/// previous artifacts needed by the execution backend.
pub(crate) struct TypecheckPlan {
    state: Option<IncrementalState>,
    cached_types: Types,
    external_types: Types,
    current_fingerprints: BTreeMap<ObjectId, String>,
}

impl TypecheckPlan {
    /// Returns true if no runtime validation is needed.
    pub(crate) fn is_up_to_date(&self) -> bool {
        self.state.is_none()
    }
}

/// The dirty frontier and previous artifacts for one typecheck invocation.
///
/// Passed from [`plan()`] to the execution backend when at least one object
/// needs runtime validation.
struct IncrementalState {
    /// Objects whose semantic fingerprint changed or that have no cached artifact.
    dirty: BTreeSet<ObjectId>,
    /// Cached artifacts from the previous successful typecheck, keyed by object ID.
    previous_artifacts: BTreeMap<ObjectId, TypecheckedObjectArtifact>,
}

/// Persisted per-object typecheck artifact stored in the build artifact database.
#[derive(Debug, Clone)]
struct TypecheckedObjectArtifact {
    /// SHA-256 of the compiled statement, sorted indexes, and sorted constraints.
    semantic_fingerprint: String,
    /// Hash of the validated output columns; a change triggers dependent re-validation.
    output_fingerprint: String,
    /// Validated column names, types, and nullability used to stub clean dependencies.
    columns: BTreeMap<String, ColumnType>,
    /// The kind of object (view, materialized view, etc.).
    object_kind: ObjectKind,
}

/// Build an incremental typecheck plan by comparing current semantic
/// fingerprints against cached artifacts in the build artifact database.
///
/// For each object that requires runtime validation (views and materialized
/// views), computes a semantic fingerprint from the compiled statement,
/// sorted indexes, and sorted constraints. An object is dirty if:
///
/// - no cached artifact exists for it, or
/// - the cached artifact's semantic fingerprint differs from the current one.
///
/// Returns a [`TypecheckPlan`] with `state: None` when nothing is dirty,
/// or `state: Some(...)` carrying the dirty set and previous artifacts.
pub(crate) fn plan(
    directory: &Path,
    profile: &str,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
    project: &Project,
    external_types: Types,
) -> Result<TypecheckPlan, TypesError> {
    let db = BuildArtifact::open(directory, profile, profile_suffix, variables)
        .map_err(TypesError::from)?;
    let rows = db.load_typecheck_artifacts().map_err(TypesError::from)?;

    let sort_start = std::time::Instant::now();
    let sorted = project
        .get_sorted_objects()
        .map_err(TypesError::DependencyError)?;
    timing!("  plan: get_sorted", sort_start.elapsed());

    let fingerprint_start = std::time::Instant::now();
    let mut current_fingerprints = BTreeMap::new();
    let mut dirty = BTreeSet::new();
    let mut previous_artifacts = BTreeMap::new();
    let mut cached_tables = BTreeMap::new();
    let mut cached_kinds = BTreeMap::new();

    for (object_id, object) in &sorted {
        if !requires_typecheck(&object.stmt) {
            continue;
        }

        let semantic_fingerprint = compute_semantic_fingerprint(object);
        current_fingerprints.insert(object_id.clone(), semantic_fingerprint.clone());

        let key = object_id.to_string();
        let Some(row) = rows.get(&key) else {
            dirty.insert(object_id.clone());
            continue;
        };

        let artifact = TypecheckedObjectArtifact {
            semantic_fingerprint: row.semantic_fingerprint.clone(),
            output_fingerprint: row.output_fingerprint.clone(),
            columns: row.columns.clone(),
            object_kind: ObjectKind::from_db_str(&row.object_kind),
        };

        cached_tables.insert(key.clone(), artifact.columns.clone());
        cached_kinds.insert(key.clone(), artifact.object_kind);
        previous_artifacts.insert(object_id.clone(), artifact.clone());

        if artifact.semantic_fingerprint != semantic_fingerprint {
            dirty.insert(object_id.clone());
        }
    }

    timing!(
        &format!(
            "  plan: fingerprint_objects ({})",
            current_fingerprints.len()
        ),
        fingerprint_start.elapsed()
    );

    let cached_types = Types {
        version: 1,
        tables: cached_tables,
        kinds: cached_kinds,
        comments: BTreeMap::new(),
    };

    if dirty.is_empty() {
        return Ok(TypecheckPlan {
            state: None,
            cached_types,
            external_types,
            current_fingerprints,
        });
    }

    verbose!("Incremental typecheck: {} dirty object(s)", dirty.len());

    Ok(TypecheckPlan {
        state: Some(IncrementalState {
            dirty,
            previous_artifacts,
        }),
        cached_types,
        external_types,
        current_fingerprints,
    })
}

/// Execute the typecheck plan against the in-process catalog backend.
///
/// If no objects are dirty (`plan.state` is `None`), returns immediately.
pub(crate) fn execute(
    project: &Project,
    project_root: &Path,
    profile: &str,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
    plan: TypecheckPlan,
) -> Result<TypecheckPlan, TypeCheckError> {
    let Some(state) = plan.state else {
        return Ok(plan);
    };

    catalog::execute(
        project,
        project_root,
        profile,
        profile_suffix,
        variables,
        state,
        &plan.cached_types,
        &plan.external_types,
        plan.current_fingerprints,
    )
}

/// Persist updated typecheck artifacts to the build artifact database and prune
/// artifacts for objects no longer in the project.
fn write_typecheck_outputs(
    directory: &Path,
    profile: &str,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
    current_fingerprints: &BTreeMap<ObjectId, String>,
    updated_artifacts: &BTreeMap<ObjectId, TypecheckedObjectArtifact>,
) -> Result<(), TypeCheckError> {
    let mut db = BuildArtifact::open(directory, profile, profile_suffix, variables)
        .map_err(TypesError::from)
        .map_err(TypeCheckError::TypesCacheWriteFailed)?;
    let mut rows = Vec::new();
    let mut keep = BTreeSet::new();
    for (object_id, semantic_fingerprint) in current_fingerprints {
        let Some(artifact) = updated_artifacts.get(object_id) else {
            continue;
        };
        keep.insert(object_id.to_string());
        rows.push((
            object_id.to_string(),
            semantic_fingerprint.clone(),
            artifact.output_fingerprint.clone(),
            artifact.object_kind.as_str().to_string(),
            &artifact.columns,
        ));
    }
    db.upsert_typecheck_results(&rows)
        .map_err(TypesError::from)
        .map_err(TypeCheckError::TypesCacheWriteFailed)?;
    db.prune_typecheck_results(&keep)
        .map_err(TypesError::from)
        .map_err(TypeCheckError::TypesCacheWriteFailed)?;

    Ok(())
}

/// Result of a backend execution pass: merged types and updated artifacts
/// ready to be persisted by [`write_typecheck_outputs()`].
struct CompletedState {
    merged_types: Types,
    updated_artifacts: BTreeMap<ObjectId, TypecheckedObjectArtifact>,
}

/// Functional core of incremental dirty propagation.
///
/// Tracks which objects are dirty and cascades dirtiness through the reverse
/// dependency graph when an object's output columns change. This is the
/// canonical "functional core, imperative shell" pattern: the propagator is
/// pure state management, and the backend modules (`docker`, `in_process`)
/// provide the imperative execution loop.
///
/// ## Invariant
///
/// Objects are processed in topological order. When [`report_columns()`](Self::report_columns)
/// detects that an object's output fingerprint changed compared to its
/// previous artifact, all reverse dependents are added to the dirty set.
/// Because processing is topological, those dependents have not yet been
/// visited and will be re-validated in the same pass.
struct DirtyPropagator {
    dirty: BTreeSet<ObjectId>,
    cached_types: Types,
    reverse_deps: BTreeMap<ObjectId, BTreeSet<ObjectId>>,
    previous_artifacts: BTreeMap<ObjectId, TypecheckedObjectArtifact>,
    rechecked_artifacts: BTreeMap<ObjectId, TypecheckedObjectArtifact>,
}

impl DirtyPropagator {
    fn new(
        dirty: BTreeSet<ObjectId>,
        cached_types: Types,
        reverse_deps: BTreeMap<ObjectId, BTreeSet<ObjectId>>,
        previous_artifacts: BTreeMap<ObjectId, TypecheckedObjectArtifact>,
    ) -> Self {
        Self {
            dirty,
            cached_types,
            reverse_deps,
            previous_artifacts,
            rechecked_artifacts: BTreeMap::new(),
        }
    }

    fn is_dirty(&self, object_id: &ObjectId) -> bool {
        self.dirty.contains(object_id)
    }

    /// Record validated columns for a dirty object.
    ///
    /// Returns `true` if the output fingerprint changed compared to the
    /// previous artifact, meaning reverse dependents were marked dirty.
    fn report_columns(
        &mut self,
        object_id: &ObjectId,
        artifact: TypecheckedObjectArtifact,
    ) -> bool {
        let old_output_fingerprint = self
            .previous_artifacts
            .get(object_id)
            .map(|artifact| artifact.output_fingerprint.clone());

        let propagated = old_output_fingerprint.as_ref() != Some(&artifact.output_fingerprint);
        if propagated {
            if let Some(dependents) = self.reverse_deps.get(object_id) {
                for dep in dependents {
                    self.dirty.insert(dep.clone());
                }
            }
        }

        self.rechecked_artifacts.insert(object_id.clone(), artifact);
        propagated
    }

    /// Build the merged types by overlaying rechecked artifacts on top of
    /// cached types for all view IDs in topological order.
    fn into_merged_cache(&self, sorted_view_ids: &[ObjectId]) -> Types {
        let mut merged_tables = BTreeMap::new();
        let mut merged_kinds = BTreeMap::new();
        for object_id in sorted_view_ids {
            let fqn = object_id.to_string();
            if let Some(artifact) = self.rechecked_artifacts.get(object_id) {
                merged_tables.insert(fqn.clone(), artifact.columns.clone());
                merged_kinds.insert(fqn, artifact.object_kind);
            } else if let Some(cols) = self.cached_types.get_table(&fqn) {
                merged_tables.insert(fqn.clone(), cols.clone());
                merged_kinds.insert(fqn, self.cached_types.get_kind(&object_id.to_string()));
            }
        }
        Types {
            version: 1,
            tables: merged_tables,
            kinds: merged_kinds,
            comments: BTreeMap::new(),
        }
    }

    /// Collect the final artifact map: rechecked artifacts for dirty objects,
    /// previous artifacts for clean objects.
    fn into_updated_artifacts(
        &self,
        sorted_view_ids: &[ObjectId],
    ) -> BTreeMap<ObjectId, TypecheckedObjectArtifact> {
        let mut updated = BTreeMap::new();
        for object_id in sorted_view_ids {
            if let Some(artifact) = self.rechecked_artifacts.get(object_id) {
                updated.insert(object_id.clone(), artifact.clone());
            } else if let Some(artifact) = self.previous_artifacts.get(object_id) {
                updated.insert(object_id.clone(), artifact.clone());
            }
        }
        updated
    }
}

/// Read-only context for dependency planning during typecheck execution.
struct DepContext<'a> {
    cached_types: &'a Types,
    external_types: &'a Types,
    object_map: &'a BTreeMap<ObjectId, &'a crate::project::ir::compiled::DatabaseObject>,
    dependency_graph: &'a BTreeMap<ObjectId, BTreeSet<ObjectId>>,
}

/// A planned action to create a dependency object in the backend.
#[derive(Debug, Clone, PartialEq, Eq)]
enum DepAction {
    /// Create a stub table from cached internal typecheck artifacts.
    StubInternal(ObjectId),
    /// Create a stub table from `types.lock` external type definitions.
    StubExternal(ObjectId),
    /// Create the object from its compiled SQL (used for tables and uncached views).
    CreateFromAst(ObjectId),
}

impl DepAction {
    fn object_id(&self) -> &ObjectId {
        match self {
            DepAction::StubInternal(id)
            | DepAction::StubExternal(id)
            | DepAction::CreateFromAst(id) => id,
        }
    }
}

/// Decide how to create a dependency in the backend.
///
/// Decision tree:
/// - Already created in this session → skip
/// - Has a `types.lock` entry → [`StubExternal`](DepAction::StubExternal)
/// - Is a table → [`CreateFromAst`](DepAction::CreateFromAst)
/// - Is a view/MV with cached artifact → [`StubInternal`](DepAction::StubInternal)
/// - Is a view/MV without cache → recursively plan transitive deps via DFS,
///   then [`CreateFromAst`](DepAction::CreateFromAst)
fn plan_dep_creation(
    dep_id: &ObjectId,
    created: &BTreeSet<String>,
    ctx: &DepContext<'_>,
) -> Vec<DepAction> {
    let dep_fqn = dep_id.to_string();
    if created.contains(&dep_fqn) {
        return Vec::new();
    }

    if ctx.external_types.get_table(&dep_fqn).is_some() {
        return vec![DepAction::StubExternal(dep_id.clone())];
    }

    let Some(typed_object) = ctx.object_map.get(dep_id) else {
        return Vec::new();
    };

    match &typed_object.stmt {
        Statement::CreateTable(_) => vec![DepAction::CreateFromAst(dep_id.clone())],
        stmt if requires_typecheck(stmt) => {
            if ctx.cached_types.get_table(&dep_fqn).is_some() {
                vec![DepAction::StubInternal(dep_id.clone())]
            } else {
                let mut actions = Vec::new();
                let mut visited = BTreeSet::new();
                if let Some(deps) = ctx.dependency_graph.get(dep_id) {
                    for sub_dep in deps {
                        plan_deps_dfs(sub_dep, &mut actions, &mut visited, created, ctx);
                    }
                }
                actions.push(DepAction::CreateFromAst(dep_id.clone()));
                actions
            }
        }
        _ => Vec::new(),
    }
}

/// Recursive DFS helper for [`plan_dep_creation()`] that collects transitive
/// dependency actions for uncached views.
fn plan_deps_dfs(
    dep_id: &ObjectId,
    actions: &mut Vec<DepAction>,
    visited: &mut BTreeSet<String>,
    created: &BTreeSet<String>,
    ctx: &DepContext<'_>,
) {
    let dep_fqn = dep_id.to_string();
    if visited.contains(&dep_fqn) || created.contains(&dep_fqn) {
        return;
    }
    visited.insert(dep_fqn.clone());

    if ctx.external_types.get_table(&dep_fqn).is_some() {
        actions.push(DepAction::StubExternal(dep_id.clone()));
        return;
    }

    let Some(typed_object) = ctx.object_map.get(dep_id) else {
        return;
    };

    match &typed_object.stmt {
        Statement::CreateTable(_) => actions.push(DepAction::CreateFromAst(dep_id.clone())),
        stmt if requires_typecheck(stmt) => {
            if ctx.cached_types.get_table(&dep_fqn).is_some() {
                actions.push(DepAction::StubInternal(dep_id.clone()));
            } else {
                if let Some(deps) = ctx.dependency_graph.get(dep_id) {
                    for sub_dep in deps {
                        plan_deps_dfs(sub_dep, actions, visited, created, ctx);
                    }
                }
                actions.push(DepAction::CreateFromAst(dep_id.clone()));
            }
        }
        _ => {}
    }
}

fn requires_typecheck(stmt: &Statement) -> bool {
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
