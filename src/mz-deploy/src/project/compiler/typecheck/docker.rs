//! Docker-based runtime typechecking.
//!
//! Validates dirty objects against a persistent Materialize container managed
//! by [`super::docker_runtime::DockerRuntime`].
//!
//! ## Strategy
//!
//! All objects are created as **temporary** within a single database session.
//! Names are flattened via [`NormalizingVisitor::flattening`] to avoid schema
//! conflicts (e.g., `db.schema.view` becomes `"db.schema.view"`). Materialized
//! views are lowered to temporary views (query only, no cluster) because the
//! container has no user clusters. The session — and all temporary objects —
//! is torn down when the client disconnects.
//!
//! [`NormalizingVisitor::flattening`]: crate::project::resolve::normalize::NormalizingVisitor::flattening

use super::{
    CompletedState, DepAction, DepContext, IncrementalState, TypeCheckError, TypeCheckErrors,
    TypecheckPlan, TypecheckedObjectArtifact, build_object_paths, build_typecheck_error,
    compute_semantic_fingerprint, fqn_from_object_id, object_kind_for_stmt, plan_dep_creation,
    requires_typecheck, write_typecheck_outputs,
};
use crate::client::Client;
use crate::project::ast::Statement;
use crate::project::ir::compiled::FullyQualifiedName;
use crate::project::ir::object_id::ObjectId;
use crate::project::resolve::normalize::NormalizingVisitor;
use crate::types::{ColumnType, type_hash};
use crate::verbose;
use mz_sql_parser::ast::{ColumnOption, CreateViewStatement, IfExistsBehavior, ViewDefinition};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// Execute the incremental typecheck plan against a connected Materialize
/// container.
///
/// Only dirty objects are validated at runtime; clean objects are stubbed from
/// cached column artifacts. After execution, updated artifacts are persisted
/// to the build artifact database.
///
/// Returns a new [`TypecheckPlan`] with `state: None` (all objects clean) and
/// the merged `cached_types` reflecting the latest validated columns.
pub(super) async fn execute(
    client: &Client,
    project: &super::Project,
    project_root: &Path,
    profile: &str,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
    state: IncrementalState,
    cached_types: &super::Types,
    external_types: &super::Types,
    current_fingerprints: BTreeMap<ObjectId, String>,
) -> Result<TypecheckPlan, TypeCheckError> {
    let object_paths = build_object_paths(project, project_root);
    let sorted_objects = project.get_sorted_objects()?;

    verbose!(
        "Type checking {} runtime object(s) in dependency order",
        sorted_objects.len()
    );

    let completed_state = typecheck_incremental(
        client,
        project,
        project_root,
        &object_paths,
        &sorted_objects,
        cached_types,
        external_types,
        state,
    )
    .await?;

    write_typecheck_outputs(
        project_root,
        profile,
        profile_suffix,
        variables,
        &current_fingerprints,
        &completed_state.updated_artifacts,
    )?;

    Ok(TypecheckPlan {
        state: None,
        cached_types: completed_state.merged_types,
        external_types: external_types.clone(),
        current_fingerprints,
    })
}

async fn typecheck_incremental(
    client: &Client,
    project: &super::Project,
    project_root: &Path,
    object_paths: &BTreeMap<ObjectId, PathBuf>,
    sorted_objects: &[(ObjectId, &crate::project::ir::compiled::DatabaseObject)],
    cached_types: &super::Types,
    external_types: &super::Types,
    state: IncrementalState,
) -> Result<CompletedState, TypeCheckError> {
    let reverse_deps = project.build_reverse_dependency_graph();
    let mut propagator = super::DirtyPropagator::new(
        state.dirty,
        cached_types.clone(),
        reverse_deps,
        state.previous_artifacts,
    );
    let mut created = BTreeSet::new();

    let object_map: BTreeMap<ObjectId, &crate::project::ir::compiled::DatabaseObject> =
        sorted_objects
            .iter()
            .map(|(oid, obj)| (oid.clone(), *obj))
            .collect();

    let ctx = DepContext {
        cached_types,
        external_types,
        object_map: &object_map,
        dependency_graph: &project.dependency_graph,
        object_paths,
        project_root,
    };

    let mut errors = Vec::new();
    for (object_id, typed_object) in sorted_objects {
        if !requires_typecheck(&typed_object.stmt) {
            continue;
        }
        if !propagator.is_dirty(object_id) {
            continue;
        }

        if let Some(deps) = project.dependency_graph.get(object_id) {
            for dep in deps {
                ensure_dep_exists(dep, &mut created, client, &ctx).await?;
            }
        }

        verbose!("Type checking (dirty): {}", object_id);

        let fqn = fqn_from_object_id(object_id);
        if let Some(statement) = create_temporary_view_sql(&typed_object.stmt, &fqn) {
            let sql = statement.to_string();
            match client.execute(&sql, &[]).await {
                Ok(_) => {
                    let new_columns = client.types().query_object_columns(object_id, true).await?;
                    propagator.report_columns(
                        object_id,
                        TypecheckedObjectArtifact {
                            semantic_fingerprint: compute_semantic_fingerprint(typed_object),
                            output_fingerprint: type_hash(&new_columns),
                            columns: new_columns,
                            object_kind: object_kind_for_stmt(&typed_object.stmt),
                        },
                    );
                }
                Err(e) => {
                    let error =
                        build_typecheck_error(object_id, &sql, &e, object_paths, project_root);
                    verbose!("  ✗ Type check failed: {}", error.error_message);
                    errors.push(error);
                }
            }
        }

        created.insert(object_id.to_string());
    }

    if !errors.is_empty() {
        return Err(TypeCheckError::Multiple(TypeCheckErrors { errors }));
    }

    let view_ids: Vec<ObjectId> = sorted_objects
        .iter()
        .filter(|(_, typed_obj)| requires_typecheck(&typed_obj.stmt))
        .map(|(oid, _)| oid.clone())
        .collect();

    Ok(CompletedState {
        merged_types: propagator.into_merged_cache(&view_ids),
        updated_artifacts: propagator.into_updated_artifacts(&view_ids),
    })
}

async fn ensure_dep_exists(
    dep_id: &ObjectId,
    created: &mut BTreeSet<String>,
    client: &Client,
    ctx: &DepContext<'_>,
) -> Result<(), TypeCheckError> {
    let actions = plan_dep_creation(dep_id, created, ctx);
    for action in actions {
        execute_dep_action(&action, created, client, ctx).await?;
    }
    Ok(())
}

/// Execute one planned [`DepAction`] against the Docker client.
async fn execute_dep_action(
    action: &DepAction,
    created: &mut BTreeSet<String>,
    client: &Client,
    ctx: &DepContext<'_>,
) -> Result<(), TypeCheckError> {
    let object_id = action.object_id();
    let fqn = object_id.to_string();
    if created.contains(&fqn) {
        return Ok(());
    }

    match action {
        DepAction::StubInternal(_) => {
            let cached_cols = ctx
                .cached_types
                .get_table(&fqn)
                .expect("cached stub exists");
            create_stub_table(client, &fqn, cached_cols).await?;
        }
        DepAction::StubExternal(_) => {
            let external_cols = ctx
                .external_types
                .get_table(&fqn)
                .expect("external stub exists");
            create_stub_table(client, &fqn, external_cols).await?;
        }
        DepAction::CreateFromAst(id) => {
            let typed_object = ctx
                .object_map
                .get(id)
                .expect("CreateFromAst only emitted when object exists");
            let ast_fqn = fqn_from_object_id(id);
            if let Some(statement) = create_temporary_view_sql(&typed_object.stmt, &ast_fqn) {
                let sql = statement.to_string();
                client.execute(&sql, &[]).await.map_err(|e| {
                    let error =
                        build_typecheck_error(id, &sql, &e, ctx.object_paths, ctx.project_root);
                    TypeCheckError::TypeCheckFailed(error)
                })?;
            }
        }
    }

    created.insert(fqn);
    Ok(())
}

/// Create a temporary stub table with columns from cached or external types.
async fn create_stub_table(
    client: &Client,
    fqn: &str,
    columns: &BTreeMap<String, ColumnType>,
) -> Result<(), TypeCheckError> {
    let mut col_defs = Vec::new();
    for (col_name, col_type) in columns {
        let nullable = if col_type.nullable { "" } else { " NOT NULL" };
        col_defs.push(format!("{} {}{}", col_name, col_type.r#type, nullable));
    }

    let create_sql = format!(
        "CREATE TEMPORARY TABLE \"{}\" ({})",
        fqn,
        col_defs.join(", ")
    );

    client.execute(&create_sql, &[]).await.map_err(|e| {
        TypeCheckError::DatabaseSetupError(format!(
            "failed to create stub table for '{}': {}",
            fqn, e
        ))
    })?;

    Ok(())
}

/// Transform a compiled statement into a temporary object for Docker validation.
///
/// - Views → temporary views with flattened names
/// - Materialized views → temporary views (query only, no cluster or `ASSERT NOT NULL`)
/// - Tables → temporary tables with constraints and options stripped
/// - Other statement types → `None` (not typechecked)
fn create_temporary_view_sql(stmt: &Statement, fqn: &FullyQualifiedName) -> Option<Statement> {
    let mut visitor = NormalizingVisitor::flattening(fqn);

    match stmt {
        Statement::CreateView(view) => {
            let mut view = view.clone();
            view.temporary = true;

            let normalized = Statement::CreateView(view)
                .normalize_name_with(&visitor, &fqn.to_item_name())
                .normalize_dependencies_with(&mut visitor);

            Some(normalized)
        }
        Statement::CreateMaterializedView(mv) => {
            let view_stmt = CreateViewStatement {
                if_exists: IfExistsBehavior::Error,
                temporary: true,
                definition: ViewDefinition {
                    name: mv.name.clone(),
                    columns: mv.columns.clone(),
                    query: mv.query.clone(),
                },
            };
            let normalized = Statement::CreateView(view_stmt)
                .normalize_name_with(&visitor, &fqn.to_item_name())
                .normalize_dependencies_with(&mut visitor);

            Some(normalized)
        }
        Statement::CreateTable(table) => {
            let mut table = table.clone();
            table.temporary = true;
            table.constraints.clear();
            table.with_options.clear();
            for col in &mut table.columns {
                col.options.retain(|opt| {
                    !matches!(
                        opt.option,
                        ColumnOption::ForeignKey { .. } | ColumnOption::Check(_)
                    )
                });
            }
            let normalized =
                Statement::CreateTable(table).normalize_name_with(&visitor, &fqn.to_item_name());
            Some(normalized)
        }
        Statement::CreateTableFromSource(_) => None,
        _ => None,
    }
}
