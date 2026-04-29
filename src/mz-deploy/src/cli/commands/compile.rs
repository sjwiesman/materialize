// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compile command — validate project and show deployment plan.
//!
//! Compiles the project through a multi-stage pipeline:
//!
//! 1. **Parse** — Load and parse SQL files from the project directory.
//! 2. **Validate** — Check project structure, dependencies, and constraints.
//! 3. **Build graph** — Assemble the dependency-aware project graph.
//! 4. **Typecheck** — Incrementally validate SQL against Materialize. Only
//!    objects whose definitions changed since the last build are re-validated;
//!    unchanged builds skip typechecking entirely.
//! 5. **Post-validate** — Run constraint column validation with full type
//!    metadata now available.
//! 6. **Display** — Print the deployment plan with dependencies and SQL.
//!
//! **Key behavior:** Constraint validation is split across stages. FK target
//! *types* are validated before typechecking (stage 2), while FK *column names*
//! are validated after typechecking (stage 5) once complete column metadata is
//! available.

use crate::cli::CliError;
use crate::cli::progress;
use crate::config::Settings;
use crate::project::ir::graph::{ModStatement, Project};
use crate::project::ir::object_id::ObjectId;
use crate::{project, timing, verbose};
use std::fs::canonicalize;
use std::time::{Duration, Instant};

/// Compile and validate the project, showing the deployment plan.
///
/// This command:
/// - Loads and parses SQL files from the project directory
/// - Validates the project structure and dependencies
/// - Type-checks SQL statements (incremental when possible)
/// - Displays the deployment plan including dependencies and SQL statements
///
/// Type checking uses compiler-owned incremental artifacts to identify dirty
/// runtime objects. Dependencies are restored lazily, and unchanged builds
/// skip type checking entirely.
///
/// # Arguments
/// * `settings` - Resolved project and profile configuration
/// * `show_progress` - If true, displays progress indicators during compilation
///
/// # Returns
/// Compiled planned project ready for deployment
///
/// # Errors
/// Returns `CliError::Project` if compilation or validation fails
pub async fn run(settings: &Settings, show_progress: bool) -> Result<Project, CliError> {
    let settings = settings.clone();
    mz_ore::task::spawn_blocking(
        || "compile-run",
        move || run_inner(&settings, show_progress, false),
    )
    .await
}

/// Compile the project without type checking.
///
/// Used by `apply` commands which create infrastructure objects that don't
/// exist yet in the database — type checking would fail because it validates
/// views against the live catalog, but the tables they reference haven't
/// been created yet.
pub async fn run_without_typecheck(
    settings: &Settings,
    show_progress: bool,
) -> Result<Project, CliError> {
    let settings = settings.clone();
    mz_ore::task::spawn_blocking(
        || "compile-run",
        move || run_inner(&settings, show_progress, false),
    )
    .await
}

fn run_inner(
    settings: &Settings,
    show_progress: bool,
    skip_typecheck: bool,
) -> Result<Project, CliError> {
    let start_time = Instant::now();
    let directory = &settings.directory;

    if show_progress {
        let path = canonicalize(settings.directory.as_path())?;
        progress::stage_start(&format!("Loading project from: {}", path.display()));
    }

    let parse_start = Instant::now();
    let planned_project = project::plan_sync(
        directory.clone(),
        settings.profile_name.clone().as_str(),
        settings.profile_suffix(),
        settings.variables(),
    )?;

    let parse_duration = parse_start.elapsed();
    timing!("project::plan", parse_duration);

    let object_count: usize = planned_project
        .databases
        .iter()
        .flat_map(|db| &db.schemas)
        .map(|schema| schema.objects.len())
        .sum();
    let schema_count: usize = planned_project
        .databases
        .iter()
        .map(|db| db.schemas.len())
        .sum();

    if show_progress {
        progress::stage_success(
            &format!("Found {} objects in {} schemas", object_count, schema_count),
            parse_duration,
        );
    }

    if show_progress {
        progress::stage_start("Validating project structure");
    }
    let validate_start = Instant::now();

    let sorted = planned_project.topological_sort()?;
    let validate_duration = validate_start.elapsed();
    timing!("topological_sort", validate_duration);

    if show_progress {
        progress::stage_success(
            &format!("All {} objects validated", sorted.len()),
            validate_duration,
        );
    }

    if show_progress {
        progress::stage_start("Building dependency graph");
        if !planned_project.external_dependencies.is_empty() {
            progress::info(&format!(
                "{} external dependencies detected",
                planned_project.external_dependencies.len()
            ));
        }
        if !planned_project.cluster_dependencies.is_empty() {
            progress::info(&format!(
                "{} clusters required",
                planned_project.cluster_dependencies.len()
            ));
        }
    }

    let validation = project::analysis::deps::validate_dependencies(
        &settings.dependencies,
        &planned_project.external_dependencies,
    );

    if !validation.unused.is_empty() {
        let mut unused: Vec<_> = validation.unused.iter().collect();
        unused.sort();
        for dep in unused {
            progress::warn(&format!(
                "unused dependency: \"{}\" is declared in project.toml but not referenced",
                dep
            ));
        }
    }

    if !validation.undeclared.is_empty() {
        let mut undeclared: Vec<_> = validation.undeclared.into_iter().collect();
        undeclared.sort();
        return Err(CliError::UndeclaredDependencies { undeclared });
    }

    let types_lock = crate::types::load_types_lock(directory).unwrap_or_default();

    let tc = crate::project_cache::ProjectCache::open(
        directory,
        &settings.profile_name,
        settings.profile_suffix(),
        settings.variables(),
    )
    .ok()
    .flatten();

    validate_constraints_with_types(&planned_project, &types_lock, tc.as_ref())?;

    if !skip_typecheck {
        let typecheck_duration = typecheck_project(settings, &planned_project, show_progress)?;

        if show_progress {
            if let Some(duration) = typecheck_duration {
                progress::stage_success(&format!("{} objects passed", object_count), duration);
            }
        }

        // Post-typecheck column validation: two-tier lookup (TypesCache then types_lock)
        {
            let tc = crate::project_cache::ProjectCache::open(
                directory,
                &settings.profile_name,
                settings.profile_suffix(),
                settings.variables(),
            )
            .ok()
            .flatten();

            let constraint_ids = collect_constraint_fqns(&planned_project);
            let mut column_map = tc
                .as_ref()
                .map(|cache| cache.get_column_names(&constraint_ids.iter().collect::<Vec<_>>()))
                .unwrap_or_default();
            // Add types_lock columns for any objects not in the cache
            for id in &constraint_ids {
                let key = id.to_string().to_lowercase();
                if !column_map.contains_key(&key) {
                    if let Some(cols) = types_lock.get_table(id) {
                        column_map.insert(key, cols.keys().map(|c| c.to_lowercase()).collect());
                    }
                }
            }
            let col_errors =
                project::compiler::validate_constraint_columns(&planned_project, &column_map);
            if !col_errors.is_empty() {
                return Err(project::error::ProjectError::from(
                    project::error::ValidationErrors::new(col_errors),
                )
                .into());
            }
        }
    }

    if show_progress && crate::log::verbose_enabled() {
        print_verbose_details(&planned_project, &sorted);
    }

    if show_progress {
        let total_duration = start_time.elapsed();
        progress::summary("Project successfully compiled", total_duration);
    }

    Ok(planned_project)
}

/// Perform type checking using the in-process catalog backend.
fn typecheck_project(
    settings: &Settings,
    planned_project: &Project,
    show_progress: bool,
) -> Result<Option<Duration>, CliError> {
    let directory = &settings.directory;
    use crate::project::compiler::typecheck;

    if show_progress {
        progress::stage_start("Type checking");
    }
    let typecheck_start = Instant::now();

    let external_types = crate::types::load_types_lock(directory).unwrap_or_default();

    typecheck::run(
        directory,
        &settings.profile_name,
        settings.profile_suffix(),
        settings.variables(),
        planned_project,
        external_types,
    )?;
    timing!("typecheck", typecheck_start.elapsed());

    Ok(Some(typecheck_start.elapsed()))
}

/// Validate FK constraints before runtime typecheck.
///
/// FK target types are validated using all available type information.
/// Column name validation is partial at this stage — only objects with
/// known schemas are checked. Full column validation runs after typecheck
/// produces complete metadata.
fn validate_constraints_with_types(
    planned_project: &Project,
    types_lock: &crate::types::Types,
    types_cache: Option<&crate::project_cache::ProjectCache>,
) -> Result<(), CliError> {
    let get_kind = |id: &ObjectId| -> crate::types::ObjectKind {
        types_cache
            .and_then(|tc| tc.get_kind(id))
            .or_else(|| types_lock.kinds.get(id).copied())
            .unwrap_or(crate::types::ObjectKind::Table)
    };
    let fk_errors = project::compiler::validate_constraint_fk_targets(planned_project, get_kind);
    if !fk_errors.is_empty() {
        return Err(
            project::error::ProjectError::from(project::error::ValidationErrors::new(fk_errors))
                .into(),
        );
    }

    // Pre-typecheck column validation uses types_lock only
    let column_map: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> =
        types_lock
            .tables
            .iter()
            .map(|(id, columns)| {
                let col_names = columns.keys().map(|c| c.to_lowercase()).collect();
                (id.to_string().to_lowercase(), col_names)
            })
            .collect();
    let col_errors = project::compiler::validate_constraint_columns(planned_project, &column_map);
    if !col_errors.is_empty() {
        return Err(
            project::error::ProjectError::from(project::error::ValidationErrors::new(col_errors))
                .into(),
        );
    }

    Ok(())
}

/// Collect all object IDs referenced by constraints in the project.
///
/// Returns IDs for both parent objects (that have constraints) and FK
/// reference targets, enabling targeted column validation.
fn collect_constraint_fqns(planned_project: &Project) -> Vec<ObjectId> {
    let mut ids = std::collections::BTreeSet::new();
    for obj in planned_project.iter_objects() {
        if !obj.typed_object.constraints.is_empty() {
            ids.insert(obj.id.clone());
        }
        for constraint in &obj.typed_object.constraints {
            if let Some(refs) = &constraint.references {
                let ref_name = refs.object.name();
                if ref_name.0.len() == 3 {
                    ids.insert(ObjectId::new(
                        ref_name.0[0].to_string(),
                        ref_name.0[1].to_string(),
                        ref_name.0[2].to_string(),
                    ));
                }
            }
        }
    }
    ids.into_iter().collect()
}

/// Print verbose details about the project (only shown with VERBOSE env var)
fn print_verbose_details(planned_project: &Project, sorted: &[ObjectId]) {
    let mod_stmts = planned_project.iter_mod_statements();
    print_external_dependencies(planned_project);
    print_cluster_dependencies(planned_project);
    print_dependency_graph(planned_project);
    print_deployment_order(sorted);
    print_module_setup_statements(&mod_stmts);
    print_full_sql_plan(&mod_stmts);
    print_sorted_object_sql(planned_project);
}

/// Prints dependencies that are referenced but not declared in this project tree.
///
/// These are the objects operators must provision externally before deployment.
fn print_external_dependencies(planned_project: &Project) {
    if planned_project.external_dependencies.is_empty() {
        return;
    }
    verbose!("\nExternal Dependencies (not defined in this project):");
    let mut external: Vec<_> = planned_project.external_dependencies.iter().collect();
    external.sort();
    for dep in external {
        verbose!("  - {}", dep);
    }
}

/// Prints cluster prerequisites inferred from object and index definitions.
fn print_cluster_dependencies(planned_project: &Project) {
    if planned_project.cluster_dependencies.is_empty() {
        return;
    }
    verbose!("\nCluster Dependencies:");
    let mut clusters: Vec<_> = planned_project.cluster_dependencies.iter().collect();
    clusters.sort_by_key(|c| &c.name);
    for cluster in clusters {
        verbose!("  - {}", cluster.name);
    }
}

/// Prints per-object dependency edges for troubleshooting deployment ordering.
///
/// External dependencies are annotated inline to separate project-internal edges
/// from dependencies that are expected to pre-exist.
fn print_dependency_graph(planned_project: &Project) {
    verbose!("\nDependency Graph:");
    for (object_id, deps) in &planned_project.dependency_graph {
        if deps.is_empty() {
            continue;
        }
        verbose!("  {} depends on:", object_id);
        for dep in deps {
            if planned_project.external_dependencies.contains(dep) {
                verbose!("    - {} (external)", dep);
            } else {
                verbose!("    - {}", dep);
            }
        }
    }
}

/// Prints final object deployment order derived from topological sorting.
fn print_deployment_order(sorted: &[ObjectId]) {
    verbose!("\nDeployment order:");
    for (idx, object_id) in sorted.iter().enumerate() {
        verbose!("  {}. {}", idx + 1, object_id);
    }
}

/// Prints module setup statements that run before object SQL.
///
/// This section shows database/schema-level setup artifacts separately from
/// object creation steps, which helps explain side-effect ordering.
fn print_module_setup_statements(mod_stmts: &[ModStatement]) {
    if mod_stmts.is_empty() {
        return;
    }
    verbose!("\nModule Setup Statements:");
    for (idx, mod_stmt) in mod_stmts.iter().enumerate() {
        match mod_stmt {
            ModStatement::Database {
                database,
                statement,
            } => {
                verbose!("  {}. Database {}: {}", idx + 1, database, statement);
            }
            ModStatement::Schema {
                database,
                schema,
                statement,
            } => {
                verbose!(
                    "  {}. Schema {}.{}: {}",
                    idx + 1,
                    database,
                    schema,
                    statement
                );
            }
        }
    }
}

/// Prints executable SQL for module setup statements in run order.
fn print_full_sql_plan(mod_stmts: &[ModStatement]) {
    verbose!("\nSQL Deployment Plan (fully qualified)");
    for (idx, mod_stmt) in mod_stmts.iter().enumerate() {
        match mod_stmt {
            ModStatement::Database {
                database,
                statement,
            } => {
                verbose!("-- Module Setup {}: Database {}", idx + 1, database);
                verbose!("{};", statement);
                verbose!();
            }
            ModStatement::Schema {
                database,
                schema,
                statement,
            } => {
                verbose!(
                    "-- Module Setup {}: Schema {}.{}",
                    idx + 1,
                    database,
                    schema
                );
                verbose!("{};", statement);
                verbose!();
            }
        }
    }
}

/// Prints full SQL payload for each object deployment step.
///
/// Includes statement SQL and attached indexes/grants/comments so verbose output
/// reflects what the deploy command would execute end-to-end.
fn print_sorted_object_sql(planned_project: &Project) {
    if let Ok(objects) = planned_project.get_sorted_objects() {
        for (idx, (object_id, typed_obj)) in objects.iter().enumerate() {
            verbose!("-- Step {}: {}", idx + 1, object_id);
            verbose!("{};", typed_obj.stmt);
            for index in &typed_obj.indexes {
                verbose!("{};", index);
            }
            for grant in &typed_obj.grants {
                verbose!("{};", grant);
            }
            for comment in &typed_obj.comments {
                verbose!("{};", comment);
            }
            verbose!();
        }
    }
}
