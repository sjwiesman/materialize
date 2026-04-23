//! `mz-deploy dev` — developer inner-loop overlay command.
//!
//! Creates per-developer overlay databases (`<base_db>__<profile>`) from
//! the dirty subset of the project's views, materialized views, and indexes.
//! The overlay is drop-and-rebuilt on every invocation.
//!
//! Requires the `materialize_developer` role plus `CREATEDB` at run time.

use std::collections::BTreeSet;

use crate::cli::error::CliError;
use crate::client::{Client, quote_identifier};
use crate::config::Settings;
use crate::project::SchemaQualifier;
use crate::project::analysis::changeset::ChangeSet;
use crate::project::ast::Statement;
use crate::project::ir::compiled::FullyQualifiedName;
use crate::project::ir::graph;
use crate::project::ir::object_id::ObjectId;
use crate::project::resolve::normalize::NormalizingVisitor;
use crate::verbose;

/// Top-level entry point for `mz-deploy dev`.
///
/// Orchestrates role/privilege validation, dirty-set computation, plan
/// printing, and the drop+create DDL phases.
///
/// # Arguments
///
/// * `settings` — loaded project settings (profile, directory, etc.)
/// * `down` — when `true`, only run the drop phase and exit immediately.
/// * `dry_run` — when `true`, print the plan but issue no DDL.
pub async fn run(settings: &Settings, down: bool, dry_run: bool) -> Result<(), CliError> {
    let profile = settings.connection();
    let directory = &settings.directory;

    // Project identifier used in the manifest: directory basename.
    let project_name = directory
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string();
    let profile_name = settings.profile_name.clone();

    // Compile the project (profile_suffix baked into names).
    let planned_project = super::compile::run(settings, true).await?;

    // In-project databases — post-suffix names from the compiled project.
    let in_project_databases: BTreeSet<String> = planned_project
        .databases
        .iter()
        .map(|db| db.name.clone())
        .collect();

    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    // Validate role first, then CREATEDB.
    let role = crate::cli::commands::setup::validate_connection(&client).await?;
    crate::cli::commands::setup::require_developer(role)?;

    let sample_overlay_db = in_project_databases
        .iter()
        .next()
        .map(|db| format!("{}__{}", db, profile_name))
        .unwrap_or_else(|| format!("overlay__{}", profile_name));
    crate::cli::commands::setup::require_createdb(&client, &profile.username, &sample_overlay_db)
        .await?;

    // --down branch: drop everything and exit.
    if down {
        drop_phase(
            &client,
            &profile_name,
            &project_name,
            in_project_databases.iter().map(|s| s.as_str()),
        )
        .await?;
        println!("Overlay removed.");
        return Ok(());
    }

    // Build snapshots and compute the change set.
    let new_snapshot =
        crate::project::analysis::deployment_snapshot::build_snapshot_from_planned(
            &planned_project,
        )?;
    let production_snapshot =
        crate::project::analysis::deployment_snapshot::load_from_database(&client, None).await?;

    // When production is empty, change_set = None → overlay the full project
    // (same as stage's full-deployment path).  No error on first run.
    let change_set = if production_snapshot.objects.is_empty() {
        verbose!("Full deployment: no production deployment found");
        None
    } else {
        Some(ChangeSet::from_deployment_snapshot_comparison(
            &production_snapshot,
            &new_snapshot,
            &planned_project,
        ))
    };

    // Select the initial object set, mirroring stage's select_stage_objects.
    let all_objects = if let Some(ref cs) = change_set {
        if cs.is_empty() {
            vec![]
        } else {
            verbose!("{}", cs);
            planned_project.get_sorted_objects_filtered(&cs.objects_to_deploy)?
        }
    } else {
        planned_project.get_sorted_objects()?
    };

    // Filter to views and materialized views only; silently skip everything else.
    let mut skipped = 0usize;
    let overlay_objects_refs: Vec<_> = all_objects
        .into_iter()
        .filter(|(_, typed_obj)| match &typed_obj.stmt {
            Statement::CreateView(_) | Statement::CreateMaterializedView(_) => true,
            _ => {
                skipped += 1;
                false
            }
        })
        .collect();
    if skipped > 0 {
        verbose!("skipped {} object(s) of unsupported type (tables/sources/sinks)", skipped);
    }

    // Collect object IDs for create_phase and derive dirty_schemas from the
    // filtered set.
    let overlay_object_ids: Vec<ObjectId> = overlay_objects_refs
        .iter()
        .map(|(id, _)| id.clone())
        .collect();
    let dirty_schemas: BTreeSet<SchemaQualifier> = overlay_objects_refs
        .iter()
        .map(|(id, _)| SchemaQualifier {
            database: id.database.clone(),
            schema: id.schema.clone(),
        })
        .collect();

    // Print plan preamble.
    print_plan(&dirty_schemas, &profile_name);

    if dry_run {
        return Ok(());
    }

    // Skip create_phase when the overlay set is empty.
    if dirty_schemas.is_empty() {
        drop_phase(
            &client,
            &profile_name,
            &project_name,
            in_project_databases.iter().map(|s| s.as_str()),
        )
        .await?;
        println!("Dev overlay ready (nothing to overlay).");
        return Ok(());
    }

    // Drop then rebuild.
    drop_phase(
        &client,
        &profile_name,
        &project_name,
        in_project_databases.iter().map(|s| s.as_str()),
    )
    .await?;
    create_phase(
        &client,
        &profile_name,
        &project_name,
        None, // profile_suffix — compiler already applied any suffix
        &in_project_databases,
        &dirty_schemas,
        &overlay_object_ids,
        &planned_project,
    )
    .await?;

    println!("Dev overlay ready.");
    Ok(())
}

/// Print the dev plan preamble: dirty schemas and the overlay database names
/// that will be created.
fn print_plan(dirty_schemas: &BTreeSet<SchemaQualifier>, profile_name: &str) {
    if dirty_schemas.is_empty() {
        println!("Dirty set is empty — nothing to overlay.");
        return;
    }
    println!("→ Dirty schemas:");
    for qual in dirty_schemas {
        println!("    {}.{}", qual.database, qual.schema);
    }

    let overlay_dbs: BTreeSet<String> = dirty_schemas
        .iter()
        .map(|q| format!("{}__{}", q.database, profile_name))
        .collect();
    println!("→ Overlay databases:");
    for db in &overlay_dbs {
        println!("    {}", db);
    }
}

/// Phase 1 of the dev rebuild procedure: drop every overlay database
/// that was recorded for this (profile, project), then do a fallback
/// sweep over the in-project database names to catch overlays whose
/// manifest rows were lost (e.g., catalog restore).
///
/// Leaves the manifest empty for this (profile, project) on return.
pub(crate) async fn drop_phase(
    client: &Client,
    profile_name: &str,
    project_name: &str,
    in_project_databases: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<(), CliError> {
    let overlays = client.dev_overlays();

    // 1. Drop everything the manifest knows about.
    let existing = overlays.list_overlays(profile_name, project_name).await?;
    for db in &existing {
        let sql = format!("DROP DATABASE IF EXISTS {} CASCADE", quote_identifier(db));
        client.execute(&sql, &[]).await?;
    }

    // 2. Purge manifest rows.
    overlays.delete_overlays(profile_name, project_name).await?;

    // 3. Fallback sweep: drop <base_db>__<profile> for every in-project DB
    //    even if the manifest was empty. Idempotent.
    for base_db in in_project_databases {
        let overlay_db = format!("{}__{}", base_db.as_ref(), profile_name);
        let sql = format!(
            "DROP DATABASE IF EXISTS {} CASCADE",
            quote_identifier(&overlay_db),
        );
        client.execute(&sql, &[]).await?;
    }

    Ok(())
}

/// Phase 2 of the dev rebuild procedure: create overlay databases, schemas,
/// and objects for the dirty portion of the project.
///
/// # Steps
///
/// 1. **2a — overlay databases**: For each unique `base_db` found in
///    `dirty_schemas`, issue `CREATE DATABASE IF NOT EXISTS <base_db>__<profile>`
///    and immediately insert a manifest row.  The insert happens *before* any
///    schema or object creation so that the manifest is always a conservative
///    over-approximation — if the process crashes mid-phase the next `drop_phase`
///    will still clean up.
///
/// 2. **2b — overlay schemas**: For each `(base_db, schema)` in `dirty_schemas`,
///    issue `CREATE SCHEMA IF NOT EXISTS <overlay_db>.<schema>`.
///
/// 3. **2c — overlay objects**: Iterate `overlay_objects` in dependency order
///    (via `planned_project.get_sorted_objects_filtered`), rewrite each object's
///    own name and its dependency references through `NormalizingVisitor::overlay`,
///    then execute the resulting `CREATE` statement followed by any associated
///    index statements.
///
/// # Cluster handling
///
/// `normalize_cluster_with` and `normalize_index_clusters` are intentionally
/// **not** called — `dev` passes `IN CLUSTER` references through unchanged.
pub(crate) async fn create_phase(
    client: &Client,
    profile_name: &str,
    project_name: &str,
    profile_suffix: Option<&str>,
    in_project_databases: &BTreeSet<String>,
    dirty_schemas: &BTreeSet<SchemaQualifier>,
    overlay_objects: &[ObjectId],
    planned_project: &graph::Project,
) -> Result<(), CliError> {
    // --- Phase 2a: create overlay databases and record manifest rows. ---
    let mut created_overlay_dbs: BTreeSet<String> = BTreeSet::new();
    for qualifier in dirty_schemas {
        let base_db = &qualifier.database;
        let overlay_db = format!("{}__{}", base_db, profile_name);
        if created_overlay_dbs.insert(overlay_db.clone()) {
            client
                .execute(
                    &format!(
                        "CREATE DATABASE IF NOT EXISTS {}",
                        quote_identifier(&overlay_db),
                    ),
                    &[],
                )
                .await?;
            // Insert manifest row immediately so the manifest is always an
            // over-approximation of what exists.
            client
                .dev_overlays()
                .insert_overlay(profile_name, project_name, &overlay_db)
                .await?;
        }
    }

    // --- Phase 2b: create overlay schemas. ---
    for qualifier in dirty_schemas {
        let overlay_db = format!("{}__{}", &qualifier.database, profile_name);
        let sql = format!(
            "CREATE SCHEMA IF NOT EXISTS {}.{}",
            quote_identifier(&overlay_db),
            quote_identifier(&qualifier.schema),
        );
        client.execute(&sql, &[]).await?;
    }

    // --- Phase 2c: create overlay objects in dependency order. ---
    let overlay_set: BTreeSet<ObjectId> = overlay_objects.iter().cloned().collect();
    let sorted = planned_project.get_sorted_objects_filtered(&overlay_set)?;

    for (object_id, typed_object) in &sorted {
        let original_fqn = FullyQualifiedName::from_object_id(object_id.clone());
        let mut visitor = NormalizingVisitor::overlay(
            &original_fqn,
            profile_name,
            profile_suffix,
            in_project_databases,
            dirty_schemas,
        );

        let stmt = typed_object
            .stmt
            .clone()
            .normalize_name_with(&visitor, &original_fqn.to_item_name())
            .normalize_dependencies_with(&mut visitor);
        // NOTE: normalize_cluster_with is intentionally omitted —
        // dev passes IN CLUSTER references through unchanged.

        client.execute(&stmt.to_string(), &[]).await?;

        // Apply indexes (cluster references also pass through unchanged).
        let mut indexes = typed_object.indexes.clone();
        visitor.normalize_index_references(&mut indexes);
        // NOTE: normalize_index_clusters is intentionally omitted.
        for index in &indexes {
            client.execute(&index.to_string(), &[]).await?;
        }
    }

    Ok(())
}
