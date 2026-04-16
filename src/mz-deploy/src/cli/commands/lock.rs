//! Generate data contracts command — resolves declared dependencies from
//! `project.toml` into a `types.lock` file.
//!
//! Reads the `dependencies` list from `project.toml` and queries the target
//! database for each declared object's column schema and kind. Also discovers
//! `CREATE TABLE FROM SOURCE` tables via full project compilation (a
//! lightweight syntax-only path is a planned follow-up). Hard-errors if any
//! declared dependency does not exist in the target database.

use crate::cli::CliError;
use crate::cli::progress;
use crate::client::Client;
use crate::config::Settings;
use crate::project::ir::object_id::ObjectId;

/// Resolve declared dependencies into a types.lock file.
pub async fn run(settings: &Settings) -> Result<(), CliError> {
    let directory = &settings.directory;

    progress::info("Resolving declared dependencies...");

    // Discover source tables via compilation (pragmatic first step;
    // lightweight syntax-only extraction is a follow-up optimization)
    let source_tables = discover_source_tables(settings)?;

    if settings.dependencies.is_empty() && source_tables.is_empty() {
        progress::info("No declared dependencies or source tables found - types.lock not needed");
        return Ok(());
    }

    progress::info(&format!(
        "Found {} declared dependencies and {} source tables",
        settings.dependencies.len(),
        source_tables.len()
    ));

    // Connect to the database
    let profile = settings.connection();
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    // Query types for declared dependencies and source tables.
    // If SHOW COLUMNS fails (e.g., object doesn't exist), catch the error
    // and report it as DeclaredDependenciesMissing with a user-friendly hint
    // instead of a raw database error.
    let declared: Vec<ObjectId> = settings.dependencies.iter().cloned().collect();
    let types = match client
        .types()
        .query_types_for_objects(&declared, &source_tables)
        .await
    {
        Ok(types) => types,
        Err(_) => {
            // Query failed — likely because a declared dependency doesn't exist.
            // Probe each declared dependency individually to identify which are missing.
            let mut missing = Vec::new();
            for dep in &declared {
                let probe = client
                    .types()
                    .query_types_for_objects(std::slice::from_ref(dep), &[])
                    .await;
                if probe.is_err() {
                    missing.push(dep.clone());
                }
            }
            if !missing.is_empty() {
                return Err(CliError::DeclaredDependenciesMissing { missing });
            }
            // If all individual probes succeed, the error was something else
            // (e.g., a source table issue). Re-run to get the original error.
            client
                .types()
                .query_types_for_objects(&declared, &source_tables)
                .await?
        }
    };

    types.write_types_lock(directory)?;

    progress::success(&format!(
        "Successfully generated types.lock with {} object schemas",
        types.tables.len()
    ));

    Ok(())
}

/// Discover CREATE TABLE FROM SOURCE tables by compiling the project.
fn discover_source_tables(settings: &Settings) -> Result<Vec<ObjectId>, CliError> {
    let planned = crate::project::plan_sync(
        &settings.directory,
        &settings.profile_name,
        settings.profile_suffix(),
        settings.variables(),
    )?;
    Ok(planned.get_tables_from_source().collect())
}
