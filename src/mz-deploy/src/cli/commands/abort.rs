//! Abort command - cleanup a staged deployment.

use crate::cli::CliError;
use crate::cli::progress;
use crate::client::{Client, ConnectionError, DeploymentMode};
use crate::config::Settings;
use crate::log;
use crate::verbose;
use std::fmt;

#[derive(serde::Serialize)]
struct AbortResult {
    deploy_id: String,
    schemas_dropped: usize,
    clusters_dropped: usize,
}

impl fmt::Display for AbortResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "  \u{2713} Successfully aborted deployment '{}'",
            self.deploy_id
        )
    }
}

/// Abort a staged or preview deployment by dropping schemas, clusters, and deployment records.
///
/// This command:
/// - Validates that the deployment exists and hasn't been promoted
/// - Checks role authorization: preview deployments can be aborted by either
///   `materialize_developer` or `materialize_deployer`, while stage deployments
///   require `materialize_deployer`
/// - Drops all staging schemas (with _<deploy_id> suffix)
/// - Drops all staging clusters (with _<deploy_id> suffix)
/// - Deletes deployment tracking records
///
/// # Arguments
/// * `settings` - CLI settings containing connection and directory info
/// * `deploy_id` - Staging deployment ID to abort
///
/// # Returns
/// Ok(()) if abort succeeds
///
/// # Errors
/// Returns `CliError::Connection` if the deployment doesn't exist
/// Returns `CliError::Connection` if the deployment was already promoted
/// Returns `CliError::RoleNotAuthorized` if the user lacks the required role
pub async fn run(settings: &Settings, deploy_id: &str) -> Result<(), CliError> {
    let profile = settings.connection();
    progress::info(&format!("Aborting staged deployment: {}", deploy_id));

    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    super::setup::ensure(&client).await?;
    let role = super::setup::validate_connection(&client).await?;

    // Check role based on deployment mode — preview allows developer or deployer,
    // stage requires deployer only.
    let metadata = client
        .deployments()
        .get_deployment_metadata(deploy_id)
        .await
        .map_err(CliError::Connection)?
        .ok_or_else(|| {
            CliError::Connection(ConnectionError::DeploymentNotFound {
                deploy_id: deploy_id.to_string(),
            })
        })?;

    match metadata.mode {
        DeploymentMode::Preview => {
            if super::setup::require_deployer(role).is_err() {
                super::setup::require_developer(role)?;
            }
        }
        DeploymentMode::Stage => super::setup::require_deployer(role)?,
    }

    // Verify the deployment hasn't been promoted. We skip validate_staging()
    // here because it also rejects preview deployments (which can be aborted
    // but not promoted). We already have the metadata from the role check above.
    if metadata.promoted_at.is_some() {
        return Err(CliError::Connection(
            ConnectionError::DeploymentAlreadyPromoted {
                deploy_id: deploy_id.to_string(),
            },
        ));
    }

    // Get staging schemas and clusters
    let staging_schemas = client
        .introspection()
        .get_staging_schemas(deploy_id)
        .await?;

    let staging_clusters = client
        .introspection()
        .get_staging_clusters(deploy_id)
        .await?;

    verbose!("Dropping staging resources:");
    verbose!("  Schemas: {}", staging_schemas.len());
    verbose!("  Clusters: {}", staging_clusters.len());
    verbose!();

    // Drop staging schemas
    if !staging_schemas.is_empty() {
        verbose!("Dropping staging schemas...");
        client
            .introspection()
            .drop_staging_schemas(&staging_schemas)
            .await?;
        for sq in &staging_schemas {
            verbose!("  Dropped {}.{}", sq.database, sq.schema);
        }
    }

    // Drop staging clusters
    if !staging_clusters.is_empty() {
        verbose!("Dropping staging clusters...");
        client
            .introspection()
            .drop_staging_clusters(&staging_clusters)
            .await?;
        for cluster in &staging_clusters {
            verbose!("  Dropped {}", cluster);
        }
    }

    // Delete deployment records
    verbose!("Deleting deployment records...");

    // Clean up cluster tracking records
    client
        .deployments()
        .delete_deployment_clusters(deploy_id)
        .await
        .map_err(|source| CliError::DeploymentStateWriteFailed { source })?;

    // Clean up pending statements (for sinks)
    client
        .deployments()
        .delete_pending_statements(deploy_id)
        .await
        .map_err(|source| CliError::DeploymentStateWriteFailed { source })?;

    // Clean up replacement MV records
    client
        .deployments()
        .delete_replacement_mvs(deploy_id)
        .await
        .map_err(|source| CliError::DeploymentStateWriteFailed { source })?;

    // Clean up apply state schemas if they exist (from interrupted apply)
    client
        .deployments()
        .delete_apply_state_schemas(deploy_id)
        .await
        .map_err(|source| CliError::DeploymentStateWriteFailed { source })?;

    client.deployments().delete_deployment(deploy_id).await?;

    let result = AbortResult {
        deploy_id: deploy_id.to_string(),
        schemas_dropped: staging_schemas.len(),
        clusters_dropped: staging_clusters.len(),
    };
    log::output(&result);

    Ok(())
}
