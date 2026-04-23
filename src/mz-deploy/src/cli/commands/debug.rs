//! Debug command - test database connection.

use crate::cli::CliError;
use crate::client::{Client, Profile, SERVER_CLUSTER_NAME};
use crate::config::Settings;
use crate::log;
use crate::project::compiler::typecheck::{DockerRuntime, DockerStatus};
use owo_colors::OwoColorize;
use std::fmt;

/// Health of the `_mz_deploy_server` cluster as observed by `debug`.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum ServerClusterHealth {
    /// Cluster exists and has replication_factor > 0.
    Healthy,
    /// Cluster exists but is not usable (e.g., replication_factor == 0).
    NotReady { reason: String },
    /// Cluster is not present in `mz_catalog.mz_clusters`.
    Missing,
}

async fn check_server_cluster(client: &Client) -> Result<ServerClusterHealth, CliError> {
    match client.introspection().get_cluster(SERVER_CLUSTER_NAME).await? {
        None => Ok(ServerClusterHealth::Missing),
        Some(c) if c.replication_factor.unwrap_or(0) > 0 => Ok(ServerClusterHealth::Healthy),
        Some(_) => Ok(ServerClusterHealth::NotReady {
            reason: "replication factor is 0".into(),
        }),
    }
}

#[derive(serde::Serialize)]
struct DebugOutput {
    profile: String,
    host: String,
    port: u16,
    environment_id: String,
    cluster: String,
    version: String,
    role: String,
    docker_status: String,
}

impl fmt::Display for DebugOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}: {}", "Profile".green(), self.profile.cyan())?;
        writeln!(
            f,
            "{} {}:{}",
            "Connected to".green(),
            self.host.cyan(),
            self.port.to_string().cyan()
        )?;
        writeln!(f, "  {}: {}", "Environment".dimmed(), self.environment_id)?;
        writeln!(f, "  {}: {}", "Cluster".dimmed(), self.cluster)?;
        writeln!(f, "  {}: {}", "Version".dimmed(), self.version)?;
        writeln!(f, "  {}: {}", "Role".dimmed(), self.role.yellow())?;

        let docker_label = match self.docker_status.as_str() {
            "running" => format!(
                "{}: {}",
                "Docker".green(),
                "installed, daemon running".green()
            ),
            "not_running" => format!(
                "{}: {}",
                "Docker".green(),
                "installed, daemon not running".yellow()
            ),
            _ => format!("{}: {}", "Docker".green(), "not installed".yellow()),
        };
        write!(f, "{}", docker_label)?;

        Ok(())
    }
}

/// Test database connection with the specified profile.
///
/// # Arguments
/// * `profile` - Database profile containing connection information
///
/// # Returns
/// Ok(()) if connection succeeds
///
/// # Errors
/// Returns `CliError::Connection` if connection fails
pub async fn run(settings: &Settings) -> Result<(), CliError> {
    let profile = settings.connection();

    // Run database connection and Docker check in parallel since they're independent.
    let (db_result, docker_status) = tokio::join!(
        connect_and_query(profile),
        DockerRuntime::check_availability(),
    );

    let (version, environment_id, role, cluster) = db_result?;

    let docker_status_str = match docker_status {
        DockerStatus::Running => "running",
        DockerStatus::NotRunning => "not_running",
        DockerStatus::NotInstalled => "not_installed",
    };

    let output = DebugOutput {
        profile: profile.name.clone(),
        host: profile.host.to_string(),
        port: profile.port,
        environment_id,
        cluster,
        version,
        role,
        docker_status: docker_status_str.to_string(),
    };
    log::output(&output);

    Ok(())
}

async fn connect_and_query(
    profile: &Profile,
) -> Result<(String, String, String, String), CliError> {
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    let row = client
        .query_one(
            r#"
        SELECT
            mz_version() AS version,
            mz_environment_id() AS environment_id,
            current_role() as role"#,
            &[],
        )
        .await?;

    let version: String = row.get("version");
    let environment_id: String = row.get("environment_id");
    let role: String = row.get("role");

    let row = client.query_one("show cluster", &[]).await?;
    let cluster: String = row.get("cluster");

    Ok((version, environment_id, role, cluster))
}
