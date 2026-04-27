//! Debug command - test database connection.

use crate::cli::CliError;
use crate::client::{Client, SERVER_CLUSTER_NAME};
use crate::config::Settings;
use crate::docker_runtime::{DockerRuntime, DockerStatus};
use crate::log;
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
    match client
        .introspection()
        .get_cluster(SERVER_CLUSTER_NAME)
        .await?
    {
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
    server_cluster_health: ServerClusterHealth,
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
        writeln!(f, "  {}: {}", "Version".dimmed(), self.version)?;
        writeln!(f, "  {}: {}", "Role".dimmed(), self.role.yellow())?;

        let cluster_line = match &self.server_cluster_health {
            ServerClusterHealth::Healthy => format!(
                "{}: {} ({})",
                "Server cluster".green(),
                SERVER_CLUSTER_NAME.cyan(),
                "healthy".green(),
            ),
            ServerClusterHealth::NotReady { reason } => format!(
                "{}: {} ({}: {})\n  hint: run `mz-deploy setup`",
                "Server cluster".green(),
                SERVER_CLUSTER_NAME.cyan(),
                "not ready".yellow(),
                reason,
            ),
            ServerClusterHealth::Missing => format!(
                "{}: {} ({})\n  hint: run `mz-deploy setup`",
                "Server cluster".green(),
                SERVER_CLUSTER_NAME.cyan(),
                "missing".red(),
            ),
        };
        writeln!(f, "{}", cluster_line)?;

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

    // Connect once, then run DB-side queries (version/env/role + cluster health)
    // in parallel with the Docker check.
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    let (session_result, cluster_result, docker_status) = tokio::join!(
        query_session_info(&client),
        check_server_cluster(&client),
        DockerRuntime::check_availability(),
    );

    let (version, environment_id, role) = session_result?;
    let cluster_health = cluster_result?;

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
        server_cluster_health: cluster_health,
        version,
        role,
        docker_status: docker_status_str.to_string(),
    };
    log::output(&output);

    Ok(())
}

async fn query_session_info(client: &Client) -> Result<(String, String, String), CliError> {
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

    Ok((version, environment_id, role))
}
