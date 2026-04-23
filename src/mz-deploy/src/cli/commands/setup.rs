//! Setup command and connection validation for deployment tracking infrastructure.
//!
//! Provides three concerns:
//! - **`ensure()`** — Idempotent creation of the `_mz_deploy` database and tables.
//! - **`validate_connection()`** — Pre-flight checks that the connected cluster is
//!   usable and the current role has exactly one mz-deploy role membership.
//! - **`run()`** — The `setup` CLI command entry point.

use crate::cli::CliError;
use crate::client::{Client, SERVER_CLUSTER_NAME, SERVER_CLUSTER_SIZE, quote_identifier};
use crate::config::Settings;
use crate::info;

/// The mz-deploy role assigned to the current database user.
///
/// Every non-setup command requires the connected role to be a member of exactly
/// one of these three roles. Having zero or multiple memberships is an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MzDeployRole {
    /// Can apply infrastructure, delete objects, and stage/promote/abort deployments.
    Deployer,
    /// Read-only access to deployment state (list, describe, log).
    Developer,
    /// Read-only monitoring access to deployment state.
    Monitor,
}

impl MzDeployRole {
    /// Role name as it appears in Materialize.
    pub fn role_name(&self) -> &'static str {
        match self {
            Self::Deployer => "materialize_deployer",
            Self::Developer => "materialize_developer",
            Self::Monitor => "materialize_monitor",
        }
    }
}

impl std::fmt::Display for MzDeployRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.role_name())
    }
}

/// All mz-deploy roles in check order.
const ALL_ROLES: &[(MzDeployRole, &str)] = &[
    (MzDeployRole::Deployer, "materialize_deployer"),
    (MzDeployRole::Developer, "materialize_developer"),
    (MzDeployRole::Monitor, "materialize_monitor"),
];

/// Create the `_mz_deploy_server` cluster if missing and grant `USAGE` on it
/// to the three `materialize_*` roles. Idempotent.
///
/// If the cluster already exists at a different size, leave it alone — operators
/// may have intentionally resized. GRANTs are safe to re-run.
async fn ensure_server_cluster(client: &Client) -> Result<(), CliError> {
    let exists = client
        .introspection()
        .get_cluster(SERVER_CLUSTER_NAME)
        .await?
        .is_some();

    if !exists {
        let sql = format!(
            "CREATE CLUSTER {} (SIZE = '{}')",
            quote_identifier(SERVER_CLUSTER_NAME),
            SERVER_CLUSTER_SIZE,
        );
        client.execute(&sql, &[]).await?;
    }

    for (_, role_name) in ALL_ROLES {
        let sql = format!(
            "GRANT USAGE ON CLUSTER {} TO {}",
            quote_identifier(SERVER_CLUSTER_NAME),
            role_name,
        );
        client.execute(&sql, &[]).await?;
    }

    Ok(())
}

/// Ensure the deployment tracking infrastructure exists.
///
/// This is the shared entry point used by both the explicit `setup` command
/// and by other commands (`stage`, `promote`, `list`, `describe`, `log`)
/// that need the `_mz_deploy` database to be present.
pub async fn ensure(client: &Client) -> Result<(), CliError> {
    client.deployments().create_deployments().await?;
    ensure_server_cluster(client).await?;
    Ok(())
}

/// Validate that the current connection is in a good state for mz-deploy operations.
///
/// Checks:
/// 1. The connected cluster has replication factor > 0.
/// 2. The current role has USAGE privilege on the cluster.
/// 3. The current role is a member of exactly one mz-deploy role.
///
/// Returns the detected role on success.
pub async fn validate_connection(client: &Client) -> Result<MzDeployRole, CliError> {
    // 1. Check cluster replication factor
    let row = client.query_one("SHOW CLUSTER", &[]).await?;
    let cluster_name: String = row.get("cluster");

    let cluster = client
        .introspection()
        .get_cluster(&cluster_name)
        .await?
        .ok_or_else(|| CliError::Message(format!("cluster '{}' does not exist", cluster_name)))?;

    if cluster.replication_factor == Some(0) || cluster.replication_factor.is_none() {
        return Err(CliError::ClusterNotReady {
            cluster: cluster_name,
            reason: "replication factor is 0".to_string(),
        });
    }

    // 2. Check USAGE privilege on the cluster
    let has_usage: bool = client
        .query_one(
            "SELECT has_cluster_privilege(current_role(), $1, 'USAGE') AS has_usage",
            &[&cluster_name],
        )
        .await?
        .get("has_usage");

    if !has_usage {
        return Err(CliError::ClusterNotReady {
            cluster: cluster_name,
            reason: "current role does not have USAGE privilege".to_string(),
        });
    }

    // 3. Check role membership — exactly one of the three mz-deploy roles
    let mut matched_roles = Vec::new();
    for (role_enum, role_name) in ALL_ROLES {
        let row = client
            .query_one(
                "SELECT pg_has_role(current_role(), $1, 'MEMBER') AS is_member",
                &[&role_name],
            )
            .await?;
        let is_member: bool = row.get("is_member");
        if is_member {
            matched_roles.push(*role_enum);
        }
    }

    match matched_roles.len() {
        0 => Err(CliError::NoMzDeployRole),
        1 => Ok(matched_roles[0]),
        _ => Err(CliError::MultipleMzDeployRoles {
            roles: matched_roles.iter().map(|r| r.to_string()).collect(),
        }),
    }
}

/// Require that the validated role is `Deployer`.
///
/// Used by all state-mutating commands: `stage`, `promote`, `abort`,
/// all `apply` variants, and `delete`.
pub fn require_deployer(role: MzDeployRole) -> Result<(), CliError> {
    if role != MzDeployRole::Deployer {
        return Err(CliError::RoleNotAuthorized {
            current_role: role.to_string(),
            required_role: "materialize_deployer".to_string(),
        });
    }
    Ok(())
}

/// Require that the validated role is `Developer`.
///
/// Used by `preview` command. Strict — only accepts `Developer`,
/// deployers should use `stage` instead.
pub fn require_developer(role: MzDeployRole) -> Result<(), CliError> {
    if role != MzDeployRole::Developer {
        return Err(CliError::RoleNotAuthorized {
            current_role: role.to_string(),
            required_role: "materialize_developer".to_string(),
        });
    }
    Ok(())
}

/// Initialize deployment tracking database and tables.
///
/// Connects to Materialize and creates the `_mz_deploy` database with all
/// required tracking tables if they don't already exist.
///
/// # Arguments
/// * `settings` - Application settings with connection profile
///
/// # Errors
/// Returns `CliError::Connection` if the database connection fails
pub async fn run(settings: &Settings) -> Result<(), CliError> {
    let profile = settings.connection();
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    ensure(&client).await?;

    info!("Deployment tracking initialized in _mz_deploy database");
    Ok(())
}
