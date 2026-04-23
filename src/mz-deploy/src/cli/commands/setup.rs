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

/// Ensure the deployment tracking infrastructure exists.
///
/// Three phases, idempotent on re-run:
/// 1. Create the `_mz_deploy_server` cluster if missing.
/// 2. Create the `_mz_deploy` database and run `setup_schema.sql` (tables,
///    views, indexes) on first creation.
/// 3. Create the three `materialize_*` roles if missing and apply grants.
///
/// Called by both the explicit `setup` command and by other commands that
/// need the infrastructure to be present (`stage`, `promote`, `list`, etc.).
pub async fn ensure(client: &Client) -> Result<(), CliError> {
    // Phase 1: cluster.
    if client
        .introspection()
        .get_cluster(SERVER_CLUSTER_NAME)
        .await?
        .is_none()
    {
        let sql = format!(
            "CREATE CLUSTER {} (SIZE = '{}')",
            quote_identifier(SERVER_CLUSTER_NAME),
            SERVER_CLUSTER_SIZE,
        );
        client.execute(&sql, &[]).await?;
    }

    // Phase 2: database + objects. The SQL file is the single source of
    // truth for the _mz_deploy schema; it runs exactly once per DB lifetime.
    let db_exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM mz_databases WHERE name = '_mz_deploy') AS exists",
            &[],
        )
        .await?
        .get("exists");

    if !db_exists {
        client.execute("CREATE DATABASE _mz_deploy", &[]).await?;
        client
            .batch_execute(include_str!("setup_schema.sql"))
            .await?;
    }

    // Phase 3: roles + grants. GRANTs are safe to re-run and heal drift.
    for (role, role_name) in ALL_ROLES {
        if !client.introspection().role_exists(role_name).await? {
            client
                .execute(&format!("CREATE ROLE {}", role_name), &[])
                .await?;
        }

        // Common: navigation + read through the public view layer.
        for sql in [
            format!(
                "GRANT USAGE ON CLUSTER {} TO {}",
                quote_identifier(SERVER_CLUSTER_NAME),
                role_name
            ),
            format!("GRANT USAGE ON DATABASE _mz_deploy TO {}", role_name),
            format!("GRANT USAGE ON SCHEMA _mz_deploy.public TO {}", role_name),
            format!("GRANT USAGE ON SCHEMA _mz_deploy.tables TO {}", role_name),
            format!(
                "GRANT SELECT ON ALL TABLES IN SCHEMA _mz_deploy.public TO {}",
                role_name
            ),
        ] {
            client.execute(&sql, &[]).await?;
        }

        // Deployer-only: writes on physical tables.
        if *role == MzDeployRole::Deployer {
            client
                .execute(
                    &format!(
                        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES \
                         IN SCHEMA _mz_deploy.tables TO {}",
                        role_name,
                    ),
                    &[],
                )
                .await?;
        }
    }

    Ok(())
}

/// Validate that the current role has a valid mz-deploy role membership.
///
/// The cluster-side checks (`replication_factor`, `USAGE`) are gone because
/// every connection is pinned to `_mz_deploy_server` by `connection.rs`.
/// A missing or unhealthy cluster is surfaced as a connection/query error;
/// `debug` is the diagnostic tool.
///
/// Returns the detected role on success.
pub async fn validate_connection(client: &Client) -> Result<MzDeployRole, CliError> {
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
