//! Setup command and connection validation for deployment tracking infrastructure.
//!
//! Provides three concerns:
//! - **`setup()`** — Idempotent, self-healing creation of the `_mz_deploy`
//!   database, tables, views, indexes, and roles. The **only** function that
//!   writes to `_mz_deploy`. Invoked exclusively by the explicit `setup` CLI
//!   command, which must run as a superuser: phase 4 issues
//!   `GRANT ... ON SYSTEM` statements (CREATEDB, CREATECLUSTER) that only a
//!   superuser can execute. Safe to re-run.
//! - **`verify()`** — Read-only existence check. Every non-`setup` command
//!   calls this and surfaces `CliError::SetupRequired` if the infrastructure
//!   is missing or partially installed. Never writes.
//! - **`validate_connection()`** — Pre-flight checks that the connected role
//!   has exactly one mz-deploy role membership.
//! - **`run()`** — The `setup` CLI command entry point.

use crate::cli::CliError;
use crate::cli::error::MissingObject;
use crate::client::{
    Client, ConnectionError, SERVER_CLUSTER_NAME, SERVER_CLUSTER_SIZE, quote_identifier,
};
use crate::config::Settings;
use crate::info;
use std::collections::BTreeSet;

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

/// Bring the deployment tracking infrastructure up to the current schema.
///
/// The only function in this crate that mutates `_mz_deploy`. Every statement
/// is idempotent so re-running `setup` against an existing installation
/// heals drift (missing tables, missing roles, missing grants) without
/// losing data.
///
/// Four phases:
/// 1. Create the `_mz_deploy_server` cluster if missing.
/// 2. Create the `_mz_deploy` database (`IF NOT EXISTS`).
/// 3. Run every statement in [`super::setup_schema::SETUP_STATEMENTS`] —
///    each uses `IF NOT EXISTS` so existing objects are left alone. Seed the
///    version row with a pre-check (no `INSERT IF NOT EXISTS` form).
/// 4. Create the three `materialize_*` roles if missing and re-apply grants.
///
/// Must be run by a **superuser**. Phase 4 issues `GRANT ... ON SYSTEM`
/// statements (CREATEDB, CREATECLUSTER) which only superusers can execute,
/// so the function refuses early via [`require_superuser`] when invoked
/// by an ordinary role. The superuser also needs:
/// - Ownership of `_mz_deploy_server` (granted at creation in phase 1) to
///   `GRANT USAGE` on it.
/// - `CREATEDB` to create the database.
/// - `CREATEROLE` to create the roles.
///
/// Ordinary commands do **not** call this function — they call
/// [`verify`] and surface [`CliError::SetupRequired`] if it fails. See the
/// module docs for the full model.
pub async fn setup(client: &Client) -> Result<(), CliError> {
    require_superuser(client).await?;

    // Phase 1: server cluster. `CREATE CLUSTER` has no `IF NOT EXISTS` form,
    // so pre-check. The first create is what makes the calling role the owner,
    // which is required to GRANT USAGE below.
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

    // Phase 2: database.
    client
        .execute("CREATE DATABASE IF NOT EXISTS _mz_deploy", &[])
        .await?;

    // Only the database owner can re-run `setup` — the GRANTs in phase 4
    // require ownership. Refuse early with a message naming the owner so
    // a second admin knows exactly whose hands to transfer ownership from.
    let owner_row = client
        .query_one(
            "SELECT r.name AS owner, current_user() AS current_role \
             FROM mz_databases d \
             JOIN mz_roles r ON d.owner_id = r.id \
             WHERE d.name = '_mz_deploy'",
            &[],
        )
        .await?;
    let owner: String = owner_row.get("owner");
    let current_role: String = owner_row.get("current_role");
    if owner != current_role {
        return Err(CliError::SetupNotDatabaseOwner {
            owner,
            current_role,
        });
    }

    // Phase 3: schema DDL. Each statement uses `IF NOT EXISTS`. Executed one
    // at a time — `batch_execute` wraps multi-statement input in an implicit
    // transaction, which Materialize rejects for DDL.
    for stmt in super::setup_schema::SETUP_STATEMENTS {
        client.execute(*stmt, &[]).await?;
    }

    // Version row is seeded on first setup and left alone thereafter. No
    // `INSERT IF NOT EXISTS` form exists in Materialize, so pre-check.
    let has_version: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM _mz_deploy.tables.version) AS exists",
            &[],
        )
        .await?
        .get("exists");
    if !has_version {
        client
            .execute("INSERT INTO _mz_deploy.tables.version VALUES (1)", &[])
            .await?;
    }

    // Phase 4: roles + grants. GRANTs are safe to re-run.
    for (role, role_name) in ALL_ROLES {
        if !client.introspection().role_exists(role_name).await? {
            client
                .execute(&format!("CREATE ROLE {}", role_name), &[])
                .await?;
        }

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

        if *role == MzDeployRole::Deployer {
            // Promote creates short-lived `apply_<deploy_id>_pre` and
            // `apply_<deploy_id>_post` schemas inside `_mz_deploy` to
            // serialize the apply-state handshake, so the deployer role
            // needs CREATE on the database.
            client
                .execute(
                    &format!("GRANT CREATE ON DATABASE _mz_deploy TO {}", role_name,),
                    &[],
                )
                .await?;
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
            // `stage` and `apply clusters` create project clusters; `promote`
            // creates the short-lived apply schemas under `_mz_deploy`.
            client
                .execute(
                    &format!(
                        "GRANT CREATEDB, CREATECLUSTER ON SYSTEM TO {}",
                        role_name,
                    ),
                    &[],
                )
                .await?;
        } else if *role == MzDeployRole::Developer {
            client
                .execute(
                    &format!(
                        "GRANT SELECT, INSERT, DELETE \
                         ON TABLE _mz_deploy.tables.dev_overlays TO {}",
                        role_name,
                    ),
                    &[],
                )
                .await?;
            // `dev` creates per-developer overlay databases.
            client
                .execute(
                    &format!("GRANT CREATEDB ON SYSTEM TO {}", role_name,),
                    &[],
                )
                .await?;
        }
    }

    Ok(())
}

/// Check that every object `setup` would create is already present.
///
/// Existence-only — never writes, never grants, never checks columns. If the
/// infrastructure was once fully initialized it stays verified unless
/// something is dropped out from under us. Upgrading mz-deploy to a release
/// that adds new tables will trip this check until the admin re-runs
/// `setup`.
///
/// Every non-`setup` command calls this before touching `_mz_deploy`.
pub async fn verify(client: &Client) -> Result<(), CliError> {
    let missing = discover_missing(client).await?;
    if missing.is_empty() {
        return Ok(());
    }
    Err(CliError::SetupRequired { missing })
}

async fn discover_missing(client: &Client) -> Result<Vec<MissingObject>, ConnectionError> {
    let mut missing = Vec::new();

    if client
        .introspection()
        .get_cluster(SERVER_CLUSTER_NAME)
        .await?
        .is_none()
    {
        missing.push(MissingObject::Cluster(SERVER_CLUSTER_NAME.to_string()));
    }

    let db_exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM mz_databases WHERE name = '_mz_deploy') AS exists",
            &[],
        )
        .await?
        .get("exists");

    if !db_exists {
        missing.push(MissingObject::Database("_mz_deploy".to_string()));
    } else {
        let rows = client
            .query(
                "SELECT s.name AS schema_name, o.name AS object_name, o.type AS object_type \
                 FROM mz_objects o \
                 JOIN mz_schemas s ON o.schema_id = s.id \
                 JOIN mz_databases d ON s.database_id = d.id \
                 WHERE d.name = '_mz_deploy' AND s.name IN ('tables', 'public')",
                &[],
            )
            .await?;
        let present: BTreeSet<(String, String)> = rows
            .iter()
            .map(|r| {
                (
                    r.get::<_, String>("schema_name"),
                    r.get::<_, String>("object_name"),
                )
            })
            .collect();

        for (schema, name, kind) in super::setup_schema::EXPECTED_OBJECTS {
            if !present.contains(&(schema.to_string(), name.to_string())) {
                missing.push(MissingObject::SchemaObject {
                    schema: schema.to_string(),
                    name: name.to_string(),
                    kind: kind.to_string(),
                });
            }
        }
    }

    for (_role, role_name) in ALL_ROLES {
        if !client.introspection().role_exists(role_name).await? {
            missing.push(MissingObject::Role(role_name.to_string()));
        }
    }

    Ok(missing)
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
/// Used by `mz-deploy dev`. Strict — only accepts `Developer`; deployers
/// should use `stage` instead.
pub fn require_developer(role: MzDeployRole) -> Result<(), CliError> {
    if role != MzDeployRole::Developer {
        return Err(CliError::RoleNotAuthorized {
            current_role: role.to_string(),
            required_role: "materialize_developer".to_string(),
        });
    }
    Ok(())
}

/// Verify the connecting role is a superuser.
///
/// `setup` issues `GRANT ... ON SYSTEM` statements (CREATEDB, CREATECLUSTER)
/// which only a superuser can execute. Materialize cloud admin users
/// satisfy this through the cloud RBAC layer; on a self-hosted cluster
/// only `mz_system` qualifies.
async fn require_superuser(client: &Client) -> Result<(), CliError> {
    let row = client
        .query_one(
            "SELECT mz_is_superuser() AS is_superuser, \
             current_user() AS current_role",
            &[],
        )
        .await?;
    let is_superuser: bool = row.get("is_superuser");
    if !is_superuser {
        let current_role: String = row.get("current_role");
        return Err(CliError::SetupRequiresSuperuser { current_role });
    }
    Ok(())
}

/// Verify the current role has `CREATEDB` privilege. `mz-deploy dev`
/// calls this before attempting to create overlay databases.
///
/// `sample_overlay_db` is used only for the error message, so the user
/// sees a concrete name they'd fail to create.
pub async fn require_createdb(
    client: &Client,
    current_role: &str,
    sample_overlay_db: &str,
) -> Result<(), CliError> {
    let row = client
        .query_one(
            "SELECT has_system_privilege(current_user, 'CREATEDB') AS has_createdb",
            &[],
        )
        .await?;
    let has: bool = row.get("has_createdb");
    if !has {
        return Err(CliError::MissingCreatedb {
            role: current_role.to_string(),
            overlay_db: sample_overlay_db.to_string(),
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

    setup(&client).await?;

    info!("Deployment tracking initialized in _mz_deploy database");
    Ok(())
}
