---
name: mz-deploy server cluster
description: Dedicated _mz_deploy_server cluster that mz-deploy creates at setup and always connects to
type: design
---

# mz-deploy server cluster

## Summary

mz-deploy today requires the operator to pre-provision a Materialize cluster with
`USAGE` granted to the acting role, and documents the pre-flight check that
enforces it. That burden on operators is avoidable: mz-deploy knows exactly what
kind of cluster it wants, so it should own one.

This change has mz-deploy create and use a dedicated `_mz_deploy_server`
cluster (size `25cc`). Every mz-deploy command connects to it by default via
libpq options; `setup` creates it and grants `USAGE` to the three
`materialize_*` roles; `debug` reports its health.

## Goals

- Remove the "which cluster do I put this on?" question from the mz-deploy
  user's setup flow.
- Make cluster membership an implementation detail of mz-deploy, not an
  operator responsibility.
- Keep all existing commands working with zero call-site changes.

## Non-goals

- Managing the cluster's size over its lifetime. Operators may resize manually;
  `setup` does not fight them.
- Supporting multiple concurrent mz-deploy server clusters (e.g. per-env).

## Design

### Constants

Two constants live in `src/mz-deploy/src/client.rs` (the top-level client
module), so both `connection.rs` and `setup.rs` can use them without a
circular dependency:

```rust
pub const SERVER_CLUSTER_NAME: &str = "_mz_deploy_server";
pub const SERVER_CLUSTER_SIZE: &str = "25cc";
```

The leading `_` makes it sort first in `SHOW CLUSTERS` and signals a
system-owned resource.

### `connection.rs`: force the cluster in libpq options

`connect_with_profile(profile)` is unchanged in signature but always forces
`cluster=_mz_deploy_server` via the libpq `options` parameter. Any
user-supplied `cluster` key in `profile.options` is silently overwritten.

Implementation: before calling `build_options_string`, clone `profile.options`
and `insert("cluster", SERVER_CLUSTER_NAME)`. Because `options` is a
`BTreeMap`, the insert overwrites any existing value.

No new error classification is needed. If `_mz_deploy_server` doesn't exist
when a command runs, the first query that needs a cluster surfaces a server
error directly — acceptable for v1.

Every existing call site (`stage`, `promote`, `abort`, `list`, `describe`,
`log`, `apply_all`, `delete`, `executor`, `setup`, `debug`) keeps its current
code unchanged.

### `setup.rs`: create the cluster, grant USAGE

`ensure()` grows one additional step, in order (both idempotent):

1. `create_deployments()` — unchanged; creates `_mz_deploy` DB, tables, roles,
   and DB/schema/table grants.
2. **New:** `ensure_server_cluster()` — creates the cluster if missing, grants
   `USAGE` to the three `materialize_*` roles.

```rust
async fn ensure_server_cluster(client: &Client) -> Result<(), CliError> {
    let exists = client
        .introspection()
        .get_cluster(SERVER_CLUSTER_NAME)
        .await?
        .is_some();

    if !exists {
        client.execute(
            &format!("CREATE CLUSTER {} (SIZE = '{}')",
                quote_identifier(SERVER_CLUSTER_NAME), SERVER_CLUSTER_SIZE),
            &[],
        ).await?;
    }

    for role in ["materialize_deployer", "materialize_developer", "materialize_monitor"] {
        client.execute(
            &format!("GRANT USAGE ON CLUSTER {} TO {}",
                quote_identifier(SERVER_CLUSTER_NAME), role),
            &[],
        ).await?;
    }
    Ok(())
}
```

`GRANT` is safely re-runnable in Materialize, so no per-role existence check is
needed. If the cluster already exists at a different size, `setup` leaves it
alone — operators may have intentionally resized.

**`validate_connection()` shrinks.** Since every command now connects to
`_mz_deploy_server` (enforced by connection.rs) and USAGE was granted at
setup, the cluster-side checks `validate_connection` performed —
`SHOW CLUSTER`, `get_cluster(name)`, `has_cluster_privilege` — become
redundant. A missing or unhealthy cluster is now surfaced as a connection
error, and `debug` is the diagnostic tool.

The new body of `validate_connection`:

```rust
pub async fn validate_connection(client: &Client) -> Result<MzDeployRole, CliError> {
    let mut matched = Vec::new();
    for (role_enum, role_name) in ALL_ROLES {
        let is_member: bool = client.query_one(
            "SELECT pg_has_role(current_role(), $1, 'MEMBER') AS is_member",
            &[&role_name],
        ).await?.get("is_member");
        if is_member {
            matched.push(*role_enum);
        }
    }
    match matched.len() {
        0 => Err(CliError::NoMzDeployRole),
        1 => Ok(matched[0]),
        _ => Err(CliError::MultipleMzDeployRoles {
            roles: matched.iter().map(|r| r.to_string()).collect(),
        }),
    }
}
```

`require_deployer` / `require_developer` are unchanged.

### `debug.rs`: explicit server-cluster health line

`debug` connects the same way as every other command, so the session is already
pinned to `_mz_deploy_server`. The existing `SHOW cluster` call becomes
redundant — it's dropped in favor of an explicit health query against
`_mz_deploy_server`:

```rust
enum ServerClusterHealth {
    Healthy,                              // exists, replication_factor > 0
    NotReady { reason: String },          // exists but replication_factor == 0 / NULL
    Missing,                              // not in mz_catalog.mz_clusters
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
```

Added to the existing `tokio::join!` alongside the DB query and Docker check.

`DebugOutput` replaces its `cluster` field with a `server_cluster_health:
ServerClusterHealth` field. This is a JSON-output schema change for anyone
scripting against `mz-deploy debug --format json`; call out in release notes.

Rendered as:

- **Healthy** — `Server cluster: _mz_deploy_server (healthy)` in green.
- **NotReady** — `Server cluster: _mz_deploy_server (not ready: <reason>)` in
  yellow, with hint `run \`mz-deploy setup\``.
- **Missing** — `Server cluster: _mz_deploy_server (missing)` in red, same
  hint.

### Errors

- `CliError::ClusterNotReady { cluster, reason }` stays. Its only remaining
  user is debug's rendered health message.
- No new variants are needed.

### Docs

- `cli/help/setup.md` — note that `setup` creates `_mz_deploy_server` at 25cc
  and grants `USAGE` to the three roles.
- `cli/help/debug.md` — mention the server-cluster line (if this file exists;
  verify during implementation).
- Any doc that mentions `[profile.options]` — note that `cluster` is reserved
  by mz-deploy and will be silently overridden.

## Testing

No new unit tests. Manual verification during implementation:

1. Fresh region, run `mz-deploy setup` — verify `_mz_deploy_server` is created
   at 25cc and the three roles have `USAGE`.
2. Re-run `setup` — verify it's a no-op (idempotency).
3. `mz-deploy debug` — verify it reports `Server cluster: _mz_deploy_server
   (healthy)`.
4. `DROP CLUSTER _mz_deploy_server` out-of-band, run `debug` — verify it
   reports `missing` with the setup hint.
5. `ALTER CLUSTER _mz_deploy_server SET (REPLICATION FACTOR 0)`, run `debug` —
   verify `not ready` with reason.
6. Profile with `[profile.options] cluster = "foo"` — verify mz-deploy
   commands still land on `_mz_deploy_server`.
7. Profile without a password / with TLS — verify the connection path is
   unchanged.

## Migration

For existing installations where operators have already configured a cluster
and USAGE grants, `setup` remains a safe no-op on the parts that already
exist; the new `ensure_server_cluster` step adds the new cluster and grants.
The operator's previously-chosen cluster is now unused by mz-deploy (but
nothing in mz-deploy drops it). If an operator had `cluster = "..."` in their
profile options expecting that's where mz-deploy ran, it's now ignored; this
is a behavioral change called out in release notes.
