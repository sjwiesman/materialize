# _mz_deploy_server Cluster Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Have mz-deploy own a dedicated `_mz_deploy_server` cluster (25cc): `setup` creates it and grants `USAGE` to the three `materialize_*` roles; every connection pins to it via libpq options; `debug` reports its health.

**Architecture:** Minimal-surface change. One new constant pair; one override in the connection path that silently injects `cluster=_mz_deploy_server` into the libpq `options` map; one new idempotent step in `setup::ensure`; one simplified `validate_connection`; one new health line in `debug`.

**Tech Stack:** Rust (tokio, tokio-postgres, openssl), Materialize.

Spec: `docs/superpowers/specs/2026-04-23-mz-deploy-server-cluster-design.md`.

**No new unit tests** — the user opted out. Each task ends with `cargo check -p mz-deploy` for compilation, and the final task is manual verification against a running region.

---

## File Map

- `src/mz-deploy/src/client.rs` — add `SERVER_CLUSTER_NAME` / `SERVER_CLUSTER_SIZE` constants (module root, re-exported).
- `src/mz-deploy/src/client/connection.rs` — modify `connect_with_profile` to inject `cluster=_mz_deploy_server` into options.
- `src/mz-deploy/src/cli/commands/setup.rs` — add `ensure_server_cluster()`, wire into `ensure()`; shrink `validate_connection()` body.
- `src/mz-deploy/src/cli/commands/debug.rs` — add `ServerClusterHealth` enum + `check_server_cluster()`; change `DebugOutput.cluster` → `server_cluster_health`.
- `src/mz-deploy/src/cli/help/setup.md` — document new cluster creation.
- `src/mz-deploy/src/cli/help/debug.md` — document server-cluster line.
- `src/mz-deploy/src/cli/help/profiles.md` — note that `[options] cluster` is reserved.

---

### Task 1: Add shared constants

**Files:**
- Modify: `src/mz-deploy/src/client.rs`

- [ ] **Step 1: Add the constants and re-export them**

Open `src/mz-deploy/src/client.rs`. Right after the module declarations / before `pub use crate::config::Profile;` (around line 37), add:

```rust
/// Name of the dedicated cluster mz-deploy creates during `setup` and
/// pins every connection to via libpq options.
pub const SERVER_CLUSTER_NAME: &str = "_mz_deploy_server";

/// Size of the dedicated mz-deploy cluster created during `setup`.
pub const SERVER_CLUSTER_SIZE: &str = "25cc";
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p mz-deploy`
Expected: clean build (no warnings about the unused constants yet — they'll be used in Task 2).

- [ ] **Step 3: Commit**

```bash
git add src/mz-deploy/src/client.rs
git commit -m "feat(mz-deploy/client): add SERVER_CLUSTER_NAME/SIZE constants"
```

---

### Task 2: Force `cluster=_mz_deploy_server` in every connection

**Files:**
- Modify: `src/mz-deploy/src/client/connection.rs`

This is the single behavioral change that makes every command run against `_mz_deploy_server`. Inject the cluster into a cloned options map before the `build_options_string` call. Because `options` is a `BTreeMap`, `insert` overwrites any user-supplied `cluster` key silently.

- [ ] **Step 1: Change the options build to inject the cluster**

In `src/mz-deploy/src/client/connection.rs`, find this block in `connect_with_profile` (around line 99):

```rust
        config.application_name(APPLICATION_NAME);
        if let Some(inner) = build_options_string(&profile.options) {
            config.options(&inner);
        }
```

Replace with:

```rust
        config.application_name(APPLICATION_NAME);

        // Pin every connection to the mz-deploy server cluster via libpq
        // options. Any user-supplied `cluster` in profile.options is silently
        // overridden — mz-deploy owns the cluster it runs on. See the design
        // at docs/superpowers/specs/2026-04-23-mz-deploy-server-cluster-design.md.
        let mut effective_options = profile.options.clone();
        effective_options.insert(
            "cluster".to_string(),
            crate::client::SERVER_CLUSTER_NAME.to_string(),
        );
        if let Some(inner) = build_options_string(&effective_options) {
            config.options(&inner);
        }
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p mz-deploy`
Expected: clean build. The unused-constants warning from Task 1 should be gone.

- [ ] **Step 3: Verify the existing `build_options_string` tests still pass**

Run: `cargo test -p mz-deploy --lib client::connection::tests`
Expected: all pass. (We didn't change `build_options_string` itself; its unit tests still cover its pure behavior.)

- [ ] **Step 4: Commit**

```bash
git add src/mz-deploy/src/client/connection.rs
git commit -m "feat(mz-deploy/client): pin every connection to _mz_deploy_server"
```

---

### Task 3: Add `ensure_server_cluster()` in `setup.rs`

**Files:**
- Modify: `src/mz-deploy/src/cli/commands/setup.rs`

- [ ] **Step 1: Add the function**

Open `src/mz-deploy/src/cli/commands/setup.rs`. Update the imports at the top — after the existing `use crate::...` lines, add:

```rust
use crate::client::{SERVER_CLUSTER_NAME, SERVER_CLUSTER_SIZE, quote_identifier};
```

Then, immediately above the existing `pub async fn ensure` (around line 57), add:

```rust
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
```

- [ ] **Step 2: Wire `ensure_server_cluster` into `ensure()`**

Find the existing `ensure` function body:

```rust
pub async fn ensure(client: &Client) -> Result<(), CliError> {
    client.deployments().create_deployments().await?;
    Ok(())
}
```

Replace with:

```rust
pub async fn ensure(client: &Client) -> Result<(), CliError> {
    client.deployments().create_deployments().await?;
    ensure_server_cluster(client).await?;
    Ok(())
}
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p mz-deploy`
Expected: clean build.

- [ ] **Step 4: Commit**

```bash
git add src/mz-deploy/src/cli/commands/setup.rs
git commit -m "feat(mz-deploy/setup): create _mz_deploy_server cluster and grant USAGE"
```

---

### Task 4: Shrink `validate_connection()` to role-membership only

**Files:**
- Modify: `src/mz-deploy/src/cli/commands/setup.rs`

The cluster checks in `validate_connection` are now redundant — every connection is pinned to `_mz_deploy_server`, and USAGE was granted at `setup`. A missing or unhealthy cluster surfaces as a connection/query error; `debug` is the diagnostic tool.

- [ ] **Step 1: Replace the body of `validate_connection`**

Find the existing `validate_connection` (around line 70). Replace the entire function body (the doc comment can stay but should be updated):

```rust
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
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p mz-deploy`
Expected: clean build. There may be a "unused import" warning if `quote_identifier` or `SERVER_CLUSTER_NAME` are no longer referenced in this file's shrunk body — both ARE still needed by `ensure_server_cluster`. No action.

- [ ] **Step 3: Verify we didn't break `CliError::ClusterNotReady`'s remaining users**

Run: `cargo check -p mz-deploy --tests`
Expected: clean. `ClusterNotReady` survives for debug's Section 6 use.

- [ ] **Step 4: Commit**

```bash
git add src/mz-deploy/src/cli/commands/setup.rs
git commit -m "refactor(mz-deploy/setup): shrink validate_connection to role check only"
```

---

### Task 5: Add `ServerClusterHealth` + check function in `debug.rs`

**Files:**
- Modify: `src/mz-deploy/src/cli/commands/debug.rs`

This task adds only the enum and the helper. Wiring into the output happens in Task 6.

- [ ] **Step 1: Add the enum and helper**

Open `src/mz-deploy/src/cli/commands/debug.rs`. Update the imports at the top; change:

```rust
use crate::client::{Client, Profile};
```

to:

```rust
use crate::client::{Client, Profile, SERVER_CLUSTER_NAME};
```

Then, immediately after the `use` block (before the `DebugOutput` struct), add:

```rust
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
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p mz-deploy`
Expected: clean. One warning about unused function `check_server_cluster` — expected, wired in Task 6.

- [ ] **Step 3: Commit**

```bash
git add src/mz-deploy/src/cli/commands/debug.rs
git commit -m "feat(mz-deploy/debug): add ServerClusterHealth check helper"
```

---

### Task 6: Wire server-cluster health into `debug` output

**Files:**
- Modify: `src/mz-deploy/src/cli/commands/debug.rs`

- [ ] **Step 1: Replace `DebugOutput.cluster` with `server_cluster_health`**

Find the `DebugOutput` struct definition (around line 11). Replace:

```rust
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
```

with:

```rust
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
```

- [ ] **Step 2: Update the `Display` impl for `DebugOutput`**

Replace the existing `impl fmt::Display for DebugOutput` block with:

```rust
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
```

(Note: the existing `Cluster` line is dropped; it was showing `_mz_deploy_server` in every case now.)

- [ ] **Step 3: Update `run()` to add the cluster health check to the parallel join and adapt the tuple shape**

Find the `run()` function (around line 67) and replace its body:

```rust
pub async fn run(settings: &Settings) -> Result<(), CliError> {
    let profile = settings.connection();

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
```

with:

```rust
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
```

- [ ] **Step 4: Replace `connect_and_query` with `query_session_info`**

Find the existing `connect_and_query` function at the bottom of the file and replace it with:

```rust
async fn query_session_info(
    client: &Client,
) -> Result<(String, String, String), CliError> {
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
```

(The `show cluster` query is gone — we now report `_mz_deploy_server` directly via `check_server_cluster`.)

- [ ] **Step 5: Remove the now-unused `Profile` import**

The `Profile` import on the `use crate::client::{...}` line is no longer used inside the file (the old `connect_and_query` took `profile: &Profile`, but `query_session_info` takes `&Client`). Remove `Profile` from the import. The `use` line should now read:

```rust
use crate::client::{Client, SERVER_CLUSTER_NAME};
```

- [ ] **Step 6: Verify it compiles and runs**

Run: `cargo check -p mz-deploy`
Expected: clean build, no unused warnings.

Run: `cargo test -p mz-deploy --lib`
Expected: all existing tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/mz-deploy/src/cli/commands/debug.rs
git commit -m "feat(mz-deploy/debug): report _mz_deploy_server health"
```

---

### Task 7: Update help docs

**Files:**
- Modify: `src/mz-deploy/src/cli/help/setup.md`
- Modify: `src/mz-deploy/src/cli/help/debug.md`
- Modify: `src/mz-deploy/src/cli/help/profiles.md`

- [ ] **Step 1: Update `setup.md`**

Open `src/mz-deploy/src/cli/help/setup.md`. Find the numbered list under `## Behavior`. After item 6 (the GRANTs on the DB/schema/tables), add two items 7 and 8, becoming:

```markdown
7. Creates the `_mz_deploy_server` cluster at size `25cc` (if it doesn't
   exist). mz-deploy pins every connection to this cluster via libpq
   options; it is not intended for general-purpose use. If the cluster
   already exists (e.g. an operator resized it), setup leaves its
   configuration alone.
8. Grants `USAGE` on `_mz_deploy_server` to each of the three roles.
```

- [ ] **Step 2: Update `debug.md`**

Open `src/mz-deploy/src/cli/help/debug.md`. In the numbered list under `## Behavior`, replace item 3's line about "Current cluster" and restructure item 3 as:

Find:

```markdown
3. Queries and displays:
   - Profile name
   - Host and port
   - Environment ID
   - Current cluster
   - Materialize version
   - Current role
```

Replace with:

```markdown
3. Queries and displays:
   - Profile name
   - Host and port
   - Environment ID
   - Materialize version
   - Current role
   - `_mz_deploy_server` health: `healthy`, `not ready` (reason), or `missing`
```

- [ ] **Step 3: Update `profiles.md` to note the reserved `cluster` key**

Open `src/mz-deploy/src/cli/help/profiles.md`. Find the `## Per-profile connection options` section. In the "Rules:" bullet list (around lines 120-128), add a new bullet at the end:

```markdown
- **`cluster` is reserved** — mz-deploy pins every connection to its own
  `_mz_deploy_server` cluster via libpq options. Any `cluster` value you
  set here is silently overridden.
```

- [ ] **Step 4: Commit**

```bash
git add src/mz-deploy/src/cli/help/setup.md src/mz-deploy/src/cli/help/debug.md src/mz-deploy/src/cli/help/profiles.md
git commit -m "docs(mz-deploy): document _mz_deploy_server cluster in help text"
```

---

### Task 8: Manual verification

**Files:** none (verification only).

No unit tests cover this change; manual verification against a running Materialize region is required before merging.

- [ ] **Step 1: Fresh-region setup**

On a Materialize region where `_mz_deploy_server` does not exist:

Run: `cargo run -p mz-deploy -- setup`
Expected: exits 0, prints `Deployment tracking initialized in _mz_deploy database`.

Then connect a superuser psql and run:

```sql
SELECT name, size, replication_factor FROM mz_catalog.mz_clusters WHERE name = '_mz_deploy_server';
```

Expected: one row, `size = 25cc`, `replication_factor = 1`.

Then:

```sql
SHOW GRANTS ON CLUSTER _mz_deploy_server;
```

Expected: `materialize_deployer`, `materialize_developer`, and `materialize_monitor` each have `USAGE`.

- [ ] **Step 2: Setup idempotency**

Run: `cargo run -p mz-deploy -- setup`
Expected: exits 0, no error. Cluster row in `mz_clusters` unchanged.

- [ ] **Step 3: Debug — healthy path**

Run: `cargo run -p mz-deploy -- debug`
Expected output includes `Server cluster: _mz_deploy_server (healthy)` in green.

- [ ] **Step 4: Debug — missing path**

In psql as superuser: `DROP CLUSTER _mz_deploy_server CASCADE;`

Run: `cargo run -p mz-deploy -- debug`
Expected output includes `Server cluster: _mz_deploy_server (missing)` in red, plus `hint: run \`mz-deploy setup\``.

Re-create: `cargo run -p mz-deploy -- setup` → cluster back.

- [ ] **Step 5: Debug — not-ready path**

In psql: `ALTER CLUSTER _mz_deploy_server SET (REPLICATION FACTOR 0);`

Run: `cargo run -p mz-deploy -- debug`
Expected: `Server cluster: _mz_deploy_server (not ready: replication factor is 0)` in yellow, plus hint.

Restore: `ALTER CLUSTER _mz_deploy_server SET (REPLICATION FACTOR 1);`

- [ ] **Step 6: Profile with `cluster` override is silently ignored**

Edit `~/.mz/profiles.toml` to add:

```toml
[default.options]
cluster = "some_other_cluster"
```

Run: `cargo run -p mz-deploy -- debug`
Expected: still shows `_mz_deploy_server` as the cluster (not `some_other_cluster`). Revert the profile edit.

- [ ] **Step 7: A data command still works**

Run any normal command that queries `_mz_deploy.public` (e.g., `cargo run -p mz-deploy -- list`) with a role that has `materialize_deployer`.
Expected: exits 0. (This confirms USAGE on `_mz_deploy_server` is wired correctly.)

- [ ] **Step 8: Final check — no pending changes**

Run: `git status`
Expected: clean working tree.

Run: `git log --oneline efb9b408be..HEAD`
Expected: seven new commits from Tasks 1-7 plus the spec commit.

---

## Notes for the engineer

- **Dependency ordering in `ensure()`**: `create_deployments` runs first (creates `_mz_deploy` DB, tables, roles). `ensure_server_cluster` runs second and needs the roles to exist so the `GRANT USAGE` statements succeed. Don't reorder.
- **Cluster creation doesn't need the cluster to exist first**: `CREATE CLUSTER` is a catalog-level DDL. Running it with the session still bound to `_mz_deploy_server` (which doesn't yet exist) is fine per the user's explicit confirmation during brainstorming.
- **`CliError::ClusterNotReady`**: retained for debug's rendered health message. Don't delete the variant.
- **`--output json`**: `debug`'s JSON output schema changes (`cluster: String` → `server_cluster_health: object`). Flag in release notes.
