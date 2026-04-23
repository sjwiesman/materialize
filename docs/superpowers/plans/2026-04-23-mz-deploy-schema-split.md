# Schema Split & SQL-File DDL Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move all `_mz_deploy` DDL into a SQL file; split base tables into a private `_mz_deploy.tables` schema fronted by views in `_mz_deploy.public`; add indexes on `_mz_deploy_server`; tighten grants so only `materialize_deployer` writes; delete the old `create_deployments` function; pivot three Rust helpers onto new purpose views.

**Architecture:** One new SQL file owns every schema object. `setup.rs::ensure` becomes a linear three-phase function (cluster → database + objects → roles + grants) that `include_str!`'s the SQL and `batch_execute`'s it once when the DB is first created. All SELECTs in mz-deploy continue to target `_mz_deploy.public.<name>` — the view layer is now the stable API. All writes retarget to `_mz_deploy.tables.<name>`.

**Tech Stack:** Rust (tokio-postgres), Materialize SQL.

Spec: `docs/superpowers/specs/2026-04-23-mz-deploy-schema-split-design.md`. No unit tests per user instruction.

---

## File Map

- Create: `src/mz-deploy/src/cli/commands/setup_schema.sql` — all DDL.
- Modify: `src/mz-deploy/src/cli/commands/setup.rs` — new `ensure()` orchestration; delete `ensure_server_cluster`.
- Modify: `src/mz-deploy/src/client/connection.rs` — delete `DeploymentsClient::create_deployments` accessor.
- Modify: `src/mz-deploy/src/client/deployment_ops.rs` — delete `pub(super) async fn create_deployments`; retarget 12 write sites; pivot 3 read helpers.

---

### Task 1: Create `setup_schema.sql`

**Files:**
- Create: `src/mz-deploy/src/cli/commands/setup_schema.sql`

This file contains every table, view, index, and the seed row for the version table. It is committed as a standalone artifact before anything calls it, so compilation stays green.

- [ ] **Step 1: Create the file with full contents**

Create `src/mz-deploy/src/cli/commands/setup_schema.sql` with exactly this content:

```sql
-- Schema for the _mz_deploy tracking database.
--
-- Structure:
-- - _mz_deploy.tables   — base tables. Written only by materialize_deployer.
-- - _mz_deploy.public   — views. Every SELECT in mz-deploy reads from here.
--
-- Why split them: views in _mz_deploy.public form a stable API over the
-- physical schema. We can rename columns, add indexes, or restructure tables
-- in _mz_deploy.tables without breaking older/newer mz-deploy clients that
-- still talk to the same _mz_deploy database — as long as the public views
-- keep their shape. Passthrough views (SELECT column-list FROM ...) make
-- this explicit even where the view is just a facade today.
--
-- Executed once by `mz-deploy setup` via batch_execute the first time the
-- _mz_deploy database is created. Indexes live on the _mz_deploy_server
-- cluster, which setup creates beforehand.

CREATE SCHEMA _mz_deploy.tables;

-- Base tables ---------------------------------------------------------------

CREATE TABLE _mz_deploy.tables.deployments (
    deploy_id   TEXT NOT NULL,
    deployed_at TIMESTAMPTZ NOT NULL,
    promoted_at TIMESTAMPTZ,
    database    TEXT NOT NULL,
    schema      TEXT NOT NULL,
    deployed_by TEXT NOT NULL,
    commit      TEXT,
    kind        TEXT NOT NULL,
    mode        TEXT NOT NULL
) WITH (
    PARTITION BY (deploy_id, deployed_at, promoted_at)
);

CREATE INDEX deployments_deploy_id_idx
    IN CLUSTER _mz_deploy_server
    ON _mz_deploy.tables.deployments (deploy_id);

CREATE TABLE _mz_deploy.tables.objects (
    deploy_id TEXT NOT NULL,
    database  TEXT NOT NULL,
    schema    TEXT NOT NULL,
    object    TEXT NOT NULL,
    hash      TEXT NOT NULL
) WITH (
    PARTITION BY (deploy_id, database, schema)
);

CREATE INDEX objects_deploy_id_idx
    IN CLUSTER _mz_deploy_server
    ON _mz_deploy.tables.objects (deploy_id);

CREATE TABLE _mz_deploy.tables.clusters (
    deploy_id  TEXT NOT NULL,
    cluster_id TEXT NOT NULL
) WITH (
    PARTITION BY (deploy_id)
);

CREATE TABLE _mz_deploy.tables.pending_statements (
    deploy_id      TEXT NOT NULL,
    sequence_num   INT NOT NULL,
    database       TEXT NOT NULL,
    schema         TEXT NOT NULL,
    object         TEXT NOT NULL,
    object_hash    TEXT NOT NULL,
    statement_sql  TEXT NOT NULL,
    statement_kind TEXT NOT NULL,
    executed_at    TIMESTAMPTZ
) WITH (
    PARTITION BY (deploy_id)
);

CREATE INDEX pending_statements_deploy_id_idx
    IN CLUSTER _mz_deploy_server
    ON _mz_deploy.tables.pending_statements (deploy_id);

CREATE TABLE _mz_deploy.tables.replacement_mvs (
    deploy_id          TEXT NOT NULL,
    target_database    TEXT NOT NULL,
    target_schema      TEXT NOT NULL,
    target_name        TEXT NOT NULL,
    replacement_schema TEXT NOT NULL
) WITH (
    PARTITION BY (deploy_id)
);

-- Schema version sentinel. Not used by the current client; reserved for
-- future migrations so a client can discover which _mz_deploy schema
-- revision it's talking to.
CREATE TABLE _mz_deploy.tables.version (
    version BIGINT NOT NULL
);
INSERT INTO _mz_deploy.tables.version VALUES (1);

CREATE INDEX version_idx
    IN CLUSTER _mz_deploy_server
    ON _mz_deploy.tables.version (version);

-- Purpose-built views -------------------------------------------------------

CREATE VIEW _mz_deploy.public.production AS
WITH candidates AS (
    SELECT DISTINCT ON (database, schema)
        database, schema, deploy_id, promoted_at, commit, kind
    FROM _mz_deploy.tables.deployments
    WHERE promoted_at IS NOT NULL
    ORDER BY database, schema, promoted_at DESC
)
SELECT c.database, c.schema, c.deploy_id, c.promoted_at, c.commit, c.kind
FROM candidates c
JOIN mz_schemas s ON c.schema = s.name
JOIN mz_databases d ON c.database = d.name;

CREATE INDEX production_database_schema_idx
    IN CLUSTER _mz_deploy_server
    ON _mz_deploy.public.production (database, schema);

CREATE VIEW _mz_deploy.public.staging_deployments AS
SELECT deploy_id, deployed_at, database, schema, deployed_by, commit, kind, mode
FROM _mz_deploy.tables.deployments
WHERE promoted_at IS NULL;

CREATE INDEX staging_deployments_deploy_id_idx
    IN CLUSTER _mz_deploy_server
    ON _mz_deploy.public.staging_deployments (deploy_id);

CREATE VIEW _mz_deploy.public.deployment_clusters AS
SELECT dc.deploy_id, c.name
FROM _mz_deploy.tables.clusters dc
JOIN mz_catalog.mz_clusters c ON dc.cluster_id = c.id;

CREATE INDEX deployment_clusters_deploy_id_idx
    IN CLUSTER _mz_deploy_server
    ON _mz_deploy.public.deployment_clusters (deploy_id);

CREATE VIEW _mz_deploy.public.missing_clusters AS
SELECT d.deploy_id, dc.cluster_id
FROM _mz_deploy.tables.deployments d
JOIN _mz_deploy.tables.clusters dc USING (deploy_id)
LEFT JOIN mz_catalog.mz_clusters c ON dc.cluster_id = c.id
WHERE d.promoted_at IS NULL AND c.id IS NULL;

CREATE INDEX missing_clusters_deploy_id_idx
    IN CLUSTER _mz_deploy_server
    ON _mz_deploy.public.missing_clusters (deploy_id);

-- Passthrough views ---------------------------------------------------------
-- Column lists are explicit so schema drift in _mz_deploy.tables doesn't
-- silently leak through the public API.

CREATE VIEW _mz_deploy.public.deployments AS
SELECT deploy_id, deployed_at, promoted_at, database, schema, deployed_by,
       commit, kind, mode
FROM _mz_deploy.tables.deployments;

CREATE VIEW _mz_deploy.public.objects AS
SELECT deploy_id, database, schema, object, hash
FROM _mz_deploy.tables.objects;

CREATE VIEW _mz_deploy.public.pending_statements AS
SELECT deploy_id, sequence_num, database, schema, object, object_hash,
       statement_sql, statement_kind, executed_at
FROM _mz_deploy.tables.pending_statements;

CREATE VIEW _mz_deploy.public.replacement_mvs AS
SELECT deploy_id, target_database, target_schema, target_name,
       replacement_schema
FROM _mz_deploy.tables.replacement_mvs;

CREATE VIEW _mz_deploy.public.version AS
SELECT version
FROM _mz_deploy.tables.version;
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p mz-deploy`
Expected: clean build. The SQL file has no call sites yet, so nothing changes in Rust.

- [ ] **Step 3: Commit**

```bash
git add src/mz-deploy/src/cli/commands/setup_schema.sql
git commit -m "feat(mz-deploy/setup): add setup_schema.sql with split schema DDL"
```

---

### Task 2: Rewrite `setup::ensure` to three phases

**Files:**
- Modify: `src/mz-deploy/src/cli/commands/setup.rs`

After this task, `setup::ensure` no longer calls `client.deployments().create_deployments()`. That call site is the only one — `DeploymentsClient::create_deployments` becomes unused, which Task 3 cleans up.

- [ ] **Step 1: Replace the body of `ensure` and delete `ensure_server_cluster`**

Open `src/mz-deploy/src/cli/commands/setup.rs`.

Delete the entire `ensure_server_cluster` function (currently above `ensure`; all lines from its doc comment through the closing brace).

Replace the existing `ensure` function (doc comment + body) with:

```rust
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
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p mz-deploy`
Expected: clean build (warnings are OK — Task 3 cleans up the now-unused `create_deployments`).

- [ ] **Step 3: Commit**

```bash
git add src/mz-deploy/src/cli/commands/setup.rs
git commit -m "feat(mz-deploy/setup): rewrite ensure() as three phases with SQL-file DDL"
```

---

### Task 3: Delete `create_deployments` and its accessor

**Files:**
- Modify: `src/mz-deploy/src/client/deployment_ops.rs`
- Modify: `src/mz-deploy/src/client/connection.rs`

After Task 2, `DeploymentsClient::create_deployments` and the underlying `pub(super) async fn create_deployments` have no callers. This task removes both.

- [ ] **Step 1: Delete the free function in `deployment_ops.rs`**

Open `src/mz-deploy/src/client/deployment_ops.rs`. Delete the entire `pub(super) async fn create_deployments` function. It starts with its doc comment (lines ~155–176, beginning with `/// Ensure the _mz_deploy database and deployment tracking tables exist`) and ends with the closing brace at line ~330 (just before `/// Insert schema deployment records (insert-only, no DELETE).`).

Verify with `grep -n 'create_deployments' src/mz-deploy/src/client/deployment_ops.rs` — should return zero matches after the delete.

- [ ] **Step 2: Delete the accessor in `connection.rs`**

Open `src/mz-deploy/src/client/connection.rs`. Search for `create_deployments` — there should be exactly one match, a method on `impl DeploymentsClient<'_>` that delegates to the free function. Delete that method (doc comment + body, typically a single `pub async fn` with one internal call).

Verify: `grep -rn 'create_deployments' src/mz-deploy/src/` returns zero matches.

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p mz-deploy`
Expected: clean build. If the compiler flags unused imports in `deployment_ops.rs` (e.g., an introspection helper that was only used by the deleted function), delete those imports too.

Run: `cargo test -p mz-deploy --lib`
Expected: all existing tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/mz-deploy/src/client/deployment_ops.rs src/mz-deploy/src/client/connection.rs
git commit -m "refactor(mz-deploy/client): remove create_deployments, DDL now lives in SQL file"
```

---

### Task 4: Retarget write SQL to `_mz_deploy.tables.*`

**Files:**
- Modify: `src/mz-deploy/src/client/deployment_ops.rs`

Twelve string rewrites. All the write-side SQL in `deployment_ops.rs` currently targets `_mz_deploy.public.<table>`; after this task it targets `_mz_deploy.tables.<table>`. Read-side SQL is unchanged.

- [ ] **Step 1: Apply each rewrite**

For each of the twelve sites below, open the file and change the schema name in the SQL string. The line numbers are based on the post-Task-3 file; use them as approximate guides. Search for the quoted SQL fragments to locate each one.

**Rewrites** (search for the "before" fragment; the surrounding SQL stays the same):

| # | Approx line | Before                                                  | After                                                    |
|---|-------------|---------------------------------------------------------|----------------------------------------------------------|
| 1 | ~342 | `INSERT INTO _mz_deploy.public.deployments`                    | `INSERT INTO _mz_deploy.tables.deployments`                     |
| 2 | ~382 | `INSERT INTO _mz_deploy.public.objects`                        | `INSERT INTO _mz_deploy.tables.objects`                         |
| 3 | ~455 | `INSERT INTO _mz_deploy.public.clusters`                       | `INSERT INTO _mz_deploy.tables.clusters`                        |
| 4 | ~534 | `DELETE FROM _mz_deploy.public.clusters WHERE deploy_id = $1` | `DELETE FROM _mz_deploy.tables.clusters WHERE deploy_id = $1`  |
| 5 | ~544 | `UPDATE _mz_deploy.public.deployments`                         | `UPDATE _mz_deploy.tables.deployments`                          |
| 6 | ~557 | `DELETE FROM _mz_deploy.public.deployments WHERE deploy_id = $1` | `DELETE FROM _mz_deploy.tables.deployments WHERE deploy_id = $1` |
| 7 | ~563 | `DELETE FROM _mz_deploy.public.objects WHERE deploy_id = $1`  | `DELETE FROM _mz_deploy.tables.objects WHERE deploy_id = $1`   |
| 8 | ~1301 | `INSERT INTO _mz_deploy.public.pending_statements`            | `INSERT INTO _mz_deploy.tables.pending_statements`              |
| 9 | ~1369 | `UPDATE _mz_deploy.public.pending_statements`                 | `UPDATE _mz_deploy.tables.pending_statements`                   |
| 10 | ~1388 | `DELETE FROM _mz_deploy.public.pending_statements WHERE deploy_id = $1` | `DELETE FROM _mz_deploy.tables.pending_statements WHERE deploy_id = $1` |
| 11 | ~1404 | `INSERT INTO _mz_deploy.public.replacement_mvs`              | `INSERT INTO _mz_deploy.tables.replacement_mvs`                |
| 12 | ~1733 | `DELETE FROM _mz_deploy.public.replacement_mvs WHERE deploy_id = $1` | `DELETE FROM _mz_deploy.tables.replacement_mvs WHERE deploy_id = $1` |

- [ ] **Step 2: Sanity-check the counts**

Run: `grep -c 'INSERT INTO _mz_deploy.tables' src/mz-deploy/src/client/deployment_ops.rs`
Expected: `5`.

Run: `grep -c 'UPDATE _mz_deploy.tables' src/mz-deploy/src/client/deployment_ops.rs`
Expected: `2`.

Run: `grep -c 'DELETE FROM _mz_deploy.tables' src/mz-deploy/src/client/deployment_ops.rs`
Expected: `5`.

Run: `grep -n 'INSERT INTO _mz_deploy.public\|UPDATE _mz_deploy.public\|DELETE FROM _mz_deploy.public' src/mz-deploy/src/client/deployment_ops.rs`
Expected: no output (all writes retargeted).

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p mz-deploy`
Expected: clean build.

Run: `cargo test -p mz-deploy --lib`
Expected: all existing tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/mz-deploy/src/client/deployment_ops.rs
git commit -m "refactor(mz-deploy/client): retarget writes to _mz_deploy.tables schema"
```

---

### Task 5: Pivot three read helpers onto purpose views

**Files:**
- Modify: `src/mz-deploy/src/client/deployment_ops.rs`

Three existing helpers do inline joins/filters that the new purpose views now express directly. Rewriting them is both a simplification and what makes the new view indexes pay off.

- [ ] **Step 1: Rewrite `get_deployment_clusters` (approx line 474)**

Find the function `pub(super) async fn get_deployment_clusters` and replace its inline `query` string with the simpler view-based version. Before:

```rust
    let query = r#"
        SELECT c.name
        FROM _mz_deploy.public.clusters dc
        JOIN mz_catalog.mz_clusters c ON dc.cluster_id = c.id
        WHERE dc.deploy_id = $1
        ORDER BY c.name
    "#;
```

After:

```rust
    let query = r#"
        SELECT name
        FROM _mz_deploy.public.deployment_clusters
        WHERE deploy_id = $1
        ORDER BY name
    "#;
```

(Note that the row-extraction code immediately below — `rows.iter().map(|row| row.get("name"))` — stays the same; only the SQL text changes.)

- [ ] **Step 2: Rewrite `validate_deployment_clusters` (approx line 495)**

Find the function and replace its inline query:

Before:

```rust
    let query = r#"
        SELECT dc.cluster_id
        FROM _mz_deploy.public.clusters dc
        LEFT JOIN mz_catalog.mz_clusters c ON dc.cluster_id = c.id
        WHERE dc.deploy_id = $1 AND c.id IS NULL
    "#;
```

After:

```rust
    let query = r#"
        SELECT cluster_id
        FROM _mz_deploy.public.missing_clusters
        WHERE deploy_id = $1
    "#;
```

The row-extraction (`rows.iter().map(|row| row.get("cluster_id"))`) stays the same.

Note: the new `missing_clusters` view adds a `d.promoted_at IS NULL` filter that the old inline query did not have. This has been pre-verified safe — `validate_deployment_clusters`'s sole caller (`promote.rs:685`) runs after `validate_staging` (`promote.rs:440`), which proves `promoted_at IS NULL`. The extra filter is redundant-but-safe, not a behavior change.

- [ ] **Step 3: Rewrite `list_staging_deployments` (approx line 815)**

Find the function and replace its inline query:

Before (the `query` literal inside the function):

```rust
    let query = r#"
        SELECT deploy_id,
               deployed_at,
               deployed_by,
               commit,
               kind,
               mode,
               database,
               schema
        FROM _mz_deploy.public.deployments
        WHERE promoted_at IS NULL
        ORDER BY deploy_id, database, schema
    "#;
```

After:

```rust
    let query = r#"
        SELECT deploy_id,
               deployed_at,
               deployed_by,
               commit,
               kind,
               mode,
               database,
               schema
        FROM _mz_deploy.public.staging_deployments
        ORDER BY deploy_id, database, schema
    "#;
```

(Same columns; the view's SELECT list matches. The `WHERE promoted_at IS NULL` is baked into the view, so it's dropped from the call site.)

- [ ] **Step 4: Verify it compiles and tests pass**

Run: `cargo check -p mz-deploy`
Expected: clean build.

Run: `cargo test -p mz-deploy --lib`
Expected: all existing tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/mz-deploy/src/client/deployment_ops.rs
git commit -m "refactor(mz-deploy/client): pivot cluster + staging helpers onto purpose views"
```

---

### Task 6: Manual verification

**Files:** none (verification only).

No automated tests cover this change; the plan requires manual verification against a running Materialize region before merging.

- [ ] **Step 1: Fresh-region setup**

On a region where `_mz_deploy` does not yet exist (drop it with `DROP DATABASE _mz_deploy CASCADE` as a superuser if it does):

Run: `cargo run -p mz-deploy -- setup`
Expected: exits 0.

Then as superuser in psql:

```sql
-- Schemas
SELECT name FROM mz_catalog.mz_schemas
WHERE database_id = (SELECT id FROM mz_databases WHERE name = '_mz_deploy');
```
Expected: `public`, `tables`.

```sql
-- Tables (should live in _mz_deploy.tables)
SELECT s.name AS schema, t.name AS table
FROM mz_tables t
JOIN mz_schemas s ON t.schema_id = s.id
JOIN mz_databases d ON s.database_id = d.id
WHERE d.name = '_mz_deploy'
ORDER BY s.name, t.name;
```
Expected rows: `tables | clusters`, `tables | deployments`, `tables | objects`, `tables | pending_statements`, `tables | replacement_mvs`, `tables | version`.

```sql
-- Views (should live in _mz_deploy.public)
SELECT s.name AS schema, v.name AS view
FROM mz_views v
JOIN mz_schemas s ON v.schema_id = s.id
JOIN mz_databases d ON s.database_id = d.id
WHERE d.name = '_mz_deploy'
ORDER BY s.name, v.name;
```
Expected nine rows: `public | deployment_clusters`, `public | deployments`, `public | missing_clusters`, `public | objects`, `public | pending_statements`, `public | production`, `public | replacement_mvs`, `public | staging_deployments`, `public | version`.

```sql
-- Indexes on _mz_deploy_server
SELECT i.name
FROM mz_indexes i
JOIN mz_clusters c ON i.cluster_id = c.id
WHERE c.name = '_mz_deploy_server'
ORDER BY i.name;
```
Expected eight rows: `deployment_clusters_deploy_id_idx`, `deployments_deploy_id_idx`, `missing_clusters_deploy_id_idx`, `objects_deploy_id_idx`, `pending_statements_deploy_id_idx`, `production_database_schema_idx`, `staging_deployments_deploy_id_idx`, `version_idx`.

```sql
-- Version sentinel
SELECT version FROM _mz_deploy.public.version;
```
Expected: `1`.

- [ ] **Step 2: Grant matrix**

```sql
SHOW GRANTS FOR materialize_deployer;
SHOW GRANTS FOR materialize_developer;
SHOW GRANTS FOR materialize_monitor;
```

Verify for each role:
- `USAGE` on cluster `_mz_deploy_server`.
- `USAGE` on database `_mz_deploy`, schemas `_mz_deploy.public` and `_mz_deploy.tables`.
- `SELECT` on all tables (=views) in `_mz_deploy.public`.

Verify **only** for `materialize_deployer`:
- `SELECT, INSERT, UPDATE, DELETE` on tables in `_mz_deploy.tables`.

Verify that `materialize_developer` and `materialize_monitor` do NOT have write grants on `_mz_deploy.tables`.

- [ ] **Step 3: Re-run setup — idempotency**

Run: `cargo run -p mz-deploy -- setup`
Expected: exits 0, no error. No change to DDL (the existence checks short-circuit); grants re-applied silently.

- [ ] **Step 4: Write path works as deployer**

Using a profile whose user has `materialize_deployer` granted, run a full stage + apply + promote against a throwaway project. Verify each command exits 0 and deployment rows appear:

```sql
SELECT deploy_id, promoted_at FROM _mz_deploy.public.deployments ORDER BY deployed_at DESC LIMIT 5;
SELECT deploy_id FROM _mz_deploy.public.staging_deployments;
SELECT deploy_id, database, schema FROM _mz_deploy.public.production;
```

- [ ] **Step 5: Read path works as developer (and writes fail)**

Using a profile whose user has `materialize_developer` (not deployer):

Run: `cargo run -p mz-deploy -- list`
Expected: exits 0, shows staging deployments.

Run: `cargo run -p mz-deploy -- describe <deploy_id>`
Expected: exits 0.

Attempt a manual write from psql as the developer:

```sql
-- Should fail with a permission error.
DELETE FROM _mz_deploy.tables.deployments WHERE deploy_id = 'x';
```
Expected: `ERROR: permission denied for TABLE "deployments"` (or similar).

- [ ] **Step 6: No pending changes / commit summary**

Run: `git status`
Expected: clean working tree.

Run: `git log --oneline 530e0cdc3d..HEAD`
Expected: five task commits (one per Task 1–5), in order.

---

## Notes for the engineer

- **Task ordering is load-bearing.** Task 2 must run before Task 3 (otherwise `create_deployments` becomes a compile error, not just dead code), and Task 4/5 must run after Task 3 to avoid accidentally colliding with the old DDL in the same file.
- **Phase ordering inside `ensure()` is also load-bearing.** The cluster must exist before the SQL creates indexes `IN CLUSTER _mz_deploy_server`. The database must exist before any grant targets `_mz_deploy` or its schemas. Roles must exist before any grant is made to them. The three-phase structure in Task 2 reflects this.
- **The existence check in Phase 2 remains the "have we set this up before" gate.** We intentionally skipped building a migration story (per spec's "no backwards compatibility"). Upgraders `DROP DATABASE _mz_deploy CASCADE` and re-run setup.
- **`batch_execute` runs statements sequentially without a transaction.** If a single statement in `setup_schema.sql` fails, earlier statements will already have committed. For now this is fine because the whole file runs only on a first-time-clean database; a partial failure leaves the operator with a broken state that `DROP DATABASE _mz_deploy CASCADE` cleans up.
- **`SHOW GRANTS` output parsing is eyeball-only** — no scripted check. If you want to scale this, `mz_internal.mz_show_my_cluster_privileges` and friends are queryable.
