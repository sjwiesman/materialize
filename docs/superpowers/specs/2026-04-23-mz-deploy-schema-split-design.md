---
name: mz-deploy schema split and SQL-file DDL
description: Move _mz_deploy DDL to an included SQL file, split base tables into a private `_mz_deploy.tables` schema with views fronting them from `_mz_deploy.public`, add indexes on the server cluster, and tighten grants
type: design
---

# mz-deploy schema split and SQL-file DDL

## Summary

This builds on the just-landed `_mz_deploy_server` cluster. It makes three
changes, bundled because they compose:

1. **DDL moves to a SQL file.** The ~150 lines of inline `CREATE TABLE` /
   `CREATE VIEW` strings in `deployment_ops.rs::create_deployments` move into
   `src/mz-deploy/src/cli/commands/setup_schema.sql`, included via
   `include_str!` and executed via `client.batch_execute`.

2. **Schema split.** Base tables move from `_mz_deploy.public` to a new
   `_mz_deploy.tables` schema. Every `SELECT` in mz-deploy reads from
   `_mz_deploy.public`, which now contains only views. Purpose views carry
   joins/filters; passthrough views with explicit column lists front the
   tables for direct reads. The views are our stable API: we can reshape
   tables under them without breaking older/newer client versions that talk
   to the same `_mz_deploy` database.

3. **Indexes on `_mz_deploy_server`.** Tables and purpose views get indexes
   on their dominant lookup keys (almost all `deploy_id`).

Setup orchestration flattens in `setup.rs::ensure`: cluster first, then
database + objects (via the SQL file), then roles + grants. No more
`DeploymentsClient::create_deployments` indirection. Grants tighten so that
developer/monitor roles no longer have write access to the physical tables.

Backwards compatibility: none required. Users on the old schema must
`DROP DATABASE _mz_deploy CASCADE` and re-run `mz-deploy setup`.

## Goals

- One file owns every schema object mz-deploy needs (`setup_schema.sql`).
- Views are the public API; base tables are an implementation detail.
- Setup is a single, linear function; no sub-client indirection for DDL.
- Indexes on the server cluster make point lookups O(1).

## Non-goals

- Schema migrations. The `version` table is seeded but otherwise inert.
- Backwards compatibility with the old `_mz_deploy.public.<table>` layout.
- Changing behavior of stage/promote/apply/abort flows. This is a
  storage-layer refactor with the same observable semantics.

## Design

### File layout

- `src/mz-deploy/src/cli/commands/setup.rs` — owns all setup orchestration.
- `src/mz-deploy/src/cli/commands/setup_schema.sql` — all DDL for
  `_mz_deploy.tables.*` (tables + indexes) and `_mz_deploy.public.*` (views
  + indexes). Included via `include_str!`.

The `deployment_ops.rs` module drops `pub(super) async fn create_deployments`
and the corresponding `DeploymentsClient::create_deployments` accessor.
Other `DeploymentsClient` methods stay (they have multiple callers; only
`create_deployments` was used solely by setup).

### The new `ensure()` flow

Three linear phases, inlined in `setup.rs::ensure` (no per-phase helpers
unless clarity demands):

1. **Cluster.** If `_mz_deploy_server` is missing, `CREATE CLUSTER … (SIZE = '25cc')`.
   No grants yet — roles may not exist.

2. **Database + objects.** If `_mz_deploy` is missing, `CREATE DATABASE`
   then `batch_execute(include_str!("setup_schema.sql"))`. Already-created
   databases are a no-op at this step (the cluster existence check above
   and the role loop below remain idempotent).

3. **Roles + grants.** For each of the three `materialize_*` roles: create
   if missing, then run the grant set. Three grant profiles:
   - Common (all three roles): `USAGE ON CLUSTER _mz_deploy_server`,
     `USAGE ON DATABASE _mz_deploy`, `USAGE ON SCHEMA _mz_deploy.public`,
     `USAGE ON SCHEMA _mz_deploy.tables`, `SELECT ON ALL TABLES IN SCHEMA
     _mz_deploy.public` (the views).
   - Deployer-only: `SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA
     _mz_deploy.tables`.

Grants always run; the GRANT statements are safe to re-execute.

### `setup_schema.sql` contents

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

Omitted by design:
- `clusters` passthrough — no direct reads; accessed only via
  `deployment_clusters` and `missing_clusters`.
- `clusters (deploy_id)` table index — covered indirectly by the two view
  indexes; DELETE is rare, table is small.
- `replacement_mvs (deploy_id)` table index — read twice total; small table.

### Role + grant logic (Rust)

```rust
for (role, role_name) in ALL_ROLES {
    if !client.introspection().role_exists(role_name).await? {
        client.execute(&format!("CREATE ROLE {}", role_name), &[]).await?;
    }

    // Common navigation + read privileges.
    for sql in [
        format!("GRANT USAGE ON CLUSTER {} TO {}",
            quote_identifier(SERVER_CLUSTER_NAME), role_name),
        format!("GRANT USAGE ON DATABASE _mz_deploy TO {}", role_name),
        format!("GRANT USAGE ON SCHEMA _mz_deploy.public TO {}", role_name),
        format!("GRANT USAGE ON SCHEMA _mz_deploy.tables TO {}", role_name),
        format!("GRANT SELECT ON ALL TABLES IN SCHEMA _mz_deploy.public TO {}", role_name),
    ] {
        client.execute(&sql, &[]).await?;
    }

    // Deployer-only: writes on physical tables.
    if *role == MzDeployRole::Deployer {
        client.execute(
            &format!(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES \
                 IN SCHEMA _mz_deploy.tables TO {}",
                role_name,
            ),
            &[],
        ).await?;
    }
}
```

### Rust call-site rewrites (`deployment_ops.rs`)

Every write retargets from `_mz_deploy.public.<table>` to
`_mz_deploy.tables.<table>`. Twelve sites:

| Line  | Op     | Table               |
|-------|--------|---------------------|
| 342   | INSERT | `deployments`       |
| 382   | INSERT | `objects`           |
| 455   | INSERT | `clusters`          |
| 534   | DELETE | `clusters`          |
| 544   | UPDATE | `deployments`       |
| 557   | DELETE | `deployments`       |
| 563   | DELETE | `objects`           |
| 1301  | INSERT | `pending_statements`|
| 1369  | UPDATE | `pending_statements`|
| 1388  | DELETE | `pending_statements`|
| 1404  | INSERT | `replacement_mvs`   |
| 1733  | DELETE | `replacement_mvs`   |

Three read-side rewrites that pivot helpers onto the new purpose views:

| Helper (line)                             | New SQL                                                             |
|-------------------------------------------|---------------------------------------------------------------------|
| `get_deployment_clusters` (l.478)         | `SELECT name FROM _mz_deploy.public.deployment_clusters WHERE deploy_id = $1 ORDER BY name` |
| `validate_deployment_clusters` (l.499)    | `SELECT cluster_id FROM _mz_deploy.public.missing_clusters WHERE deploy_id = $1` |
| `list_staging_deployments` (l.821)        | `SELECT ... FROM _mz_deploy.public.staging_deployments ORDER BY deploy_id, database, schema` |

All other SELECTs in `deployment_ops.rs` already target
`_mz_deploy.public.<x>` — they silently gain the view layer without code
changes.

### Cleanup

- Delete `pub(super) async fn create_deployments` in
  `deployment_ops.rs` (lines 177–330) and the matching
  `DeploymentsClient::create_deployments` accessor.
- Delete `ensure_server_cluster` helper in `setup.rs`; its work inlines
  into the new `ensure`.
- Remove any imports that `cargo check` flags as unused after the
  reshuffle.

## Testing

No new unit tests. Manual verification:

1. Fresh region, `mz-deploy setup` — verify:
   - `_mz_deploy.tables` schema exists with all six tables (including
     `version`).
   - `_mz_deploy.public` schema has nine views (four purpose, four
     passthrough, one `version`).
   - All advertised indexes exist on `_mz_deploy_server`.
   - `SELECT version FROM _mz_deploy.public.version` returns `1`.
   - Role grants match the matrix: deployer has INSERT on `tables`,
     developer/monitor do not.
2. Re-run `setup` — no errors; grants re-applied cleanly.
3. End-to-end: run a full stage → apply → promote cycle with
   `materialize_deployer`. Verify writes land in `_mz_deploy.tables.*`.
4. Run `mz-deploy list`, `describe`, `log` as `materialize_developer` —
   verify reads succeed through the views and writes fail with a
   permission error if attempted manually.
5. Drop `_mz_deploy_server` out-of-band, run any command — verify the
   existing debug/error UX behaves as Section 2 of the sibling spec
   describes.

## Migration

Not supported. Users on the pre-split schema must:

```sql
DROP DATABASE _mz_deploy CASCADE;
```

and re-run `mz-deploy setup`. Staged deployments are lost — operators
should finish or abort them before upgrading. This is called out in
release notes.
