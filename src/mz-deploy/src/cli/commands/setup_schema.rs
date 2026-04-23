//! DDL statements that materialize the `_mz_deploy` tracking database.
//!
//! Each entry is executed as its own statement by [`super::setup::ensure`].
//! Executing them individually (rather than as one multi-statement batch via
//! `batch_execute`) avoids Materialize's rejection of DDL inside the implicit
//! transaction block that a simple multi-statement query creates.
//!
//! Order matters: tables must exist before the indexes and views that
//! reference them.

/// All DDL statements required to initialize `_mz_deploy` from a clean
/// database. The `_mz_deploy` database itself is created separately by
/// [`super::setup::ensure`] immediately before iterating these.
pub(super) const SETUP_STATEMENTS: &[&str] = &[
    "CREATE SCHEMA _mz_deploy.tables",

    r#"CREATE TABLE _mz_deploy.tables.deployments (
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
    )"#,

    r#"CREATE INDEX deployments_deploy_id_idx
        IN CLUSTER _mz_deploy_server
        ON _mz_deploy.tables.deployments (deploy_id)"#,

    r#"CREATE TABLE _mz_deploy.tables.objects (
        deploy_id TEXT NOT NULL,
        database  TEXT NOT NULL,
        schema    TEXT NOT NULL,
        object    TEXT NOT NULL,
        hash      TEXT NOT NULL
    ) WITH (
        PARTITION BY (deploy_id, database, schema)
    )"#,

    r#"CREATE INDEX objects_deploy_id_idx
        IN CLUSTER _mz_deploy_server
        ON _mz_deploy.tables.objects (deploy_id)"#,

    r#"CREATE TABLE _mz_deploy.tables.clusters (
        deploy_id  TEXT NOT NULL,
        cluster_id TEXT NOT NULL
    ) WITH (
        PARTITION BY (deploy_id)
    )"#,

    r#"CREATE TABLE _mz_deploy.tables.pending_statements (
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
    )"#,

    r#"CREATE INDEX pending_statements_deploy_id_idx
        IN CLUSTER _mz_deploy_server
        ON _mz_deploy.tables.pending_statements (deploy_id)"#,

    r#"CREATE TABLE _mz_deploy.tables.replacement_mvs (
        deploy_id          TEXT NOT NULL,
        target_database    TEXT NOT NULL,
        target_schema      TEXT NOT NULL,
        target_name        TEXT NOT NULL,
        replacement_schema TEXT NOT NULL
    ) WITH (
        PARTITION BY (deploy_id)
    )"#,

    r#"CREATE TABLE _mz_deploy.tables.version (
        version BIGINT NOT NULL
    )"#,

    "INSERT INTO _mz_deploy.tables.version VALUES (1)",

    r#"CREATE INDEX version_idx
        IN CLUSTER _mz_deploy_server
        ON _mz_deploy.tables.version (version)"#,

    // Per-developer overlay database manifest.
    r#"CREATE TABLE _mz_deploy.tables.dev_overlays (
        profile       TEXT NOT NULL,
        project       TEXT NOT NULL,
        overlay_db    TEXT NOT NULL,
        created_at    TIMESTAMPTZ NOT NULL
    ) WITH (
        PARTITION BY (profile, project)
    )"#,

    r#"CREATE INDEX dev_overlays_profile_project_idx
        IN CLUSTER _mz_deploy_server
        ON _mz_deploy.tables.dev_overlays (profile, project)"#,

    r#"CREATE VIEW _mz_deploy.public.production AS
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
    JOIN mz_databases d ON c.database = d.name"#,

    r#"CREATE INDEX production_database_schema_idx
        IN CLUSTER _mz_deploy_server
        ON _mz_deploy.public.production (database, schema)"#,

    r#"CREATE VIEW _mz_deploy.public.staging_deployments AS
    SELECT deploy_id, deployed_at, database, schema, deployed_by, commit, kind, mode
    FROM _mz_deploy.tables.deployments
    WHERE promoted_at IS NULL"#,

    r#"CREATE INDEX staging_deployments_deploy_id_idx
        IN CLUSTER _mz_deploy_server
        ON _mz_deploy.public.staging_deployments (deploy_id)"#,

    r#"CREATE VIEW _mz_deploy.public.deployment_clusters AS
    SELECT dc.deploy_id, c.name
    FROM _mz_deploy.tables.clusters dc
    JOIN mz_catalog.mz_clusters c ON dc.cluster_id = c.id"#,

    r#"CREATE INDEX deployment_clusters_deploy_id_idx
        IN CLUSTER _mz_deploy_server
        ON _mz_deploy.public.deployment_clusters (deploy_id)"#,

    r#"CREATE VIEW _mz_deploy.public.missing_clusters AS
    SELECT d.deploy_id, dc.cluster_id
    FROM _mz_deploy.tables.deployments d
    JOIN _mz_deploy.tables.clusters dc USING (deploy_id)
    LEFT JOIN mz_catalog.mz_clusters c ON dc.cluster_id = c.id
    WHERE d.promoted_at IS NULL AND c.id IS NULL"#,

    r#"CREATE INDEX missing_clusters_deploy_id_idx
        IN CLUSTER _mz_deploy_server
        ON _mz_deploy.public.missing_clusters (deploy_id)"#,

    r#"CREATE VIEW _mz_deploy.public.deployments AS
    SELECT deploy_id, deployed_at, promoted_at, database, schema, deployed_by,
           commit, kind, mode
    FROM _mz_deploy.tables.deployments"#,

    r#"CREATE VIEW _mz_deploy.public.objects AS
    SELECT deploy_id, database, schema, object, hash
    FROM _mz_deploy.tables.objects"#,

    r#"CREATE VIEW _mz_deploy.public.pending_statements AS
    SELECT deploy_id, sequence_num, database, schema, object, object_hash,
           statement_sql, statement_kind, executed_at
    FROM _mz_deploy.tables.pending_statements"#,

    r#"CREATE VIEW _mz_deploy.public.replacement_mvs AS
    SELECT deploy_id, target_database, target_schema, target_name,
           replacement_schema
    FROM _mz_deploy.tables.replacement_mvs"#,

    r#"CREATE VIEW _mz_deploy.public.version AS
    SELECT version
    FROM _mz_deploy.tables.version"#,
];
