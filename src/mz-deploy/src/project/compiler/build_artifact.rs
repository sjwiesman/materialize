//! Persistence layer for the incremental compiler.
//!
//! Stores advisory build state scoped to one profile namespace. The database
//! persists four categories of state:
//!
//! - **File metadata** — Content hashes and source text, keyed by source path.
//!   Freshness is determined by file size and modification time; stale entries
//!   are transparently refreshed from disk.
//! - **Object artifacts** — Compiled object payloads, keyed by logical object
//!   identifier and content fingerprint.
//! - **Typecheck artifacts** — Per-object validation results (fingerprints,
//!   column schemas), used for incremental dirty detection.
//! - **Project snapshot** — Full compiled project graph for read-only consumers
//!   (LSP, explain).
//!
//! All cached state is advisory. Missing, corrupt, or schema-incompatible
//! entries are treated as cache misses and rebuilt from source. The compiler
//! owns the schema version; a version mismatch triggers a full rebuild of the
//! namespace-local database.

use super::cache_io::hex_digest;
use crate::project::ast::Statement;
use crate::project::ir::graph;
use crate::project::ir::infrastructure::{self, Infrastructure};
use crate::project::ir::object_id::ObjectId;
use crate::project::resolve::cte_scope::CteScope;
use crate::types::ColumnType;
use mz_sql_parser::ast::visit::{self, Visit};
use mz_sql_parser::ast::{CommentObjectType, Raw, RawClusterName, TableFactor};
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
use thiserror::Error;

const SCHEMA_VERSION: i64 = 6;
const DB_FILE: &str = "build_artifact.db";
const OBJECT_STATE_TABLE: &str = "object_state";
const TYPECHECK_OBJECTS_TABLE: &str = "typecheck_objects";
const TYPECHECK_COLUMNS_TABLE: &str = "typecheck_columns";

#[derive(Debug, Error)]
pub enum BuildArtifactError {
    #[error("failed to create compiler cache directory: {path}")]
    DirectoryCreationFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to open build artifact database: {path}")]
    DatabaseOpenFailed {
        path: PathBuf,
        #[source]
        source: rusqlite::Error,
    },
    #[error("failed to operate on build artifact database: {path}")]
    DatabaseOperationFailed {
        path: PathBuf,
        #[source]
        source: rusqlite::Error,
    },
    #[error("failed to read cached source file: {path}")]
    FileReadFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// A cached file entry returned by [`BuildArtifact::load_file_entries`].
#[derive(Debug, Clone)]
pub(crate) struct FileEntry {
    /// SHA-256 hex digest of the file contents.
    pub content_hash: String,
    /// Cached source text. Only populated when `include_contents` was true and
    /// the caller needs the file body for parsing.
    pub contents: Option<String>,
}

/// A cached object compilation artifact read from `object_state`.
#[derive(Debug, Clone)]
pub(crate) struct StoredObjectRow {
    /// Composite hash of the object key, variant paths, content hashes, and
    /// compile-time variables. Used to detect cache staleness.
    pub fingerprint: String,
    /// Bincode-serialized [`CompiledObjectArtifact`](super::CompiledObjectArtifact).
    pub payload: Vec<u8>,
}

/// Compute the path to the build artifact database file for a given project and profile.
pub(crate) fn build_artifact_path(
    root: &Path,
    profile: &str,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
) -> PathBuf {
    root.join(crate::types::BUILD_DIR)
        .join(super::COMPILER_DIR)
        .join(super::profile_namespace(profile, profile_suffix, variables))
        .join(DB_FILE)
}

pub(crate) struct BuildArtifact {
    path: PathBuf,
    conn: Connection,
}

impl BuildArtifact {
    /// Open (or create) the SQLite build artifact database for a profile namespace.
    ///
    /// The namespace directory is derived from the active profile name, optional
    /// suffix, and compile-time variable bindings, so different profiles use
    /// isolated caches. On schema version mismatch the database is dropped and
    /// recreated — safe because all cached state is advisory.
    pub(crate) fn open(
        root: &Path,
        profile: &str,
        profile_suffix: Option<&str>,
        variables: &BTreeMap<String, String>,
    ) -> Result<Self, BuildArtifactError> {
        let compiler_root = root
            .join(crate::types::BUILD_DIR)
            .join(super::COMPILER_DIR)
            .join(super::profile_namespace(profile, profile_suffix, variables));
        fs::create_dir_all(&compiler_root).map_err(|source| {
            BuildArtifactError::DirectoryCreationFailed {
                path: compiler_root.clone(),
                source,
            }
        })?;
        let path = compiler_root.join(DB_FILE);
        let conn =
            Connection::open(&path).map_err(|source| BuildArtifactError::DatabaseOpenFailed {
                path: path.clone(),
                source,
            })?;
        let db = Self { path, conn };
        db.initialize()?;
        Ok(db)
    }

    fn initialize(&self) -> Result<(), BuildArtifactError> {
        self.conn
            .execute_batch(
                "
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
                ",
            )
            .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                path: self.path.clone(),
                source,
            })?;
        self.conn
            .execute_batch(
                "
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                ",
            )
            .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                path: self.path.clone(),
                source,
            })?;

        let version: Option<i64> = self
            .conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                [],
                |row| {
                    row.get::<_, String>(0)
                        .map(|s| s.parse::<i64>().unwrap_or_default())
                },
            )
            .optional()
            .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                path: self.path.clone(),
                source,
            })?;

        if version != Some(SCHEMA_VERSION) {
            self.conn
                .execute_batch(
                    "
                    DROP TABLE IF EXISTS meta;
                    DROP TABLE IF EXISTS file_state;
                    DROP TABLE IF EXISTS object_state;
                    DROP TABLE IF EXISTS typecheck_state;
                    DROP TABLE IF EXISTS typecheck_columns;
                    DROP TABLE IF EXISTS typecheck_objects;
                    DROP TABLE IF EXISTS project_databases;
                    DROP TABLE IF EXISTS project_schemas;
                    DROP TABLE IF EXISTS project_objects;
                    DROP TABLE IF EXISTS project_dependencies;
                    DROP TABLE IF EXISTS project_external_dependencies;
                    DROP TABLE IF EXISTS project_cluster_dependencies;
                    DROP TABLE IF EXISTS project_replacement_schemas;
                    DROP TABLE IF EXISTS project_comments;
                    DROP TABLE IF EXISTS project_indexes;
                    DROP TABLE IF EXISTS project_constraints;
                    DROP TABLE IF EXISTS project_grants;
                    DROP TABLE IF EXISTS project_tests;
                    DROP TABLE IF EXISTS project_infrastructure;
                    DROP TABLE IF EXISTS project_infrastructure_properties;
                    DROP TABLE IF EXISTS project_mod_statements;
                    ",
                )
                .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                    path: self.path.clone(),
                    source,
                })?;
        }

        self.create_schema()?;
        Ok(())
    }

    fn create_schema(&self) -> Result<(), BuildArtifactError> {
        self.conn
            .execute_batch(
                "
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS file_state (
                    path TEXT PRIMARY KEY,
                    size INTEGER NOT NULL,
                    mtime_ns INTEGER NOT NULL,
                    content_hash TEXT NOT NULL,
                    contents TEXT
                );
                CREATE TABLE IF NOT EXISTS object_state (
                    object_key TEXT PRIMARY KEY,
                    fingerprint TEXT NOT NULL,
                    payload BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS typecheck_state (
                    object_key TEXT PRIMARY KEY,
                    semantic_fingerprint TEXT NOT NULL,
                    output_fingerprint TEXT NOT NULL,
                    payload BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS typecheck_objects (
                    object_key TEXT PRIMARY KEY,
                    semantic_fingerprint TEXT NOT NULL,
                    object_kind TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS typecheck_columns (
                    object_key TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    column_type TEXT NOT NULL,
                    nullable INTEGER NOT NULL,
                    position INTEGER NOT NULL,
                    PRIMARY KEY (object_key, column_name),
                    FOREIGN KEY (object_key) REFERENCES typecheck_objects(object_key)
                );
                CREATE TABLE IF NOT EXISTS project_databases (
                    name TEXT PRIMARY KEY
                );
                CREATE TABLE IF NOT EXISTS project_schemas (
                    database TEXT NOT NULL,
                    name TEXT NOT NULL,
                    schema_type TEXT NOT NULL,
                    PRIMARY KEY (database, name)
                );
                CREATE TABLE IF NOT EXISTS project_objects (
                    object_key TEXT PRIMARY KEY,
                    database TEXT NOT NULL,
                    schema TEXT NOT NULL,
                    name TEXT NOT NULL,
                    object_kind TEXT NOT NULL,
                    cluster TEXT,
                    file_path TEXT NOT NULL,
                    sql_text TEXT NOT NULL,
                    is_constraint_mv INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS project_dependencies (
                    object_key TEXT NOT NULL,
                    dependency_key TEXT NOT NULL,
                    PRIMARY KEY (object_key, dependency_key)
                );
                CREATE TABLE IF NOT EXISTS project_external_dependencies (
                    object_key TEXT NOT NULL PRIMARY KEY
                );
                CREATE TABLE IF NOT EXISTS project_cluster_dependencies (
                    cluster_name TEXT NOT NULL PRIMARY KEY
                );
                CREATE TABLE IF NOT EXISTS project_replacement_schemas (
                    database TEXT NOT NULL,
                    schema TEXT NOT NULL,
                    PRIMARY KEY (database, schema)
                );
                CREATE TABLE IF NOT EXISTS project_comments (
                    object_key TEXT NOT NULL,
                    comment_type TEXT NOT NULL,
                    target_column TEXT,
                    comment_text TEXT NOT NULL,
                    sql_text TEXT NOT NULL,
                    PRIMARY KEY (object_key, comment_type, target_column)
                );
                CREATE TABLE IF NOT EXISTS project_indexes (
                    object_key TEXT NOT NULL,
                    index_name TEXT,
                    cluster TEXT,
                    columns TEXT NOT NULL,
                    sql_text TEXT NOT NULL,
                    PRIMARY KEY (object_key, index_name)
                );
                CREATE TABLE IF NOT EXISTS project_constraints (
                    object_key TEXT NOT NULL,
                    constraint_name TEXT,
                    kind TEXT NOT NULL,
                    enforced INTEGER NOT NULL,
                    columns TEXT NOT NULL,
                    ref_object TEXT,
                    ref_columns TEXT,
                    sql_text TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS project_grants (
                    object_key TEXT NOT NULL,
                    privilege TEXT NOT NULL,
                    grantee TEXT NOT NULL,
                    sql_text TEXT NOT NULL,
                    PRIMARY KEY (object_key, privilege, grantee)
                );
                CREATE TABLE IF NOT EXISTS project_tests (
                    object_key TEXT NOT NULL,
                    test_name TEXT NOT NULL,
                    sql_text TEXT NOT NULL,
                    PRIMARY KEY (object_key, test_name)
                );
                CREATE TABLE IF NOT EXISTS project_infrastructure (
                    object_key TEXT NOT NULL PRIMARY KEY,
                    infra_type TEXT NOT NULL,
                    connector_type TEXT,
                    connection_ref TEXT,
                    source_ref TEXT,
                    external_reference TEXT
                );
                CREATE TABLE IF NOT EXISTS project_infrastructure_properties (
                    object_key TEXT NOT NULL,
                    property_key TEXT NOT NULL,
                    property_value TEXT NOT NULL,
                    secret_ref TEXT,
                    object_ref TEXT,
                    PRIMARY KEY (object_key, property_key)
                );
                CREATE TABLE IF NOT EXISTS project_aliases (
                    object_key TEXT NOT NULL,
                    alias TEXT NOT NULL,
                    target_fqn TEXT NOT NULL,
                    PRIMARY KEY (object_key, alias)
                );
                CREATE TABLE IF NOT EXISTS project_mod_statements (
                    database TEXT NOT NULL,
                    schema TEXT,
                    position INTEGER NOT NULL,
                    sql_text TEXT NOT NULL,
                    PRIMARY KEY (database, schema, position)
                );
                INSERT OR REPLACE INTO meta(key, value) VALUES ('schema_version', '6');
                ",
            )
            .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                path: self.path.clone(),
                source,
            })
    }

    /// Load file metadata and content hashes for the requested source paths.
    ///
    /// Returns cached metadata when the file is unchanged since the last build.
    /// Stale or missing entries are transparently refreshed from the filesystem.
    ///
    /// When `include_contents` is true the source text is also returned — this
    /// is used for compile misses that need to parse the file. When false, only
    /// the content hash is returned, which is sufficient for fingerprinting
    /// during the plan phase.
    pub(crate) fn load_file_entries(
        &mut self,
        paths: &BTreeSet<PathBuf>,
        include_contents: bool,
    ) -> Result<BTreeMap<PathBuf, FileEntry>, BuildArtifactError> {
        let tx = self.conn.transaction().map_err(|source| {
            BuildArtifactError::DatabaseOperationFailed {
                path: self.path.clone(),
                source,
            }
        })?;
        let mut select = tx
            .prepare(
                "SELECT size, mtime_ns, content_hash, contents FROM file_state WHERE path = ?1",
            )
            .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                path: self.path.clone(),
                source,
            })?;
        let mut upsert = tx
            .prepare(
                "
                INSERT INTO file_state(path, size, mtime_ns, content_hash, contents)
                VALUES(?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(path) DO UPDATE SET
                    size = excluded.size,
                    mtime_ns = excluded.mtime_ns,
                    content_hash = excluded.content_hash,
                    contents = excluded.contents
                ",
            )
            .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                path: self.path.clone(),
                source,
            })?;

        let mut results = BTreeMap::new();
        for path in paths {
            let (size, mtime_ns) = file_metadata_signature(path)?;

            let row: Option<(i64, i64, String, Option<String>)> = select
                .query_row([path.to_string_lossy().to_string()], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .optional()
                .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                    path: self.path.clone(),
                    source,
                })?;

            let entry = if let Some((cached_size, cached_mtime, content_hash, cached_contents)) =
                row
                && cached_size == size
                && cached_mtime == mtime_ns
            {
                match (include_contents, cached_contents) {
                    (true, Some(contents)) => FileEntry {
                        content_hash,
                        contents: Some(contents),
                    },
                    (true, None) => {
                        let contents = fs::read_to_string(path).map_err(|source| {
                            BuildArtifactError::FileReadFailed {
                                path: path.clone(),
                                source,
                            }
                        })?;
                        upsert
                            .execute(params![
                                path.to_string_lossy().to_string(),
                                size,
                                mtime_ns,
                                content_hash,
                                &contents,
                            ])
                            .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                                path: self.path.clone(),
                                source,
                            })?;
                        FileEntry {
                            content_hash,
                            contents: Some(contents),
                        }
                    }
                    (false, _) => FileEntry {
                        content_hash,
                        contents: None,
                    },
                }
            } else {
                let contents = fs::read_to_string(path).map_err(|source| {
                    BuildArtifactError::FileReadFailed {
                        path: path.clone(),
                        source,
                    }
                })?;
                let content_hash = hex_digest(Sha256::digest(contents.as_bytes()));
                upsert
                    .execute(params![
                        path.to_string_lossy().to_string(),
                        size,
                        mtime_ns,
                        &content_hash,
                        &contents,
                    ])
                    .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                        path: self.path.clone(),
                        source,
                    })?;
                FileEntry {
                    content_hash,
                    contents: include_contents.then_some(contents),
                }
            };

            results.insert(path.clone(), entry);
        }

        drop(select);
        drop(upsert);
        tx.commit()
            .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                path: self.path.clone(),
                source,
            })?;
        Ok(results)
    }

    /// Load all cached object compilation artifacts from `object_state`.
    pub(crate) fn load_object_rows(
        &self,
    ) -> Result<BTreeMap<String, StoredObjectRow>, BuildArtifactError> {
        let mut stmt = self
            .conn
            .prepare("SELECT object_key, fingerprint, payload FROM object_state")
            .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                path: self.path.clone(),
                source,
            })?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    StoredObjectRow {
                        fingerprint: row.get(1)?,
                        payload: row.get(2)?,
                    },
                ))
            })
            .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                path: self.path.clone(),
                source,
            })?;

        let mut result = BTreeMap::new();
        for row in rows {
            let (key, state) =
                row.map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                    path: self.path.clone(),
                    source,
                })?;
            result.insert(key, state);
        }
        Ok(result)
    }

    /// Persist newly compiled object artifacts into `object_state`.
    pub(crate) fn upsert_object_rows(
        &mut self,
        rows: &[ObjectStateRow],
    ) -> Result<(), BuildArtifactError> {
        let tx = self.conn.transaction().map_err(|source| {
            BuildArtifactError::DatabaseOperationFailed {
                path: self.path.clone(),
                source,
            }
        })?;
        {
            let mut stmt = tx
                .prepare(
                    "
                    INSERT INTO object_state(object_key, fingerprint, payload)
                    VALUES(?1, ?2, ?3)
                    ON CONFLICT(object_key) DO UPDATE SET
                        fingerprint = excluded.fingerprint,
                        payload = excluded.payload
                    ",
                )
                .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                    path: self.path.clone(),
                    source,
                })?;
            for row in rows {
                stmt.execute(params![row.object_key, row.fingerprint, row.payload])
                    .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                        path: self.path.clone(),
                        source,
                    })?;
            }
        }
        tx.commit()
            .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                path: self.path.clone(),
                source,
            })
    }

    /// Remove `object_state` rows for objects no longer in the current project.
    pub(crate) fn prune_object_rows(
        &mut self,
        keep: &BTreeSet<String>,
    ) -> Result<(), BuildArtifactError> {
        self.prune_rows(OBJECT_STATE_TABLE, keep)
    }

    /// Persist or update typecheck artifacts for a batch of objects.
    ///
    /// For each object, stores its semantic fingerprint, output fingerprint,
    /// kind, and column definitions. Column records for an object are fully
    /// replaced (no partial updates).
    pub(crate) fn upsert_typecheck_results(
        &mut self,
        rows: &[(
            String,
            String,
            String,
            &BTreeMap<String, ColumnType>,
        )],
    ) -> Result<(), BuildArtifactError> {
        let tx = self.conn.transaction().map_err(|source| {
            BuildArtifactError::DatabaseOperationFailed {
                path: self.path.clone(),
                source,
            }
        })?;
        {
            let mut upsert_obj = tx
                .prepare(
                    "
                    INSERT INTO typecheck_objects(object_key, semantic_fingerprint, object_kind)
                    VALUES(?1, ?2, ?3)
                    ON CONFLICT(object_key) DO UPDATE SET
                        semantic_fingerprint = excluded.semantic_fingerprint,
                        object_kind = excluded.object_kind
                    ",
                )
                .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                    path: self.path.clone(),
                    source,
                })?;
            let mut delete_cols = tx
                .prepare("DELETE FROM typecheck_columns WHERE object_key = ?1")
                .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                    path: self.path.clone(),
                    source,
                })?;
            let mut insert_col = tx
                .prepare(
                    "INSERT INTO typecheck_columns(object_key, column_name, column_type, nullable, position)
                     VALUES(?1, ?2, ?3, ?4, ?5)",
                )
                .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                    path: self.path.clone(),
                    source,
                })?;

            for (key, semantic_fp, kind, columns) in rows {
                upsert_obj
                    .execute(params![key, semantic_fp, kind])
                    .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                        path: self.path.clone(),
                        source,
                    })?;
                delete_cols.execute([key]).map_err(|source| {
                    BuildArtifactError::DatabaseOperationFailed {
                        path: self.path.clone(),
                        source,
                    }
                })?;
                for (col_name, col_type) in *columns {
                    insert_col
                        .execute(params![
                            key,
                            col_name,
                            col_type.r#type,
                            i32::from(col_type.nullable),
                            i64::try_from(col_type.position).unwrap_or(0),
                        ])
                        .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                            path: self.path.clone(),
                            source,
                        })?;
                }
            }
        }
        tx.commit()
            .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                path: self.path.clone(),
                source,
            })
    }


    /// Remove stale typecheck artifacts for objects no longer in the current
    /// project.
    pub(crate) fn prune_typecheck_results(
        &mut self,
        keep: &BTreeSet<String>,
    ) -> Result<(), BuildArtifactError> {
        self.prune_rows(TYPECHECK_COLUMNS_TABLE, keep)?;
        self.prune_rows(TYPECHECK_OBJECTS_TABLE, keep)
    }

    /// Persist a complete snapshot of the compiled project graph.
    ///
    /// Atomically replaces all prior project metadata, ensuring consumers
    /// always see a consistent view of the full project.
    pub(crate) fn write_project(
        &mut self,
        project: &graph::Project,
        root: &Path,
    ) -> Result<(), BuildArtifactError> {
        let db_path = self.path.clone();
        let db_err = |source: rusqlite::Error| BuildArtifactError::DatabaseOperationFailed {
            path: db_path.clone(),
            source,
        };

        let tx = self.conn.transaction().map_err(&db_err)?;

        tx.execute_batch(
            "
            DELETE FROM project_databases;
            DELETE FROM project_schemas;
            DELETE FROM project_objects;
            DELETE FROM project_dependencies;
            DELETE FROM project_external_dependencies;
            DELETE FROM project_cluster_dependencies;
            DELETE FROM project_replacement_schemas;
            DELETE FROM project_comments;
            DELETE FROM project_indexes;
            DELETE FROM project_constraints;
            DELETE FROM project_grants;
            DELETE FROM project_tests;
            DELETE FROM project_infrastructure;
            DELETE FROM project_infrastructure_properties;
            DELETE FROM project_aliases;
            DELETE FROM project_mod_statements;
            ",
        )
        .map_err(&db_err)?;

        {
            let mut stmts = ProjectStatements::new(&tx, &db_err)?;

            for db in &project.databases {
                stmts.ins_db.execute(params![&db.name]).map_err(&db_err)?;

                for schema in &db.schemas {
                    let schema_type = schema.schema_type.to_string();
                    stmts
                        .ins_schema
                        .execute(params![&db.name, &schema.name, schema_type.as_str()])
                        .map_err(&db_err)?;

                    for obj in &schema.objects {
                        stmts.insert_object(obj, &db.name, &schema.name, root, &db_err)?;
                    }
                }

                stmts.insert_mod_statements(db, &db_err)?;
            }

            for ext_dep in &project.external_dependencies {
                stmts
                    .ins_ext_dep
                    .execute(params![ext_dep.to_string()])
                    .map_err(&db_err)?;
            }
            for cluster in &project.cluster_dependencies {
                stmts
                    .ins_cluster_dep
                    .execute(params![&cluster.name])
                    .map_err(&db_err)?;
            }
            for rs in &project.replacement_schemas {
                stmts
                    .ins_repl_schema
                    .execute(params![&rs.database, &rs.schema])
                    .map_err(&db_err)?;
            }
        }

        tx.commit().map_err(&db_err)
    }

    fn load_row_keys(&self, table: &str) -> Result<BTreeSet<String>, BuildArtifactError> {
        let mut stmt = self
            .conn
            .prepare(&format!("SELECT object_key FROM {table}"))
            .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                path: self.path.clone(),
                source,
            })?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                path: self.path.clone(),
                source,
            })?;
        let mut keys = BTreeSet::new();
        for row in rows {
            keys.insert(
                row.map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                    path: self.path.clone(),
                    source,
                })?,
            );
        }
        Ok(keys)
    }

    fn prune_rows(
        &mut self,
        table: &str,
        keep: &BTreeSet<String>,
    ) -> Result<(), BuildArtifactError> {
        let existing = self.load_row_keys(table)?;
        let tx = self.conn.transaction().map_err(|source| {
            BuildArtifactError::DatabaseOperationFailed {
                path: self.path.clone(),
                source,
            }
        })?;
        {
            let mut stmt = tx
                .prepare(&format!("DELETE FROM {table} WHERE object_key = ?1"))
                .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                    path: self.path.clone(),
                    source,
                })?;
            for key in &existing {
                if !keep.contains(key) {
                    stmt.execute([key]).map_err(|source| {
                        BuildArtifactError::DatabaseOperationFailed {
                            path: self.path.clone(),
                            source,
                        }
                    })?;
                }
            }
        }
        tx.commit()
            .map_err(|source| BuildArtifactError::DatabaseOperationFailed {
                path: self.path.clone(),
                source,
            })
    }
}

/// Bundle of prepared INSERT statements for [`BuildArtifact::write_project`].
///
/// Groups all prepared statements needed to persist a project snapshot so they
/// can be created and dropped as a unit. Methods on this struct handle the
/// per-entity serialization logic that was previously inline in `write_project`.
struct ProjectStatements<'tx> {
    ins_db: rusqlite::Statement<'tx>,
    ins_schema: rusqlite::Statement<'tx>,
    ins_obj: rusqlite::Statement<'tx>,
    ins_dep: rusqlite::Statement<'tx>,
    ins_comment: rusqlite::Statement<'tx>,
    ins_index: rusqlite::Statement<'tx>,
    ins_constraint: rusqlite::Statement<'tx>,
    ins_grant: rusqlite::Statement<'tx>,
    ins_test: rusqlite::Statement<'tx>,
    ins_infra: rusqlite::Statement<'tx>,
    ins_infra_prop: rusqlite::Statement<'tx>,
    ins_ext_dep: rusqlite::Statement<'tx>,
    ins_cluster_dep: rusqlite::Statement<'tx>,
    ins_repl_schema: rusqlite::Statement<'tx>,
    ins_alias: rusqlite::Statement<'tx>,
    ins_mod_stmt: rusqlite::Statement<'tx>,
}

impl<'tx> ProjectStatements<'tx> {
    fn new(
        tx: &'tx rusqlite::Transaction<'_>,
        db_err: &impl Fn(rusqlite::Error) -> BuildArtifactError,
    ) -> Result<Self, BuildArtifactError> {
        Ok(Self {
            ins_db: tx
                .prepare("INSERT INTO project_databases (name) VALUES (?1)")
                .map_err(db_err)?,
            ins_schema: tx
                .prepare("INSERT INTO project_schemas (database, name, schema_type) VALUES (?1, ?2, ?3)")
                .map_err(db_err)?,
            ins_obj: tx
                .prepare("INSERT INTO project_objects (object_key, database, schema, name, object_kind, cluster, file_path, sql_text, is_constraint_mv) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)")
                .map_err(db_err)?,
            ins_dep: tx
                .prepare("INSERT INTO project_dependencies (object_key, dependency_key) VALUES (?1, ?2)")
                .map_err(db_err)?,
            ins_comment: tx
                .prepare("INSERT INTO project_comments (object_key, comment_type, target_column, comment_text, sql_text) VALUES (?1, ?2, ?3, ?4, ?5)")
                .map_err(db_err)?,
            ins_index: tx
                .prepare("INSERT INTO project_indexes (object_key, index_name, cluster, columns, sql_text) VALUES (?1, ?2, ?3, ?4, ?5)")
                .map_err(db_err)?,
            ins_constraint: tx
                .prepare("INSERT INTO project_constraints (object_key, constraint_name, kind, enforced, columns, ref_object, ref_columns, sql_text) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)")
                .map_err(db_err)?,
            ins_grant: tx
                .prepare("INSERT INTO project_grants (object_key, privilege, grantee, sql_text) VALUES (?1, ?2, ?3, ?4)")
                .map_err(db_err)?,
            ins_test: tx
                .prepare("INSERT INTO project_tests (object_key, test_name, sql_text) VALUES (?1, ?2, ?3)")
                .map_err(db_err)?,
            ins_infra: tx
                .prepare("INSERT INTO project_infrastructure (object_key, infra_type, connector_type, connection_ref, source_ref, external_reference) VALUES (?1, ?2, ?3, ?4, ?5, ?6)")
                .map_err(db_err)?,
            ins_infra_prop: tx
                .prepare("INSERT INTO project_infrastructure_properties (object_key, property_key, property_value, secret_ref, object_ref) VALUES (?1, ?2, ?3, ?4, ?5)")
                .map_err(db_err)?,
            ins_ext_dep: tx
                .prepare("INSERT INTO project_external_dependencies (object_key) VALUES (?1)")
                .map_err(db_err)?,
            ins_cluster_dep: tx
                .prepare("INSERT INTO project_cluster_dependencies (cluster_name) VALUES (?1)")
                .map_err(db_err)?,
            ins_repl_schema: tx
                .prepare("INSERT INTO project_replacement_schemas (database, schema) VALUES (?1, ?2)")
                .map_err(db_err)?,
            ins_alias: tx
                .prepare("INSERT INTO project_aliases (object_key, alias, target_fqn) VALUES (?1, ?2, ?3)")
                .map_err(db_err)?,
            ins_mod_stmt: tx
                .prepare("INSERT INTO project_mod_statements (database, schema, position, sql_text) VALUES (?1, ?2, ?3, ?4)")
                .map_err(db_err)?,
        })
    }

    fn insert_object(
        &mut self,
        obj: &graph::DatabaseObject,
        db_name: &str,
        schema_name: &str,
        root: &Path,
        db_err: &impl Fn(rusqlite::Error) -> BuildArtifactError,
    ) -> Result<(), BuildArtifactError> {
        let object_key = obj.id.to_string();
        let typed = &obj.typed_object;
        let kind = typed.stmt.kind().as_str();
        let cluster = statement_cluster(&typed.stmt);
        let file_path = typed
            .path
            .strip_prefix(root)
            .unwrap_or(&typed.path)
            .to_string_lossy()
            .to_string();
        let sql_text = format!("{};", typed.stmt);

        self.ins_obj
            .execute(params![
                &object_key,
                db_name,
                schema_name,
                obj.id.object(),
                kind,
                &cluster,
                &file_path,
                &sql_text,
                i32::from(obj.is_constraint_mv),
            ])
            .map_err(db_err)?;

        for dep in &obj.dependencies {
            self.ins_dep
                .execute(params![&object_key, dep.to_string()])
                .map_err(db_err)?;
        }

        for comment in &typed.comments {
            let (comment_type, target_column) = match &comment.object {
                CommentObjectType::Column { name } => ("column", Some(name.column.to_string())),
                _ => ("object", None),
            };
            let comment_text = comment.comment.as_deref().unwrap_or("");
            let comment_sql = format!("{};", comment);
            self.ins_comment
                .execute(params![
                    &object_key,
                    comment_type,
                    &target_column,
                    comment_text,
                    &comment_sql,
                ])
                .map_err(db_err)?;
        }

        for idx in &typed.indexes {
            let index_name = idx.name.as_ref().map(|n| n.to_string()).unwrap_or_default();
            let idx_cluster = idx.in_cluster.as_ref().map(|c| c.to_string());
            let columns_str = idx
                .key_parts
                .as_ref()
                .map(|parts| {
                    parts
                        .iter()
                        .map(|p| p.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            let idx_sql = format!("{};", idx);
            self.ins_index
                .execute(params![
                    &object_key,
                    &index_name,
                    &idx_cluster,
                    &columns_str,
                    &idx_sql,
                ])
                .map_err(db_err)?;
        }

        for constraint in &typed.constraints {
            let constraint_name = constraint
                .name
                .as_ref()
                .map(|n| n.to_string())
                .unwrap_or_default();
            let con_kind = constraint.kind.to_string();
            let enforced = i32::from(constraint.enforced);
            let columns: Vec<String> = constraint.columns.iter().map(|c| c.to_string()).collect();
            let columns_json = serde_json::to_string(&columns).unwrap_or_default();
            let (ref_object, ref_columns) = match &constraint.references {
                Some(refs) => {
                    let ref_obj = refs.object.to_string();
                    let ref_cols: Vec<String> =
                        refs.columns.iter().map(|c| c.to_string()).collect();
                    (
                        Some(ref_obj),
                        Some(serde_json::to_string(&ref_cols).unwrap_or_default()),
                    )
                }
                None => (None, None),
            };
            let con_sql = format!("{};", constraint);
            self.ins_constraint
                .execute(params![
                    &object_key,
                    &constraint_name,
                    &con_kind,
                    enforced,
                    &columns_json,
                    &ref_object,
                    &ref_columns,
                    &con_sql,
                ])
                .map_err(db_err)?;
        }

        for grant in &typed.grants {
            let privilege = grant.privileges.to_string();
            let grantee = grant
                .roles
                .iter()
                .map(|r| r.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let grant_sql = format!("{};", grant);
            self.ins_grant
                .execute(params![&object_key, &privilege, &grantee, &grant_sql])
                .map_err(db_err)?;
        }

        for test in &typed.tests {
            let test_name = test.name.to_string();
            let test_sql = format!("{};", test);
            self.ins_test
                .execute(params![&object_key, &test_name, &test_sql])
                .map_err(db_err)?;
        }

        if let Some(infra) = infrastructure::extract(&typed.stmt) {
            self.insert_infrastructure(&object_key, &infra, db_err)?;
        }

        let aliases = extract_alias_map(&typed.stmt, db_name, schema_name);
        for (alias, target_fqn) in &aliases {
            self.ins_alias
                .execute(params![&object_key, alias, target_fqn])
                .map_err(db_err)?;
        }

        Ok(())
    }

    fn insert_infrastructure(
        &mut self,
        object_key: &str,
        infra: &Infrastructure,
        db_err: &impl Fn(rusqlite::Error) -> BuildArtifactError,
    ) -> Result<(), BuildArtifactError> {
        let (infra_type, connector_type, connection_ref, source_ref, external_reference) =
            match infra {
                Infrastructure::Connection { connector_type, .. } => (
                    "connection",
                    Some(connector_type.as_str()),
                    None,
                    None,
                    None,
                ),
                Infrastructure::Source {
                    connector_type,
                    connection_ref,
                    ..
                } => (
                    "source",
                    Some(connector_type.as_str()),
                    connection_ref.as_deref(),
                    None,
                    None,
                ),
                Infrastructure::TableFromSource {
                    source_ref,
                    external_reference,
                } => (
                    "table-from-source",
                    None,
                    None,
                    Some(source_ref.as_str()),
                    external_reference.as_deref(),
                ),
            };

        self.ins_infra
            .execute(params![
                object_key,
                infra_type,
                connector_type,
                connection_ref,
                source_ref,
                external_reference,
            ])
            .map_err(db_err)?;

        let properties = match infra {
            Infrastructure::Connection { properties, .. }
            | Infrastructure::Source { properties, .. } => properties.as_slice(),
            Infrastructure::TableFromSource { .. } => &[],
        };
        for prop in properties {
            self.ins_infra_prop
                .execute(params![
                    object_key,
                    &prop.key,
                    &prop.value,
                    &prop.secret_ref,
                    &prop.object_ref,
                ])
                .map_err(db_err)?;
        }

        Ok(())
    }

    fn insert_mod_statements(
        &mut self,
        db: &graph::Database,
        db_err: &impl Fn(rusqlite::Error) -> BuildArtifactError,
    ) -> Result<(), BuildArtifactError> {
        if let Some(stmts) = &db.mod_statements {
            for (pos, stmt) in stmts.iter().enumerate() {
                self.ins_mod_stmt
                    .execute(params![
                        &db.name,
                        Option::<String>::None,
                        i64::try_from(pos).unwrap_or(0),
                        format!("{};", stmt),
                    ])
                    .map_err(db_err)?;
            }
        }

        for schema in &db.schemas {
            if let Some(stmts) = &schema.mod_statements {
                for (pos, stmt) in stmts.iter().enumerate() {
                    self.ins_mod_stmt
                        .execute(params![
                            &db.name,
                            Some(&schema.name),
                            i64::try_from(pos).unwrap_or(0),
                            format!("{};", stmt),
                        ])
                        .map_err(db_err)?;
                }
            }
        }

        Ok(())
    }
}

fn file_metadata_signature(path: &Path) -> Result<(i64, i64), BuildArtifactError> {
    let metadata = fs::metadata(path).map_err(|source| BuildArtifactError::FileReadFailed {
        path: path.to_path_buf(),
        source,
    })?;
    let size = i64::try_from(metadata.len()).unwrap_or(i64::MAX);
    let modified = metadata
        .modified()
        .map_err(|source| BuildArtifactError::FileReadFailed {
            path: path.to_path_buf(),
            source,
        })?;
    let duration = modified.duration_since(UNIX_EPOCH).map_err(|source| {
        BuildArtifactError::FileReadFailed {
            path: path.to_path_buf(),
            source: std::io::Error::other(source),
        }
    })?;
    // File mtimes are an advisory cache key; saturate if the platform
    // reports a nanosecond value larger than the on-disk schema stores.
    let mtime_ns = i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX);
    Ok((size, mtime_ns))
}

/// Extract the cluster name from a statement's `IN CLUSTER` clause, if present.
fn statement_cluster(stmt: &Statement) -> Option<String> {
    use crate::project::ast::Statement;

    let in_cluster = match stmt {
        Statement::CreateMaterializedView(mv) => mv.in_cluster.as_ref(),
        Statement::CreateSource(source) => source.in_cluster.as_ref(),
        Statement::CreateSink(sink) => sink.in_cluster.as_ref(),
        Statement::CreateView(_)
        | Statement::CreateTable(_)
        | Statement::CreateTableFromSource(_)
        | Statement::CreateSecret(_)
        | Statement::CreateConnection(_) => None,
    };

    match in_cluster {
        Some(RawClusterName::Unresolved(ident)) => Some(ident.to_string()),
        _ => None,
    }
}

/// An object compilation artifact to be written to `object_state`.
///
/// Same logical shape as [`StoredObjectRow`] but includes the `object_key`
/// for upsert targeting.
#[derive(Debug, Clone)]
pub(crate) struct ObjectStateRow {
    /// Logical object identifier (`database.schema.object`).
    pub object_key: String,
    /// Composite fingerprint used for cache invalidation (see [`StoredObjectRow`]).
    pub fingerprint: String,
    /// Bincode-serialized [`CompiledObjectArtifact`](super::CompiledObjectArtifact).
    pub payload: Vec<u8>,
}

/// AST visitor that collects FROM-clause table aliases.
///
/// Overrides `visit_query` for CTE scope management and `visit_table_factor`
/// to collect both explicit aliases (`FROM t AS alias`) and implicit bare
/// names (`FROM t` → `t`). Does not recurse into derived subqueries or
/// table functions — only direct table references produce aliases.
struct AliasVisitor<'a> {
    default_db: &'a str,
    default_schema: &'a str,
    aliases: BTreeMap<String, String>,
    cte_scope: CteScope,
}

impl<'a> AliasVisitor<'a> {
    fn new(default_db: &'a str, default_schema: &'a str) -> Self {
        Self {
            default_db,
            default_schema,
            aliases: BTreeMap::new(),
            cte_scope: CteScope::new(),
        }
    }
}

impl<'ast> Visit<'ast, Raw> for AliasVisitor<'_> {
    fn visit_query(&mut self, node: &'ast mz_sql_parser::ast::Query<Raw>) {
        let names = CteScope::collect_cte_names(&node.ctes);
        self.cte_scope.push(names);
        visit::visit_query(self, node);
        self.cte_scope.pop();
    }

    fn visit_table_factor(&mut self, node: &'ast TableFactor<Raw>) {
        match node {
            TableFactor::Table { name, alias } => {
                let unresolved = name.name();
                if unresolved.0.len() == 1 && self.cte_scope.is_cte(&unresolved.0[0].to_string()) {
                    return;
                }
                let obj_id =
                    ObjectId::from_raw_item_name(name, self.default_db, self.default_schema);
                let fqn = obj_id.to_string();
                if let Some(bare) = unresolved.0.last().map(|i| i.to_string().to_lowercase()) {
                    self.aliases.insert(bare, fqn.clone());
                }
                if let Some(alias) = alias {
                    self.aliases
                        .insert(alias.name.to_string().to_lowercase(), fqn);
                }
            }
            TableFactor::NestedJoin { .. } => {
                visit::visit_table_factor(self, node);
            }
            // Don't recurse into subqueries or table functions for alias collection
            TableFactor::Derived { .. }
            | TableFactor::Function { .. }
            | TableFactor::RowsFrom { .. } => {}
        }
    }
}

/// Extract alias → fully-qualified name map from a statement's query body.
///
/// Only `CreateView` and `CreateMaterializedView` produce aliases.
/// All keys are lowercased for case-insensitive lookup. CTE references
/// are excluded from the alias map.
pub(crate) fn extract_alias_map(
    stmt: &Statement,
    default_db: &str,
    default_schema: &str,
) -> BTreeMap<String, String> {
    let mut visitor = AliasVisitor::new(default_db, default_schema);
    match stmt {
        Statement::CreateView(s) => {
            visitor.visit_query(&s.definition.query);
        }
        Statement::CreateMaterializedView(s) => {
            visitor.visit_query(&s.query);
        }
        _ => {}
    }
    visitor.aliases
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn open_db(root: &Path) -> BuildArtifact {
        BuildArtifact::open(root, "default", None, &BTreeMap::new()).unwrap()
    }

    #[test]
    fn load_file_entries_repairs_missing_cached_contents() {
        let temp = tempdir().unwrap();
        let file = temp.path().join("model.sql");
        fs::write(&file, "CREATE VIEW v AS SELECT 1;").unwrap();

        let mut db = open_db(temp.path());
        let paths = BTreeSet::from([file.clone()]);
        db.load_file_entries(&paths, false).unwrap();
        db.conn
            .execute(
                "UPDATE file_state SET contents = NULL WHERE path = ?1",
                [file.to_string_lossy().to_string()],
            )
            .unwrap();

        let entries = db.load_file_entries(&paths, true).unwrap();
        assert_eq!(
            entries
                .get(&file)
                .and_then(|entry| entry.contents.as_deref()),
            Some("CREATE VIEW v AS SELECT 1;")
        );

        let repaired: Option<String> = db
            .conn
            .query_row(
                "SELECT contents FROM file_state WHERE path = ?1",
                [file.to_string_lossy().to_string()],
                |row| row.get(0),
            )
            .optional()
            .unwrap();
        assert_eq!(repaired.as_deref(), Some("CREATE VIEW v AS SELECT 1;"));
    }

    #[test]
    fn prune_rows_removes_stale_object_and_typecheck_entries() {
        let temp = tempdir().unwrap();
        let mut db = open_db(temp.path());
        db.upsert_object_rows(&[
            ObjectStateRow {
                object_key: "db.public.keep".into(),
                fingerprint: "keep".into(),
                payload: vec![1],
            },
            ObjectStateRow {
                object_key: "db.public.drop".into(),
                fingerprint: "drop".into(),
                payload: vec![2],
            },
        ])
        .unwrap();
        let empty_cols: BTreeMap<String, ColumnType> = BTreeMap::new();
        db.upsert_typecheck_results(&[
            (
                "db.public.keep".into(),
                "sem".into(),
                "view".into(),
                &empty_cols,
            ),
            (
                "db.public.drop".into(),
                "sem".into(),
                "view".into(),
                &empty_cols,
            ),
        ])
        .unwrap();

        let keep = BTreeSet::from([String::from("db.public.keep")]);
        db.prune_object_rows(&keep).unwrap();
        db.prune_typecheck_results(&keep).unwrap();

        let object_keys = db.load_row_keys(OBJECT_STATE_TABLE).unwrap();
        let typecheck_keys = db.load_row_keys(TYPECHECK_OBJECTS_TABLE).unwrap();
        assert_eq!(object_keys, keep);
        assert_eq!(typecheck_keys, keep);
    }

    /// Helper: parse SQL into a [`Statement`] for test construction.
    fn parse_stmt(sql: &str) -> Statement {
        let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();
        match parsed.into_iter().next().unwrap().ast {
            mz_sql_parser::ast::Statement::CreateView(s) => Statement::CreateView(s),
            mz_sql_parser::ast::Statement::CreateMaterializedView(s) => {
                Statement::CreateMaterializedView(s)
            }
            mz_sql_parser::ast::Statement::CreateTable(s) => Statement::CreateTable(s),
            other => panic!("Unexpected statement type: {:?}", other),
        }
    }

    /// Build a minimal `graph::Project` from a single object.
    fn make_project(db_name: &str, schema_name: &str, stmt: Statement) -> graph::Project {
        use crate::project::ir::compiled;

        let typed_obj = compiled::DatabaseObject {
            path: PathBuf::from("test.sql"),
            stmt,
            indexes: vec![],
            constraints: vec![],
            grants: vec![],
            comments: vec![],
            tests: vec![],
        };
        let obj_id = ObjectId::new(
            db_name.to_string(),
            schema_name.to_string(),
            typed_obj.stmt.ident().object.clone(),
        );
        let db_obj = graph::DatabaseObject {
            id: obj_id,
            typed_object: typed_obj,
            dependencies: BTreeSet::new(),
            is_constraint_mv: false,
        };
        graph::Project {
            databases: vec![graph::Database {
                name: db_name.to_string(),
                schemas: vec![graph::Schema {
                    name: schema_name.to_string(),
                    objects: vec![db_obj],
                    mod_statements: None,
                    schema_type: graph::SchemaType::Compute,
                }],
                mod_statements: None,
            }],
            dependency_graph: BTreeMap::new(),
            external_dependencies: BTreeSet::new(),
            cluster_dependencies: BTreeSet::new(),
            tests: vec![],
            replacement_schemas: BTreeSet::new(),
        }
    }

    #[test]
    fn write_project_persists_aliases() {
        let temp = tempdir().unwrap();
        let mut db = open_db(temp.path());

        let stmt = parse_stmt("CREATE VIEW v AS SELECT o.id FROM orders AS o");
        let project = make_project("mydb", "public", stmt);
        db.write_project(&project, temp.path()).unwrap();

        // Query the aliases table.
        let rows: Vec<(String, String, String)> = db
            .conn
            .prepare("SELECT object_key, alias, target_fqn FROM project_aliases ORDER BY alias")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // "orders" is the bare name alias, "o" is the explicit alias.
        // Both should map to the FQN "mydb.public.orders".
        assert_eq!(rows.len(), 2);

        let o_row = rows.iter().find(|(_, alias, _)| alias == "o").unwrap();
        assert_eq!(o_row.0, "mydb.public.v");
        assert_eq!(o_row.2, "mydb.public.orders");

        let orders_row = rows.iter().find(|(_, alias, _)| alias == "orders").unwrap();
        assert_eq!(orders_row.0, "mydb.public.v");
        assert_eq!(orders_row.2, "mydb.public.orders");
    }

    #[test]
    fn write_project_no_aliases_for_table_stmt() {
        let temp = tempdir().unwrap();
        let mut db = open_db(temp.path());

        let stmt = parse_stmt("CREATE TABLE t (id INT)");
        let project = make_project("mydb", "public", stmt);
        db.write_project(&project, temp.path()).unwrap();

        let count: i64 = db
            .conn
            .query_row("SELECT COUNT(*) FROM project_aliases", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }
}
