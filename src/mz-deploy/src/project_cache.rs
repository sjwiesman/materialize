//! Read-only interface to cached project and typecheck artifacts.
//!
//! [`ProjectCache`] provides typed access to project metadata and internal
//! object column schemas stored in the compiler's SQLite database. It holds
//! a read-only connection and serves per-object queries lazily. Consumers
//! see this as an opaque data structure — SQLite is an implementation detail.

use crate::types::{ColumnType, ObjectKind};
use rusqlite::{Connection, OpenFlags, params};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// Full metadata for one project object.
#[derive(Debug, Clone)]
pub struct CachedObject {
    pub fqn: String,
    pub database: String,
    pub schema: String,
    pub name: String,
    pub kind: ObjectKind,
    pub cluster: Option<String>,
    pub file_path: String,
    pub sql_text: String,
    pub is_constraint_mv: bool,
    pub comments: Vec<CachedComment>,
    pub indexes: Vec<CachedIndex>,
    pub constraints: Vec<CachedConstraint>,
    pub grants: Vec<CachedGrant>,
    pub aliases: BTreeMap<String, String>,
    pub infrastructure: Option<CachedInfrastructure>,
}

/// Lightweight summary of a project object (no SQL text or sub-collections).
#[derive(Debug, Clone)]
pub struct CachedObjectSummary {
    pub fqn: String,
    pub database: String,
    pub schema: String,
    pub name: String,
    pub kind: ObjectKind,
    pub cluster: Option<String>,
    pub file_path: String,
    pub is_constraint_mv: bool,
}

/// A database declared in the project.
#[derive(Debug, Clone)]
pub struct CachedDatabase {
    pub name: String,
    pub schemas: Vec<CachedSchema>,
}

/// A schema within a database, with full metadata for each contained object.
#[derive(Debug, Clone)]
pub struct CachedSchema {
    pub name: String,
    pub schema_type: String,
    pub objects: Vec<CachedObject>,
}

/// A SQL comment attached to an object or one of its columns.
#[derive(Debug, Clone)]
pub struct CachedComment {
    pub comment_type: String,
    pub target_column: Option<String>,
    pub text: String,
    pub sql_text: String,
}

/// An index defined on an object.
#[derive(Debug, Clone)]
pub struct CachedIndex {
    pub name: String,
    pub cluster: Option<String>,
    pub columns: String,
    pub sql_text: String,
}

/// A constraint defined on an object.
#[derive(Debug, Clone)]
pub struct CachedConstraint {
    pub name: String,
    pub kind: String,
    pub enforced: bool,
    pub columns: Vec<String>,
    pub ref_object: Option<String>,
    pub ref_columns: Option<Vec<String>>,
    pub sql_text: String,
}

/// A privilege grant on an object.
#[derive(Debug, Clone)]
pub struct CachedGrant {
    pub privilege: String,
    pub grantee: String,
    pub sql_text: String,
}

/// Infrastructure metadata for a source, sink, or connection.
#[derive(Debug, Clone)]
pub struct CachedInfrastructure {
    pub infra_type: String,
    pub connector_type: Option<String>,
    pub connection_ref: Option<String>,
    pub source_ref: Option<String>,
    pub external_reference: Option<String>,
    pub properties: Vec<CachedProperty>,
}

/// A key-value property within infrastructure metadata.
#[derive(Debug, Clone)]
pub struct CachedProperty {
    pub key: String,
    pub value: String,
    pub secret_ref: Option<String>,
    pub object_ref: Option<String>,
}

/// A unit test associated with an object.
#[derive(Debug, Clone)]
pub struct CachedTest {
    pub name: String,
    pub sql_text: String,
}

/// Read-only handle to compiled project metadata and typecheck artifacts.
///
/// Returns `None` from queries when data is missing rather than erroring,
/// since the cache is advisory.
pub struct ProjectCache {
    conn: Connection,
}

impl ProjectCache {
    /// Open a read-only connection to the build artifact database.
    ///
    /// Returns `Ok(None)` if the database file doesn't exist yet (project
    /// has never been compiled).
    pub fn open(
        directory: &Path,
        profile: &str,
        profile_suffix: Option<&str>,
        variables: &BTreeMap<String, String>,
    ) -> Result<Option<Self>, crate::types::TypesError> {
        let path = crate::project::compiler::build_artifact::build_artifact_path(
            directory,
            profile,
            profile_suffix,
            variables,
        );
        if !path.exists() {
            return Ok(None);
        }
        let conn = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| crate::types::TypesError::FileReadFailed {
            path: path.clone(),
            source: std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
        })?;
        Ok(Some(Self { conn }))
    }

    /// Get the column schema for an object by fully-qualified name.
    pub fn get_columns(&self, fqn: &str) -> Option<BTreeMap<String, ColumnType>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT column_name, column_type, nullable, position \
                 FROM typecheck_columns WHERE object_key = ?1",
            )
            .ok()?;
        let rows = stmt
            .query_map(params![fqn], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    ColumnType {
                        r#type: row.get(1)?,
                        nullable: row.get::<_, i32>(2)? != 0,
                        position: usize::try_from(row.get::<_, i64>(3)?).unwrap_or(0),
                        comment: None,
                    },
                ))
            })
            .ok()?;
        let mut columns = BTreeMap::new();
        for row in rows {
            let (name, col_type) = row.ok()?;
            columns.insert(name, col_type);
        }
        if columns.is_empty() {
            None
        } else {
            Some(columns)
        }
    }

    /// Get the object kind for a fully-qualified name.
    pub fn get_kind(&self, fqn: &str) -> Option<ObjectKind> {
        self.conn
            .query_row(
                "SELECT object_kind FROM typecheck_objects WHERE object_key = ?1",
                params![fqn],
                |row| {
                    let kind_str: String = row.get(0)?;
                    Ok(ObjectKind::from_db_str(&kind_str))
                },
            )
            .ok()
    }

    /// Get lowercased column names for a batch of objects.
    ///
    /// Used by constraint validation. Issues a single SQL query for all
    /// requested FQNs rather than materializing the full types cache.
    pub fn get_column_names(&self, fqns: &[&str]) -> BTreeMap<String, BTreeSet<String>> {
        if fqns.is_empty() {
            return BTreeMap::new();
        }
        let placeholders: Vec<String> = (1..=fqns.len()).map(|i| format!("?{}", i)).collect();
        let sql = format!(
            "SELECT object_key, column_name FROM typecheck_columns WHERE object_key IN ({})",
            placeholders.join(", ")
        );
        let mut stmt = match self.conn.prepare(&sql) {
            Ok(s) => s,
            Err(_) => return BTreeMap::new(),
        };
        let params: Vec<&dyn rusqlite::ToSql> =
            fqns.iter().map(|s| -> &dyn rusqlite::ToSql { s }).collect();
        let rows = match stmt.query_map(params.as_slice(), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        }) {
            Ok(r) => r,
            Err(_) => return BTreeMap::new(),
        };
        let mut result: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for row in rows {
            if let Ok((key, col)) = row {
                result
                    .entry(key.to_lowercase())
                    .or_default()
                    .insert(col.to_lowercase());
            }
        }
        result
    }

    /// Get full metadata for a project object by fully-qualified name.
    ///
    /// Queries the object row plus all associated comments, indexes,
    /// constraints, grants, and infrastructure. Returns `None` if the
    /// object doesn't exist in the cache.
    pub fn get_object(&self, fqn: &str) -> Option<CachedObject> {
        let row = self
            .conn
            .query_row(
                "SELECT object_key, database, schema, name, object_kind, cluster, \
                 file_path, sql_text, is_constraint_mv \
                 FROM project_objects WHERE object_key = ?1",
                params![fqn],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, bool>(8)?,
                    ))
                },
            )
            .ok()?;

        let (
            object_key,
            database,
            schema,
            name,
            kind_str,
            cluster,
            file_path,
            sql_text,
            is_constraint_mv,
        ) = row;
        let kind = ObjectKind::from_db_str(&kind_str);

        let comments = self.query_comments(&object_key);
        let indexes = self.query_indexes(&object_key);
        let constraints = self.query_constraints(&object_key);
        let grants = self.query_grants(&object_key);
        let aliases = self.query_aliases(&object_key);
        let infrastructure = self.query_infrastructure(&object_key);

        Some(CachedObject {
            fqn: object_key,
            database,
            schema,
            name,
            kind,
            cluster,
            file_path,
            sql_text,
            is_constraint_mv,
            comments,
            indexes,
            constraints,
            grants,
            aliases,
            infrastructure,
        })
    }

    /// Get full metadata for a project object by its source file path.
    pub fn get_object_by_path(&self, file_path: &str) -> Option<CachedObject> {
        let fqn: String = self
            .conn
            .query_row(
                "SELECT object_key FROM project_objects WHERE file_path = ?1",
                params![file_path],
                |row| row.get(0),
            )
            .ok()?;
        self.get_object(&fqn)
    }

    /// List all project objects as lightweight summaries.
    pub fn list_objects(&self) -> Vec<CachedObjectSummary> {
        let mut stmt = match self.conn.prepare(
            "SELECT object_key, database, schema, name, object_kind, cluster, \
             file_path, is_constraint_mv FROM project_objects",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = match stmt.query_map([], |row| {
            Ok(CachedObjectSummary {
                fqn: row.get(0)?,
                database: row.get(1)?,
                schema: row.get(2)?,
                name: row.get(3)?,
                kind: ObjectKind::from_db_str(&row.get::<_, String>(4)?),
                cluster: row.get(5)?,
                file_path: row.get(6)?,
                is_constraint_mv: row.get(7)?,
            })
        }) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        rows.filter_map(|r| r.ok()).collect()
    }

    /// Returns a complete project catalog — all databases, schemas, and objects
    /// with full metadata (comments, indexes, constraints, grants,
    /// infrastructure).
    pub fn list_databases_with_objects(&self) -> Vec<CachedDatabase> {
        let mut db_stmt = match self.conn.prepare("SELECT name FROM project_databases") {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let db_names: Vec<String> = match db_stmt.query_map([], |row| row.get(0)) {
            Ok(r) => r.filter_map(|r| r.ok()).collect(),
            Err(_) => return Vec::new(),
        };

        let mut result = Vec::new();
        for db_name in db_names {
            let mut schema_stmt = match self
                .conn
                .prepare("SELECT name, schema_type FROM project_schemas WHERE database = ?1")
            {
                Ok(s) => s,
                Err(_) => continue,
            };
            let schemas: Vec<CachedSchema> = match schema_stmt.query_map(params![&db_name], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            }) {
                Ok(r) => r
                    .filter_map(|r| r.ok())
                    .map(|(schema_name, schema_type)| {
                        let object_keys = self.query_object_keys_in_schema(&db_name, &schema_name);
                        let objects = object_keys
                            .iter()
                            .filter_map(|key| self.get_object(key))
                            .collect();
                        CachedSchema {
                            name: schema_name,
                            schema_type,
                            objects,
                        }
                    })
                    .collect(),
                Err(_) => Vec::new(),
            };
            result.push(CachedDatabase {
                name: db_name,
                schemas,
            });
        }
        result
    }

    /// List all external dependency FQNs.
    pub fn list_external_dependencies(&self) -> Vec<String> {
        let mut stmt = match self
            .conn
            .prepare("SELECT object_key FROM project_external_dependencies")
        {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        match stmt.query_map([], |row| row.get(0)) {
            Ok(r) => r.filter_map(|r| r.ok()).collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Get the FQNs of objects that `fqn` depends on.
    pub fn get_dependencies(&self, fqn: &str) -> Vec<String> {
        let mut stmt = match self
            .conn
            .prepare("SELECT dependency_key FROM project_dependencies WHERE object_key = ?1")
        {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        match stmt.query_map(params![fqn], |row| row.get(0)) {
            Ok(r) => r.filter_map(|r| r.ok()).collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Get the FQNs of objects that depend on `fqn` (reverse lookup).
    pub fn get_dependents(&self, fqn: &str) -> Vec<String> {
        let mut stmt = match self
            .conn
            .prepare("SELECT object_key FROM project_dependencies WHERE dependency_key = ?1")
        {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        match stmt.query_map(params![fqn], |row| row.get(0)) {
            Ok(r) => r.filter_map(|r| r.ok()).collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Get unit tests associated with an object.
    pub fn get_tests(&self, fqn: &str) -> Vec<CachedTest> {
        let mut stmt = match self
            .conn
            .prepare("SELECT test_name, sql_text FROM project_tests WHERE object_key = ?1")
        {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        match stmt.query_map(params![fqn], |row| {
            Ok(CachedTest {
                name: row.get(0)?,
                sql_text: row.get(1)?,
            })
        }) {
            Ok(r) => r.filter_map(|r| r.ok()).collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Get mod statements for a database/schema, ordered by position.
    pub fn get_mod_statements(&self, database: &str, schema: Option<&str>) -> Vec<String> {
        let mut stmt = match self.conn.prepare(
            "SELECT sql_text FROM project_mod_statements \
             WHERE database = ?1 AND schema IS ?2 \
             ORDER BY position",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        match stmt.query_map(params![database, schema], |row| row.get(0)) {
            Ok(r) => r.filter_map(|r| r.ok()).collect(),
            Err(_) => Vec::new(),
        }
    }

    fn query_comments(&self, object_key: &str) -> Vec<CachedComment> {
        let mut stmt = match self.conn.prepare(
            "SELECT comment_type, target_column, comment_text, sql_text \
             FROM project_comments WHERE object_key = ?1",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        match stmt.query_map(params![object_key], |row| {
            Ok(CachedComment {
                comment_type: row.get(0)?,
                target_column: row.get(1)?,
                text: row.get(2)?,
                sql_text: row.get(3)?,
            })
        }) {
            Ok(r) => r.filter_map(|r| r.ok()).collect(),
            Err(_) => Vec::new(),
        }
    }

    fn query_indexes(&self, object_key: &str) -> Vec<CachedIndex> {
        let mut stmt = match self.conn.prepare(
            "SELECT index_name, cluster, columns, sql_text \
             FROM project_indexes WHERE object_key = ?1",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        match stmt.query_map(params![object_key], |row| {
            Ok(CachedIndex {
                name: row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                cluster: row.get(1)?,
                columns: row.get(2)?,
                sql_text: row.get(3)?,
            })
        }) {
            Ok(r) => r.filter_map(|r| r.ok()).collect(),
            Err(_) => Vec::new(),
        }
    }

    fn query_constraints(&self, object_key: &str) -> Vec<CachedConstraint> {
        let mut stmt = match self.conn.prepare(
            "SELECT constraint_name, kind, enforced, columns, ref_object, ref_columns, sql_text \
             FROM project_constraints WHERE object_key = ?1",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        match stmt.query_map(params![object_key], |row| {
            let columns_json: String = row.get(3)?;
            let ref_columns_json: Option<String> = row.get(5)?;
            Ok(CachedConstraint {
                name: row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                kind: row.get(1)?,
                enforced: row.get(2)?,
                columns: serde_json::from_str(&columns_json).unwrap_or_default(),
                ref_object: row.get(4)?,
                ref_columns: ref_columns_json.and_then(|j| serde_json::from_str(&j).ok()),
                sql_text: row.get(6)?,
            })
        }) {
            Ok(r) => r.filter_map(|r| r.ok()).collect(),
            Err(_) => Vec::new(),
        }
    }

    fn query_grants(&self, object_key: &str) -> Vec<CachedGrant> {
        let mut stmt = match self.conn.prepare(
            "SELECT privilege, grantee, sql_text \
             FROM project_grants WHERE object_key = ?1",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        match stmt.query_map(params![object_key], |row| {
            Ok(CachedGrant {
                privilege: row.get(0)?,
                grantee: row.get(1)?,
                sql_text: row.get(2)?,
            })
        }) {
            Ok(r) => r.filter_map(|r| r.ok()).collect(),
            Err(_) => Vec::new(),
        }
    }

    fn query_infrastructure(&self, object_key: &str) -> Option<CachedInfrastructure> {
        let row = self
            .conn
            .query_row(
                "SELECT infra_type, connector_type, connection_ref, source_ref, external_reference \
                 FROM project_infrastructure WHERE object_key = ?1",
                params![object_key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                },
            )
            .ok()?;

        let (infra_type, connector_type, connection_ref, source_ref, external_reference) = row;
        let properties = self.query_infrastructure_properties(object_key);

        Some(CachedInfrastructure {
            infra_type,
            connector_type,
            connection_ref,
            source_ref,
            external_reference,
            properties,
        })
    }

    fn query_infrastructure_properties(&self, object_key: &str) -> Vec<CachedProperty> {
        let mut stmt = match self.conn.prepare(
            "SELECT property_key, property_value, secret_ref, object_ref \
             FROM project_infrastructure_properties WHERE object_key = ?1",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        match stmt.query_map(params![object_key], |row| {
            Ok(CachedProperty {
                key: row.get(0)?,
                value: row.get(1)?,
                secret_ref: row.get(2)?,
                object_ref: row.get(3)?,
            })
        }) {
            Ok(r) => r.filter_map(|r| r.ok()).collect(),
            Err(_) => Vec::new(),
        }
    }

    fn query_aliases(&self, object_key: &str) -> BTreeMap<String, String> {
        let mut stmt = match self
            .conn
            .prepare("SELECT alias, target_fqn FROM project_aliases WHERE object_key = ?1")
        {
            Ok(s) => s,
            Err(_) => return BTreeMap::new(),
        };
        match stmt.query_map(params![object_key], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        }) {
            Ok(r) => r.filter_map(|r| r.ok()).collect(),
            Err(_) => BTreeMap::new(),
        }
    }

    fn query_object_keys_in_schema(&self, database: &str, schema: &str) -> Vec<String> {
        let mut stmt = match self
            .conn
            .prepare("SELECT object_key FROM project_objects WHERE database = ?1 AND schema = ?2")
        {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        match stmt.query_map(params![database, schema], |row| row.get(0)) {
            Ok(r) => r.filter_map(|r| r.ok()).collect(),
            Err(_) => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::collections::BTreeMap;

    /// Create a test SQLite DB with typecheck + project schemas.
    fn create_test_db(path: &Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS typecheck_objects (
                object_key TEXT PRIMARY KEY,
                semantic_fingerprint TEXT NOT NULL,
                output_fingerprint TEXT NOT NULL,
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
            ",
        )
        .unwrap();
        conn
    }

    fn open_cache(path: &Path) -> ProjectCache {
        ProjectCache {
            conn: Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .unwrap(),
        }
    }

    #[test]
    fn test_open_returns_none_when_no_db() {
        let dir = tempfile::tempdir().unwrap();
        let result = ProjectCache::open(dir.path(), "default", None, &BTreeMap::new());
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_get_columns_found() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = create_test_db(&db_path);
        conn.execute(
            "INSERT INTO typecheck_objects (object_key, semantic_fingerprint, output_fingerprint, object_kind) \
             VALUES (?1, ?2, ?3, ?4)",
            params!["db.schema.my_view", "abc", "def", "view"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO typecheck_columns (object_key, column_name, column_type, nullable, position) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params!["db.schema.my_view", "id", "integer", 0, 1],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO typecheck_columns (object_key, column_name, column_type, nullable, position) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params!["db.schema.my_view", "name", "text", 1, 2],
        )
        .unwrap();
        drop(conn);

        let cache = ProjectCache {
            conn: Connection::open_with_flags(
                &db_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .unwrap(),
        };
        let columns = cache.get_columns("db.schema.my_view").unwrap();
        assert_eq!(columns.len(), 2);

        let id_col = &columns["id"];
        assert_eq!(id_col.r#type, "integer");
        assert!(!id_col.nullable);
        assert_eq!(id_col.position, 1);

        let name_col = &columns["name"];
        assert_eq!(name_col.r#type, "text");
        assert!(name_col.nullable);
        assert_eq!(name_col.position, 2);
    }

    #[test]
    fn test_get_columns_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let _conn = create_test_db(&db_path);

        let cache = ProjectCache {
            conn: Connection::open_with_flags(
                &db_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .unwrap(),
        };
        assert!(cache.get_columns("nonexistent.object").is_none());
    }

    #[test]
    fn test_get_kind_found() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = create_test_db(&db_path);
        conn.execute(
            "INSERT INTO typecheck_objects (object_key, semantic_fingerprint, output_fingerprint, object_kind) \
             VALUES (?1, ?2, ?3, ?4)",
            params!["db.schema.my_mv", "abc", "def", "materialized-view"],
        )
        .unwrap();
        drop(conn);

        let cache = ProjectCache {
            conn: Connection::open_with_flags(
                &db_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .unwrap(),
        };
        assert_eq!(
            cache.get_kind("db.schema.my_mv"),
            Some(ObjectKind::MaterializedView)
        );
    }

    #[test]
    fn test_get_kind_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let _conn = create_test_db(&db_path);

        let cache = ProjectCache {
            conn: Connection::open_with_flags(
                &db_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .unwrap(),
        };
        assert!(cache.get_kind("nonexistent.object").is_none());
    }

    #[test]
    fn test_get_column_names_batch() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = create_test_db(&db_path);

        // Insert two objects with columns
        conn.execute(
            "INSERT INTO typecheck_objects (object_key, semantic_fingerprint, output_fingerprint, object_kind) \
             VALUES (?1, ?2, ?3, ?4)",
            params!["db.schema.obj_a", "a1", "a2", "view"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO typecheck_objects (object_key, semantic_fingerprint, output_fingerprint, object_kind) \
             VALUES (?1, ?2, ?3, ?4)",
            params!["db.schema.obj_b", "b1", "b2", "table"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO typecheck_columns (object_key, column_name, column_type, nullable, position) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params!["db.schema.obj_a", "Col_X", "integer", 0, 1],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO typecheck_columns (object_key, column_name, column_type, nullable, position) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params!["db.schema.obj_b", "Col_Y", "text", 1, 1],
        )
        .unwrap();
        drop(conn);

        let cache = ProjectCache {
            conn: Connection::open_with_flags(
                &db_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .unwrap(),
        };

        let result = cache.get_column_names(&["db.schema.obj_a", "db.schema.obj_b"]);
        assert_eq!(result.len(), 2);
        assert!(result["db.schema.obj_a"].contains("col_x"));
        assert!(result["db.schema.obj_b"].contains("col_y"));
    }

    /// Insert a sample object with all metadata for testing.
    fn insert_sample_project(conn: &Connection) {
        conn.execute(
            "INSERT INTO project_databases (name) VALUES (?1)",
            params!["mydb"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO project_schemas (database, name, schema_type) VALUES (?1, ?2, ?3)",
            params!["mydb", "public", "user"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO project_objects (object_key, database, schema, name, object_kind, cluster, file_path, sql_text, is_constraint_mv) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                "mydb.public.orders",
                "mydb",
                "public",
                "orders",
                "materialized-view",
                "compute",
                "sql/orders.sql",
                "CREATE MATERIALIZED VIEW orders AS SELECT 1",
                false
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO project_objects (object_key, database, schema, name, object_kind, cluster, file_path, sql_text, is_constraint_mv) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                "mydb.public.users",
                "mydb",
                "public",
                "users",
                "view",
                None::<String>,
                "sql/users.sql",
                "CREATE VIEW users AS SELECT 1",
                false
            ],
        )
        .unwrap();
        // Comment
        conn.execute(
            "INSERT INTO project_comments (object_key, comment_type, target_column, comment_text, sql_text) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                "mydb.public.orders",
                "object",
                None::<String>,
                "Order data",
                "COMMENT ON MATERIALIZED VIEW orders IS 'Order data'"
            ],
        )
        .unwrap();
        // Index
        conn.execute(
            "INSERT INTO project_indexes (object_key, index_name, cluster, columns, sql_text) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                "mydb.public.orders",
                "orders_id_idx",
                "compute",
                "id",
                "CREATE INDEX orders_id_idx ON orders (id)"
            ],
        )
        .unwrap();
        // Constraint with JSON columns
        conn.execute(
            "INSERT INTO project_constraints (object_key, constraint_name, kind, enforced, columns, ref_object, ref_columns, sql_text) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                "mydb.public.orders",
                "orders_pk",
                "PRIMARY KEY",
                true,
                r#"["id"]"#,
                None::<String>,
                None::<String>,
                "ALTER TABLE orders ADD PRIMARY KEY (id) NOT ENFORCED"
            ],
        )
        .unwrap();
        // Grant
        conn.execute(
            "INSERT INTO project_grants (object_key, privilege, grantee, sql_text) \
             VALUES (?1, ?2, ?3, ?4)",
            params![
                "mydb.public.orders",
                "SELECT",
                "reader_role",
                "GRANT SELECT ON orders TO reader_role"
            ],
        )
        .unwrap();
        // Aliases
        conn.execute(
            "INSERT INTO project_aliases (object_key, alias, target_fqn) VALUES (?1, ?2, ?3)",
            params!["mydb.public.orders", "raw_orders", "ext.public.raw_orders"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO project_aliases (object_key, alias, target_fqn) VALUES (?1, ?2, ?3)",
            params![
                "mydb.public.orders",
                "order_items",
                "ext.public.order_items"
            ],
        )
        .unwrap();
        // Infrastructure
        conn.execute(
            "INSERT INTO project_infrastructure (object_key, infra_type, connector_type, connection_ref, source_ref, external_reference) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                "mydb.public.orders",
                "source",
                "postgres",
                "mydb.public.pg_conn",
                None::<String>,
                None::<String>
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO project_infrastructure_properties (object_key, property_key, property_value, secret_ref, object_ref) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                "mydb.public.orders",
                "PUBLICATION",
                "mz_source",
                None::<String>,
                None::<String>
            ],
        )
        .unwrap();
        // Dependencies: orders depends on users
        conn.execute(
            "INSERT INTO project_dependencies (object_key, dependency_key) VALUES (?1, ?2)",
            params!["mydb.public.orders", "mydb.public.users"],
        )
        .unwrap();
        // External dependency
        conn.execute(
            "INSERT INTO project_external_dependencies (object_key) VALUES (?1)",
            params!["ext.public.raw_data"],
        )
        .unwrap();
        // Tests
        conn.execute(
            "INSERT INTO project_tests (object_key, test_name, sql_text) VALUES (?1, ?2, ?3)",
            params![
                "mydb.public.orders",
                "test_orders_not_empty",
                "SELECT count(*) > 0 FROM orders"
            ],
        )
        .unwrap();
        // Mod statements
        conn.execute(
            "INSERT INTO project_mod_statements (database, schema, position, sql_text) \
             VALUES (?1, ?2, ?3, ?4)",
            params!["mydb", None::<String>, 0, "CREATE DATABASE mydb"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO project_mod_statements (database, schema, position, sql_text) \
             VALUES (?1, ?2, ?3, ?4)",
            params!["mydb", "public", 0, "CREATE SCHEMA public"],
        )
        .unwrap();
    }

    #[test]
    fn test_get_object_full_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = create_test_db(&db_path);
        insert_sample_project(&conn);
        drop(conn);

        let cache = open_cache(&db_path);
        let obj = cache.get_object("mydb.public.orders").unwrap();

        assert_eq!(obj.fqn, "mydb.public.orders");
        assert_eq!(obj.database, "mydb");
        assert_eq!(obj.schema, "public");
        assert_eq!(obj.name, "orders");
        assert_eq!(obj.kind, ObjectKind::MaterializedView);
        assert_eq!(obj.cluster.as_deref(), Some("compute"));
        assert_eq!(obj.file_path, "sql/orders.sql");
        assert!(!obj.is_constraint_mv);

        // Comments
        assert_eq!(obj.comments.len(), 1);
        assert_eq!(obj.comments[0].comment_type, "object");
        assert_eq!(obj.comments[0].text, "Order data");

        // Indexes
        assert_eq!(obj.indexes.len(), 1);
        assert_eq!(obj.indexes[0].name, "orders_id_idx");

        // Constraints
        assert_eq!(obj.constraints.len(), 1);
        assert_eq!(obj.constraints[0].kind, "PRIMARY KEY");
        assert_eq!(obj.constraints[0].columns, vec!["id"]);
        assert!(obj.constraints[0].enforced);

        // Grants
        assert_eq!(obj.grants.len(), 1);
        assert_eq!(obj.grants[0].privilege, "SELECT");
        assert_eq!(obj.grants[0].grantee, "reader_role");

        // Aliases
        assert_eq!(obj.aliases.len(), 2);
        assert_eq!(obj.aliases["order_items"], "ext.public.order_items");
        assert_eq!(obj.aliases["raw_orders"], "ext.public.raw_orders");

        // Infrastructure
        let infra = obj.infrastructure.unwrap();
        assert_eq!(infra.infra_type, "source");
        assert_eq!(infra.connector_type.as_deref(), Some("postgres"));
        assert_eq!(infra.connection_ref.as_deref(), Some("mydb.public.pg_conn"));
        assert_eq!(infra.properties.len(), 1);
        assert_eq!(infra.properties[0].key, "PUBLICATION");
    }

    #[test]
    fn test_get_object_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = create_test_db(&db_path);
        insert_sample_project(&conn);
        drop(conn);

        let cache = open_cache(&db_path);
        assert!(cache.get_object("nonexistent").is_none());
    }

    #[test]
    fn test_get_object_by_path() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = create_test_db(&db_path);
        insert_sample_project(&conn);
        drop(conn);

        let cache = open_cache(&db_path);
        let obj = cache.get_object_by_path("sql/orders.sql").unwrap();
        assert_eq!(obj.fqn, "mydb.public.orders");
    }

    #[test]
    fn test_list_objects() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = create_test_db(&db_path);
        insert_sample_project(&conn);
        drop(conn);

        let cache = open_cache(&db_path);
        let objects = cache.list_objects();
        assert_eq!(objects.len(), 2);

        let fqns: Vec<&str> = objects.iter().map(|o| o.fqn.as_str()).collect();
        assert!(fqns.contains(&"mydb.public.orders"));
        assert!(fqns.contains(&"mydb.public.users"));
    }

    #[test]
    fn test_list_databases() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = create_test_db(&db_path);
        insert_sample_project(&conn);
        drop(conn);

        let cache = open_cache(&db_path);
        let databases = cache.list_databases_with_objects();
        assert_eq!(databases.len(), 1);
        assert_eq!(databases[0].name, "mydb");
        assert_eq!(databases[0].schemas.len(), 1);
        assert_eq!(databases[0].schemas[0].name, "public");
        assert_eq!(databases[0].schemas[0].schema_type, "user");
        assert_eq!(databases[0].schemas[0].objects.len(), 2);

        let fqns: Vec<&str> = databases[0].schemas[0]
            .objects
            .iter()
            .map(|o| o.fqn.as_str())
            .collect();
        assert!(fqns.contains(&"mydb.public.orders"));
        assert!(fqns.contains(&"mydb.public.users"));
    }

    #[test]
    fn test_get_dependencies_and_dependents() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = create_test_db(&db_path);
        insert_sample_project(&conn);
        drop(conn);

        let cache = open_cache(&db_path);

        // orders depends on users
        let deps = cache.get_dependencies("mydb.public.orders");
        assert_eq!(deps, vec!["mydb.public.users"]);

        // users has orders as a dependent
        let dependents = cache.get_dependents("mydb.public.users");
        assert_eq!(dependents, vec!["mydb.public.orders"]);

        // no dependencies for users
        assert!(cache.get_dependencies("mydb.public.users").is_empty());
    }

    #[test]
    fn test_list_external_dependencies() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = create_test_db(&db_path);
        insert_sample_project(&conn);
        drop(conn);

        let cache = open_cache(&db_path);
        let ext = cache.list_external_dependencies();
        assert_eq!(ext, vec!["ext.public.raw_data"]);
    }

    #[test]
    fn test_get_tests() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = create_test_db(&db_path);
        insert_sample_project(&conn);
        drop(conn);

        let cache = open_cache(&db_path);
        let tests = cache.get_tests("mydb.public.orders");
        assert_eq!(tests.len(), 1);
        assert_eq!(tests[0].name, "test_orders_not_empty");

        // no tests for users
        assert!(cache.get_tests("mydb.public.users").is_empty());
    }

    #[test]
    fn test_get_mod_statements() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = create_test_db(&db_path);
        insert_sample_project(&conn);
        drop(conn);

        let cache = open_cache(&db_path);

        // Database-level mod statement
        let db_mods = cache.get_mod_statements("mydb", None);
        assert_eq!(db_mods, vec!["CREATE DATABASE mydb"]);

        // Schema-level mod statement
        let schema_mods = cache.get_mod_statements("mydb", Some("public"));
        assert_eq!(schema_mods, vec!["CREATE SCHEMA public"]);

        // No statements for unknown
        assert!(cache.get_mod_statements("unknown", None).is_empty());
    }

    #[test]
    fn test_constraint_with_ref_columns() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = create_test_db(&db_path);
        conn.execute(
            "INSERT INTO project_objects (object_key, database, schema, name, object_kind, file_path, sql_text, is_constraint_mv) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params!["db.s.t", "db", "s", "t", "table", "t.sql", "CREATE TABLE t (a INT)", false],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO project_constraints (object_key, constraint_name, kind, enforced, columns, ref_object, ref_columns, sql_text) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                "db.s.t",
                "fk_orders",
                "FOREIGN KEY",
                false,
                r#"["order_id"]"#,
                "db.s.orders",
                r#"["id"]"#,
                "ALTER TABLE t ADD FOREIGN KEY (order_id) REFERENCES orders (id)"
            ],
        )
        .unwrap();
        drop(conn);

        let cache = open_cache(&db_path);
        let obj = cache.get_object("db.s.t").unwrap();
        assert_eq!(obj.constraints.len(), 1);
        let c = &obj.constraints[0];
        assert_eq!(c.kind, "FOREIGN KEY");
        assert_eq!(c.columns, vec!["order_id"]);
        assert_eq!(c.ref_object.as_deref(), Some("db.s.orders"));
        assert_eq!(c.ref_columns, Some(vec!["id".to_string()]));
    }
}
