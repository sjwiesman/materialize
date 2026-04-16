//! Catalog response builder for the `mz-deploy/catalog` custom LSP endpoint.
//!
//! Builds a JSON representation of the project's data catalog for rendering in
//! the VS Code workspace webview. The response contains two collections:
//!
//! - **`databases`** — A tree of [`CatalogDatabase`] → [`CatalogSchema`] entries
//!   providing the sidebar navigation structure with object IDs grouped by
//!   schema.
//!
//! - **`objects`** — One [`CatalogObject`] per project object plus one per
//!   external dependency. Each object carries full metadata for the detail panel:
//!   type, cluster, file path, description, columns, dependencies, dependents,
//!   indexes, constraints, grants, and infrastructure properties.
//!
//! ## Infrastructure Properties
//!
//! Connections, sources, and tables-from-source carry structured metadata
//! extracted from their AST statements. The [`CatalogInfrastructure`] enum
//! holds connector type, key-value properties (with optional secret/object
//! refs for linking), and upstream references. The webview uses these to
//! render type-specific detail layouts.
//!
//! ## Differences from `DagResponse`
//!
//! The DAG endpoint (`dag.rs`) returns only graph topology and minimal node
//! metadata for visualization. This endpoint returns the full object metadata
//! needed for a data catalog experience — columns, constraints, grants, indexes,
//! and descriptions — but omits SQL text and edges (which are only needed for
//! graph rendering).
//!
//! ## Object Categories
//!
//! The catalog contains two kinds of objects:
//!
//! - **Project objects** — Defined in the project's `.sql` files. These carry full
//!   metadata: file path, columns, indexes, constraints, grants, and infrastructure
//!   properties.
//!
//! - **External dependencies** — Referenced by project objects but not defined in
//!   the project. These appear as stubs with `is_external: true`, carrying only
//!   an ID, parsed name parts, and any columns available from the types cache.
//!
//! Constraint-enforcement materialized views (auto-generated companions for
//! `ENFORCED` constraints) are excluded from both the sidebar tree and dependents
//! lists to avoid cluttering the catalog with internal implementation objects.
//!
//! ## Entry Points
//!
//! - [`build_catalog_response`] — Builds the full catalog from a successful
//!   project compilation.
//! - [`build_error_response`] — Returns an empty catalog with error messages
//!   when the project fails to compile.

use crate::project_cache::{CachedObject, ProjectCache};
use crate::types::{ColumnType, ObjectKind, Types};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;

/// Complete catalog response returned by the `mz-deploy/catalog` endpoint.
#[derive(Debug, Serialize)]
pub struct CatalogResponse {
    /// Database → schema tree for sidebar navigation.
    pub databases: Vec<CatalogDatabase>,
    /// All objects with full metadata for the detail panel.
    pub objects: Vec<CatalogObject>,
    /// Build errors when the project failed to compile.
    /// Empty on success; populated when `plan_sync()` fails.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<CatalogError>,
}

/// A project build error surfaced in the catalog sidebar.
#[derive(Debug, Serialize)]
pub struct CatalogError {
    /// Error message describing the build failure.
    pub message: String,
}

/// A database in the catalog sidebar tree.
#[derive(Debug, Serialize)]
pub struct CatalogDatabase {
    /// Database name.
    pub name: String,
    /// Schemas within this database.
    pub schemas: Vec<CatalogSchema>,
}

/// A schema in the catalog sidebar tree.
#[derive(Debug, Serialize)]
pub struct CatalogSchema {
    /// Schema name.
    pub name: String,
    /// Fully-qualified IDs of objects in this schema (ordering matches project).
    pub object_ids: Vec<String>,
}

/// A single object in the catalog with full metadata.
#[derive(Debug, Serialize)]
pub struct CatalogObject {
    /// Fully-qualified object ID (e.g., `"db.schema.name"`).
    pub id: String,
    /// Database name.
    pub database: String,
    /// Schema name.
    pub schema: String,
    /// Unqualified object name.
    pub name: String,
    /// Object type (e.g., `"view"`, `"materialized-view"`, `"table"`).
    pub object_type: String,
    /// Cluster the object is materialized on, if any.
    pub cluster: Option<String>,
    /// Relative file path to the `.sql` source file, or `null` for external deps.
    pub file_path: Option<String>,
    /// COMMENT ON description text.
    pub description: Option<String>,
    /// Whether this is an external dependency (not defined in the project).
    pub is_external: bool,
    /// Column schemas (from types cache), if available.
    pub columns: Option<Vec<CatalogColumn>>,
    /// Fully-qualified IDs of objects this depends on.
    pub dependencies: Vec<String>,
    /// Fully-qualified IDs of objects that depend on this.
    pub dependents: Vec<String>,
    /// Indexes defined on this object.
    pub indexes: Vec<CatalogIndex>,
    /// Constraints defined on this object.
    pub constraints: Vec<CatalogConstraint>,
    /// Grants on this object.
    pub grants: Vec<CatalogGrant>,
    /// Infrastructure properties for connections, sources, and tables-from-source.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub infrastructure: Option<CatalogInfrastructure>,
}

/// A column in an object's schema.
#[derive(Debug, Serialize)]
pub struct CatalogColumn {
    /// Column name.
    pub name: String,
    /// SQL type name.
    pub type_name: String,
    /// Whether the column is nullable.
    pub nullable: bool,
    /// COMMENT ON COLUMN description, if any.
    pub comment: Option<String>,
}

/// An index on an object.
#[derive(Debug, Serialize)]
pub struct CatalogIndex {
    /// Index name.
    pub name: String,
    /// Cluster the index is on, if specified.
    pub cluster: Option<String>,
    /// Indexed column expressions.
    pub columns: Vec<String>,
}

/// A constraint on an object.
#[derive(Debug, Serialize)]
pub struct CatalogConstraint {
    /// Constraint kind (e.g., `"PRIMARY KEY"`, `"UNIQUE"`, `"FOREIGN KEY"`).
    pub kind: String,
    /// Constraint name.
    pub name: String,
    /// Columns involved.
    pub columns: Vec<String>,
    /// Whether the constraint is enforced.
    pub enforced: bool,
    /// Referenced object for foreign keys.
    pub references: Option<String>,
    /// Referenced columns for foreign keys.
    pub reference_columns: Option<Vec<String>>,
}

/// A grant on an object.
#[derive(Debug, Serialize)]
pub struct CatalogGrant {
    /// Privilege name (e.g., `"SELECT"`, `"INSERT"`, `"ALL"`).
    pub privilege: String,
    /// Role the privilege is granted to.
    pub role: String,
}

/// Structured metadata for infrastructure objects, extracted from the AST.
///
/// Connections, sources, and tables-from-source carry type-specific metadata
/// (connector type, configuration properties, upstream references) that the
/// webview uses to render dedicated detail layouts.
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum CatalogInfrastructure {
    /// Connection properties (HOST, PORT, USER, PASSWORD, etc.)
    #[serde(rename = "connection")]
    Connection {
        /// Connection type (e.g., "Postgres", "Kafka", "MySQL").
        connector_type: String,
        /// Key-value properties extracted from ConnectionOption values.
        properties: Vec<CatalogProperty>,
    },
    /// Source configuration (connection ref, cluster, publication/topic, etc.)
    #[serde(rename = "source")]
    Source {
        /// Source type (e.g., "Postgres", "Kafka", "Load Generator").
        connector_type: String,
        /// The connection object this source uses (for linking).
        #[serde(skip_serializing_if = "Option::is_none")]
        connection_ref: Option<String>,
        /// Key-value properties extracted from source options.
        properties: Vec<CatalogProperty>,
    },
    /// Table-from-source metadata.
    #[serde(rename = "table_from_source")]
    TableFromSource {
        /// The source object this table reads from (for linking).
        source_ref: String,
        /// External reference (e.g., "public.orders" in the upstream DB).
        #[serde(skip_serializing_if = "Option::is_none")]
        external_reference: Option<String>,
    },
}

/// A key-value property extracted from an infrastructure object's AST.
#[derive(Debug, Serialize)]
pub struct CatalogProperty {
    /// Property name (e.g., "HOST", "DATABASE", "PUBLICATION").
    pub key: String,
    /// Display value. For secrets: the secret's fully-qualified name.
    pub value: String,
    /// If this property references a secret, the secret's object ID (for linking).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<String>,
    /// If this property references another object, its object ID (for linking).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_ref: Option<String>,
}

use super::helpers::{extract_cached_column_comments, extract_cached_description};

/// Convert a [`CachedIndex`] into a [`CatalogIndex`].
fn index_to_catalog(idx: &crate::project_cache::CachedIndex) -> CatalogIndex {
    let columns: Vec<String> = idx
        .columns
        .split(", ")
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    CatalogIndex {
        name: idx.name.clone(),
        cluster: idx.cluster.clone(),
        columns,
    }
}

/// Convert a [`CachedConstraint`] into a [`CatalogConstraint`].
fn constraint_to_catalog(c: &crate::project_cache::CachedConstraint) -> CatalogConstraint {
    CatalogConstraint {
        kind: c.kind.clone(),
        name: c.name.clone(),
        columns: c.columns.clone(),
        enforced: c.enforced,
        references: c.ref_object.clone(),
        reference_columns: c.ref_columns.clone(),
    }
}

/// Convert cached grant records into [`CatalogGrant`] entries.
fn grants_to_catalog(grants: &[crate::project_cache::CachedGrant]) -> Vec<CatalogGrant> {
    grants
        .iter()
        .map(|g| CatalogGrant {
            privilege: g.privilege.clone(),
            role: g.grantee.clone(),
        })
        .collect()
}

/// Converts cached infrastructure metadata to its catalog representation.
///
/// Maps the infrastructure type string (`"connection"`, `"source"`,
/// `"table-from-source"`) to the corresponding enum variant. Unrecognized
/// types default to [`CatalogInfrastructure::Connection`].
impl From<crate::project_cache::CachedInfrastructure> for CatalogInfrastructure {
    fn from(infra: crate::project_cache::CachedInfrastructure) -> Self {
        let properties: Vec<CatalogProperty> =
            infra.properties.into_iter().map(Into::into).collect();
        match infra.infra_type.as_str() {
            "connection" => CatalogInfrastructure::Connection {
                connector_type: infra.connector_type.unwrap_or_default(),
                properties,
            },
            "source" => CatalogInfrastructure::Source {
                connector_type: infra.connector_type.unwrap_or_default(),
                connection_ref: infra.connection_ref,
                properties,
            },
            "table-from-source" => CatalogInfrastructure::TableFromSource {
                source_ref: infra.source_ref.unwrap_or_default(),
                external_reference: infra.external_reference,
            },
            _ => CatalogInfrastructure::Connection {
                connector_type: infra.connector_type.unwrap_or_default(),
                properties,
            },
        }
    }
}

/// Converts a cached property to its catalog representation, preserving
/// secret and object references for webview linking.
impl From<crate::project_cache::CachedProperty> for CatalogProperty {
    fn from(p: crate::project_cache::CachedProperty) -> Self {
        CatalogProperty {
            key: p.key,
            value: p.value,
            secret_ref: p.secret_ref,
            object_ref: p.object_ref,
        }
    }
}

/// Build column metadata with type, nullability, and optional descriptions.
///
/// Resolves columns from the best available source of type information.
fn build_columns(
    id_str: &str,
    get_columns: &dyn Fn(&str) -> Option<BTreeMap<String, ColumnType>>,
    column_comments: &BTreeMap<String, String>,
) -> Option<Vec<CatalogColumn>> {
    get_columns(id_str).map(|cols| {
        let mut sorted: Vec<_> = cols.iter().collect();
        sorted.sort_by_key(|(_, ct)| ct.position);
        sorted
            .into_iter()
            .map(|(name, ct)| CatalogColumn {
                name: name.clone(),
                type_name: ct.r#type.clone(),
                nullable: ct.nullable,
                comment: column_comments.get(name).cloned(),
            })
            .collect()
    })
}

/// Returns the user-visible type name for an object.
///
/// Distinguishes source-backed tables from regular tables; all other
/// kinds use their standard display name.
fn cached_object_type(obj: &CachedObject) -> &str {
    if obj.kind == ObjectKind::Table {
        obj.infrastructure
            .as_ref()
            .map(|i| i.infra_type.as_str())
            .unwrap_or("table")
    } else {
        obj.kind.as_str()
    }
}

/// Returns objects that depend on the given object, excluding auto-generated
/// internal objects (e.g. those created for constraint enforcement).
fn dependents_for(
    fqn: &str,
    project_cache: &ProjectCache,
    constraint_mv_fqns: &std::collections::BTreeSet<String>,
) -> Vec<String> {
    project_cache
        .get_dependents(fqn)
        .into_iter()
        .filter(|d| !constraint_mv_fqns.contains(d))
        .collect()
}

/// Build a [`CatalogObject`] for a project-owned object from its cached representation.
///
/// Project objects carry full metadata: file path, description, columns,
/// dependencies, dependents, indexes, constraints, grants, and infrastructure
/// properties. Columns are resolved from the types cache first, falling back
/// to the types lock.
fn build_catalog_object(
    obj: &CachedObject,
    get_columns: &dyn Fn(&str) -> Option<BTreeMap<String, ColumnType>>,
    root: &Path,
    project_cache: &ProjectCache,
    constraint_mv_fqns: &std::collections::BTreeSet<String>,
) -> CatalogObject {
    let file_path = Some(root.join(&obj.file_path).to_string_lossy().to_string());
    let column_comments = extract_cached_column_comments(&obj.comments);

    CatalogObject {
        database: obj.database.clone(),
        schema: obj.schema.clone(),
        name: obj.name.clone(),
        object_type: cached_object_type(obj).to_string(),
        cluster: obj.cluster.clone(),
        file_path,
        description: extract_cached_description(&obj.comments),
        is_external: false,
        columns: build_columns(&obj.fqn, get_columns, &column_comments),
        dependencies: project_cache.get_dependencies(&obj.fqn),
        dependents: dependents_for(&obj.fqn, project_cache, constraint_mv_fqns),
        indexes: obj.indexes.iter().map(index_to_catalog).collect(),
        constraints: obj.constraints.iter().map(constraint_to_catalog).collect(),
        grants: grants_to_catalog(&obj.grants),
        infrastructure: obj.infrastructure.clone().map(Into::into),
        id: obj.fqn.clone(),
    }
}

/// Build a stub [`CatalogObject`] for an external dependency.
///
/// External dependencies are objects referenced but not defined in the project.
/// They have no file path, no infrastructure, and columns come solely from the
/// types cache.
fn build_external_object(
    ext_fqn: &str,
    get_columns: &dyn Fn(&str) -> Option<BTreeMap<String, ColumnType>>,
    project_cache: &ProjectCache,
    constraint_mv_fqns: &std::collections::BTreeSet<String>,
) -> CatalogObject {
    // Parse db.schema.name from the FQN string.
    let parts: Vec<&str> = ext_fqn.splitn(3, '.').collect();
    let (database, schema, name) = match parts.as_slice() {
        [db, sch, nm] => (db.to_string(), sch.to_string(), nm.to_string()),
        _ => (String::new(), String::new(), ext_fqn.to_string()),
    };
    CatalogObject {
        id: ext_fqn.to_string(),
        database,
        schema,
        name,
        object_type: "external".to_string(),
        cluster: None,
        file_path: None,
        description: None,
        is_external: true,
        columns: build_columns(ext_fqn, get_columns, &BTreeMap::new()),
        dependencies: Vec::new(),
        dependents: dependents_for(ext_fqn, project_cache, constraint_mv_fqns),
        indexes: Vec::new(),
        constraints: Vec::new(),
        grants: Vec::new(),
        infrastructure: None,
    }
}

/// Build the catalog response from a [`ProjectCache`] and types lock fallback.
///
/// Walks all cached project objects to create the database/schema tree and object
/// metadata via [`build_catalog_object`]. External dependencies are included as
/// stub objects via [`build_external_object`]. Constraint MVs are excluded from
/// both the tree and the dependents lists.
pub fn build_catalog_response(
    project_cache: &ProjectCache,
    types_lock: &Types,
    root: &Path,
) -> CatalogResponse {
    let get_columns = |fqn: &str| -> Option<BTreeMap<String, ColumnType>> {
        project_cache
            .get_columns(fqn)
            .or_else(|| types_lock.get_table(fqn).cloned())
    };

    let cached_dbs = project_cache.list_databases_with_objects();

    let constraint_mv_fqns: std::collections::BTreeSet<String> = cached_dbs
        .iter()
        .flat_map(|db| &db.schemas)
        .flat_map(|s| &s.objects)
        .filter(|obj| obj.is_constraint_mv)
        .map(|obj| obj.fqn.clone())
        .collect();

    let mut objects = Vec::new();
    let mut databases = Vec::new();

    for db in &cached_dbs {
        let mut catalog_schemas = Vec::new();

        for schema in &db.schemas {
            let mut schema_object_ids = Vec::new();

            for obj in &schema.objects {
                if obj.is_constraint_mv {
                    continue;
                }
                schema_object_ids.push(obj.fqn.clone());
                objects.push(build_catalog_object(
                    obj,
                    &get_columns,
                    root,
                    project_cache,
                    &constraint_mv_fqns,
                ));
            }

            catalog_schemas.push(CatalogSchema {
                name: schema.name.clone(),
                object_ids: schema_object_ids,
            });
        }

        databases.push(CatalogDatabase {
            name: db.name.clone(),
            schemas: catalog_schemas,
        });
    }

    for ext_fqn in &project_cache.list_external_dependencies() {
        objects.push(build_external_object(
            ext_fqn,
            &get_columns,
            project_cache,
            &constraint_mv_fqns,
        ));
    }

    CatalogResponse {
        databases,
        objects,
        errors: Vec::new(),
    }
}

/// Build a catalog response for a failed project build.
///
/// Returns an empty catalog with the build error message so the sidebar
/// can show what went wrong instead of spinning on "Waiting for project data..."
pub fn build_error_response(error: Option<&str>) -> CatalogResponse {
    let errors = match error {
        Some(msg) => vec![CatalogError {
            message: msg.to_string(),
        }],
        None => Vec::new(),
    };
    CatalogResponse {
        databases: Vec::new(),
        objects: Vec::new(),
        errors,
    }
}
