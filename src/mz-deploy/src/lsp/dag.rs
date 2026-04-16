//! DAG response builder for the `mz-deploy/dag` custom LSP endpoint.
//!
//! Builds a lightweight JSON representation of the project's dependency graph
//! for rendering in the VS Code workspace webview. The response contains two
//! collections:
//!
//! - **`objects`** — One [`DagNode`] per project object plus one per external
//!   dependency. Each node carries enough metadata for rendering (type, schema,
//!   file path) but intentionally omits SQL, columns, and constraints to keep
//!   the payload small.
//!
//! - **`edges`** — One [`DagEdge`] per dependency relationship. Edge kinds are
//!   inferred from the target object's type using [`infer_edge_kind`].

use crate::project_cache::{CachedObject, ProjectCache};
use crate::types::ObjectKind;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// Complete DAG response returned by the `mz-deploy/dag` endpoint.
#[derive(Debug, Serialize)]
pub struct DagResponse {
    /// All objects in the project, including external dependencies.
    pub objects: Vec<DagNode>,
    /// Dependency edges between objects.
    pub edges: Vec<DagEdge>,
}

/// A single node in the DAG.
#[derive(Debug, Serialize)]
pub struct DagNode {
    /// Fully-qualified object ID (e.g., `"db.schema.name"`).
    pub id: String,
    /// Unqualified object name.
    pub name: String,
    /// Object type (e.g., `"view"`, `"materialized-view"`, `"table"`).
    pub object_type: String,
    /// Schema the object belongs to.
    pub schema: String,
    /// Whether this is an external dependency (not defined in the project).
    pub is_external: bool,
    /// Relative file path to the `.sql` source file, or `null` for external deps.
    pub file_path: Option<String>,
}

/// A dependency edge between two objects.
#[derive(Debug, Serialize)]
pub struct DagEdge {
    /// Fully-qualified ID of the upstream (dependency) object.
    pub source: String,
    /// Fully-qualified ID of the downstream (dependent) object.
    pub target: String,
    /// Semantic kind of this dependency relationship.
    pub kind: EdgeKind,
}

/// The semantic kind of a dependency edge.
///
/// The semantic kind of a dependency edge.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeKind {
    /// Secret consumed by a connection.
    UsesCredential,
    /// Connection consumed by a source.
    UsesConnection,
    /// Source materialized into a table.
    MaterializesFrom,
    /// Data dependency via SQL query.
    TransformsFrom,
    /// Dependency on an object not defined in this project.
    External,
}

/// Infer the semantic edge kind from the target object's type.
fn infer_edge_kind(target_type: &str) -> EdgeKind {
    match target_type {
        "connection" => EdgeKind::UsesCredential,
        "source" => EdgeKind::UsesConnection,
        "table-from-source" => EdgeKind::MaterializesFrom,
        _ => EdgeKind::TransformsFrom,
    }
}

/// Compute the display type name for a cached object.
///
/// For tables, checks infrastructure metadata to distinguish
/// `"table-from-source"` from plain `"table"`. All other kinds use
/// their kebab-case `ObjectKind::as_str()` representation.
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

/// Build the DAG response from a [`ProjectCache`].
///
/// Walks all cached project objects to create nodes and edges, then adds nodes
/// for external dependencies (with `is_external: true` and no `file_path`).
/// File paths are resolved relative to `root`.
pub fn build_dag_response(project_cache: &ProjectCache, root: &Path) -> DagResponse {
    let mut objects = Vec::new();
    let mut edges = Vec::new();
    // Track which object FQNs we've seen so we can add external deps after.
    let mut seen_fqns = BTreeMap::new();

    let external_deps: BTreeSet<String> = project_cache
        .list_external_dependencies()
        .into_iter()
        .collect();

    let cached_dbs = project_cache.list_databases_with_objects();
    for db in &cached_dbs {
        for schema in &db.schemas {
            for obj in &schema.objects {
                // Skip constraint MVs — they're implementation details.
                if obj.is_constraint_mv {
                    continue;
                }

                let obj_type = cached_object_type(obj);
                let file_path = Some(root.join(&obj.file_path).to_string_lossy().to_string());

                seen_fqns.insert(obj.fqn.clone(), obj_type);

                objects.push(DagNode {
                    id: obj.fqn.clone(),
                    name: obj.name.clone(),
                    object_type: obj_type.to_string(),
                    schema: schema.name.clone(),
                    is_external: false,
                    file_path,
                });

                // Build edges from this object's dependencies.
                for dep_fqn in &project_cache.get_dependencies(&obj.fqn) {
                    let kind = if external_deps.contains(dep_fqn) {
                        EdgeKind::External
                    } else {
                        infer_edge_kind(obj_type)
                    };
                    edges.push(DagEdge {
                        source: dep_fqn.clone(),
                        target: obj.fqn.clone(),
                        kind,
                    });
                }
            }
        }
    }

    // Add external dependency nodes.
    for ext_fqn in &external_deps {
        if !seen_fqns.contains_key(ext_fqn) {
            // Parse schema and name from the FQN (db.schema.name).
            let parts: Vec<&str> = ext_fqn.splitn(3, '.').collect();
            let (schema, name) = match parts.as_slice() {
                [_, sch, nm] => (sch.to_string(), nm.to_string()),
                _ => (String::new(), ext_fqn.clone()),
            };
            objects.push(DagNode {
                id: ext_fqn.clone(),
                name,
                object_type: "external".to_string(),
                schema,
                is_external: true,
                file_path: None,
            });
        }
    }

    DagResponse { objects, edges }
}
