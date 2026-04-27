//! Phase 1 of typechecking: build the base catalog.
//!
//! Seeds builtins (via [`CatalogRuntime::open`]), bootstraps namespaces,
//! registers external `types.lock` entries as stub tables, and registers all
//! non-typechecked project objects (tables, sources, sinks, secrets,
//! connections) from their compiled SQL. Returns the catalog wrapped in `Arc`
//! plus a map of column metadata for the registered non-typechecked objects.

use super::catalog::{CatalogRuntime, create_catalog_item_sql, relation_desc_to_columns};
use super::{ObjectTypeCheckError, TypeCheckError, TypeCheckErrors, requires_typecheck};
use crate::project::ir::compiled::FullyQualifiedName;
use crate::project::ir::graph::Project;
use crate::project::ir::object_id::ObjectId;
use crate::types::{ColumnType, Types};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Result of phase 1.
pub(super) struct BaseCatalog {
    pub(super) catalog: Arc<CatalogRuntime>,
    /// Column maps for objects that don't undergo typechecking but whose
    /// schemas downstream views depend on (tables, sources, etc., plus
    /// external `types.lock` entries).
    pub(super) base_columns: BTreeMap<ObjectId, BTreeMap<String, ColumnType>>,
}

/// Build the base catalog. Errors from registering non-typechecked objects are
/// accumulated; if any are present after this phase, the caller should abort
/// before running phase 2.
pub(super) fn build_base_catalog(
    project: &Project,
    external_types: &Types,
) -> Result<BaseCatalog, TypeCheckError> {
    let mut runtime = CatalogRuntime::open()?;
    runtime.bootstrap_namespaces(project, external_types);

    // Stub external types.lock entries.
    for (fqn, columns) in &external_types.tables {
        let Some(object_id) = fqn.parse::<ObjectId>();
        runtime.create_stub_table(&object_id, columns)?;
    }

    // Register every non-typechecked project object.
    let mut base_columns: BTreeMap<ObjectId, BTreeMap<String, ColumnType>> = BTreeMap::new();
    let mut errors: Vec<ObjectTypeCheckError> = Vec::new();
    for db_obj in project.iter_objects() {
        if requires_typecheck(&db_obj.typed_object.stmt) {
            continue;
        }
        let object_id = ObjectId {
            database: db_obj.id.database.clone(),
            schema: db_obj.id.schema.clone(),
            object: db_obj.id.object.clone(),
        };
        let fqn: FullyQualifiedName = object_id.clone().into();
        let Some(sql) = create_catalog_item_sql(&db_obj.typed_object.stmt, &fqn) else {
            continue;
        };
        match runtime.create_or_replace_item(&object_id, &sql) {
            Ok(desc) => {
                base_columns.insert(object_id, relation_desc_to_columns(&desc));
            }
            Err(err) => errors.push(err),
        }
    }

    if !errors.is_empty() {
        return Err(TypeCheckError::Multiple(TypeCheckErrors { errors }));
    }

    Ok(BaseCatalog {
        catalog: Arc::new(runtime),
        base_columns,
    })
}
