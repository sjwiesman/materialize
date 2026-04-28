// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Bootstrap the shared catalog used by every per-task typecheck.
//!
//! Seeds builtins (via [`CatalogRuntime::open`]), bootstraps namespaces,
//! registers external `types.lock` entries as stub tables, and registers all
//! non-typechecked project objects (tables, sources, sinks, secrets,
//! connections) from their compiled SQL. Returns the catalog wrapped in `Arc`
//! plus a map of column metadata for the registered non-typechecked objects.
//! The parallel executor forks per-task catalogs from this baseline.

use super::catalog::CatalogRuntime;
use super::convert::{create_catalog_item_sql, relation_desc_to_columns};
use super::{ObjectTypeCheckError, TypeCheckError};
use crate::project::ast::Statement;
use crate::project::ir::compiled::FullyQualifiedName;
use crate::project::ir::graph::Project;
use crate::project::ir::object_id::ObjectId;
use crate::types::{ColumnType, Types};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Build the shared catalog used by every per-task typecheck. Errors from
/// registering non-typechecked objects are accumulated; if any are present
/// after this phase, the caller should abort before running phase 2.
pub(super) fn bootstrap_catalog(
    project: &Project,
    external_types: &Types,
) -> Result<
    (
        Arc<CatalogRuntime>,
        BTreeMap<ObjectId, BTreeMap<String, ColumnType>>,
    ),
    TypeCheckError,
> {
    let mut runtime = CatalogRuntime::open()?;
    runtime.bootstrap_namespaces(project, external_types);

    let mut base_columns: BTreeMap<ObjectId, BTreeMap<String, ColumnType>> = BTreeMap::new();
    let mut errors: Vec<ObjectTypeCheckError> = Vec::new();
    let mut registered_from_create: BTreeSet<ObjectId> = BTreeSet::new();
    for db_obj in project.iter_objects() {
        if matches!(
            db_obj.typed_object.stmt,
            Statement::CreateView(_) | Statement::CreateMaterializedView(_)
        ) {
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
        match runtime.create_item(&object_id, &sql) {
            Ok(desc) => {
                base_columns.insert(object_id.clone(), relation_desc_to_columns(&desc));
                registered_from_create.insert(object_id);
            }
            Err(err) => errors.push(err),
        }
    }

    for (id, columns) in &external_types.tables {
        if registered_from_create.contains(id) {
            continue;
        }
        runtime.create_stub_table(id, columns)?;
    }

    if !errors.is_empty() {
        return Err(TypeCheckError::Multiple(errors));
    }

    Ok((Arc::new(runtime), base_columns))
}
