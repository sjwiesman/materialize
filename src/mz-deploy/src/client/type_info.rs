//! Column-schema introspection for the data-contract and type-checking systems.
//!
//! Methods on [`TypeInfoClient`] query the Materialize system catalog for
//! external dependencies and `CREATE TABLE FROM SOURCE` tables, returning their
//! column names, types, nullability, object kinds, and comments as a
//! [`Types`](crate::types::Types) snapshot.
//!
//! Plain `CREATE TABLE` objects are excluded — their schemas are derived from
//! the SQL AST during type checking and do not need server queries.
//!
//! - **`lock`** uses [`query_types_for_objects`](TypeInfoClient::query_types_for_objects)
//!   to generate `types.lock` for declared dependencies and source tables. This
//!   method issues a single catalog query per object joining `mz_columns`,
//!   `mz_objects`, and `mz_comments` to retrieve columns, kind, and comments.
//! - **`query_external_types`** delegates to `query_types_for_objects`, extracting
//!   object lists from the compiled project graph.
//! - **Incremental type checking** uses [`query_object_columns`](TypeInfoClient::query_object_columns)
//!   to query a single view's output columns inline after validation, enabling
//!   type-hash comparison for dirty propagation.

use crate::client::connection::TypeInfoClient;
use crate::client::errors::ConnectionError;
use crate::client::quote_identifier;
use crate::project::ir::graph;
use crate::project::ir::object_id::ObjectId;
use crate::types::{ColumnType, ObjectKind, Types};
use std::collections::BTreeMap;

impl TypeInfoClient<'_> {
    /// Query column schemas, object kinds, and comments for the given objects.
    ///
    /// Uses a single catalog query per object joining `mz_catalog.mz_columns`,
    /// `mz_catalog.mz_objects`, `mz_catalog.mz_schemas`, `mz_catalog.mz_databases`,
    /// and `mz_internal.mz_comments` to retrieve columns, types, nullability,
    /// object kind, and both object-level and column-level comments in one shot.
    ///
    /// Source tables are always recorded as `ObjectKind::Table` regardless of the
    /// catalog result.
    ///
    /// This is the core implementation used by both [`query_external_types`](Self::query_external_types)
    /// (project-graph driven) and the `lock` command (declared-dependency driven).
    pub async fn query_types_for_objects(
        &self,
        objects: &[ObjectId],
        source_tables: &[ObjectId],
    ) -> Result<Types, ConnectionError> {
        let mut tables = BTreeMap::new();
        let mut kinds = BTreeMap::new();
        let mut comments = BTreeMap::new();

        let source_table_set: std::collections::BTreeSet<String> =
            source_tables.iter().map(|oid| oid.to_string()).collect();

        let all_oids: Vec<&ObjectId> = objects.iter().chain(source_tables.iter()).collect();

        for oid in &all_oids {
            let rows = self
                .client
                .query(
                    "SELECT \
                        c.name, \
                        c.type, \
                        c.nullable, \
                        c.position::int8 AS position, \
                        o.type AS object_type, \
                        obj_comment.comment AS object_comment, \
                        col_comment.comment AS column_comment \
                     FROM mz_catalog.mz_columns c \
                     JOIN mz_catalog.mz_objects o ON c.id = o.id \
                     JOIN mz_catalog.mz_schemas s ON o.schema_id = s.id \
                     JOIN mz_catalog.mz_databases d ON s.database_id = d.id \
                     LEFT JOIN mz_internal.mz_comments obj_comment \
                        ON o.id = obj_comment.id AND obj_comment.object_sub_id IS NULL \
                     LEFT JOIN mz_internal.mz_comments col_comment \
                        ON o.id = col_comment.id AND col_comment.object_sub_id = c.position \
                     WHERE d.name = $1 AND s.name = $2 AND o.name = $3 \
                     ORDER BY c.position",
                    &[&oid.database, &oid.schema, &oid.object],
                )
                .await?;

            let fqn = oid.to_string();
            let mut columns = BTreeMap::new();
            let mut object_kind_set = false;

            for row in &rows {
                let name: String = row.get("name");
                let type_str: String = row.get("type");
                let nullable: bool = row.get("nullable");
                let position: i64 = row.get("position");
                let col_comment: Option<String> = row.get("column_comment");

                // Extract object-level metadata from the first row
                if !object_kind_set {
                    let object_type: String = row.get("object_type");
                    let kind = if source_table_set.contains(&fqn) {
                        ObjectKind::Table
                    } else {
                        match object_type.as_str() {
                            "table" => ObjectKind::Table,
                            "view" => ObjectKind::View,
                            "materialized-view" => ObjectKind::MaterializedView,
                            "source" => ObjectKind::Source,
                            "sink" => ObjectKind::Sink,
                            "secret" => ObjectKind::Secret,
                            "connection" => ObjectKind::Connection,
                            _ => ObjectKind::Table,
                        }
                    };
                    kinds.insert(fqn.clone(), kind);

                    let obj_comment: Option<String> = row.get("object_comment");
                    if let Some(comment) = obj_comment {
                        comments.insert(fqn.clone(), comment);
                    }

                    object_kind_set = true;
                }

                columns.insert(
                    name,
                    ColumnType {
                        r#type: type_str,
                        nullable,
                        position: usize::try_from(position).unwrap_or(0),
                        comment: col_comment,
                    },
                );
            }

            tables.insert(fqn, columns);
        }

        Ok(Types {
            version: 1,
            tables,
            kinds,
            comments,
        })
    }

    /// Query SHOW COLUMNS for external dependencies and `CREATE TABLE FROM SOURCE` tables.
    ///
    /// Plain `CREATE TABLE` objects are excluded — their schemas are derived from
    /// the SQL AST during type checking. Only `CreateTableFromSource` tables need
    /// their columns queried from the live server.
    ///
    /// Delegates to [`query_types_for_objects`](Self::query_types_for_objects).
    pub async fn query_external_types(
        &self,
        project: &graph::Project,
    ) -> Result<Types, ConnectionError> {
        let external: Vec<ObjectId> = project.external_dependencies.iter().cloned().collect();
        let source_tables: Vec<ObjectId> = project.get_tables_from_source().collect();
        self.query_types_for_objects(&external, &source_tables)
            .await
    }

    /// Query column types for a single object via `SHOW COLUMNS`.
    ///
    /// When `flatten` is true, the object is referenced using the flattened
    /// `"db.schema.object"` form (for temporary views). Otherwise it uses
    /// the standard `db.schema.object` quoting.
    pub async fn query_object_columns(
        &self,
        oid: &ObjectId,
        flatten: bool,
    ) -> Result<BTreeMap<String, ColumnType>, ConnectionError> {
        let object_ref = if flatten {
            format!("\"{}.{}.{}\"", oid.database, oid.schema, oid.object)
        } else {
            let quoted_db = quote_identifier(&oid.database);
            let quoted_schema = quote_identifier(&oid.schema);
            let quoted_object = quote_identifier(&oid.object);
            format!("{}.{}.{}", quoted_db, quoted_schema, quoted_object)
        };

        let rows = self
            .client
            .query(&format!("SHOW COLUMNS FROM {}", object_ref), &[])
            .await?;

        let mut columns = BTreeMap::new();
        for (position, row) in rows.iter().enumerate() {
            let name: String = row.get("name");
            let type_str: String = row.get("type");
            let nullable: bool = row.get("nullable");

            columns.insert(
                name,
                ColumnType {
                    r#type: type_str,
                    nullable,
                    position,
                    comment: None,
                },
            );
        }

        Ok(columns)
    }

    /// Query types for internal project views from the database.
    pub async fn query_internal_types(
        &self,
        object_ids: &[&ObjectId],
        flatten: bool,
    ) -> Result<Types, ConnectionError> {
        let mut objects = BTreeMap::new();

        for oid in object_ids {
            let columns = self.query_object_columns(oid, flatten).await?;
            objects.insert(oid.to_string(), columns);
        }

        Ok(Types {
            version: 1,
            tables: objects,
            kinds: BTreeMap::new(),
            comments: BTreeMap::new(),
        })
    }
}
