// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fully qualified `database.schema.object` identifier type.
//!
//! [`ObjectId`] is the canonical way to refer to a database object throughout
//! project compilation and graph analysis. It is used as a map key,
//! dependency graph node, and display type in error messages.
//!
//! ## Invariant
//!
//! An `ObjectId` is always fully qualified: all three components (`database`,
//! `schema`, `object`) are non-empty strings. Partially qualified references
//! are resolved into `ObjectId`s by [`ObjectId::from_item_name`] and
//! [`ObjectId::from_raw_item_name`], which fill in missing components from
//! the current file's database/schema context.
//!
//! ## Resolution Examples
//!
//! ```text
//! from_item_name("sales", default_db="materialize", default_schema="public")
//!   → ObjectId { database: "materialize", schema: "public", object: "sales" }
//!
//! from_item_name("analytics.summary", default_db="materialize", default_schema="public")
//!   → ObjectId { database: "materialize", schema: "analytics", object: "summary" }
//!
//! from_item_name("other_db.staging.events", ...)
//!   → ObjectId { database: "other_db", schema: "staging", object: "events" }
//! ```

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::path::Path;
use std::str::FromStr;
use tower_lsp::lsp_types::Url;

use mz_sql_parser::ast::{Ident, RawItemName, UnresolvedItemName};

/// A fully qualified object identifier.
///
/// Used to uniquely identify database objects across the project.
/// Format: `database.schema.object`
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectId {
    pub database: String,
    pub schema: String,
    pub object: String,
}

impl ObjectId {
    /// Create a new ObjectId with the given database, schema, and object names.
    pub fn new(database: String, schema: String, object: String) -> Self {
        Self {
            database,
            schema,
            object,
        }
    }

    /// Get the database name.
    #[inline]
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Get the schema name.
    #[inline]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Get the object name.
    #[inline]
    pub fn object(&self) -> &str {
        &self.object
    }

    /// Resolve an [`UnresolvedItemName`] into a fully qualified [`ObjectId`].
    ///
    /// Name parts are resolved based on how many components are present:
    /// - 1-part (`object`) — uses both `default_database` and `default_schema`.
    /// - 2-part (`schema.object`) — uses `default_database`.
    /// - 3-part (`database.schema.object`) — used as-is.
    pub fn from_item_name(
        name: &UnresolvedItemName,
        default_database: &str,
        default_schema: &str,
    ) -> Self {
        match name.0.as_slice() {
            [object] => Self::new(
                default_database.to_string(),
                default_schema.to_string(),
                object.to_string(),
            ),
            [schema, object] => Self::new(
                default_database.to_string(),
                schema.to_string(),
                object.to_string(),
            ),
            [database, schema, object] => {
                Self::new(database.to_string(), schema.to_string(), object.to_string())
            }
            _ => Self::new(
                default_database.to_string(),
                default_schema.to_string(),
                "unknown".to_string(),
            ),
        }
    }

    /// Resolve a [`RawItemName`] into a fully qualified [`ObjectId`].
    ///
    /// Unwraps the inner [`UnresolvedItemName`] and delegates to
    /// [`from_item_name`](Self::from_item_name).
    pub fn from_raw_item_name(
        name: &RawItemName,
        default_database: &str,
        default_schema: &str,
    ) -> Self {
        // RawItemName wraps UnresolvedItemName
        Self::from_item_name(name.name(), default_database, default_schema)
    }

    /// Convert to an [`UnresolvedItemName`] (the reverse of [`from_item_name`](Self::from_item_name)).
    ///
    /// Always produces a fully qualified 3-part name (`database.schema.object`).
    pub fn to_unresolved_item_name(&self) -> UnresolvedItemName {
        UnresolvedItemName(vec![
            Ident::new(&self.database).expect("valid database"),
            Ident::new(&self.schema).expect("valid schema"),
            Ident::new(&self.object).expect("valid object"),
        ])
    }

    /// Parse an ObjectId from a fully qualified name string.
    ///
    /// # Arguments
    /// * `fqn` - Fully qualified name in the format "database.schema.object"
    ///
    /// # Returns
    /// ObjectId if the FQN is valid (has exactly 3 dot-separated parts)
    ///
    /// # Errors
    /// Returns error if the FQN format is invalid
    /// Derive the default database and schema from a file's URI.
    ///
    /// Expects the file to be under `<root>/models/<database>/<schema>/`.
    /// Returns `None` if the path doesn't match the expected layout.
    pub fn default_db_schema_from_uri(file_uri: &Url, root: &Path) -> Option<(String, String)> {
        let file_path = file_uri.to_file_path().ok()?;
        let models_dir = root.join("models");
        let relative = file_path.strip_prefix(&models_dir).ok()?;

        let components: Vec<_> = relative
            .components()
            .map(|c| c.as_os_str().to_string_lossy().to_string())
            .collect();

        // Expected: [database, schema, file.sql] or deeper
        if components.len() >= 3 {
            Some((components[0].clone(), components[1].clone()))
        } else {
            None
        }
    }
}

impl FromStr for ObjectId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 3 {
            return Err(format!(
                "invalid object id '{}': expected format 'database.schema.object'",
                s
            ));
        }
        Ok(ObjectId {
            database: parts[0].to_string(),
            schema: parts[1].to_string(),
            object: parts[2].to_string(),
        })
    }
}

impl std::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.database, self.schema, self.object)
    }
}

impl Serialize for ObjectId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ObjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ObjectIdVisitor;

        impl<'de> de::Visitor<'de> for ObjectIdVisitor {
            type Value = ObjectId;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an object ID")
            }

            fn visit_str<E>(self, value: &str) -> Result<ObjectId, E>
            where
                E: de::Error,
            {
                ObjectId::from_str(value).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_str(ObjectIdVisitor)
    }
}
