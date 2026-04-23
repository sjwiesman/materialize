//! Name transformation for `mz-deploy dev` overlay compilation.
//!
//! This module provides [`OverlayTransformer`], which implements the spec's
//! three-step reference resolution rule for schema-level overlays:
//!
//! 1. **External references** — if the database is not in `in_project_databases`,
//!    emit the name verbatim. `profile_suffix` does **not** apply to external refs.
//! 2. **In-project references** — apply `profile_suffix` to the database (if
//!    configured) to obtain `base_db`.
//! 3. **Dirty schemas** — if `(base_db, schema)` is in `dirty_schemas`, rewrite
//!    the database component to `<base_db>__<profile_name>`. Otherwise emit
//!    `<base_db>.<schema>.<object>` (production reference).
//!
//! Unqualified / partially qualified names are fully qualified using the
//! transformer's `fqn` context before the rule is applied.

use std::collections::BTreeSet;

use mz_sql_parser::ast::{Ident, UnresolvedItemName};

use crate::project::SchemaQualifier;
use crate::project::ir::compiled::FullyQualifiedName;
use crate::project::resolve::normalize::transformers::NameTransformer;

/// Transforms references for `mz-deploy dev` overlay compilation.
///
/// Applies the spec's three-step reference resolution rule:
///
/// 1. If the referenced database is not in `in_project_databases`, leave
///    the name verbatim (external dependency). `profile_suffix` does NOT
///    apply to external refs.
/// 2. Otherwise apply `profile_suffix` to the database (if configured) to
///    get `base_db`.
/// 3. If `(base_db, schema)` is in `dirty_schemas`, rewrite the database
///    component to `<base_db>__<profile_name>`. Otherwise emit
///    `<base_db>.<schema>.<object>` (production reference).
///
/// Unqualified / partially qualified names are fully qualified using
/// `fqn` before the rule is applied.
pub(crate) struct OverlayTransformer<'a> {
    pub(crate) fqn: &'a FullyQualifiedName,
    pub(crate) profile_name: &'a str,
    pub(crate) profile_suffix: Option<&'a str>,
    pub(crate) in_project_databases: &'a BTreeSet<String>,
    /// Dirty schemas, keyed by the **post-`profile_suffix`** database name.
    ///
    /// Example: if `profile_suffix = Some("_staging")` and schema
    /// `public` in project-owned database `app` is dirty, the entry is
    /// `SchemaQualifier::new("app_staging", "public")` — not
    /// `SchemaQualifier::new("app", "public")`. Task 7 builds this
    /// set after applying the suffix.
    pub(crate) dirty_schemas: &'a BTreeSet<SchemaQualifier>,
}

impl<'a> NameTransformer for OverlayTransformer<'a> {
    fn transform_name(&self, name: &UnresolvedItemName) -> UnresolvedItemName {
        // Normalize to 3-part name first
        let (database, schema, object) = match name.0.len() {
            1 => {
                // Unqualified: use fqn database + schema
                let database = Ident::new(self.fqn.database()).expect("valid database identifier");
                let schema = Ident::new(self.fqn.schema()).expect("valid schema identifier");
                let object = name.0[0].clone();
                (database, schema, object)
            }
            2 => {
                // Schema-qualified: prepend fqn database
                let database = Ident::new(self.fqn.database()).expect("valid database identifier");
                let schema = name.0[0].clone();
                let object = name.0[1].clone();
                (database, schema, object)
            }
            3 => {
                // Already fully qualified
                let database = name.0[0].clone();
                let schema = name.0[1].clone();
                let object = name.0[2].clone();
                (database, schema, object)
            }
            _ => {
                // Invalid — return as-is (matches FullyQualifyingTransformer behavior)
                return name.clone();
            }
        };

        let db_str = database.to_string();

        // Step 1: external check — leave verbatim if not in project.
        if !self.in_project_databases.contains(&db_str) {
            return UnresolvedItemName(vec![database, schema, object]);
        }

        // Step 2: apply profile_suffix to get base_db.
        let base_db_str = match self.profile_suffix {
            Some(suffix) => format!("{}{}", db_str, suffix),
            None => db_str,
        };

        // Step 3: dirty check — rewrite to overlay db if dirty, else prod.
        let qualifier = SchemaQualifier::new(base_db_str.clone(), schema.to_string());
        let final_db_str = if self.dirty_schemas.contains(&qualifier) {
            format!("{}__{}", base_db_str, self.profile_name)
        } else {
            base_db_str
        };

        let final_db = Ident::new(&final_db_str).expect("valid database identifier");
        UnresolvedItemName(vec![final_db, schema, object])
    }

    fn database_name(&self) -> &str {
        self.fqn.database()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project::ir::compiled::FullyQualifiedName;
    use crate::project::ir::object_id::ObjectId;

    fn make_fqn(database: &str, schema: &str, object: &str) -> FullyQualifiedName {
        FullyQualifiedName::from_object_id(ObjectId::new(
            database.to_string(),
            schema.to_string(),
            object.to_string(),
        ))
    }

    fn make_name(parts: &[&str]) -> UnresolvedItemName {
        UnresolvedItemName(
            parts
                .iter()
                .map(|s| Ident::new(*s).expect("valid identifier"))
                .collect(),
        )
    }

    /// Build an OverlayTransformer for tests that need in-project dbs.
    fn make_transformer<'a>(
        fqn: &'a FullyQualifiedName,
        profile_name: &'a str,
        profile_suffix: Option<&'a str>,
        in_project_databases: &'a BTreeSet<String>,
        dirty_schemas: &'a BTreeSet<SchemaQualifier>,
    ) -> OverlayTransformer<'a> {
        OverlayTransformer {
            fqn,
            profile_name,
            profile_suffix,
            in_project_databases,
            dirty_schemas,
        }
    }

    // 1. External reference: database not in in_project_databases → verbatim.
    #[test]
    fn external_reference_unchanged() {
        let fqn = make_fqn("mydb", "public", "ctx");
        let in_project = BTreeSet::from(["mydb".to_string()]);
        let dirty: BTreeSet<SchemaQualifier> = BTreeSet::new();
        let t = make_transformer(&fqn, "alice", None, &in_project, &dirty);

        let input = make_name(&["external_db", "analytics", "events"]);
        let result = t.transform_name(&input);

        assert_eq!(result.0[0].as_str(), "external_db");
        assert_eq!(result.0[1].as_str(), "analytics");
        assert_eq!(result.0[2].as_str(), "events");
    }

    // 2. In-project DB, clean schema, no profile_suffix → unchanged 3-part name.
    #[test]
    fn in_project_clean_schema_routes_to_prod_no_suffix() {
        let fqn = make_fqn("mydb", "public", "ctx");
        let in_project = BTreeSet::from(["mydb".to_string()]);
        let dirty: BTreeSet<SchemaQualifier> = BTreeSet::new();
        let t = make_transformer(&fqn, "alice", None, &in_project, &dirty);

        let input = make_name(&["mydb", "public", "orders"]);
        let result = t.transform_name(&input);

        assert_eq!(result.0[0].as_str(), "mydb");
        assert_eq!(result.0[1].as_str(), "public");
        assert_eq!(result.0[2].as_str(), "orders");
    }

    // 3. In-project DB, schema IS dirty, no profile_suffix → db becomes db__profile.
    #[test]
    fn in_project_dirty_schema_routes_to_overlay_no_suffix() {
        let fqn = make_fqn("mydb", "public", "ctx");
        let in_project = BTreeSet::from(["mydb".to_string()]);
        let dirty = BTreeSet::from([SchemaQualifier::new(
            "mydb".to_string(),
            "public".to_string(),
        )]);
        let t = make_transformer(&fqn, "alice", None, &in_project, &dirty);

        let input = make_name(&["mydb", "public", "orders"]);
        let result = t.transform_name(&input);

        assert_eq!(result.0[0].as_str(), "mydb__alice");
        assert_eq!(result.0[1].as_str(), "public");
        assert_eq!(result.0[2].as_str(), "orders");
    }

    // 4. In-project DB, schema dirty, profile_suffix="_staging" → db_staging__profile.
    #[test]
    fn profile_suffix_composes_with_overlay() {
        let fqn = make_fqn("db", "public", "ctx");
        let in_project = BTreeSet::from(["db".to_string()]);
        // dirty_schemas uses the POST-suffix base_db name
        let dirty = BTreeSet::from([SchemaQualifier::new(
            "db_staging".to_string(),
            "public".to_string(),
        )]);
        let t = make_transformer(&fqn, "alice", Some("_staging"), &in_project, &dirty);

        let input = make_name(&["db", "public", "orders"]);
        let result = t.transform_name(&input);

        assert_eq!(result.0[0].as_str(), "db_staging__alice");
        assert_eq!(result.0[1].as_str(), "public");
        assert_eq!(result.0[2].as_str(), "orders");
    }

    // 5. In-project DB, clean schema, profile_suffix="_staging" → db becomes db_staging.
    #[test]
    fn profile_suffix_on_clean_schema() {
        let fqn = make_fqn("db", "public", "ctx");
        let in_project = BTreeSet::from(["db".to_string()]);
        let dirty: BTreeSet<SchemaQualifier> = BTreeSet::new();
        let t = make_transformer(&fqn, "alice", Some("_staging"), &in_project, &dirty);

        let input = make_name(&["db", "public", "orders"]);
        let result = t.transform_name(&input);

        assert_eq!(result.0[0].as_str(), "db_staging");
        assert_eq!(result.0[1].as_str(), "public");
        assert_eq!(result.0[2].as_str(), "orders");
    }

    // 6. External DB + profile_suffix set → still emitted verbatim.
    #[test]
    fn external_reference_ignores_profile_suffix() {
        let fqn = make_fqn("mydb", "public", "ctx");
        let in_project = BTreeSet::from(["mydb".to_string()]);
        let dirty: BTreeSet<SchemaQualifier> = BTreeSet::new();
        let t = make_transformer(&fqn, "alice", Some("_staging"), &in_project, &dirty);

        let input = make_name(&["ext_db", "raw", "clicks"]);
        let result = t.transform_name(&input);

        assert_eq!(result.0[0].as_str(), "ext_db");
        assert_eq!(result.0[1].as_str(), "raw");
        assert_eq!(result.0[2].as_str(), "clicks");
    }

    // 7. Sparse overlay: in-project DB that has SOME schema dirty, but this
    //    reference targets a non-dirty schema → routes to prod (not overlay).
    #[test]
    fn in_project_dirty_db_with_non_dirty_schema() {
        let fqn = make_fqn("mydb", "public", "ctx");
        let in_project = BTreeSet::from(["mydb".to_string()]);
        // "mydb.analytics" is dirty, but NOT "mydb.public"
        let dirty = BTreeSet::from([SchemaQualifier::new(
            "mydb".to_string(),
            "analytics".to_string(),
        )]);
        let t = make_transformer(&fqn, "alice", None, &in_project, &dirty);

        let input = make_name(&["mydb", "public", "orders"]);
        let result = t.transform_name(&input);

        // "public" is not dirty → production reference, no overlay rewrite
        assert_eq!(result.0[0].as_str(), "mydb");
        assert_eq!(result.0[1].as_str(), "public");
        assert_eq!(result.0[2].as_str(), "orders");
    }

    // 8. Pre-suffix key in dirty_schemas must NOT match — proves the
    //    post-suffix convention: the lookup uses base_db (post-suffix),
    //    so a pre-suffix key is always a miss.
    #[test]
    fn dirty_schemas_keyed_pre_suffix_does_not_match() {
        // If a caller mistakenly keys dirty_schemas using the pre-suffix
        // database name, the lookup must miss — proving that the
        // convention is post-suffix.
        let fqn = make_fqn("mydb", "public", "ctx");
        let in_project = BTreeSet::from(["mydb".to_string()]);
        // Pre-suffix key (WRONG — caller should use "mydb_staging").
        let dirty = BTreeSet::from([SchemaQualifier::new(
            "mydb".to_string(),
            "public".to_string(),
        )]);
        let t = make_transformer(&fqn, "alice", Some("_staging"), &in_project, &dirty);

        let input = make_name(&["mydb", "public", "orders"]);
        let result = t.transform_name(&input);

        // Pre-suffix key doesn't match post-suffix lookup → no overlay,
        // just the suffixed production database name.
        assert_eq!(result.0[0].as_str(), "mydb_staging");
        assert_eq!(result.0[1].as_str(), "public");
        assert_eq!(result.0[2].as_str(), "orders");
    }

    // 9. Unqualified (1-part) name: fqn database + schema used, then
    //    routed to overlay if schema is dirty.
    #[test]
    fn unqualified_name_resolved_via_fqn_then_routed_to_overlay() {
        let fqn = make_fqn("mydb", "public", "ctx");
        let in_project = BTreeSet::from(["mydb".to_string()]);
        let dirty = BTreeSet::from([SchemaQualifier::new(
            "mydb".to_string(),
            "public".to_string(),
        )]);
        let t = make_transformer(&fqn, "alice", None, &in_project, &dirty);

        // 1-part: just "orders"
        let input = make_name(&["orders"]);
        let result = t.transform_name(&input);

        assert_eq!(result.0[0].as_str(), "mydb__alice");
        assert_eq!(result.0[1].as_str(), "public");
        assert_eq!(result.0[2].as_str(), "orders");
    }

    // 10. Schema-qualified (2-part) name: fqn database prepended, then
    //     routed to overlay if the explicit schema is dirty.
    #[test]
    fn schema_qualified_name_resolved_via_fqn_then_routed_to_overlay() {
        let fqn = make_fqn("mydb", "public", "ctx");
        let in_project = BTreeSet::from(["mydb".to_string()]);
        let dirty = BTreeSet::from([SchemaQualifier::new(
            "mydb".to_string(),
            "analytics".to_string(),
        )]);
        let t = make_transformer(&fqn, "alice", None, &in_project, &dirty);

        // 2-part: "analytics.summary" — fqn database prepended
        let input = make_name(&["analytics", "summary"]);
        let result = t.transform_name(&input);

        assert_eq!(result.0[0].as_str(), "mydb__alice");
        assert_eq!(result.0[1].as_str(), "analytics");
        assert_eq!(result.0[2].as_str(), "summary");
    }
}
