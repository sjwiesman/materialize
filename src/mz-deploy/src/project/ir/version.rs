// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Object version naming and lookup.
//!
//! A versioned object deploys under the name `<base>@<version>`. `@` cannot
//! appear in a bare identifier, so the name always renders quoted and the
//! scheme reserves no part of the unquoted namespace.

use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn physical_name(base: &str, version: u32) -> String {
    format!("{base}@{version}")
}

/// Split a deployed object name into its base name and version, the inverse of
/// [`physical_name`].
pub(crate) fn parse_physical_name(deployed: &str) -> (&str, Option<u32>) {
    let Some((base, version_text)) = deployed.rsplit_once('@') else {
        return (deployed, None);
    };

    if base.is_empty() || !is_valid_version_text(version_text) {
        return (deployed, None);
    }

    // The shape check constrains digits but not magnitude, and callers pass
    // through catalog names with no filename length limit, so a suffix too large
    // for `u32` is simply not a version, the same as `x@prod` is not.
    match version_text.parse() {
        Ok(version) => (base, Some(version)),
        Err(_) => (deployed, None),
    }
}

/// A version suffix is a positive integer with no leading zeros, so that the
/// number and its spelling stay in one-to-one correspondence.
fn is_valid_version_text(text: &str) -> bool {
    !text.is_empty() && !text.starts_with('0') && text.chars().all(|c| c.is_ascii_digit())
}

/// The versioned object that a reference is written inside, which resolution needs
/// to tell a self-reference apart from a bare reference made elsewhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EnclosingObject {
    database: String,
    schema: String,
    base: String,
    version: u32,
}

impl EnclosingObject {
    pub(crate) fn new(database: String, schema: String, base: String, version: u32) -> Self {
        Self {
            database,
            schema,
            base,
            version,
        }
    }

    /// Whether a reference to `database.schema.object` names this very object.
    pub(crate) fn encloses(&self, database: &str, schema: &str, object: &str) -> bool {
        self.database == database && self.schema == schema && self.base == object
    }
}

/// Every versioned object declared in the project, keyed by database, schema, and
/// base name. An unversioned object is absent.
#[derive(Debug, Default, Clone)]
pub(crate) struct VersionMap {
    versions: BTreeMap<(String, String, String), BTreeSet<u32>>,
}

impl VersionMap {
    pub(crate) fn insert(&mut self, database: &str, schema: &str, base: &str, version: u32) {
        self.versions
            .entry((database.to_string(), schema.to_string(), base.to_string()))
            .or_default()
            .insert(version);
    }

    /// The highest declared version, or `None` if the name is not versioned.
    fn newest(&self, database: &str, schema: &str, base: &str) -> Option<u32> {
        self.versions
            .get(&(database.to_string(), schema.to_string(), base.to_string()))?
            .last()
            .copied()
    }

    /// The deployed name that a reference spelled `object` resolves to, or `None`
    /// when the spelling already names its target directly.
    ///
    /// A spelling carrying a version suffix is a pin, and a spelling with no
    /// declared versions is unversioned, so both name their target already. A
    /// spelling naming the object `enclosing` declares resolves to the enclosing
    /// version. Anything else resolves to the newest version held here.
    pub(crate) fn resolve_reference(
        &self,
        database: &str,
        schema: &str,
        object: &str,
        enclosing: Option<&EnclosingObject>,
    ) -> Option<String> {
        if parse_physical_name(object).1.is_some() {
            return None;
        }
        if let Some(enclosing) = enclosing
            && enclosing.encloses(database, schema, object)
        {
            return Some(physical_name(object, enclosing.version));
        }
        let newest = self.newest(database, schema, object)?;
        Some(physical_name(object, newest))
    }

    /// Every `(database, schema, base name, version)` entry, in sorted order.
    pub(crate) fn entries(&self) -> impl Iterator<Item = (&str, &str, &str, u32)> {
        self.versions
            .iter()
            .flat_map(|((database, schema, base), versions)| {
                versions.iter().map(move |&version| {
                    (database.as_str(), schema.as_str(), base.as_str(), version)
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_map() -> VersionMap {
        let mut map = VersionMap::default();
        map.insert("materialize", "public", "orders", 1);
        map.insert("materialize", "public", "orders", 3);
        map.insert("materialize", "public", "orders", 2);
        map.insert("materialize", "other", "orders", 7);
        map
    }

    #[mz_ore::test]
    fn test_newest_returns_highest_version() {
        assert_eq!(
            sample_map().newest("materialize", "public", "orders"),
            Some(3)
        );
    }

    #[mz_ore::test]
    fn test_newest_is_scoped_per_schema() {
        assert_eq!(
            sample_map().newest("materialize", "other", "orders"),
            Some(7)
        );
    }

    #[mz_ore::test]
    fn test_newest_unknown_name_is_none() {
        assert_eq!(
            sample_map().newest("materialize", "public", "customers"),
            None
        );
    }

    #[mz_ore::test]
    fn test_newest_unknown_schema_is_none() {
        assert_eq!(
            sample_map().newest("materialize", "missing", "orders"),
            None
        );
    }

    #[mz_ore::test]
    fn test_entries_are_sorted() {
        let map = sample_map();
        assert_eq!(
            map.entries().collect::<Vec<_>>(),
            vec![
                ("materialize", "other", "orders", 7),
                ("materialize", "public", "orders", 1),
                ("materialize", "public", "orders", 2),
                ("materialize", "public", "orders", 3),
            ]
        );
    }

    #[mz_ore::test]
    fn test_resolve_reference_bare_name_takes_newest() {
        assert_eq!(
            sample_map().resolve_reference("materialize", "public", "orders", None),
            Some("orders@3".to_string())
        );
    }

    #[mz_ore::test]
    fn test_resolve_reference_leaves_a_pin_alone() {
        assert_eq!(
            sample_map().resolve_reference("materialize", "public", "orders@1", None),
            None
        );
    }

    #[mz_ore::test]
    fn test_resolve_reference_leaves_an_unversioned_name_alone() {
        assert_eq!(
            sample_map().resolve_reference("materialize", "public", "customers", None),
            None
        );
    }

    #[mz_ore::test]
    fn test_resolve_reference_is_scoped_per_schema() {
        // The same base name in a schema with no declared versions is not
        // versioned, even though another schema declares it.
        assert_eq!(
            sample_map().resolve_reference("materialize", "unrelated", "orders", None),
            None
        );
    }

    #[mz_ore::test]
    fn test_resolve_reference_from_inside_a_version_resolves_to_that_version() {
        let enclosing = EnclosingObject::new(
            "materialize".to_string(),
            "public".to_string(),
            "orders".to_string(),
            1,
        );
        assert_eq!(
            sample_map().resolve_reference("materialize", "public", "orders", Some(&enclosing)),
            Some("orders@1".to_string())
        );
    }

    #[mz_ore::test]
    fn test_resolve_reference_from_inside_a_version_still_takes_newest_for_others() {
        let enclosing = EnclosingObject::new(
            "materialize".to_string(),
            "public".to_string(),
            "shipments".to_string(),
            1,
        );
        assert_eq!(
            sample_map().resolve_reference("materialize", "public", "orders", Some(&enclosing)),
            Some("orders@3".to_string())
        );
    }

    #[mz_ore::test]
    fn test_resolve_reference_enclosing_is_scoped_per_schema() {
        let enclosing = EnclosingObject::new(
            "materialize".to_string(),
            "other".to_string(),
            "orders".to_string(),
            7,
        );
        assert_eq!(
            sample_map().resolve_reference("materialize", "public", "orders", Some(&enclosing)),
            Some("orders@3".to_string())
        );
    }

    #[mz_ore::test]
    fn test_physical_name() {
        assert_eq!(physical_name("orders", 1), "orders@1");
        assert_eq!(physical_name("orders", 42), "orders@42");
    }

    #[mz_ore::test]
    fn test_physical_name_renders_quoted_as_an_ident() {
        let ident = mz_sql_parser::ast::Ident::new_unchecked(physical_name("orders", 2));
        assert_eq!(ident.to_string(), "\"orders@2\"");
    }

    #[mz_ore::test]
    fn test_parse_physical_name_round_trips_with_physical_name() {
        for (base, version) in [("orders", 1u32), ("orders", 42), ("a", 100)] {
            let deployed = physical_name(base, version);
            assert_eq!(parse_physical_name(&deployed), (base, Some(version)));
        }
    }

    #[mz_ore::test]
    fn test_parse_physical_name_no_at_sign_is_base_only() {
        assert_eq!(parse_physical_name("orders"), ("orders", None));
    }

    #[mz_ore::test]
    fn test_parse_physical_name_rejects_non_version_suffix() {
        // Leading zero: not the canonical spelling of any integer.
        assert_eq!(parse_physical_name("orders@01"), ("orders@01", None));
        // Not a number at all.
        assert_eq!(parse_physical_name("orders@prod"), ("orders@prod", None));
        // Empty text after `@`.
        assert_eq!(parse_physical_name("orders@"), ("orders@", None));
        // Empty base before `@`.
        assert_eq!(parse_physical_name("@1"), ("@1", None));
        // All digits, no leading zero, but too large for a `u32`. Must not panic.
        assert_eq!(
            parse_physical_name("orders@99999999999"),
            ("orders@99999999999", None)
        );
    }
}
