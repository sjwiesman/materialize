// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Profile-specific file override resolution.
//!
//! Files can be named `name#<profile>.sql` to override `name.sql` when a
//! particular profile is active. The `#` delimiter is split on the **last**
//! occurrence (`rsplit_once`).
//!
//! ## Resolution Algorithm
//!
//! 1. **Parse** each file stem via [`parse_file_stem`] using `rsplit_once('#')`
//!    to separate `(object_name, profile)`. Files without `#` (or with an
//!    empty object/profile part) are treated as the default (no profile).
//! 2. **Group** files by object name into [`ObjectFiles`], recording the default
//!    file and any profile-specific overrides. Duplicates within the same
//!    group (e.g., two defaults, or two overrides for the same profile) are
//!    rejected with `LoadError::DuplicateProfileObject`.
//!
//! Callers select the active variant themselves by checking
//! `overrides.get(profile).or(default.as_ref())`.
//!
//! **Key Insight:** `#` cannot appear in a SQL identifier, so a well-formed
//! variant filename contains exactly one `#`. This lets object names freely
//! contain underscores: `my_pg_conn#staging` → `("my_pg_conn", "staging")`.

use crate::project::error::LoadError;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// A model filename stem split into its parts.
///
/// The canonical spelling is `<base>@<version>#<profile>`, and both suffixes
/// are optional. `@` and `#` are reserved characters in model filenames.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ParsedStem<'a> {
    pub base: &'a str,
    pub version: Option<u32>,
    pub profile: Option<&'a str>,
}

/// Why a filename stem is not a legal model filename.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum StemError {
    /// `@` appeared after `#`, as in `orders#staging@1`, which would otherwise
    /// read as the profile `staging@1`.
    VersionAfterProfile,
    /// The stem contains `@` but does not form `<base>@<positive integer>`.
    InvalidVersion { stem: String },
}

/// Split a file stem into its base name, version, and profile. A stem whose `#`
/// split would leave an empty part is treated as a plain name.
pub(crate) fn parse_file_stem(stem: &str) -> Result<ParsedStem<'_>, StemError> {
    let (name_part, profile) = match stem.rsplit_once('#') {
        Some((base, profile)) if !base.is_empty() && !profile.is_empty() => (base, Some(profile)),
        _ => (stem, None),
    };

    if profile.is_some_and(|p| p.contains('@')) {
        return Err(StemError::VersionAfterProfile);
    }

    let Some((base, version_text)) = name_part.split_once('@') else {
        return Ok(ParsedStem {
            base: name_part,
            version: None,
            profile,
        });
    };

    if base.is_empty() || version_text.contains('@') {
        return Err(StemError::InvalidVersion {
            stem: name_part.to_string(),
        });
    }

    Ok(ParsedStem {
        base,
        version: Some(parse_version(version_text, name_part)?),
        profile,
    })
}

/// Parse the text after `@` as a version number. Leading zeros are rejected so
/// that a version and its filename spelling stay in one-to-one correspondence.
fn parse_version(text: &str, stem: &str) -> Result<u32, StemError> {
    let invalid = || StemError::InvalidVersion {
        stem: stem.to_string(),
    };

    if text.is_empty() || text.starts_with('0') || !text.chars().all(|c| c.is_ascii_digit()) {
        return Err(invalid());
    }

    text.parse::<u32>().map_err(|_| invalid())
}

impl StemError {
    /// A human-readable explanation, used to build `LoadError::InvalidVersionSuffix`.
    pub(crate) fn reason(&self) -> String {
        match self {
            Self::VersionAfterProfile => {
                "version suffix must come before the profile suffix, as in 'orders@2#staging'"
                    .to_string()
            }
            Self::InvalidVersion { stem } => format!(
                "'{stem}' does not form '<name>@<version>', where version is a \
                 positive integer without leading zeros"
            ),
        }
    }
}

/// All files for a single object version, grouped by profile.
#[derive(Debug, Clone)]
pub(crate) struct ObjectFiles {
    /// The base object name, with version and profile suffixes removed
    pub name: String,
    pub version: Option<u32>,
    /// The default file (no profile suffix), if any
    pub default: Option<PathBuf>,
    /// Profile-specific override files, keyed by profile name
    pub overrides: BTreeMap<String, PathBuf>,
}

/// Collect all `.sql` files from a directory grouped by object name without resolving.
///
/// Returns all variants (default + all profile overrides) for each object.
/// This is used to load and validate all profile variants before resolving.
pub(crate) fn collect_all_sql_files(directory: &Path) -> Result<Vec<ObjectFiles>, LoadError> {
    let entries: Vec<_> = std::fs::read_dir(directory)
        .map_err(|e| LoadError::DirectoryReadFailed {
            path: directory.to_path_buf(),
            source: e,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| LoadError::EntryReadFailed {
            directory: directory.to_path_buf(),
            source: e,
        })?;

    let mut groups: BTreeMap<(String, Option<u32>), ObjectFiles> = BTreeMap::new();

    for entry in entries {
        let path = entry.path();

        if path.extension().and_then(|e| e.to_str()) != Some("sql") {
            continue;
        }

        let file_stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| LoadError::InvalidFileName { path: path.clone() })?
            .to_string();

        let parsed = parse_file_stem(&file_stem).map_err(|e| LoadError::InvalidVersionSuffix {
            path: path.clone(),
            reason: e.reason(),
        })?;

        let group = groups
            .entry((parsed.base.to_string(), parsed.version))
            .or_insert_with(|| ObjectFiles {
                name: parsed.base.to_string(),
                version: parsed.version,
                default: None,
                overrides: BTreeMap::new(),
            });

        match parsed.profile {
            None => {
                if let Some(existing) = &group.default {
                    return Err(LoadError::DuplicateProfileObject {
                        name: parsed.base.to_string(),
                        profile: "default".to_string(),
                        path1: existing.clone(),
                        path2: path,
                    });
                }
                group.default = Some(path);
            }
            Some(p) => {
                if let Some(existing) = group.overrides.get(p) {
                    return Err(LoadError::DuplicateProfileObject {
                        name: parsed.base.to_string(),
                        profile: p.to_string(),
                        path1: existing.clone(),
                        path2: path,
                    });
                }
                group.overrides.insert(p.to_string(), path);
            }
        }
    }

    // A bare reference resolves to the newest version, so a base name declared
    // both with and without a version has two defensible meanings.
    let versioned: BTreeSet<&String> = groups
        .keys()
        .filter(|(_, version)| version.is_some())
        .map(|(name, _)| name)
        .collect();

    for ((name, _), files) in &groups {
        if files.version.is_none() && versioned.contains(name) {
            let path = files
                .default
                .clone()
                .or_else(|| files.overrides.values().next().cloned())
                .expect("a group is only created when a file is added to it");
            return Err(LoadError::MixedVersionedAndUnversioned {
                name: name.clone(),
                path,
            });
        }
    }

    Ok(groups.into_values().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_file_stem tests ---

    #[mz_ore::test]
    fn test_parse_no_delimiter() {
        let parsed = parse_file_stem("pg_conn").unwrap();
        assert_eq!(parsed.base, "pg_conn");
        assert_eq!(parsed.version, None);
        assert_eq!(parsed.profile, None);
    }

    #[mz_ore::test]
    fn test_parse_with_profile() {
        let parsed = parse_file_stem("pg_conn#staging").unwrap();
        assert_eq!(parsed.base, "pg_conn");
        assert_eq!(parsed.version, None);
        assert_eq!(parsed.profile, Some("staging"));
    }

    #[mz_ore::test]
    fn test_parse_object_name_with_underscores() {
        // Underscores in the object name are preserved; only the `#`
        // separates the profile.
        let parsed = parse_file_stem("stg_stripe__payments#staging").unwrap();
        assert_eq!(parsed.base, "stg_stripe__payments");
        assert_eq!(parsed.version, None);
        assert_eq!(parsed.profile, Some("staging"));
    }

    #[mz_ore::test]
    fn test_parse_empty_profile() {
        // "pg_conn#" → empty profile part, treated as plain name
        let parsed = parse_file_stem("pg_conn#").unwrap();
        assert_eq!(parsed.base, "pg_conn#");
        assert_eq!(parsed.version, None);
        assert_eq!(parsed.profile, None);
    }

    #[mz_ore::test]
    fn test_parse_empty_object_name() {
        // "#staging" → empty object name, treated as plain name
        let parsed = parse_file_stem("#staging").unwrap();
        assert_eq!(parsed.base, "#staging");
        assert_eq!(parsed.version, None);
        assert_eq!(parsed.profile, None);
    }

    #[mz_ore::test]
    fn test_parse_no_underscores() {
        let parsed = parse_file_stem("simple").unwrap();
        assert_eq!(parsed.base, "simple");
        assert_eq!(parsed.version, None);
        assert_eq!(parsed.profile, None);
    }

    #[mz_ore::test]
    fn test_parse_single_underscore() {
        let parsed = parse_file_stem("my_table").unwrap();
        assert_eq!(parsed.base, "my_table");
        assert_eq!(parsed.version, None);
        assert_eq!(parsed.profile, None);
    }

    // --- version suffix parsing ---

    #[mz_ore::test]
    fn test_parse_version_only() {
        let parsed = parse_file_stem("orders@2").unwrap();
        assert_eq!(parsed.base, "orders");
        assert_eq!(parsed.version, Some(2));
        assert_eq!(parsed.profile, None);
    }

    #[mz_ore::test]
    fn test_parse_version_and_profile() {
        let parsed = parse_file_stem("orders@2#staging").unwrap();
        assert_eq!(parsed.base, "orders");
        assert_eq!(parsed.version, Some(2));
        assert_eq!(parsed.profile, Some("staging"));
    }

    #[mz_ore::test]
    fn test_parse_multi_digit_version() {
        let parsed = parse_file_stem("orders@42").unwrap();
        assert_eq!(parsed.version, Some(42));
    }

    #[mz_ore::test]
    fn test_parse_version_after_profile_rejected() {
        // The reverse order would otherwise parse as the profile "staging@1".
        assert_eq!(
            parse_file_stem("orders#staging@1"),
            Err(StemError::VersionAfterProfile)
        );
    }

    #[mz_ore::test]
    fn test_parse_zero_version_rejected() {
        assert!(parse_file_stem("orders@0").is_err());
    }

    #[mz_ore::test]
    fn test_parse_leading_zero_version_rejected() {
        // Leading zeros would make two filenames map to one version.
        assert!(parse_file_stem("orders@01").is_err());
    }

    #[mz_ore::test]
    fn test_parse_non_numeric_version_rejected() {
        assert!(parse_file_stem("orders@latest").is_err());
    }

    #[mz_ore::test]
    fn test_parse_empty_base_rejected() {
        assert!(parse_file_stem("@1").is_err());
    }

    #[mz_ore::test]
    fn test_parse_repeated_version_rejected() {
        assert!(parse_file_stem("orders@1@2").is_err());
    }

    #[mz_ore::test]
    fn test_parse_unversioned_still_works() {
        let parsed = parse_file_stem("pg_conn#staging").unwrap();
        assert_eq!(parsed.base, "pg_conn");
        assert_eq!(parsed.version, None);
        assert_eq!(parsed.profile, Some("staging"));
    }

    // --- grouping and mixed-declaration rejection ---

    #[mz_ore::test]
    fn test_collect_groups_versions_separately() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("orders@1.sql"), "SELECT 1;").unwrap();
        std::fs::write(dir.path().join("orders@2.sql"), "SELECT 2;").unwrap();
        std::fs::write(dir.path().join("orders@2#staging.sql"), "SELECT 3;").unwrap();

        let result = collect_all_sql_files(dir.path()).unwrap();
        assert_eq!(result.len(), 2);

        let v1 = result.iter().find(|f| f.version == Some(1)).unwrap();
        assert_eq!(v1.name, "orders");
        assert!(v1.default.is_some());
        assert!(v1.overrides.is_empty());

        let v2 = result.iter().find(|f| f.version == Some(2)).unwrap();
        assert_eq!(v2.name, "orders");
        assert!(v2.default.is_some());
        assert_eq!(v2.overrides.len(), 1);
        assert!(v2.overrides.contains_key("staging"));
    }

    #[mz_ore::test]
    fn test_collect_rejects_mixed_versioned_and_unversioned() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("orders.sql"), "SELECT 1;").unwrap();
        std::fs::write(dir.path().join("orders@1.sql"), "SELECT 2;").unwrap();

        let err = collect_all_sql_files(dir.path()).unwrap_err();
        assert!(
            matches!(err, LoadError::MixedVersionedAndUnversioned { ref name, .. } if name == "orders"),
            "unexpected error: {err:?}"
        );
    }

    #[mz_ore::test]
    fn test_collect_rejects_bad_version_suffix() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("orders@latest.sql"), "SELECT 1;").unwrap();

        let err = collect_all_sql_files(dir.path()).unwrap_err();
        assert!(
            matches!(err, LoadError::InvalidVersionSuffix { .. }),
            "unexpected error: {err:?}"
        );
    }

    // --- collect_all_sql_files tests ---

    #[mz_ore::test]
    fn test_collect_all_sql_files_basic() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("conn.sql"), "SELECT 1;").unwrap();
        std::fs::write(dir.path().join("conn#staging.sql"), "SELECT 2;").unwrap();
        std::fs::write(dir.path().join("conn#prod.sql"), "SELECT 3;").unwrap();
        std::fs::write(dir.path().join("table.sql"), "SELECT 4;").unwrap();

        let result = collect_all_sql_files(dir.path()).unwrap();
        assert_eq!(result.len(), 2);

        let conn = result.iter().find(|f| f.name == "conn").unwrap();
        assert!(conn.default.is_some());
        assert_eq!(conn.overrides.len(), 2);
        assert!(conn.overrides.contains_key("staging"));
        assert!(conn.overrides.contains_key("prod"));

        let table = result.iter().find(|f| f.name == "table").unwrap();
        assert!(table.default.is_some());
        assert!(table.overrides.is_empty());
    }

    #[mz_ore::test]
    fn test_collect_all_sql_files_override_only() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("secret#staging.sql"), "SELECT 1;").unwrap();

        let result = collect_all_sql_files(dir.path()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "secret");
        assert!(result[0].default.is_none());
        assert_eq!(result[0].overrides.len(), 1);
    }

    #[mz_ore::test]
    fn test_collect_all_sql_files_duplicate_override_errors() {
        // The filesystem guarantees unique filenames within a directory, so
        // the `DuplicateProfileObject` branch inside `collect_all_sql_files`
        // is unreachable from real inputs. This test just confirms a basic
        // call succeeds.
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("conn.sql"), "SELECT 1;").unwrap();
        let result = collect_all_sql_files(dir.path()).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[mz_ore::test]
    fn test_collect_all_sql_files_ignores_non_sql() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("conn.sql"), "SELECT 1;").unwrap();
        std::fs::write(dir.path().join("readme.md"), "hello").unwrap();

        let result = collect_all_sql_files(dir.path()).unwrap();
        assert_eq!(result.len(), 1);
    }
}
