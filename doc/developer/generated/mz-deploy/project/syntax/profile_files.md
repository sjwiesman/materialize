---
source: src/mz-deploy/src/project/syntax/profile_files.rs
revision: a647094cc4
---

# mz_deploy::project::syntax::profile_files

Resolves profile-specific file overrides. A file named `name#<profile>.sql`
overrides `name.sql` when that profile is active. Because `#` cannot appear in a
SQL identifier, a well-formed variant filename contains exactly one `#`, leaving
object names free to contain underscores.

`parse_file_stem` splits a file stem into `(object_name, optional_profile)` using
`rsplit_once('#')` (the last `#` wins). If either side is empty (`name#` or
`#profile`) the whole stem is treated as a plain object name with no profile.

`ObjectFiles` groups all files for one object name: the `name`, the optional
`default` path (no profile suffix), and `overrides`, a `BTreeMap` from profile
name to path.

`collect_all_sql_files` reads a directory, ignores non-`.sql` entries, and groups
the remaining files by object name into `ObjectFiles`. A second default for an
object, or a second override for the same profile, raises
`LoadError::DuplicateProfileObject`. It does not pick an active variant — callers
do that themselves via `overrides.get(profile).or(default.as_ref())`. Directory
and entry read failures surface as `LoadError::DirectoryReadFailed` /
`EntryReadFailed`, and a stem that is not valid UTF-8 as
`LoadError::InvalidFileName`.
