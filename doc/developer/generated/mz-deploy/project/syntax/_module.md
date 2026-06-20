---
source: src/mz-deploy/src/project/syntax.rs
revision: a647094cc4
---

# mz_deploy::project::syntax

Source-facing compiler inputs — the modules that describe how bytes on disk
become structured compiler inputs. This is the first stage of the pipeline:
directory and file discovery, profile-specific file variants, variable
substitution, and SQL parsing with source locations.

Child modules:

- **`variables`** — psql-style variable substitution applied to raw SQL before
  parsing. Variables are defined per-profile in `project.toml` and passed in as
  a map; several reference forms are supported.
- **`parser`** — wraps `mz_sql_parser` to turn `.sql` contents into AST
  statements, attaching file-path context so parse errors point back to the
  source file (producing `LocatedStatement`s with byte offsets).
- **`input`** — the source-owned data types holding parsed statements and source
  locations for a single database object before any semantic validation.
- **`profile_files`** — resolves profile-specific file overrides, where
  `name#<profile>.sql` overrides `name.sql` when that profile is active.
