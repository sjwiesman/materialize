---
source: src/mz-deploy/src/project/compiler/cache/schema.rs
revision: a647094cc4
---

# mz_deploy::project::compiler::cache::schema

The SQLite table definitions for the compiler cache database, as three
`pub(super)` constants. `SCHEMA_VERSION` is the integer version the compiler
owns; `build_artifact` compares it against the stored `meta.schema_version`
and, on mismatch, runs `DROP_SQL` (which drops every table) followed by
`CREATE_SQL`. All state here is advisory, so a full drop-and-recreate on
version change is safe.

`CREATE_SQL` defines the full schema with `CREATE TABLE IF NOT EXISTS`:
`meta` (key/value, holding the schema version); `file_state` (per-path size,
mtime, content hash, and optional contents); the object-artifact tables
`object_state` (header with fingerprint, kind, and the object's db/schema/
path/statement SQL) plus the positional fragment tables
`object_state_indexes`, `object_state_grants`, `object_state_comments`, and
`object_state_tests`; the typecheck tables `typecheck_objects` and
`typecheck_columns` (the latter with a foreign key back to the former);
`external_type_digest`; and the project-snapshot tables `project_databases`,
`project_schemas`, `project_objects` (with indexes on `file_path` and on
`(database, schema)`), `project_dependencies` (with an index on
`dependency_key` for reverse lookups), `project_external_dependencies`,
`project_cluster_dependencies`, `project_replacement_schemas`,
`project_comments`, `project_indexes`, `project_grants`, `project_tests`,
`project_infrastructure`, `project_infrastructure_properties`,
`project_aliases`, and `project_mod_statements`.
