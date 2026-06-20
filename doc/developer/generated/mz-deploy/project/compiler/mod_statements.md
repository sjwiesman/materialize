---
source: src/mz-deploy/src/project/compiler/mod_statements.rs
revision: a647094cc4
---

# mz_deploy::project::compiler::mod_statements

Validates the companion "mod" files that carry database-level and schema-level
DDL alongside object files. Mod files may contain only a restricted set of
statements that target the database or schema they live under.

`validate_database_mod_statements` accepts a database name, the file path, the
parsed statements, and an error accumulator. It permits `COMMENT ON DATABASE`
(must target this database), `GRANT ... ON DATABASE` (must target this
database, rejecting `SYSTEM` grants and wrong object types), and
`ALTER DEFAULT PRIVILEGES` scoped `IN DATABASE` to this database — rejecting
`IN SCHEMA` and unscoped (`All`) forms. Every other statement type produces an
`InvalidDatabaseModStatement` error whose `statement_type` is the AST variant
name parsed out of the debug rendering.

`validate_schema_mod_statements` is the schema analogue and additionally
mutates the statements: it permits `COMMENT ON SCHEMA`, `GRANT ... ON SCHEMA`,
`SET api = stable` (the only allowed `SET`, gating the schema's stability
mode), and `ALTER DEFAULT PRIVILEGES` scoped `IN SCHEMA` to this schema. For
each matching target it normalizes the schema name to be fully qualified via
the local `normalize_schema_name` closure (prepending the database to an
unqualified `UnresolvedSchemaName`). It rejects `IN DATABASE` and unscoped
`ALTER DEFAULT PRIVILEGES`, invalid `SET api` values, other `SET` variables,
and all other statement types. Errors are constructed through
`ValidationError::with_file_and_sql` with the rendered statement SQL.

Two private helpers support this: `comment_object_type_name` maps a
`CommentObjectType` variant to a human-readable label for mismatch messages,
and `schema_name_matches` returns true when a target string equals either the
bare schema name or the `database.schema` qualified form.
