---
source: src/mz-deploy/src/project/resolve/normalize/mod_rewriter.rs
revision: a647094cc4
---

# mz_deploy::project::resolve::normalize::mod_rewriter

AST-level suffixing of database and schema names in mod-file statements. When a
profile suffix is active, database names must be suffixed (`app` → `app_dev`);
when a staging suffix is active, schema names must be suffixed (`public` →
`public_staging`). Rewriting at the AST level via the auto-generated `VisitMut`
traversal touches only real identifier nodes, never string literals or comments.

`rewrite_database_names` and `rewrite_schema_names` are the entry points,
applying the rewrite to each statement in a slice. They cover the statement types
permitted in mod files: `COMMENT ON DATABASE/SCHEMA`, `GRANT … ON
DATABASE/SCHEMA`, and `ALTER DEFAULT PRIVILEGES IN DATABASE/SCHEMA`. Because the
generated traversal reaches database/schema names through two different hooks
depending on AST position, each rewriter overrides both: the associated-type
hooks (`visit_database_name_mut` / `visit_schema_name_mut`, used by COMMENT and
ALTER DEFAULT PRIVILEGES) and the concrete-struct hooks
(`visit_unresolved_database_name_mut` / `visit_unresolved_schema_name_mut` and
`visit_object_name_mut`, used by GRANT via `UnresolvedObjectName::Database/Schema`).

`DatabaseNameRewriter` and `SchemaNameRewriter` are the two `VisitMut`
implementations; both share their core logic with the free helpers
`rewrite_database_name` (suffixes the database ident only if it matches the
target) and `rewrite_schema_name` (suffixes the last ident of the schema name —
the schema part — when it matches, so both `schema` and `db.schema` forms work).
Non-matching identifiers pass through unchanged.
