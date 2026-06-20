---
source: src/mz-deploy/src/project/roles.rs
revision: a647094cc4
---

# mz_deploy::project::roles

Loads and validates role definitions from the optional `<root>/roles/` directory.
Each `.sql` file defines one role via a required `CREATE ROLE` statement plus
optional `ALTER ROLE`, `GRANT ROLE`, and `COMMENT` statements.

`RoleDefinition` holds the role `name`, the `create_stmt`, and the `alter_stmts`,
`grants`, and `comments` for that role. `load_roles` returns an empty vec when
`roles/` is absent; otherwise it collects file variants via
`collect_all_sql_files`, validates every variant independently, resolves the
active variant per profile (override match, falling back to default), and
accumulates failures into `ValidationErrors`.

`classify_role_statements` sorts the parsed `LocatedStatement`s and emits
offset-positioned `ValidationError`s: an `ALTER ROLE` must target this role; a
`GRANT ROLE` must include this role among `role_names`; a `COMMENT` must be a
`CommentObjectType::Role` targeting this role; any other statement raises
`InvalidRoleStatement`. All target checks are case-insensitive. The file must
contain exactly one `CREATE ROLE` whose name matches the filename, or the
corresponding missing/multiple/name-mismatch error is raised.
