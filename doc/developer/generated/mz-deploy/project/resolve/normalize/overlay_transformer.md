---
source: src/mz-deploy/src/project/resolve/normalize/overlay_transformer.rs
revision: a647094cc4
---

# mz_deploy::project::resolve::normalize::overlay_transformer

Name transformer for `mz-deploy dev` overlay compilation, implementing the
two-step reference-resolution rule for schema-level overlays.

`OverlayTransformer` implements `NameTransformer` and `ClusterTransformer`. Its
`transform_name` first leaves database-less system-catalog references (a 2-part
name whose first part `is_system_schema`) verbatim. It then normalizes the input
to a 3-part `(database, schema, object)` name using the transformer's `fqn`
context: a 1-part name borrows the fqn database and schema, a 2-part name borrows
the fqn database, a 3-part name is used as-is, and anything else is returned
unchanged. With the database known, it applies two checks. Step 1 (external): if
the database is not in `in_project_databases`, the name is emitted verbatim as an
external dependency. Step 2 (dirty): if the `(database, schema)` pair is in
`dirty_schemas`, the database component is rewritten to `<database>__<profile_name>`
(the overlay database); otherwise the name is emitted as a production reference.
This routes references on a per-schema basis, so a project database with only
some schemas dirty still points clean-schema references at production.

`transform_cluster` rewrites every cluster reference to `target_cluster`
regardless of the input. `database_name` returns the fqn database. Any
`profile_suffix` is already applied to in-project names by the project planner
before `dev` constructs this transformer, so no suffix handling happens here.
