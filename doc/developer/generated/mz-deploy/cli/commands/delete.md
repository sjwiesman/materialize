---
source: src/mz-deploy/src/cli/commands/delete.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::delete

Implements the `delete` command, which drops an object from Materialize and
removes its corresponding project file.

The `ObjectKind` enum (Cluster, Connection, NetworkPolicy, Role, Secret, Source,
Table) carries a user-facing `label` and a SQL `sql_keyword`. `DeleteTarget`
resolves an object name to a `(file_path, drop_sql)` pair: clusters, network
policies, and roles map to top-level directories and a name-quoted `DROP`;
connections, secrets, sources, and tables parse the name as an `ObjectId` and
map to `models/<db>/<schema>/<object>.sql` with a fully qualified `DROP`.

`run` resolves the target and errors if the file is not present (the object is
not managed by the project). It refuses JSON output without `--yes`, and
otherwise prompts interactively for confirmation. It connects, runs
`setup::verify`, `setup::validate_connection`, and `setup::require_deployer`,
then executes the `DROP`. Dependency errors ("depended upon" / "depends on") are
rewritten into a friendly message; other failures become
`CliError::Connection`. On success it removes the project file (warning, then
erroring, if removal fails) and reports a `DeleteResult`.
