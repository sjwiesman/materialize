---
source: src/mz-deploy/src/cli/commands/lock.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::lock

Implements the data-contracts (`lock`) command, which resolves the project's
declared dependencies into a `types.lock` file.

`run` announces progress, then calls `discover_source_tables` to find
`CREATE TABLE FROM SOURCE` tables by compiling the project (via
`project::plan_sync` and `get_tables_from_source`). If there are no declared
dependencies and no source tables, it finishes early. Otherwise it connects with
the active profile and calls `client.types().query_types_for_objects`, passing
the declared `ObjectId`s and the discovered source tables, which returns each
object's column schema and kind plus a list of any objects not found in the
target catalog. Missing objects produce `CliError::DeclaredDependenciesMissing`;
otherwise the resolved `Types` are written to `types.lock` via
`write_types_lock`, and progress reports the number of locked objects.
