---
source: src/mz-deploy/src/lib.rs
revision: a647094cc4
---

# mz-deploy

The `mz-deploy` crate is a command-line tool for developing and deploying Materialize projects. A project is a directory of `.sql` files that declare databases, schemas, views, materialized views, sinks, and the infrastructure they run on (clusters, roles, network policies, sources, connections, secrets, tables). The crate compiles that directory into a dependency-ordered graph, validates and typechecks it without a server connection, diffs it against a live environment, and executes blue/green schema migrations using Materialize's zero-downtime deployment primitives.

The crate is organized into four major layers plus a set of supporting modules:

* `cli` — the command-line interface. `bin/mz-deploy/main.rs` defines the clap CLI and loads `Settings` once; `cli/commands/` holds one module per subcommand; `cli/executor.rs` is the shared plan-then-execute machinery for the `apply` family; and `cli/error.rs`, `cli/extended_help.rs`, `cli/git.rs`, `cli/progress.rs`, and `cli/render.rs` provide error formatting, the embedded help system, git state detection, progress output, and `annotate_snippets` diagnostic rendering.
* `client` — the database client layer. `client/connection.rs` wraps a `tokio_postgres` connection in a `Client` exposing domain-grouped sub-clients (deployments, introspection, provisioning, validation, type_info, dev_overlays). `client/deployment_ops.rs` performs every read and write of the `_mz_deploy` metadata schema, including the promote crash-recovery state machine. The remaining modules cover introspection queries, idempotent DDL provisioning, pre-deployment validation, column-schema introspection for `types.lock`, dev overlay manifests, domain model types, and connection errors.
* `project` — the offline compile pipeline. `syntax` parses `.sql` files (with psql-style variable substitution) into located statements; `resolve` normalizes and qualifies ASTs; `compiler` discovers project files, matches them against an incremental SQLite cache, validates each object, and typechecks against a stub catalog driving Materialize's real planner; `ir` holds the compiled and graph project representations; and `analysis` extracts dependencies, sorts topologically, validates the graph, builds deployment snapshots, and computes changesets via a Datalog fixed-point. `project/error.rs` defines the error hierarchy.
* `secret_resolver` — resolves `CREATE SECRET` provider expressions (`env_var`, `aws_secret_manager`, `json_field`) on the operator's machine at apply time, so project SQL never contains secret values.

Supporting top-level modules:

* `config` — loads `project.toml` (project root, checked in) and `profiles.toml` (machine-local connection profiles), and resolves the active profile.
* `lsp` — a `tower-lsp` language server (`mz-deploy lsp`) that answers completion, hover, navigation, symbols, code lenses, code actions, and diagnostics from the same `graph::Project` IR and typecheck cache the CLI builds.
* `docker_runtime` — manages the shared local Materialize container used by `test` and `explain` for fully offline execution.
* `fs` — an overlay filesystem that shadows on-disk files with in-memory contents, used to compile unsaved editor buffers.
* `log` — human-facing output: the `info!`/`verbose!` macros write to stderr, and the `Render` trait routes structured results to stdout as JSON under `--output json` or pretty-printed to stderr otherwise.
* `diagnostics` — converts typed compile errors into `PositionalDiagnostic`s rendered with `annotate_snippets`.
* `types` — the `types.lock` data contract that pins external dependencies' column schemas for the offline typechecker.

The deployment lifecycle splits the world in two. Project *logic* — views and materialized views — deploys blue/green: `stage` builds the changed subset in suffixed schemas and clusters alongside production, `wait` watches the staging clusters hydrate, and `promote` swaps staging into production in one atomic transaction followed by resumable post-swap work (deferred sinks, `APPLY REPLACEMENT` for stable-API schemas, sink repointing, and dropping the old world). `abort` discards an unpromoted deployment, and `dev` builds throwaway per-developer overlay databases. Project *infrastructure* — clusters, roles, network policies, secrets, connections, sources, tables — is reconciled idempotently by the `apply` family. All deployment state lives in a `_mz_deploy` database on the target server, so conflict detection and crash recovery are plain SQL.

Key dependencies: `mz-sql`, `mz-sql-parser`, `mz-catalog` (builtin functions and types for the offline typechecker), `mz-repr`, `tokio-postgres`, `clap`, `rusqlite` (the compiler cache), `rayon` (parallel compile and typecheck), `tower-lsp`, and `annotate_snippets`. The companion VS Code extension (`misc/vscode-ext/`) spawns `mz-deploy lsp` and otherwise shells out to the CLI.
