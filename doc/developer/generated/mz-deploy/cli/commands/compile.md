---
source: src/mz-deploy/src/cli/commands/compile.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::compile

Implements the `compile` command, which validates the project and produces the
deployment plan (`project::ir::graph::Project`). It runs the offline compile
pipeline — parse, validate, build the dependency graph, incremental typecheck,
and (verbosely) display.

`run` delegates to `run_with_fs` with a disk-backed `crate::fs::FileSystem`.
`run_with_fs` accepts a caller-supplied filesystem (e.g. an overlay of unsaved
editor buffers, used by the LSP/extension) and dispatches the CPU-bound work to
`mz_ore::task::spawn_blocking`, which calls `run_inner`.
`run_without_typecheck` is the variant used by `apply` commands that create
not-yet-existing infrastructure objects, where typechecking against the live
catalog would fail.

`run_inner` canonicalizes and announces the directory via `progress`, calls
`project::plan_sync` to build the planned project, then validates declared vs.
referenced dependencies with `analysis::deps::validate_dependencies`: unused
declarations emit warnings; undeclared references return
`CliError::UndeclaredDependencies`. Unless `skip_typecheck` is set,
`typecheck_project` runs `project::compiler::typecheck::run` against the loaded
`types.lock`, logging incremental stats (ran/skipped/schema_stable/schema_changed).
When verbose output is enabled, `print_verbose_details` dumps external
dependencies, cluster dependencies, and the per-object dependency graph (external
edges annotated inline).
