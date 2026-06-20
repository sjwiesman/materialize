---
source: src/mz-deploy/src/bin/mz-deploy/main.rs
revision: a647094cc4
---

# mz_deploy::bin::main

Binary entry point for `mz-deploy`. It defines the `clap` CLI, loads
configuration, and dispatches each subcommand to the corresponding handler in
`mz_deploy::cli`.

`Args` is the top-level parser: a flattened `GlobalArgs` (global `--directory`,
`--verbose`, `--quiet`, `--profile` / `MZ_DEPLOY_PROFILE`, `--docker-image`,
`--profiles-dir` / `MZ_DEPLOY_PROFILES_DIR`, and `--output` text/json) plus a
required `Command`. Because clap 4 has no native subcommand grouping, every
`Command` variant is hidden from the auto-generated flat list and a hand-written
grouped listing (`GROUPED_HELP`, organized as Getting started / Develop /
Infrastructure / Deploy) is rendered via `after_help` and a custom
`help_template`. A test (`grouped_help_lists_all_subcommands`) asserts
`GROUPED_HELP` stays in sync with the `Command` variants' clap `about` text.

The `Command` enum declares every subcommand (compile, clean, explain, apply,
promote, stage, setup, debug, sql, mcp, describe, dev, lock, test, abort, list,
log, new, init, profile, wait, delete, lsp, completions, help) with extended
per-command help, alongside the nested `ProfileCommand`, `ApplyCommand`, and
`DeleteCommand` subcommand enums and the `OutputFormat` value enum. Build version
is supplied by `mz_build_info`.

`main` (a `#[tokio::main]` async fn) parses args, sets the global verbose/quiet/
JSON flags in `mz_deploy::log`, runs `run`, and on error emits JSON to stdout (and
exits 1) under JSON mode or a formatted error via `cli::display_error` otherwise.
`run` destructures the args, builds a `load_settings` closure over `Settings::load`
(passing a per-command `needs_connection` flag), and matches on the command to
call the appropriate `cli::commands::*` handler — including special cases like
`completions` (generating shell completions), `help` (printing extended-help
text), `lsp` (delegating to `mz_deploy::lsp::run`), and the commands that reject
`--output json`.
