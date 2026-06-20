---
source: src/mz-deploy/src/bin/mz-deploy/main.rs
revision: a647094cc4
---

# mz_deploy::bin::main

The binary entry point for `mz-deploy`. This is the binary package directory; it
contains only `main.rs` and has no sibling defining module.

`main.rs` parses CLI arguments with `clap`, configures output mode (verbose,
quiet, JSON), loads project and profile configuration via `Settings::load`, and
dispatches each subcommand to its handler in `mz_deploy::cli`. The `Args`
structure carries global flags (project directory, profile, profiles directory,
Docker image, output format) and the `Command` enum, whose variants cover the
full surface — getting started (`new`, `init`, `profile`, `setup`, `debug`),
develop (`compile`, `clean`, `test`, `explain`, `dev`, `lsp`, `sql`, `mcp`),
infrastructure (`lock`, `apply`, `delete`), and deploy (`stage`, `wait`,
`promote`, `abort`, `describe`, `list`, `log`). Subcommands are hidden from
clap's flat help list and presented through a custom grouped `after_help`
listing; a test keeps that listing in sync with the `Command` variants. Errors
surface either as JSON or through `cli::display_error`.

Child file:

- **`main.rs`** — the sole leaf: argument parsing, settings loading, command
  dispatch, and help/completions generation.
