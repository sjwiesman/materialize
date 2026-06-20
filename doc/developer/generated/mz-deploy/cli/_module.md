---
source: src/mz-deploy/src/cli.rs
revision: a647094cc4
---

# mz_deploy::cli

The command-line interface layer. Clap argument parsing and top-level dispatch
live in the `bin/mz-deploy/main.rs` binary; this module supplies the machinery
those parsed commands run against. It defines the CLI's shared error type and
the function that renders errors to the terminal, and re-exports `CliError` at
the module root.

`display_error` writes a `CliError` to stderr and exits with status code 1.
Errors carrying source positions (parse, validation, typecheck) render in
rustc style — a caret under the offending token, drawn through
`annotate_snippets` by way of the `render` submodule. Errors without positions
fall back to a plain colored `error: <message>` line. Either path appends the
error's optional `hint()` as a `help:` line.

## Submodules

- **`commands`** — One module per CLI subcommand (`stage`, `apply_*`,
  `compile`, `promote`, and the rest), each exposing a `run()` entry point. Has
  a `test/` subdirectory for unit-test lowering.
- **`executor`** — Shared apply machinery that orchestrates a command's
  lifecycle: loads configuration, establishes the database connection, and
  dispatches to the appropriate command module.
- **`error`** — Defines `CliError`, the enum unifying every user-facing error
  with an optional hint. Re-exported at the `cli` root.
- **`extended_help`** — Long-form help text shown for `--help`.
- **`git`** — Git interaction helpers used by commands that inspect the working
  tree.
- **`progress`** — Terminal progress and status reporting (spinners, success
  and failure lines).
- **`render`** — Converts positioned errors into `annotate_snippets` output for
  `display_error`.
