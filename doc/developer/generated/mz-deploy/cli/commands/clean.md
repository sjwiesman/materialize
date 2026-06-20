---
source: src/mz-deploy/src/cli/commands/clean.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::clean

Implements the `clean` command, which deletes the project's `target/` build
directory (named by `crate::types::BUILD_DIR`, joined onto `settings.directory`).

`run` calls `std::fs::remove_dir_all` on that path. A `NotFound` error is
treated as success, making the command idempotent: a missing `target/` yields a
"nothing to clean" outcome rather than an error; any other I/O error is wrapped
in `CliError::Io`. The result is reported via a `CleanResult` struct holding the
path and a `removed` flag; its `Display` impl prints either "Removed <path>" or
"Nothing to clean (<path> not present)", and it is `Serialize` for JSON output
through `log::output`.
