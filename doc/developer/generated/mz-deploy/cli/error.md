---
source: src/mz-deploy/src/cli/error.rs
revision: a647094cc4
---

# mz_deploy::cli::error

Top-level error type for CLI commands. `CliError` is a `thiserror` enum that
either wraps lower-level errors transparently (`Project`, `Config`,
`Connection`, `DeploymentSnapshot`, `Dependency`, `Types`, `Validation`,
`TestValidationFailed`, `TypeCheckFailed`) or carries CLI-specific variants for
situations the command layer detects directly — deployment conflicts, git
state (`GitShaFailed`, `GitDirty`), production-overwrite refusals, setup
prerequisites (`SetupRequired`, `SetupNotDatabaseOwner`,
`SetupRequiresSuperuser`), SQL execution failures, role authorization, missing
or undeclared dependencies, hydration timeouts, and a catch-all `Message`.

`MissingObject` is a helper enum (`Cluster`, `Database`, `SchemaObject`,
`Role`) listing the specific `_mz_deploy` infrastructure pieces that
`setup::verify` expected but did not find; it is carried by
`CliError::SetupRequired` so the hint can name what is missing.

The central method is `CliError::hint`, which returns an optional
human-readable, color-aware (`owo_colors`) remediation string for most
variants — for example, how to rebase a conflicting deployment, how to grant
CREATEDB, how to declare external dependencies in `project.toml`, or which
`mz-deploy` subcommand to run next. Variants that wrap other errors return
`None` because the wrapped error provides its own context. Two `From`
implementations convert `DatabaseValidationError` into `CliError::Validation`
and `String` into `CliError::Message`.
