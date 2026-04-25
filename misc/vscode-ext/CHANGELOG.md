# Changelog

## 0.1.0

Initial release.

- LSP client for `mz-deploy` projects (go-to-definition, hover, completion,
  parse diagnostics, code lenses).
- Code-lens commands `mz-deploy.runTest` and `mz-deploy.runExplain` that run
  the corresponding `mz-deploy` subcommand in a terminal.
- `mz-deploy.path` setting controls which `mz-deploy` binary the extension
  spawns.
- Pre-flight check on activation: if the configured binary is missing or
  won't run, the extension surfaces an actionable error dialog with an
  "Open Settings" button instead of failing silently.
