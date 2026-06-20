---
source: src/mz-deploy/src/cli/extended_help.rs
revision: a647094cc4
---

# mz_deploy::cli::extended_help

Detailed per-command help text, compiled into the binary from markdown files in
the `help/` directory.

`COMMANDS` is a static slice pairing each canonical command name with its help
text via `include_str!`. `ALIASES` maps alternative names (e.g. `build` →
`compile`, `deploy` → `promote`, `ready` → `wait`, `show` → `describe`) to their
canonical commands.

`help_for` resolves a name through `resolve_alias` and returns the matching help
text, or `None` for unknown commands. `all_help` concatenates every command's
help with a `--- mz-deploy help <cmd> ---` delimiter header for bulk ingestion.
`print_unknown_command` writes a colored error to stderr and lists all available
commands with their aliases. `resolve_alias` is the private alias-lookup helper.
