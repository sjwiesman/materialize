---
source: src/mz-deploy/src/cli/commands/new_project.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::new_project

Scaffolds a new mz-deploy project: the standard directory layout, starter config
files, and optionally a git repository.

`run` (`mz-deploy new <name>`) creates a fresh directory (erroring if it already
exists) and scaffolds into it; `init` (`mz-deploy init`) scaffolds the current
directory, deriving the project name from the working directory. Both call
`scaffold`, print a success line, then `prompt_default_profile` and
`print_skill_hint`. `ScaffoldOpts` carries the `init_git` flag.

`scaffold` creates `models/materialize/public`, `clusters`, `roles`,
`network-policies`, and `.vscode`, drops `.gitkeep` files, and writes
`.gitignore`, `project.toml`, `.vscode/extensions.json`, and a `README.md` (with
`{{name}}` substituted) — all from templates embedded with `include_str!` from
`../scaffold/`. When `init_git` is set it runs `git init`, `git add .`, and an
initial `git commit` authored as "Materialize Inc", erroring if any step fails.

`prompt_default_profile` interactively offers the profiles from `profiles.toml`
and records the chosen one via `write_mzprofile`; it is skipped silently for
JSON/quiet output or a non-TTY stdin so it never blocks CI. `print_skill_hint`
nudges users to install the Materialize agent skill.
