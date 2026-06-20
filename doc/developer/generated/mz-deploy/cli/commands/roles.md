---
source: src/mz-deploy/src/cli/commands/roles.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::roles

Implements the `roles apply` command, which converges live role state to match
the project's role definitions.

`run` connects an apply client via `connect_apply_client`, builds a dry-run
`DeploymentExecutor`, accumulates one phase from `plan`, and executes it unless
`dry_run` is set. `plan` loads `RoleDefinition`s with `roles::load_roles`
(scoped by profile name and variable substitutions) and returns an `ApplyResult`
with phase `"roles"`; an empty definition set short-circuits.

Planning runs in two passes. Pass 1 calls `create_role` for every definition so
that inter-role `GRANT ROLE` dependencies are satisfiable; `create_role` checks
existence via `introspection().role_exists` and runs the `create_stmt` only when
missing (`ObjectAction::Created` vs. `UpToDate`). Pass 2 calls `configure_role`
per definition and assembles an `ObjectResult` from the create and configure
statements (paired with `itertools::zip_eq`).

`configure_role` executes the definition's `ALTER ROLE`, `GRANT ROLE`, and
`COMMENT` statements, then reconciles drift: it revokes role memberships present
in `introspection().get_role_members` but not in the desired set, and resets
session-default parameters present in `introspection().get_role_parameters` but
not declared by the definition's `ALTER ROLE ... SET` options. Comparisons are
case-insensitive.
