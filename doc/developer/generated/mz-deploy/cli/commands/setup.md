---
source: src/mz-deploy/src/cli/commands/setup.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::setup

Owns the `_mz_deploy` tracking infrastructure and the connection/role validation
that every other command relies on.

`setup` is the only function in the crate that writes to `_mz_deploy`, and it is
fully idempotent (each statement uses `IF NOT EXISTS`, the version row is
pre-checked). Its phases: create the `_mz_deploy_server` cluster (sized by
`cluster_size`) if missing; create the `_mz_deploy` database; run every statement
in `setup_schema::SETUP_STATEMENTS` one at a time (DDL can't run in an implicit
multi-statement transaction); and, only when RBAC is active, create the three
`materialize_*` roles and (re)apply their grants. The RBAC path is gated on
superuser (`require_superuser`) and database ownership; the deployer and developer
roles receive the extra `CREATE`/system privileges their workflows need.

`verify` is a read-only existence check used by every non-`setup` command,
returning `CliError::SetupRequired` (with `MissingObject`s) when the cluster,
database, expected schema objects (`setup_schema::EXPECTED_OBJECTS`), or — under
RBAC — the roles are absent. `validate_connection` returns the connected role's
`MzDeployRole` (Superuser, Deployer, Developer, Monitor): it short-circuits to
`Superuser` when RBAC is inactive or the role is a superuser, and otherwise
requires membership in exactly one mz-deploy role. `require_deployer` /
`require_developer` gate state-mutating commands.

`rbac_active` combines emulator mode with `is_rbac_enabled` (emulator profiles
force RBAC off); supporting helpers `is_superuser`, `require_superuser`, and
`require_createdb` perform the privilege checks. `run` is the CLI entry point:
connect (unpinned), call `setup`, and report success.
