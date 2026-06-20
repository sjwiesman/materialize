---
source: src/mz-deploy/src/cli/commands/apply_secrets.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::apply_secrets

Reconciles `CREATE SECRET` objects: creates missing secrets and updates existing
ones. `PHASE_NAME` is `"secrets"` and `GRANT_KIND` is `GrantObjectKind::Secret`.

The `Secrets` struct holds a `SecretResolver` built from the profile's security
config. Both handlers resolve the secret value client-side with
`resolver.resolve_secret_for_cli` before producing SQL. `handle_new` resolves and
emits a `CREATE SECRET`; `handle_existing` resolves and emits an
`AlterSecretStatement`. Because secret values are sensitive, each handler returns
the resolved/altered statement string in a `redacted_statements` vector (rather
than letting it flow through the normal collected-statements channel), and the
applied action is `ObjectAction::Created` or `ObjectAction::Altered`
respectively. Both call `apply_objects::reconcile_grants_and_comments` to handle
grants and comments.

`plan` gathers the project's secret objects, returns an empty result if there are
none, batch-checks existence via `check_catalog_objects_exist` against the secret
catalog table, prepares schemas for to-be-created secrets through
`apply_plan.prepare_schemas`, and then dispatches each object to the matching
handler, building an `ObjectResult` that carries the executor's collected
statements plus the `redacted_statements`. `run(settings, dry_run)` compiles and
connects via `compile_apply_project_and_connect`, adds the planned phase to an
`ApplyPlan`, and executes it unless `dry_run`. `matches` filters for
`Statement::CreateSecret`.
