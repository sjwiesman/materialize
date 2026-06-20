---
source: src/mz-deploy/src/cli/commands/abort.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::abort

Implements `mz-deploy abort`, which discards an unpromoted staging deployment by
tearing down its staging resources and deleting all of its tracking records. The
single public entry point `run(settings, deploy_id)` connects with the configured
profile, runs the shared `setup::verify` / `setup::validate_connection` checks,
and requires the `materialize_deployer` role via `setup::require_deployer`.

Before mutating anything, `run` loads the deployment's metadata through
`client.deployments().get_deployment_metadata` and errors with
`ConnectionError::DeploymentNotFound` if it is absent. It refuses to abort a
deployment whose `promoted_at` is set, returning
`ConnectionError::DeploymentAlreadyPromoted` — abort only applies to staging.

It then queries the staging schemas and clusters (suffixed with `_<deploy_id>`)
via the introspection client and drops them with `drop_staging_schemas` and
`drop_staging_clusters`. After the catalog objects are gone, it clears the
`_mz_deploy` bookkeeping for the deployment: `delete_deployment_clusters`,
`delete_pending_statements` (deferred sinks), `delete_replacement_mvs`,
`delete_apply_state_schemas` (left behind by an interrupted apply), and finally
`delete_deployment`. The first four deletions wrap failures in
`CliError::DeploymentStateWriteFailed`.

`AbortResult` is a serializable summary (`deploy_id`, `schemas_dropped`,
`clusters_dropped`) printed through `log::output`; its `Display` impl renders a
single success line. Progress through the teardown is reported via the
`verbose!` macro.
