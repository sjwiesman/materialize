---
source: src/mz-deploy/src/client/deployment_ops.rs
revision: 592cecbfd1
---

# mz_deploy::client::deployment_ops

Deployment-tracking operations backing `DeploymentsClient` and
`DeploymentsClientMut`. Reads and writes the `_mz_deploy` metadata tables and
views and queries the catalog for hydration state.

Record management functions (insert-only where noted) cover the lifecycle:
`insert_schema_deployments`, `append_deployment_objects`,
`insert_deployment_clusters` (resolves cluster names to ids, erroring if any are
missing), `get_deployment_clusters`, `validate_deployment_clusters`,
`delete_deployment_clusters`, `update_promoted_at`, `delete_deployment`, and the
read paths `get_schema_deployments`, `get_deployment_objects` (returns a
`DeploymentSnapshot`), `get_deployment_metadata`, `get_deployment_details`,
`list_staging_deployments`, `list_deployment_history`,
`check_deployment_conflicts`, `list_production_clusters` (walks the catalog so
the `dev` guard works as `materialize_developer`), and `deployment_table_exists`.
Pending-statement and replacement-MV tracking is handled by
`insert_pending_statements`, `get_pending_statements`, `mark_statement_executed`,
`delete_pending_statements`, `insert_replacement_mvs`, `get_replacement_mvs`, and
`delete_replacement_mvs`. The thin `DeploymentsClient` impl block exposes these
as methods, including `validate_staging` which checks a deployment exists and is
unpromoted.

Hydration status is the second concern. `FailureReason` (`NoReplicas`,
`AllReplicasProblematic`), `ClusterDeploymentStatus` (`Ready`, `Hydrating`,
`Lagging`, `Failing`), `ClusterStatusContext`, and `HydrationStatusUpdate`
describe per-cluster state. `hydration_status_query` builds the shared CTE SQL
(problematic-replica detection via repeated OOM kills, hydration counts,
wallclock lag, with a configurable `DEFAULT_ALLOWED_LAG_SECS` of 300) and is
careful to use `LIKE $1 ESCAPE '\'` on the staging-suffix pattern.
`get_deployment_hydration_status` runs it as a one-shot SELECT;
`DeploymentsClientMut::subscribe_deployment_hydration` runs the same query under
a `SUBSCRIBE` cursor in a transaction, filtering out retractions
(`mz_diff == -1`) and yielding `HydrationStatusUpdate`s as a stream.

Apply (cutover) state uses a pair of `_mz_deploy` schemas, `apply_<id>_pre` and
`apply_<id>_post`, whose `swapped=` comments are exchanged during the atomic swap.
`create_apply_state_schemas`, `get_apply_state` (returning `ApplyState`:
`NotStarted`/`PreSwap`/`PostSwap`), and `delete_apply_state_schemas` manage this
crash-recoverable marker.
