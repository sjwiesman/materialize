---
source: src/mz-deploy/src/cli/commands/wait.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::wait

Implements `mz-deploy wait`, which watches a staging deployment's clusters until
they are hydrated. `run(settings, deploy_id, once, timeout, allowed_lag_secs)`
connects, validates the deployment is staged-and-not-promoted, and dispatches to
one of four modes based on `once` and whether JSON output is enabled: snapshot
(human or JSON) or continuous (live dashboard or NDJSON).

Snapshot mode (`run_snapshot` / `run_snapshot_json`) calls
`get_deployment_hydration_status_with_lag` once. `run_snapshot` prints each
cluster via `print_cluster_status` plus a `print_summary` footer, and returns
`CliError::DeploymentFailing` if any cluster is failing, `CliError::Clusters
Hydrating` if any is not ready, or `Ok(())` once all are ready. `run_snapshot_json`
emits a single JSON document with the same exit semantics.

Continuous mode (`run_continuous` / `run_continuous_json`) takes an initial
status snapshot, then subscribes via `deployments_mut().subscribe_deployment_
hydration`, applying an optional `tokio::time::timeout` that surfaces
`CliError::ReadyTimeout`. `monitor_hydration_live` maintains a per-cluster
`ClusterStatusContext` map and re-renders an in-place terminal dashboard
(using crossterm cursor/clear control and `render_dashboard`) on each
`HydrationStatusUpdate`, returning once every cluster is `Ready`.
`monitor_hydration_ndjson` does the same but emits one NDJSON line per update.

Rendering helpers: `print_cluster_status` shows a per-cluster progress bar and
status line keyed on `ClusterDeploymentStatus` (Ready / Hydrating / Lagging /
Failing); `render_progress_bar` builds the Unicode bar; `print_summary` tallies
counts by status; `render_dashboard` composes the header, per-cluster blocks, and
summary; `format_duration` formats elapsed time; and `update_to_status` maps a
`HydrationStatusUpdate` (including `FailureReason`) into a
`ClusterDeploymentStatus`.
