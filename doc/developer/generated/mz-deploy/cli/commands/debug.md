---
source: src/mz-deploy/src/cli/commands/debug.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::debug

Implements the `debug` command, which probes the active profile's environment
and reports a health summary.

`run` checks local Docker availability via `DockerRuntime::check_availability`
(mapped to `running` / `not_running` / `not_installed`), then attempts a
connection with `Client::connect_with_profile`. On success it concurrently
(`tokio::join!`) queries `mz_environment_id()` through `query_session_info` and
the server cluster health through `check_server_cluster`, producing a
`RemoteOutput::Success`; on connection failure it produces `RemoteOutput::Failure`
carrying the profile host and port.

`check_server_cluster` looks up `SERVER_CLUSTER_NAME` and classifies it into the
`ServerClusterHealth` enum: `Healthy` (replication factor > 0), `NotReady` (e.g.
replication factor 0), or `Missing`. The aggregate `DebugOutput` (profile name,
Docker status, remote result) is `Serialize` for JSON and has a `Display` impl
that prints color-coded lines using `owo_colors`, including a "run `mz-deploy
setup`" hint when the server cluster is missing or not ready.
