---
source: src/mz-deploy/src/cli/commands/log.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::log

Implements the deployment-history command, which shows promoted deployments in
reverse chronological order, analogous to `git log`.

`run` connects with the active profile, runs `setup::verify` and
`setup::validate_connection`, then calls `deployments().list_deployment_history`
with an optional `limit`, producing a `HistoryOutput` over
`DeploymentHistoryEntry` values. With JSON output enabled it serializes directly;
when empty it prints guidance to run `stage` and `apply --staging-env`;
otherwise it formats the entries and, if stderr is a TTY, pipes them through a
pager via `display_with_pager`.

`HistoryOutput` is `Serialize` (transparent over the vector) and has a `Display`
impl that renders each entry with color via `owo_colors`: the deploy id and
`DeploymentKind`, optional commit, promoter, the promotion timestamp in local
time, and the included schemas. `display_with_pager` spawns `less -RFX` and pipes
the formatted content to it, falling back to printing directly if `less` is
unavailable.
