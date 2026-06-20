---
source: src/mz-deploy/src/cli/commands/list.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::list

Implements the deployments-listing command, which shows active staging
deployments (those with `promoted_at IS NULL`), analogous to `git branch`.

`run` connects with the active profile, runs `setup::verify` and
`setup::validate_connection`, then calls `deployments().list_staging_deployments`.
For each environment (sorted by name) it fetches per-cluster hydration status via
`get_deployment_hydration_status_with_lag`, passing `allowed_lag_secs` as the
lag threshold, and assembles a `ListDeployment` (deploy id, deployed-at/by, git
commit, `DeploymentKind`, schemas, and cluster statuses). The collection is
wrapped in a `ListOutput`.

`ListOutput` is `Serialize` (transparent over the vector) for JSON and has a
`Display` impl that renders each deployment with color via `owo_colors`: a
relative timestamp ("N minutes/hours/days ago"), the deployer, the kind, an
optional commit, an aggregate cluster-readiness line ("all ready" or "M of N
ready" computed from `ClusterDeploymentStatus::Ready`), and the schemas. When
there are no staging deployments it prints guidance to run `mz-deploy stage .`.
