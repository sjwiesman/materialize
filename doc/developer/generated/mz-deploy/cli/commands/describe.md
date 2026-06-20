---
source: src/mz-deploy/src/cli/commands/describe.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::describe

Implements the `describe` command, which shows detailed information about a
single deployment identified by `deploy_id`.

`run` connects with the active profile, runs `setup::verify` and
`setup::validate_connection`, then fetches deployment metadata with
`deployments().get_deployment_details` (returning `CliError::Message` if the
deployment is not found) and the object set with
`deployments().get_deployment_objects`. It assembles a `DescribeOutput`
combining the deploy id, the `DeploymentDetails`, and a `BTreeMap<ObjectId,
String>` of objects to their content hashes.

`DescribeOutput` is `Serialize` for JSON and has a `Display` impl that renders a
color-coded summary via `owo_colors`: the deployment id and `DeploymentKind`, an
optional git commit, who deployed it and when (formatted in local time), a
promoted-at line (for `DeploymentKind::Objects`) or a "staging" status when not
yet promoted, the list of schemas, and the objects with truncated 12-character
hashes.
