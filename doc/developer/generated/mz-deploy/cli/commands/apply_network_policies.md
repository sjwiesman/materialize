---
source: src/mz-deploy/src/cli/commands/apply_network_policies.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::apply_network_policies

Converges live network-policy state to match the project's definitions. Unlike
the other database-object apply phases, network policies are loaded directly from
the project's network-policy files rather than the compiled object graph.

`plan(settings, client, executor)` loads definitions via
`network_policies::load_network_policies` (using the profile name and project
variables) and returns an empty `"network_policies"` `ApplyResult` when there are
none. Otherwise it plans each `NetworkPolicyDefinition` through
`plan_network_policy` and collects the per-policy `ObjectResult`s.

`plan_network_policy` drains any prior executor statements, then checks
`introspection().network_policy_exists`. If the policy exists it issues an
`AlterNetworkPolicyStatement` carrying the definition's name and options
(`ObjectAction::Altered`); otherwise it executes the definition's `create_stmt`
(`ObjectAction::Created`). It then reconciles grants via
`grants::reconcile_named_object` with `GrantNamedObjectKind::NetworkPolicy`, runs
the definition's comment statements, and returns an `ObjectResult` with the
collected statements.

`run(settings, dry_run)` connects with `connect_apply_client`, builds an
`ApplyPlan`, adds the planned phase, and executes it unless `dry_run` is set,
returning the plan.
