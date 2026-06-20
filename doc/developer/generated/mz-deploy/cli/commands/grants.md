---
source: src/mz-deploy/src/cli/commands/grants.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::grants

Shared helpers for grant reconciliation used across the `apply` commands. The
core model: apply all desired `GRANT`s idempotently, query the live grant state
and the default-privilege ("protected") grants, then `REVOKE` everything that is
current but neither desired nor protected.

`GrantObjectKind` (Table, Source, Secret, Connection) and
`GrantNamedObjectKind` (Cluster, NetworkPolicy) each encapsulate the per-type
specifics: catalog table name, SQL object type, the privilege set that `ALL`
expands to, the display label, and how to build a `GrantTargetSpecification` for
a given object. `reconcile` (for schema-qualified database objects) and
`reconcile_named_object` (for clusters / network policies) drive the three-step
algorithm, sourcing live and default grants through `client.introspection()`.

Pure helpers do the set math: `desired_grants` extracts `(grantee, privilege)`
pairs from parsed `GRANT` statements (expanding `ALL`, lowercasing grantees);
`parse_privilege` maps privilege-type strings to the `Privilege` enum, returning
`None` (and skipping) for types unknown to this build so the CLI tolerates newer
servers; `stale_grant_revocations` computes the 3-way difference (current −
desired − protected), comparing case-insensitively, and emits
`RevokePrivilegesStatement`s; `execute_revocations` runs them, printing per-grant
status outside dry-run. A large unit-test module covers `desired_grants` and
`stale_grant_revocations` across object types and edge cases.
