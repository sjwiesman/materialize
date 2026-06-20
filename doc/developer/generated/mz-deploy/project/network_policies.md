---
source: src/mz-deploy/src/project/network_policies.rs
revision: a647094cc4
---

# mz_deploy::project::network_policies

Loads and validates network policy definitions from the optional
`<root>/network-policies/` directory. Each `.sql` file defines one policy via a
required `CREATE NETWORK POLICY` statement plus optional `GRANT` and `COMMENT`
statements.

`NetworkPolicyDefinition` holds the policy `name`, the `create_stmt`, and the
`grants` and `comments` targeting it. `load_network_policies` returns an empty vec
when the directory is absent; otherwise it collects file variants via
`collect_all_sql_files`, validates every variant independently, resolves the
active variant per profile (override match, falling back to default), and
accumulates failures into `ValidationErrors`.

`classify_network_policy_statements` sorts the parsed `LocatedStatement`s and
emits offset-positioned `ValidationError`s: a GRANT must be an
`ObjectType::NetworkPolicy` grant targeting this policy; a COMMENT must be a
`CommentObjectType::NetworkPolicy` targeting this policy; any other statement
raises `InvalidNetworkPolicyStatement`. Target checks are case-insensitive. The
file must contain exactly one `CREATE NETWORK POLICY` whose name matches the
filename, or the matching missing/multiple/name-mismatch error is raised.
