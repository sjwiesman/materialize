# mz-deploy Support for Cluster Autoscaling Strategies

- Associated: [Cluster Autoscaling and Background Reconfiguration](20260522_cluster_autoscaling.md)
- Associated: [mz-deploy](20260610_mz_deploy.md)

## The Problem

Materialize supports autoscaling strategies for managed clusters via the
`AUTO SCALING STRATEGY` cluster option. v1 carries a single sub-policy,
`ON HYDRATION`, which runs an extra burst replica at `HYDRATION SIZE` while
objects on the cluster are un-hydrated, lingering for an optional
`LINGER DURATION` after the steady replicas hydrate:

```sql
CREATE CLUSTER analytics (
  SIZE = '100cc',
  AUTO SCALING STRATEGY = (
    ON HYDRATION (HYDRATION SIZE = '3200cc', LINGER DURATION = '600s')
  )
);
```

`mz-deploy` manages clusters in three places, and none of them understand the
new option:

1. **Creating new clusters.** `mz-deploy clusters apply` executes the
   `CREATE CLUSTER` statement from `clusters/<name>.sql` verbatim
   (`cli/commands/clusters.rs::plan_cluster`), so a definition that includes
   `AUTO SCALING STRATEGY` happens to work on first creation. But nothing
   validates the option at compile time, and everything downstream of creation
   ignores it.

2. **Reconciling cluster configuration.** After a cluster exists,
   `plan_cluster` diffs only `SIZE` and `REPLICATION FACTOR` against
   `mz_catalog.mz_clusters` (via `project/clusters.rs::extract_size` /
   `extract_replication_factor` and `client/introspection.rs::get_cluster`).
   Adding, changing, or removing `AUTO SCALING STRATEGY` in the definition file
   is silently reported as `UpToDate` and never applied.

3. **Staging (blue/green) cluster copies.** `mz-deploy stage` clones each
   production cluster into `<name><staging_suffix>` by capturing its config
   with `introspection::get_cluster_config` and replaying it with
   `provisioning::create_cluster_with_config`. The captured `ClusterConfig`
   carries only size, replication factor, replicas, and grants, so staging
   clusters are created **without** the autoscaling policy. This is exactly
   backwards for `ON HYDRATION`: staging clusters hydrate the entire staged
   object graph from scratch, which is the situation the hydration burst
   exists to accelerate.

## Success Criteria

- A `clusters/<name>.sql` definition may declare `AUTO SCALING STRATEGY`.
  `clusters apply` creates the cluster with the policy and, on subsequent
  runs, converges a drifted live policy back to the file (add, change, and
  remove all reconcile).
- `mz-deploy stage` creates staging clusters that carry the production
  cluster's autoscaling policy, so staged hydration benefits from the burst.
- Dry-run output shows the exact `CREATE`/`ALTER` statements including the
  policy, like it does for other cluster options today.
- Invalid combinations that the server rejects (burst size equal to cluster
  size, policy on an unmanaged cluster, policy with a non-`MANUAL` schedule)
  are surfaced at `mz-deploy compile` time where possible, not first at apply.
- `mz-deploy` keeps working against regions that predate the feature.

## Out of Scope

- Managing or copying autoscaling **runtime state** (the in-flight burst
  record in `mz_cluster_auto_scaling_strategies.state`). That is owned by the
  cluster controller. `mz-deploy` only manages the configured policy.
- `mz-deploy dev` overlays. `dev` deploys onto a user-supplied existing
  cluster and never creates clusters, so no change is needed there.
- Unmanaged clusters. The server rejects `AUTO SCALING STRATEGY` on them, and
  stage's unmanaged-cluster copy path is unaffected.
- Waiting for or reasoning about burst replicas in `mz-deploy wait`. The
  burst is transparent to hydration tracking.

## Solution Proposal

### Where the live policy is read from

The configured policy is introspectable via
`mz_internal.mz_cluster_auto_scaling_strategies` (a builtin materialized view
over `mz_catalog_raw`, indexed on `cluster_id`):

- `strategy` (jsonb, marked **Unstable**): the serde JSON of the durable
  `AutoScalingStrategy`, i.e.
  `{"on_hydration": {"hydration_size": "3200cc", "linger_duration": {"secs": 600, "nanos": 0} | null}}`.
  Rows can also exist for a just-removed policy while a burst lingers, in
  which case `strategy` is JSON `null` and must be treated as "no policy".
- `state` (jsonb): the in-flight burst record. `mz-deploy` ignores it.

The JSON field names match `mz_sql::plan::AutoScalingStrategy` /
`plan::OnHydration` exactly (both mirror the durable struct), so the column
deserializes directly into the plan type. `mz-deploy` already depends on
`mz_sql` (the typecheck catalog backend implements
`CatalogCluster::auto_scaling_strategy`).

### Phase 1: model and introspection

`client/models.rs`:

- Add `auto_scaling_strategy: Option<mz_sql::plan::AutoScalingStrategy>` to
  `ClusterOptions` (the managed-cluster options bundle used by both create and
  alter paths) and to `Cluster` (the introspection row model). Reusing the
  plan type gives us `PartialEq` for drift detection and `Deserialize` for the
  jsonb column, with no new mirror type to keep in sync.

`client/introspection.rs`:

- `get_cluster`, `list_clusters`, and `get_cluster_config` gain a
  `LEFT JOIN mz_internal.mz_cluster_auto_scaling_strategies s ON s.cluster_id = c.id`
  and select `s.strategy`. Deserialize non-null, non-JSON-null values into the
  plan type. JSON `null` and missing rows both map to `None`.
- **Version tolerance.** Older regions do not have the view. Probe once per
  connection (e.g. `SELECT ... FROM mz_catalog.mz_objects WHERE name = 'mz_cluster_auto_scaling_strategies'`,
  cached on the client) and fall back to the current queries with
  `auto_scaling_strategy = None` when absent. Reconciliation then treats the
  feature as unsupported and skips policy diffing rather than failing every
  command against an older region.

### Phase 2: definition extraction and rendering

`project/clusters.rs`:

- Add `extract_auto_scaling_strategy(&CreateClusterStatement<Raw>) -> Result<Option<AutoScalingStrategy>, ...>`
  alongside `extract_size`/`extract_replication_factor`. It finds the
  `ClusterOptionName::AutoScalingStrategy` option, converts the AST value with
  the same rules as the server planner (empty block `()` normalizes to `None`,
  `HYDRATION SIZE` via `String::try_from_value`, `LINGER DURATION` via
  `Duration::try_from_value`), and reports a validation error on a malformed
  value instead of deferring to apply time.
- Prefer exposing `mz_sql`'s existing `plan_auto_scaling_strategy` /
  `unplan_auto_scaling_strategy` (currently private, pure functions in
  `plan/statement/ddl.rs`) as `pub` and calling them, rather than duplicating
  the conversion. If we keep them private, the mz-deploy copy must cite them
  as the source of truth.

Shared rendering helper (in `client` next to the SQL emitters):

- One function that renders the option clause from
  `Option<AutoScalingStrategy>` by constructing the
  `ClusterAutoScalingStrategyOptionValue` AST (the `unplan` direction) and
  using `AstDisplay`, producing
  `AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = '3200cc', LINGER DURATION = '600s'))`.
  Both the stage `CREATE CLUSTER` and the apply `ALTER CLUSTER` paths use it,
  so the emitted SQL cannot drift from the parsed form. Round-tripping the
  output through the real parser is a unit test.

### Phase 3: apply reconciliation

`cli/commands/clusters.rs::plan_cluster`:

- Compute `desired = extract_auto_scaling_strategy(&def.create_stmt)` and
  `live = existing_cluster.auto_scaling_strategy`, and compare with the
  existing size/replication-factor drift check. `Option` equality is the
  comparison: a definition that omits `LINGER DURATION` differs from a live
  policy with an explicit linger, because the durable state stores exactly
  what was configured and `None` defers to the system default.
- Drift handling:
  - `desired` is `Some`: fold `AUTO SCALING STRATEGY = (...)` into the single
    `ALTER CLUSTER ... SET (...)` statement together with `SIZE` and
    `REPLICATION FACTOR` when any of the three drift. One combined statement
    lets the server validate the final configuration as a whole (e.g. a size
    change that would collide with the burst size fails atomically instead of
    half-applying).
  - `desired` is `None` and `live` is `Some`: emit
    `ALTER CLUSTER ... RESET (AUTO SCALING STRATEGY)` as an additional
    statement (`SET` and `RESET` cannot be combined). The file is the source
    of truth, so an undeclared policy is removed, consistent with how grant
    reconciliation revokes undeclared grants. `RESET` is deliberately not
    feature-gated server-side, so this also works when the DDL gate was
    turned off after a policy was created.
  - When introspection reported the feature as unsupported (older region),
    skip policy diffing entirely.
- `ObjectAction` reporting is unchanged: any of the three dimensions drifting
  yields `Altered`, and dry-run shows the statements via the executor's
  statement log as today.
- Note: `provisioning::alter_cluster` is currently uncalled (`plan_cluster`
  builds its own SQL). Either extend it and route `plan_cluster` through it,
  or remove it. Do not leave a second, stale ALTER emitter.

### Phase 4: stage cluster copies

- `get_cluster_config` (Phase 1) now captures the policy into
  `ClusterConfig::Managed { options, .. }` via `ClusterOptions`.
- `provisioning::create_cluster` appends the rendered
  `AUTO SCALING STRATEGY = (...)` clause when `options.auto_scaling_strategy`
  is `Some`. `create_cluster_with_config` and the stage flow
  (`stage.rs::create_staging_clusters`) then copy the policy with no further
  changes, and `log_cluster_creation` verbose output mentions it.
- The runtime `state` column is never read for copying. A production cluster
  mid-burst clones as "policy configured, no burst", and the staging cluster's
  own controller starts a burst if staging hydration warrants one.
- Failure mode: if the `enable_auto_scaling_strategy` DDL gate is off in the
  target environment while the production cluster still carries a policy, the
  staging `CREATE CLUSTER` fails. Surface the server error as-is. Silently
  dropping the policy would create a staging cluster that hydrates at a
  different effective capacity than production.

### Phase 5: compile-time validation

Mirror the server's cross-option checks in cluster file validation
(`project/clusters.rs::classify_cluster_statements` or a sibling), so
`mz-deploy compile` rejects definitions that apply would reject:

- `HYDRATION SIZE` equal to the cluster `SIZE` (a no-op burst).
- `AUTO SCALING STRATEGY` together with `REPLICAS` (unmanaged) or with a
  non-`MANUAL` `SCHEDULE`.
- A malformed option value (from Phase 2 extraction).

These duplicate server checks by design (fast feedback in CI without a
region), the same trade-off the existing cluster validations make.

### Phase 6: docs and tests

Docs:

- `cli/help/apply-clusters.md`: document that `AUTO SCALING STRATEGY` is
  reconciled and that omitting it resets a live policy.
- Stage help: document that staging clusters inherit the production policy.

Unit tests:

- `project/clusters.rs`: extraction for present, absent, empty-block, and
  no-linger variants, plus the new compile-time validations.
- `client` tests: jsonb fixture (matching the `mz_catalog_raw` serde shape,
  including `linger_duration: null` and `strategy: null`) deserializes
  correctly, and the rendering helper's output re-parses to an equal AST.
- Drift matrix in `plan_cluster` planning: {file none, file some} x
  {live none, live equal, live different} produce the expected statement
  sets (nothing, combined `SET`, `RESET`).

Integration tests (`test/mz-deploy/mzcompose.py`):

- Enable the `enable_auto_scaling_strategy` system parameter in the harness.
- New project fixture with a policy-bearing cluster covering the lifecycle:
  fresh `clusters apply` creates it (assert via
  `mz_internal.mz_cluster_auto_scaling_strategies`), re-apply is `UpToDate`,
  editing the policy alters, deleting the option resets.
- Stage scenario: stage a project whose objects live on a policy-bearing
  cluster and assert the `<name><suffix>` staging cluster has an identical
  `strategy` row. Promote and abort flows are unchanged but exercised.

## Alternatives

- **String-compare `SHOW CREATE CLUSTER` instead of structured diffing.**
  Simpler introspection, but brittle against formatting and option-order
  changes, and it cannot distinguish which dimension drifted for combined
  versus reset statements. The existing size/rf reconciliation is structured,
  so the policy follows suit.
- **A dedicated mz-deploy mirror type instead of `mz_sql::plan::AutoScalingStrategy`.**
  Decouples mz-deploy from `mz_sql` internals, but mz-deploy already depends
  on `mz_sql` for typechecking, and a mirror type is one more thing to keep in
  sync with the serde shape of the unstable jsonb column.
- **Leave undeclared policies alone instead of resetting.** Safer for users
  who set policies out-of-band, but breaks the declarative model and diverges
  from grant reconciliation. Users who want "no opinion" can keep the option
  out of scope by not adopting it in files, but once any run of
  `clusters apply` manages a cluster, the file is authoritative. Flagged as
  the main behavioral decision below.

## Open Questions

1. **Absence semantics.** Is "option absent in file resets the live policy"
   acceptable for existing projects, or does it need a release-note callout
   (first `clusters apply` after upgrading mz-deploy could strip manually
   configured policies)? The alternative is requiring an explicit
   `AUTO SCALING STRATEGY = ()` to reset, treating absence as "unmanaged".
2. **Exposing `plan_auto_scaling_strategy`/`unplan_auto_scaling_strategy`
   from `mz_sql`.** Preferred over duplication, but it widens the `mz_sql`
   public surface. Needs a quick owner sign-off.
3. **Profile variants.** Nothing new is needed (`clusters/<name>#<profile>.sql`
   already selects per-profile definitions), but do we want the docs to
   recommend smaller or absent burst sizes in staging profiles?
