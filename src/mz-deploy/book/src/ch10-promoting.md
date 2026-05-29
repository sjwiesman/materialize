# Promoting

*What you'll learn: how `promote` atomically swaps a staging deployment into production — and what happens when two deployments race.*

## The atomic swap

`mz-deploy promote <DEPLOY_ID>` cuts over a staging deployment to production in a single database transaction. Under the hood it issues `ALTER ... SWAP` statements for every schema and cluster that changed. Production and staging exchange names in one shot: `public` becomes `public_abc123`, and `public_abc123` becomes `public`. From the perspective of any query running against the database, the switch happens between statements — there is no window where both the old and new objects exist under the same name at the same time.

Because the swap is a single transaction, it either completes entirely or not at all. If the transaction fails before committing, production is untouched. Once it commits, production is running your new code.

```bash
mz-deploy promote abc123
```

A dry run lets you inspect exactly what would be swapped before you commit:

```bash
mz-deploy promote abc123 --dry-run
mz-deploy promote abc123 --dry-run --output json  # Machine-readable plan for CI
```

## What running queries see

Because the swap commits as a single transaction, there is no window where a client can observe names that are partially old and partially new. Between any two SQL statements, a client resolves object names against exactly one schema generation — either the old production schemas or the new ones.

Queries that started before the swap committed finish against the schema version they began on. Materialize holds the query's snapshot stable for its duration, so a long-running query is not interrupted mid-flight by the swap. Queries that start after the swap has committed resolve against the new schemas.

Long-running `SUBSCRIBE` cursors are a different case. A subscription holds an open cursor against a specific object. If the object that cursor points to is dropped during the swap — because the schema it lives in was swapped out — the cursor will terminate with an error at the swap boundary. Clients should be prepared to reconnect and re-establish their subscription against the new schema. This is expected behavior, not a failure of the promotion itself; it is the cost of naming continuity across a schema swap.

For most applications running short-lived queries, promote is invisible: one statement resolves old names, the next resolves new ones, and users never notice. The gap between the two is the duration of the swap transaction, typically under a second.

Most queries see nothing more than the boundary between two statements. Subscriptions across the boundary must reconnect.

## Readiness checks

Before executing the swap, `promote` verifies that every staging cluster is ready. A cluster is considered ready when:

- All materialized views on that cluster are fully hydrated (their initial computation has completed).
- Wallclock lag is within the `--allowed-lag` threshold (default: 300 seconds).
- At least one healthy replica exists and is not OOM-looping.

If any cluster fails this check, `promote` exits without touching production.

The standard pre-promote workflow is to run `mz-deploy wait` first and let it block until all clusters reach the ready state:

```bash
mz-deploy wait abc123       # Live progress dashboard — blocks until ready
mz-deploy promote abc123    # Safe to run once wait exits 0
```

`mz-deploy wait` displays a per-cluster progress view that updates in real time as materialized views hydrate. Once every cluster is ready, it exits with code 0. You can also use `--once` for a non-blocking snapshot check:

```bash
mz-deploy wait abc123 --once   # Check current state and exit immediately
```

Hydration time depends on how much historical data each materialized view needs to process. A brand-new deployment on a large dataset can take minutes to hours. A deployment that changed one MV in a hot path may be ready in seconds.

To adjust the lag tolerance for your environment, pass `--allowed-lag` to both commands with the same value:

```bash
mz-deploy wait abc123 --allowed-lag 60
mz-deploy promote abc123 --allowed-lag 60
```

If you need to promote without waiting — for example, in a rollback where speed matters more than lag — skip the check entirely:

```bash
mz-deploy promote abc123 --no-ready-check
```

## Post-swap work

After the atomic swap commits, `promote` continues with several cleanup and finalization steps. These run after production traffic has already switched over.

**Deferred sink creation.** Sinks are held back during `stage` because they should not start producing output until the deployment is live. Immediately after the swap, `promote` creates them against the newly-promoted production schemas.

**Replacement MV application.** For schemas marked with `SET api = stable`, changed materialized views are not part of the schema swap. Instead, `promote` applies each one in sequence using `ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT`. This updates the MV's computation in place while preserving its identity, so downstream consumers outside your project do not see a disruption. The application order follows the dependency graph — a replacement MV is always applied before any replacement MVs that depend on it. Chapter 11 covers the stable-API path in full.

**Sink repointing.** Sinks that depended on old production objects are updated to point at the new production schemas after the swap.

**Promotion timestamp.** `promote` records a timestamp marking this deployment as complete. This timestamp becomes the new baseline for the next `stage` diff.

**Old resource cleanup.** The schemas and clusters that were in production before the swap are now sitting under staging names (for example, the old `public` schema is now named `public_abc123`). Once all post-swap work is done, `promote` drops them.

## Resumability

If `promote` crashes or is interrupted after the atomic swap but before cleanup finishes, re-running the same command is safe:

```bash
mz-deploy promote abc123   # Re-run after an interruption
```

`promote` detects that the swap already completed — the staging deployment is in a post-swap state — and skips directly to the post-swap steps. It will not attempt a second swap. Deferred sinks that were not yet created are created; resources that were not yet dropped are dropped.

This means that even if your CI runner dies mid-promote, you do not need to manually inspect or repair the database. Re-run the same promote command and it finishes the job.

## `--force`

By default, `promote` checks whether any production schema or cluster was modified by another deployment after your staging deployment was created. If one was, it exits with a conflict error rather than overwriting someone else's work.

`--force` bypasses this check:

```bash
mz-deploy promote abc123 --force
```

When you run with `--force`, the entire affected production schema is replaced by whatever your staging deployment contains. If another deployment promoted changes to that schema after you staged, those changes are gone. Because schemas are swapped wholesale — not merged — there is no partial overlap. Use `--force` only when you have confirmed that clobbering the other changes is intentional.

## Concurrent deployments and promote conflicts

Multiple staging deployments can coexist and run in parallel. Each one has its own suffixed schemas and clusters (`public_abc123`, `public_def456`), so they do not interfere with each other during the staging phase.

The constraint comes at promote time. Two deployments "overlap" when they touch any of the same schemas or clusters. Overlap is determined at the schema level, not the object level — if deployment A changes `customer` in `public` and deployment B changes `customer_summary` in `public`, both touch the `public` schema and they overlap, even though they modified different objects.

When two overlapping deployments race to promote, the first one to commit wins. The second promote will fail with a conflict error because production was modified after the second deployment was staged:

```
Deployment conflict: production schema "public" was modified after this
deployment was staged. Re-stage to pick up the latest production state,
or use --force to override.
```

The safe path after a conflict is to re-stage:

```bash
mz-deploy abort def456        # Clean up the stale staging deployment
mz-deploy stage               # Re-stage against the updated production state
mz-deploy wait <new-id>       # Wait for hydration
mz-deploy promote <new-id>    # Promote the fresh deployment
```

Re-staging diffs your project against the current production state — which now includes the first deployment's changes — and rebuilds a staging environment that accounts for both sets of changes. The new staging deployment will not conflict with anything because it is based on the latest snapshot.

Deployments that touch entirely different schemas and clusters can be promoted in any order without conflicts. If your team works in well-separated schema boundaries, concurrent deployments are common and routine.

### Minimizing conflicts through schema ownership

The first-promote-wins rule is mechanically correct, but the structural question is how to design your project's schema boundaries so that conflicts are rare in the first place.

**Coarse-grained schemas** keep most objects in a small number of large schemas. The advantage is lower cognitive overhead: fewer schema names to track, fewer cross-schema references, less `SET api = stable` surface to maintain. The disadvantage is that any change to any object in a schema creates a conflict for every other in-flight deployment that also touches that schema. When a single team ships sequentially this is fine. When multiple teams or pipelines ship concurrently, conflicts become routine and the re-stage tax accumulates.

**Fine-grained schemas** assign a small, focused set of objects to each schema — one schema per logical surface, one per team, or one per independent pipeline. Deployments are unlikely to overlap because each one touches a narrowly scoped schema. The costs are more project files, more explicit cross-schema references, and more `SET api = stable` boundaries to reason about.

The principle: keep schemas coarse where a single team or pipeline owns everything; introduce finer-grained schemas at the boundaries where independent teams or pipelines need to ship without coordinating. Publish the contract between them as a `SET api = stable` schema so that downstream consumers are insulated from the internal churn.

If two teams find themselves regularly stomping each other's deploys, the structural fix is more schemas, not better timing.

## Aborting after a successful promote

You cannot undo a promotion with `abort`. `abort` only operates on staging deployments that have not yet been promoted. Once `promote` completes successfully and the old production resources are dropped, there is nothing left to abort.

To revert a promotion, treat it as a new deployment:

```bash
git revert <commit>           # Undo the change in version control
mz-deploy stage               # Stage the reverted project
mz-deploy promote <new-id>    # Promote the rollback
```

Because `promote` uses the same atomic `ALTER ... SWAP` mechanism, the rollback promotion is itself atomic. Production traffic switches back in a single transaction.

---

You can now:

- Promote a staging deployment to production.
- Wait for hydration before promoting.
- Understand resumability semantics if `promote` is interrupted.
- Handle a promote conflict between two concurrent deployments.
