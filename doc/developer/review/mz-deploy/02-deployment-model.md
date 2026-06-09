# mz-deploy review: the deployment model

This is the highest-risk subsystem in the PR: it issues DDL — including `DROP
... CASCADE` — against production Materialize regions. Reviewers of
`src/mz-deploy/src/client/` and the `stage`/`promote`/`wait`/`abort` commands
should read this first.

All line numbers reference the branch head (`origin/mz-deploy`,
commit `1878d7c5d8`).

## Blue/green in one paragraph

`mz-deploy stage` compiles the project, diffs it against the live environment,
and creates a *parallel* copy of every changed schema and cluster under a
suffixed name (`public_<deploy_id>`, `compute_<deploy_id>`). `mz-deploy wait`
monitors hydration of the staging clusters. `mz-deploy promote` then performs
an atomic cutover using `ALTER SCHEMA ... SWAP` / `ALTER CLUSTER ... SWAP`
inside a single transaction, runs post-swap steps (sinks, replacement MVs),
and drops the now-stale suffixed schemas/clusters `CASCADE`.

## Lifecycle walkthrough

### 1. `stage` (`cli/commands/stage.rs`, 2,033 lines)

- `run()` (line 186) → `analyze_project_changes()` (line 325): diffs the
  compiled project against a production snapshot and classifies each object.
- `record_stage_metadata()` (line 574): writes deployment metadata to the
  `_mz_deploy` tracking database (created by `mz-deploy setup`):
  - `_mz_deploy.deployments` — one row per schema deployment (append-only)
  - `_mz_deploy.objects` — semantic hash of every object at deploy time
  - `_mz_deploy.clusters` — staging↔production cluster mapping
  - `_mz_deploy.pending_statements` — sinks queued for post-swap creation
  - `_mz_deploy.replacement_mvs` — queued `APPLY REPLACEMENT` targets
- `create_resources_with_rollback()` (line 715): creates suffixed
  databases/schemas (line 773), clones production cluster configs into
  staging clusters (line 820), deploys views/MVs/indexes into the staging
  schemas. **Tables, sources, secrets, and connections are not staged** —
  they are managed by the separate `apply` command family.
- On failure, best-effort rollback (lines 1071–1148) drops staging
  schemas/clusters and deletes the metadata rows; it continues past partial
  failures (`--no-rollback` disables this).

### 2. `wait` (`cli/commands/wait.rs`, 661 lines)

- One-shot mode queries the `_mz_deploy.deployment_hydration_status` view;
  continuous mode `SUBSCRIBE`s to it and renders a live dashboard.
- Hydration state machine (`client/deployment_ops.rs:18-34`):
  `Hydrating` → `Ready` (all hydrated and lag ≤ threshold, default 300 s) |
  `Lagging` (lag above threshold) | `Failing` (no replicas, or replicas
  OOM-looping).

### 3. `promote` (`cli/commands/promote.rs`, 1,085 lines)

- `generate_deployment_plan()` (line 370) reads all deployment state once.
- `execute_atomic_swap()` (line 713) wraps the cutover in one transaction
  (`BEGIN` at line 723, `COMMIT` at line 786, `ROLLBACK` on each error path):
  1. Create marker schemas `_mz_deploy.apply_<id>_pre` (comment
     `swapped=false`) and `apply_<id>_post` (comment `swapped=true`).
  2. `ALTER SCHEMA <prod> SWAP WITH <staging>` for each schema (line 735).
  3. `ALTER CLUSTER <prod> SWAP WITH <staging>` for each cluster (line 750).
  4. Swap the two marker schemas (line 768) — atomically flipping the
     `swapped=` comment that records whether the cutover happened.
- `run_post_swap_steps()` (line 511) — **outside the transaction**:
  - `execute_pending_sinks()` (line 801): create sinks (they cannot run
    against staging).
  - `apply_replacement_mvs()` (line 888): `ALTER MATERIALIZED VIEW ... APPLY
    REPLACEMENT`, then `DROP SCHEMA IF EXISTS ... CASCADE` of the
    replacement schema (line 929).
  - `repoint_dependent_sinks()` (line 948): `ALTER SINK ... SET FROM` for
    sinks that read from swapped objects.
- Cleanup drops old (now-suffixed) schemas and clusters `CASCADE`
  (lines 1062, 1078) and removes the marker schemas.

### 4. Crash recovery

The marker-schema comment is the persistent state machine
(`client/deployment_ops.rs:1052-1145`):

| Observation | State | Resumption |
|---|---|---|
| no marker schemas | `NotStarted` | promote from scratch |
| `_pre` has `swapped=false` | `PreSwap` | redo swap transaction |
| `_pre` has `swapped=true` | `PostSwap` | redo only post-swap steps |

Re-running `promote <deploy_id>` resumes from the recorded state. There is no
automatic retry; resumption is operator-driven.

## Concurrency model

- **No deployment lock.** Concurrent stages coexist via distinct suffixes.
- **Conflict detection, not locking:** `check_deployment_conflicts()`
  (`client/validation.rs`) rejects promotion if any target schema was
  promoted *after* this deployment was staged — git-merge-style.
  `promote --force` bypasses this check.
- `wait --allowed-lag` / `promote --no-ready-check` control the hydration
  gate; `--no-ready-check` promotes without verifying staging is caught up.

## SQL generation

- Metadata reads/writes use parameterized queries (`$1, $2, ...`).
- DDL (swap, drop, create schema/cluster) is built with `format!()` and
  manual double-quoting; `quote_identifier()` (`client.rs:55`) escapes
  embedded quotes. Identifier sources are catalog introspection, validated
  project files, and generated deploy IDs — not raw user strings — but every
  `format!`-built DDL site deserves a quoting review (see risk register R1).

## Grants (`cli/commands/grants.rs`, 762 lines)

Three-step idempotent reconciliation (line 139): apply desired `GRANT`s →
read live grants from `mz_catalog` → `REVOKE` stale grants, excluding
default-privilege grants. Grants are *not* transactional with object
creation; failures fail forward.

## What the executor is (`cli/executor.rs`, 671 lines)

Not a DAG scheduler. It accumulates SQL per "phase" (clusters → roles →
network policies → secrets → connections → sources → tables, ordered by the
caller), batches objects sharing a `transaction_group` into one
`BEGIN`/`COMMIT`, and supports dry-run by collecting statements without
executing. Secret-bearing statements are carried separately as
`redacted_statements` so they are never printed or serialized.

## Questions reviewers should answer

1. **Swap transaction completeness** (`promote.rs:713-795`): is every
   statement between `BEGIN`/`COMMIT` guaranteed to be in the same session?
   What happens if the connection drops between `COMMIT` and the first
   post-swap step?
2. **Post-swap non-atomicity**: sink creation, `APPLY REPLACEMENT`, and sink
   repointing run outside the transaction. If `repoint_dependent_sinks()`
   fails, does the subsequent `DROP ... CASCADE` of old schemas destroy the
   sinks that were never repointed? (The code logs a warning and continues —
   verify this is acceptable.)
3. **CASCADE blast radius**: enumerate exactly what can hang off the dropped
   suffixed schemas/clusters at drop time (`promote.rs:1062,1078`,
   `stage.rs` rollback path). Anything a user created manually in a staging
   schema dies silently.
4. **Conflict-detection window**: staging snapshot → promote is a long
   window; the timestamp comparison in `check_deployment_conflicts()` is the
   only guard. Are catalog timestamps it relies on monotonic and reliable?
5. **Marker-schema race**: two operators running `promote <same id>`
   concurrently — both can pass `get_apply_state()` before either swaps.
   Idempotency of the individual steps is the only defense; verify each step
   actually is idempotent.
6. **Stable-schema limitation** (`stage.rs:357-381`): adding new objects to
   an existing stable-API schema is rejected by
   `validate_no_new_objects_in_existing_stable_schemas()` because the swap
   would lose them. Confirm the validation is airtight, since the failure
   mode is data loss.
