# Stateless External Autoscaler

- Associated: (TBD, link epics/issues)
- Companion to: [Reactive Cluster Autoscaling](20261114_reactive_cluster_autoscaling.md)

## The Problem

[Reactive Cluster Autoscaling](20261114_reactive_cluster_autoscaling.md)
proposes a load-driven strategy inside the cluster controller. It needs new
replica instrumentation, a new controller signal, catalog state, and SQL
syntax before it can make its first decision, and its defaults cannot be
calibrated until it runs against real workloads.

Almost everything that design's decision needs is already queryable over SQL.
A process outside `environmentd` can read it, apply the same model, and issue
`ALTER CLUSTER ... SET (SIZE)`, which is already durable, graceful, and rolled
back on failure. That process can ship without changes to Materialize, can run
against any environment including self-managed ones, and gives us the data to
validate the model before we build it in.

This document specifies that process. It reuses the demand model from the
companion design and does not re-derive it. What is specific here is how to
get the inputs from outside, and how to make safe decisions while holding no
state between runs.

## Success Criteria

- The autoscaler runs against an unmodified Materialize environment using only
  a SQL connection.
- It holds no state between runs. Killing it at any point, running it on a
  different machine, or running two copies at once is safe.
- Its entire configuration is a connection and a list of clusters with a
  minimum size, a maximum size, and a cooldown period.
- It never adds load to a replica that is hydrating.
- A replica that is too busy to answer is handled as evidence of overload and
  not as an error.
- Every run reports, per cluster, the inputs it read and the decision it made
  or declined to make.
- The SASO goals of the companion design hold: a stable workload produces no
  size changes, and a changed workload converges in a few steps.

## Out of Scope

- **Sources and sinks.** The busy-time read uses compute introspection.
  Clusters that only run storage objects are scaled on memory, lag, and
  out-of-memory kills, without a CPU estimate.
- **Replication factor, scale to zero, and predictive scaling.** As in the
  companion design.
- **Unmanaged clusters and clusters with `SCHEDULE = ON REFRESH`.** Skipped
  with a warning.
- **A long-running service with its own storage.** Statelessness is the point.

## Solution Proposal

### Summary

A small program that performs one **run** and exits. A run reads catalog
relations for every configured cluster, takes a short busy-time sample from
each hydrated replica by targeting it, computes a recommended size with the
[demand model](20261114_reactive_cluster_autoscaling.md#the-demand-model), and
issues at most one `ALTER CLUSTER` per cluster. It is invoked on a schedule by
whatever the operator already has (a Kubernetes `CronJob`, cron, a workflow
engine). A `--loop` flag repeats runs in one process for convenience and
carries nothing from one run to the next.

Everything a stateful controller would remember is read back from Materialize
each run: load history from the metrics and lag history relations, the time
and outcome of the last size change from the audit log and
`mz_cluster_reconfigurations`.

### Configuration

```yaml
connection:
  host: example.materialize.cloud
  port: 6875
  user: autoscaler
  password_file: /var/run/secrets/mz-password   # or MZ_PASSWORD
  sslmode: require
  database: materialize

defaults:
  cooldown: 1h
  sample_interval: 30s        # gap between the two busy-time reads
  replica_read_timeout: 10s   # statement timeout for targeted reads

clusters:
  - name: analytics
    min_size: 100cc
    max_size: 800cc
    cooldown: 2h
  - name: serving
    min_size: 200cc
    max_size: 400cc

dry_run: false
```

- The connection block is ordinary pgwire configuration. The password comes
  from a file or the environment, never from the config file itself.
- Each cluster entry names a managed cluster and gives `min_size`, `max_size`,
  and optionally `cooldown`. Clusters not listed are never touched.
- Configuration is validated at startup against `mz_cluster_replica_sizes`:
  both sizes exist, are enabled, and `min_size` is no larger than `max_size`.
  The relation lists enabled sizes only, so existing in it is being enabled.
  A cluster whose current size lies outside its range is moved into the range
  by the normal decision path.
- The ladder between the two bounds is the set of sizes of the same family.
  The catalog does not expose a size family, so the family is taken from the
  name: sizes whose names differ only in their numbers, such as `100cc` to
  `800cc`. For sizes that do not follow a pattern, an optional `sizes` list on
  the cluster entry gives the ladder explicitly. Bounds from different
  families without that list are rejected at startup.
- The model parameters (`u_target`, `m_target`, the progress-ratio clamp) are
  flags with the companion design's defaults. They are not per-cluster.
- `dry_run` computes and reports every decision and issues no `ALTER`.

The role needs `SELECT` on the system relations used below, `USAGE` on each
listed cluster for the targeted reads, and the right to alter each listed
cluster. Run it as a dedicated role so its actions are distinguishable in
`mz_audit_events`.

### A run

For each configured cluster, in order:

1. **Catalog read.** From a session on `mz_catalog_server`, read the cluster's
   size and replicas, any in-flight or most recent reconfiguration, per-replica
   hydration and status, and the metrics and lag history over the cooldown
   window. This touches no user cluster.
2. **Gate.** Decide from the catalog read alone whether the cluster is
   eligible for a decision and which replicas may be sampled.
3. **Replica read.** For each hydrated replica, take a busy-time sample by
   targeting the replica.
4. **Decide.** Compute a recommendation and apply the decision rules.
5. **Act.** Issue `ALTER CLUSTER` if a rule fired and `dry_run` is off.
6. **Report.** Emit one structured record with the inputs and the outcome.

Clusters are independent. A failure on one is reported and does not stop the
run for the others.

### Gating

The catalog read sorts each cluster into one state. Only `eligible` proceeds
to the replica read.

| State | Condition | Behavior |
|---|---|---|
| `reconfiguring` | `mz_cluster_reconfigurations.status` is in progress | Do nothing. One transition at a time. |
| `oom` | A realized-size replica has an `oom-killed` status inside the window and after the last settled reconfiguration | Scale up now. Skips cooldown and the replica read. |
| `cooling-down` | Last settled reconfiguration, successful or failed, is younger than `cooldown` | Do nothing. |
| `hydrating` | No realized-size replica is fully hydrated | Do nothing. |
| `eligible` | Otherwise | Sample hydrated replicas. |

A replica counts as hydrated when every index and materialized view on the
cluster reports `hydrated` for it in `mz_hydration_statuses`, and every source
and sink reports `hydrated` for some replica of the cluster. Sources and sinks
run on one replica of a multi-replica cluster and never report for the others.
Hydration is evaluated per replica, so in a cluster with one hydrated and one
hydrating replica only the first is sampled.

An out-of-memory kill only counts when it is newer than the last settled
reconfiguration. An older kill was either answered by that reconfiguration or
could not be, and acting on it again would bypass the cooldown on every run
until it leaves the window.

The cooldown starts from the latest `finalized`, `timed-out`, or
`resource-exhausted` reconfiguration event in `mz_audit_events`. Those events
record the target and not the previous shape, so a reconfiguration that
changed something other than the size also starts a cooldown.

**Hydrating replicas are never targeted.** A hydrating replica is busy by
definition. Its busy time describes the hydration and says nothing about the
steady-state workload, so the sample would be discarded anyway. Querying it
competes with the hydration for the same workers and can only slow it down.
It is also the state where a targeted read is most likely to time out, and a
timeout there must not be read as overload.

The `oom` state is the one decision that needs no measurement. A replica that
was killed for memory cannot report how much it needed. The target is the next
size in range with at least twice the memory.

### The replica read

For each hydrated replica the autoscaler opens a session, sets `cluster` and
`cluster_replica` to target it, and sets `statement_timeout` to
`replica_read_timeout`. It then reads, in one query:

- elapsed nanoseconds per worker for the root operator of each dataflow
  (`mz_scheduling_elapsed_per_worker`, `mz_dataflow_addresses`). The elapsed
  time of an operator includes the operators nested in it, so the root alone
  is the dataflow's total and summing every operator would count time more
  than once.
- output and input frontiers per export, with the export's dataflow
  (`mz_compute_exports`, `mz_compute_frontiers`,
  `mz_compute_import_frontiers`)
- `mz_now()`, the timestamp the read was served at

It waits `sample_interval` and reads again. The difference between the two
reads gives, for the interval between the two `mz_now()` values:

- busy cores per dataflow and per worker, counting only root operators present
  in both reads so that a dropped dataflow cannot produce a negative delta
- the progress ratio per dataflow, as output frontier advance over input
  frontier advance, both taken from the replica itself

These are exactly the inputs to the demand model, and reading the frontiers
from the replica gives the local progress ratio directly, without
reconstructing it from wallclock lag.

Frontiers advance in ticks of about a second, so two reads a sample interval
apart can disagree by a tick at each end for a dataflow that is keeping up. A
measured ratio of at least 0.9 therefore counts as 1. The threshold is a flag.

A size's CPU capacity is its process count times the smaller of its workers
per process and its CPU limit in cores. Busy time is worker time, and a worker
uses at most one core.

Replicas of one cluster are sampled concurrently so the run takes one
`sample_interval` per cluster, and clusters can be sampled concurrently up to
a limit set by a flag. The targeted sessions use serializable isolation, since
introspection reads need no linearizability.

### Timeouts as signal

A targeted read can end three ways.

- **Both reads succeed.** The sample is used.
- **A read hits `statement_timeout`.** The replica is hydrated, reachable,
  and its workers did not get to a trivial introspection query within the
  timeout. That is a measurement: the workers are saturated. The sample is
  recorded as **unresponsive**, with CPU demand unknown and at least the
  replica's capacity.
- **The connection to `environmentd` fails.** This says nothing about the
  replica. The cluster is reported as `error` and no decision is made.

Keeping the second and third apart is what makes a timeout safe to act on.
Materialize reports a statement timeout as SQLSTATE `57014` with the message
`canceling statement due to statement timeout`, on a connection that stays
open. A cancel requested by a user shares the code, so both the code and the
message are checked. Anything else, including any other SQL error, is treated
as the third case.

An unresponsive sample is retried once within the run. If the retry also
times out, the cluster's recommendation is one step up the ladder. One step,
because the measurement is censored in the same way an out-of-memory kill is:
it bounds demand from below and does not size it. The next run after the
transition takes a real sample on the larger size and refines.

An unresponsive sample on any replica also vetoes a scale-down, with or
without the retry.

### Deciding without memory

The stateful design keeps a window of samples so that one quiet interval
cannot trigger a downsize and one spike cannot trigger an upsize. A run here
has one sample of 30 seconds. The window comes from history Materialize
already retains:

| Need | Source |
|---|---|
| CPU and memory over the window | `mz_cluster_replica_metrics_history` |
| Whether the cluster kept up over the window | `mz_wallclock_global_lag_recent_history` |
| Out-of-memory kills in the window | `mz_cluster_replica_status_history` |
| When the size last changed, and by whom | `mz_audit_events` |
| Whether a reconfiguration is in flight | `mz_cluster_reconfigurations` |
| Outcome, target, time, and role of past attempts | `mz_audit_events` |

The division of labor is that **the live sample sizes the decision and the
history vetoes it.** Process CPU from the metrics history is too coarse to
choose a size, for the reasons in the companion design, and good enough to
say "this cluster was not quiet for the last hour."

The window is the cluster's `cooldown`. One knob serves as both the minimum
time between size changes and the period that must look quiet before a
downsize.

**Scale up** when the live recommendation is larger than the current size.
The cooldown gate has already passed. No history check is needed. The cost of
a wrong upsize is bounded by the cooldown and corrected by the next downsize.

**Scale up one step** on a confirmed unresponsive sample, as above. If another
replica answered and its sample asks for more than one step, the larger
recommendation wins.

**Scale down** when all of these hold:

- the live recommendation is smaller than the current size
- every dataflow in the live sample has a progress ratio of 1
- no sample was unresponsive
- over the window, peak process CPU as a fraction of the *target* size's
  capacity is at or below `u_target`
- over the window, peak memory fits the target size at `m_target`
- over the window, lag on the cluster's objects stayed bounded
- the target is not the target of a failed attempt younger than four cooldowns

The target of a scale-down is the cheapest size, no smaller than the live
recommendation, for which the two peak conditions hold. A window whose peaks
rule out the live recommendation can still allow a size between it and the
current one. The window must also be covered: a cluster whose metrics history
starts later than the window does is not scaled down yet.

A size above `max_size` is the exception. The range is a cost bound, so such a
cluster is moved to `max_size` even when a veto holds, and further down only
when none does.

**Otherwise do nothing** and report why: `steady`, `at-min`, `at-max`,
`skew-limited` (a dataflow is behind, one worker is saturated, and a larger
size is not recommended), or `backing-off`.

A failed attempt is a reconfiguration that settled as `timed-out` or
`resource-exhausted`. Any failed attempt counts as a size change for cooldown.
One started by the autoscaler's role also marks its target for the longer
backoff. `mz_cluster_reconfigurations` holds neither a time nor a role, so
both come from `mz_audit_events`: the controller writes the settling event
with its time and target, and the role is the user of the latest `started`
event before it, which is the `ALTER` that set that target. Only the most
recent failure is consulted. That is enough to stop the common loop, where the
same downsize is attempted every cooldown and fails every time because the
target cannot hydrate.

**Without a live sample**, for clusters with introspection disabled and
clusters that run no compute objects, the same rules apply with the CPU demand
left out. Memory sizes a scale-up. Lag above the bound together with window
process CPU at or above `u_target` of the current size buys one step. Process
CPU over the window, which only vetoes elsewhere, sizes a scale-down here.

### Acting

The action is a plain statement:

```sql
ALTER CLUSTER analytics SET (SIZE = '400cc');
```

With background reconfiguration, which is the default, this writes the durable
reconfiguration record and returns. The cluster controller creates the new
replicas, waits for them to hydrate and catch up, cuts over, and drops the old
ones. If the target never becomes ready the controller rolls back at the
deadline and the cluster keeps serving at its old size. The autoscaler does
not wait. It exits, and a later run sees `reconfiguring`, then `cooling-down`,
then `eligible` again.

The autoscaler gets its safety from that mechanism. It needs no cut-over,
readiness, or rollback logic of its own, and an interrupted run leaves nothing
half done.

### Statelessness and concurrency

- **Crash at any point.** Before the `ALTER`, nothing happened. After it, the
  controller owns the transition.
- **Two copies running.** Both read the same state and reach the same
  decision. Each re-reads the cluster's size and reconfiguration record just
  before its `ALTER` and issues nothing if either changed during the run, so
  the second copy normally stands down. The check is not atomic with the
  `ALTER`. If both get through, a second `ALTER` to the same target folds into
  the in-progress record, and one to a different target retargets the
  reconfiguration, which the controller handles. Subsequent runs see
  `reconfiguring`.
- **A human resizes the cluster.** The audit log shows a size change, so the
  cooldown starts from it. The autoscaler then continues from the new size.
  A manual size outside the configured range is pulled back into it after the
  cooldown, which is what the range means.
- **Run frequency.** Decisions are gated by cooldown, so running more often
  than once a minute or so buys nothing except faster reaction to
  out-of-memory kills. A reasonable schedule is every one to five minutes.

### Reporting

Each run emits one JSON record per cluster to stdout:

```json
{
  "cluster": "analytics",
  "state": "eligible",
  "current_size": "200cc",
  "recommended_size": "400cc",
  "decision": "scale-up",
  "reason": "cpu demand 5.1 cores exceeds 4 cores at 0.7 target",
  "replicas": [
    {"name": "r1", "sampled": true, "busy_cores": 3.6,
     "max_worker_utilization": 0.97, "min_progress_ratio": 0.7,
     "slowest_dataflow": "u123"}
  ],
  "window": {"peak_cpu_percent": 91, "peak_memory_bytes": 12884901888},
  "dry_run": false
}
```

The record also carries `target_size`, an `action` (`altered`, `dry-run`,
`none`, `superseded`, or `failed`), a `mode` (`sampled` or `fallback`), the
CPU demand, and per replica the sample status and timeout count.

Because nothing is stored in the catalog, these records are the explanation
for every decision. `mz_audit_events` separately records each `ALTER` under
the autoscaler's role. The process exits non-zero when it could not evaluate a
cluster or when Materialize rejected an `ALTER`, not when it decided to do
nothing. An invalid configuration has its own exit code.

### Known inaccuracies

Carried over from reading introspection from outside, and accepted for this
version:

- **Short-lived dataflows are undercounted.** Work done by a dataflow that is
  created and dropped between the two reads is not seen. This understates
  demand on clusters dominated by ad hoc queries. The window veto on process
  CPU catches the downsize case.
- **Worker overhead is not measured.** Elapsed time covers operators only. The
  estimate runs slightly low and `u_target` headroom absorbs it.
- **The sample is short.** Thirty seconds is one draw. Upsizes can react to a
  spike that the stateful design's activation window would have filtered. The
  cooldown bounds how often that can happen.
- **Clusters with introspection disabled** have no busy-time relation. They
  fall back to memory, lag, process CPU, and out-of-memory decisions and are
  reported as such.
- **Foreground reconfiguration.** With `enable_background_alter_cluster` off
  the `ALTER` waits for the cut-over and the run waits with it, up to the
  catalog session's statement timeout. The record the statement wrote outlives
  the wait, so a timeout there is reported as a successful action.

### Relationship to the built-in design

This is the shadow phase of the companion design, made concrete, and a usable
product for environments that want autoscaling before the built-in strategy
exists. The decision function is the same, so the records this process emits
are the calibration data for the built-in defaults. When the built-in strategy
ships, a cluster moves over by setting `ON LOAD` with the same bounds and
removing its entry from this process's configuration. Running both on one
cluster should be rejected by convention, not by mechanism.

### Testing

- **Decision function.** Pure, table-driven: catalog snapshot, samples, and
  history in, decision out. Shared with the built-in strategy where practical.
- **mzcompose.** A step-load scenario asserting the direction of each size
  change, since how far a given load pushes a cluster depends on the machine.
  A scenario that holds a replica hydrating and asserts no session ever
  targets it. A scenario in which targeted reads time out and asserts a
  one-step upsize. It uses a statement timeout of one millisecond in place of
  a saturated replica, which exercises the same error from Materialize without
  depending on how hard a test can load a replica. Open question 5 still needs
  a real saturation run. A scenario that kills the autoscaler mid-run at each
  step and asserts the cluster converges anyway.
- **Dry run against real environments** before enabling actions anywhere.

## Minimal Viable Prototype

`misc/python/materialize/cluster_autoscaler`, run as `bin/cluster-autoscaler`.
The decision function and the sampling arithmetic are pure and unit tested.
The mzcompose scenarios are in `test/cluster-autoscaler`. The first use is in
`dry_run` mode, since that already answers the open questions about signal
quality and needs no permission to alter anything.

## Alternatives

**Skip the replica read and decide on catalog relations only.** Simplest, and
never touches a user cluster. It reduces to threshold rules on process CPU and
lag, which can detect overload and cannot size the fix. Kept as the fallback
path when introspection is unavailable.

**Use the parks histogram for busy time.** It is per worker and includes
overhead, but park durations are rounded up to a power of two before logging.
The error is small near saturation and large near idle, which is where
downsize decisions are made. Useful as a saturation cross-check only.

**Keep state in a table in Materialize.** A table would give an exact sample
window and a full failure history. It also makes the process depend on write
access and a schema, and correctness on that table's contents. The retained
history relations are a sufficient substitute.

**A long-running service holding a `SUBSCRIBE` per replica.** More precise and
continuous. It keeps a dataflow installed on every replica permanently, holds
state in memory that a restart loses, and is harder to operate than a job.

**Wait for the built-in strategy.** It is the better end state. It is also
several changes away from its first decision, and it needs the data this
process produces.

## Open questions

1. **Is a confirmed timeout enough to scale up on its own?** The proposal
   says yes after one retry. A stricter rule would also require rising lag or
   high process CPU in the history. Stricter is safer against a slow
   `environmentd` and slower to react to a truly saturated replica.
2. **One cooldown or two?** A single period gates both directions and doubles
   as the downsize window. Upsizes after a recent downsize might deserve a
   shorter wait.
3. **Publishing.** The prototype is a Python tool under `misc/`, which is
   faster to iterate on than a Rust binary and cannot share the decision
   function with the built-in strategy. Where it is published for self-managed
   users is a product question.
4. **Lag bound for the downsize veto.** "Stayed bounded" needs a number. The
   prototype defaults to 60s, the controller's
   `cluster_reconfiguration_allowed_lag`, as a flag. It may be too strict for
   clusters whose inputs are themselves slow.
5. **Does the targeted read measurably disturb a busy replica?** Expected to
   be negligible for two small queries per run. Should be measured in the
   saturation scenario.
6. **Privileges.** `ALTER CLUSTER` requires ownership of the cluster, and the
   targeted reads require `USAGE` on it. The grant set in the tool's README
   needs to be confirmed against a live environment.
7. **Should a failed scale-up back off as well?** Only a failed scale-down
   target is held back for four cooldowns. A failed scale-up is retried after
   one, on the grounds that being too small costs freshness.
