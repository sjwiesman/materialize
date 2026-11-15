# Reactive Cluster Autoscaling

- Associated: (TBD, link epics/issues)
- Builds on: [Cluster Autoscaling and Background Reconfiguration](20260522_cluster_autoscaling.md)

> [!NOTE]
> This is the second revision of this design, and the branch implements it.
> The first revision sized clusters from a per-dataflow demand model against
> a utilization target. That approach is described under
> [Alternatives](#alternatives).

## The Problem

A cluster's `SIZE` is chosen by a human, once, for a workload that changes.
Users size for peak and overpay the rest of the time, or size for the average
and serve stale data during spikes. Finding the right size today means
reading `mz_cluster_replica_utilization`, guessing, issuing `ALTER CLUSTER`,
waiting for rehydration, and repeating. Each iteration costs a full hydration,
so a wrong guess is expensive in both directions.

The cluster controller from the [cluster autoscaling design](20260522_cluster_autoscaling.md)
gives us the mechanism: durable, background, zero-downtime size changes with a
readiness gate and rollback. It has strategies for hydration (`ON HYDRATION`)
and refresh windows (`ON REFRESH`). It has no strategy that reacts to load.

### The objective

A cluster should run at **the smallest size at which a larger size would not
make its data fresher**, subject to fitting in memory.

This is a statement about a marginal benefit, and the policy is built to test
it directly. It is deliberately not a utilization target. A replica at 95%
utilization that keeps up with its inputs is as fresh as a larger one would
be, so it is the right size. A replica at 30% utilization that falls behind
because one worker holds a hot key is not helped by a larger size, so it is
also at the right size.

Two properties of Materialize shape how the objective is pursued.

1. **A resize costs a rehydration.** The new replica is built from persist,
   which takes minutes to hours and bills for the overlap. Decisions must be
   few, and a wrong one is expensive.
2. **A resize is also an experiment.** During a graceful reconfiguration the
   old and new replicas run the same dataflows on the same inputs at the same
   time. Their freshness can be compared before anything is committed. No
   other streaming system we looked at has this, and it lets the policy
   verify a size instead of trusting a model of it.

## Success Criteria

- A user can give a cluster a size range and stop thinking about size.
- **Freshness.** When a cluster falls behind its inputs and a larger size in
  range would help, the cluster moves to a size that keeps up, in one or two
  reconfigurations.
- **Cost.** When a smaller size in range would be as fresh as the current one
  and fits in memory, the cluster moves to it.
- **Do no harm.** A scale-down never serves data staler than the size it
  replaced, beyond a stated tolerance. A size that fails is not retried
  blindly.
- **Stability.** A stable workload produces no size changes. The cluster never
  alternates between two sizes.
- **Bounded cost.** Steady-state spend never exceeds the configured maximum
  size. Transitions bill for overlap, as manual graceful reconfigurations do.
- **Explainability.** Every decision, and every decision not taken, is
  attributable to recorded signal values a user can query.
- Operators can disable the strategy environment-wide without affecting the
  other strategies.

## Out of Scope

- **Query latency.** The objective is freshness. Queries consume worker time
  and so can cause lag, which the policy sees. A cluster whose queries are
  slow while its data is fresh is not scaled.
- **Predictive or scheduled scaling.** This design reacts to observed load.
- **Changing `REPLICATION FACTOR`.** Replica count is an availability choice.
- **Scale to zero.**
- **Fixing skew.** A dataflow limited by one hot worker cannot be fixed by a
  larger size. We detect and report it.
- **Sources and sinks.** Their lag is measured against an external system and
  needs a different signal. They are deferred. See [Rollout](#rollout).
- **Combination with `SCHEDULE = ON REFRESH`.** Rejected at plan time.
- **Hydration.** A larger replica always hydrates faster, so the objective is
  about steady state only. `ON HYDRATION` covers hydration and composes with
  this strategy.

## Solution Proposal

### Summary

Add an `ON LOAD` autoscaling strategy to the cluster controller. It watches
two things per cluster: how far each replica's outputs trail its inputs, and
how much of its memory and disk the replica's heap uses.

- When a cluster stays behind its inputs, the strategy **scales up**. A
  lagging replica is saturated, so how far short it fell gives the size
  needed. If the larger size turns out not to help, the strategy goes back.
- When a cluster has kept up for a full window and its heap would fit a
  smaller size at about half of that size's memory and disk, the strategy
  **tries the smaller size**. The smaller replica runs beside the current one
  and must match its freshness for a trial period before the cluster cuts
  over. If it cannot, it is dropped and the size is backed off.

Size changes are written as the same durable `reconfiguration` record that
`ALTER CLUSTER ... SET (SIZE)` writes. The graceful reconfiguration strategy
carries them out, extended with the trial.

### SQL surface

```sql
CREATE CLUSTER c (
  SIZE = '100cc',
  AUTO SCALING STRATEGY = (
    ON LOAD (
      MIN SIZE = '50cc',
      MAX SIZE = '800cc',
      SCALE DOWN AFTER = '1h'   -- optional
    )
  )
);
```

- `MIN SIZE` and `MAX SIZE` are required. The maximum is the user's cost bound,
  so we do not default it. Both must belong to the same size family as `SIZE`,
  and `MIN SIZE <= SIZE <= MAX SIZE`.
- The candidate sizes are the enabled sizes of that family, ordered by credits
  per hour. The range is the stretch of that ladder between the two bounds.
- `SIZE` remains the realized size. With `ON LOAD` set, the controller moves
  it. `mz_clusters.size` and `SHOW CREATE CLUSTER` show what is running now.
- A user `ALTER CLUSTER ... SET (SIZE)` is still allowed inside the range. A
  user-written reconfiguration always wins over an autoscaler-written one.
- `SCALE DOWN AFTER` is how long the cluster must have kept up before a
  smaller size is tried. A workload with a daily peak should set it to a day.

All other tuning lives in dyncfgs.

### Definitions

**Local lag.** For a dataflow on a replica, the gap between the least write
frontier of the dataflow's direct inputs and the replica's own output frontier
for it. It measures only what this replica has not yet processed. An input
that is itself stale upstream contributes nothing, so a cluster is never
scaled for a problem it does not own.

**Replica lag** is the largest local lag among the replica's hydrated
dataflows. **Cluster lag** is the smallest replica lag among the replicas at
the realized size, since replicas do the same work and the freshest one
answers for the cluster.

**Tolerance** (`τ`, default 2s). Lag at or below the tolerance counts as
fresh. Frontiers advance in ticks of the timestamp interval, 1s by default,
and a persist write adds a little more, so some lag exists at any size.

**Behind.** A sample is behind when cluster lag exceeds the tolerance and did
not shrink over the sample interval. A cluster that is catching up on its own
is not behind.

**Progress ratio** (`ρ`). Over a sample interval, how far the most lagging
dataflow's output frontier advanced divided by how far its inputs advanced. A
dataflow that advanced 30 seconds of timestamps while its inputs advanced 60
processed half of what arrived, `ρ = 0.5`.

**Steady-state heap.** Heap bytes as a fraction of the heap limit, which is
the size's memory plus its disk, on the busiest process of a hydrated replica.
Measured outside hydration, where usage runs higher.

### Samples

The strategy evaluates one sample per busy-time window (default 60s) and keeps
a window of them in memory. A sample is taken only from replicas that are at
the realized size, hydrated, and past a warm-up period, so that it describes
the workload and not a hydration or a transition. Each sample holds cluster
lag and whether the cluster was behind, the progress ratio, steady-state heap,
total and busiest-worker busy time, and whether a replica was killed for
exceeding its memory.

The window is a live signal and is empty after an `environmentd` restart.
Every rule needs a minimum number of samples, so a restart can delay a
decision and cannot cause one.

### Scale up

**Trigger.** The cluster was behind in each of the last few samples (the
activation window, default 3). There is no utilization threshold. Lag is the
definition of workers not keeping up with their inputs, and it is what the
user cares about.

**How far.** A replica that is behind is using all the capacity it has, so its
capacity is known: it is the current size. The capacity needed is

```
needed = cores(current) / ρ · (1 + lag / T_catchup)
```

The first factor is the capacity to keep pace. The second adds enough to work
off the backlog within the catch-up time (default 15 minutes). `ρ` is the
median over the activation window, floored at 0.25. A dataflow whose output
did not move at all says only that it needs more, so the floor bounds one step
at 4x and lets the next step refine. The target is the cheapest size in range
with at least the needed cores, and at least one step up.

This is the lag-based half of what the Flink autoscaler does, in time units
where Flink counts records. It needs no busy-time measurement, because the
replica is saturated by definition.

**Skew.** If the cluster is behind and its workers were, taken together, busy
less than half of the time, more workers will not help. With the busiest
worker saturated this is a hot key and the status is `skew-limited`. Without,
the dataflow is waiting on something other than CPU and the status is
`stalled`. Neither scales.

**Afterwards.** Once the larger size has cut over and warmed up, the first
full activation window at it checks whether it helped. If the cluster is
still behind and its progress ratio did not improve, workers were not the
bottleneck, so by the objective the smaller size was right. The strategy goes
back to it and backs off the larger one. That window starts no other lag
decision.

**Memory.** Two rules, independent of lag:

- A replica at the realized size is killed for exceeding its memory. The
  cluster scales up immediately to the next size with at least twice the heap
  limit. The activation window is skipped, because a killed replica cannot
  report what it needed.
- Steady-state heap stays above a high-water mark (default 80%) for a
  sustained period (default 15 minutes). Every sample in the period must be
  above the mark, so a spike from a large transaction or a merge does not
  count. The cluster then scales up to the cheapest size at which the heap
  would be at or below 50%. This acts before the kill. A cluster that fills
  its memory faster than the period is still caught by the first rule.

### Scale down

A smaller size is **proposed** when all of these held over the whole
scale-down window (`SCALE DOWN AFTER`, default 1h, and at least twice the
cluster's last hydration time):

1. No sample was behind, and no replica was killed for memory.
2. The peak steady-state heap in the window would be at or below 50% of the
   candidate's memory plus disk.
3. The peak total worker busy time in the window would fit the candidate's
   cores with a margin (default 80%).

The candidate is the cheapest size in range that passes 2 and 3 and is not
backed off. Heap is taken as independent of size, which the trial checks.

Condition 2 is a requirement on the result. A cluster should sit at about half
of its memory and disk in steady state, which leaves room for hydration peaks
and for growth. Condition 3 exists to avoid starting trials that cannot pass.
It is a filter and not a target, and it is the only slack the policy keeps
against load growing between decisions.

Because each ladder step roughly doubles memory, 50% at the smaller size is
25% at the current one. The scale-up mark is 80%, sustained. A cluster that has just
scaled down sits at 50% and has to grow by more than half before it scales
back up, so the two memory rules cannot chase each other.

### The trial

A proposal is tested before it is committed. The strategy writes a
reconfiguration record for the candidate that carries two extra requirements:
a lag allowance equal to the tolerance, and a hold duration (default 15
minutes).

The graceful reconfiguration strategy creates the candidate replicas beside
the current ones and waits for them to hydrate. From then on it requires, for
the whole hold duration without interruption, that the candidate would not
itself be a reason to scale up:

- its lag is within the tolerance of the current replicas' lag,
- its steady-state heap is below the high-water mark,
- it has not been killed for memory.

If the hold completes, the cluster cuts over and the old replicas are dropped.
If the candidate fails a check, the hold restarts, and if it has not passed by
the record's deadline the reconfiguration rolls back. The cluster never
stopped serving from the current replicas. The strategy then backs off that
size.

The trial is the objective, checked literally. If the smaller replica is as
fresh as the larger one on the same inputs at the same time, then scaling up
could not make the data fresher. It also makes the policy robust to its own
estimates. Conditions 2 and 3 only choose what to try. Being wrong costs the
credits of one hydration, and never freshness.

A trial covers only the load during the trial. Peaks that recur on a longer
cycle are covered by condition 3 looking at the peak over the window, which is
why the window should span the workload's cycle.

### Backoff

A size that failed is not retried until its backoff expires. Backoff starts at
1h and doubles per consecutive failure up to 24h.

- A scale-down whose trial failed or timed out backs off that size and every
  smaller one. The next proposal tries the next size up.
- A scale-up that did not help backs off that size and every larger one.
- A reconfiguration that ran out of resources backs off its target.

Backoff is cleared when the user changes the policy or the set of objects on
the cluster changes.

### Decisions not taken

The strategy reports why it is holding still: `steady`, `at-max`, `at-min`,
`skew-limited`, `stalled`, `backing-off`, or `warming-up` when it does not yet
have enough samples at the current size.

### Stability

- A scale-up needs sustained lag and a scale-down needs a full quiet window,
  so the two cannot trigger from the same samples.
- A scale-down is committed only after the smaller size has proven itself, so
  it cannot be followed by an immediate scale-up for the same load.
- The memory rules are separated by more than a ladder step, as shown above.
- Samples are excluded during transitions and warm-up, so the strategy never
  reacts to its own reconfiguration.
- Failed sizes back off.

### Signals

| Signal | Source | Status |
|---|---|---|
| Input and per-replica output frontiers per dataflow | compute controller | exists in process |
| Heap bytes and heap limit per process | replica metrics, 60s poll | exists in process |
| Memory-kill status | orchestrator status events | exists in process |
| Hydration state and time per replica | compute controller | exists in process |
| Busy time per replica: total and busiest worker | replica introspection | new |

Busy time is used for two things only: telling skew from saturation, and
condition 3. The replica reports, once per busy-time window, one row with the
worker count, the non-parked time summed over workers, and the largest
non-parked time of any worker, which the adapter appends to
`mz_internal.mz_cluster_replica_busy_history`. Windows end at multiples of the
window length, `compute_worker_busy_window`, since the Unix epoch, and the
coordinator samples at the same boundaries, so a busy-time window and the
frontier advance it is compared with cover the same interval. The export does not grow with the number of workers or dataflows.

The strategy interface gains one request and one signal, the size ladder and
the sample window for a cluster.

### The strategy

`OnLoadStrategy` implements the existing `Strategy` trait.

- `signal_request` asks for the load signal when the cluster has an `ON LOAD`
  policy and the break-glass flag is on.
- `update_state` evaluates the rules as a pure function of the cluster state,
  the sample window, and `now`. When a rule fires it writes a `reconfiguration`
  record with `origin = LoadAutoscaling` and `on_timeout = ROLLBACK`. For a
  scale-down the record also carries the trial's lag allowance, heap mark and
  hold duration. It maintains the `load` record described below.
- `desired_replicas` returns nothing. Graceful reconfiguration contributes the
  target replicas and the baseline holds the realized set.

`GracefulReconfigurationStrategy` gains the hold. Today it cuts over on the
first tick that finds the target replicas ready, meaning hydrated and within
`cluster_reconfiguration_allowed_lag` of the outgoing replicas. A record with
a hold duration instead stamps the time the targets first became ready, clears
the stamp if they stop being ready, and cuts over once the stamp is older than
the hold. The readiness check takes the record's lag allowance in place of the
dyncfg and adds the record's heap check. User reconfigurations carry no hold and behave
as they do today.

Durable state, alongside `reconfiguration` and `burst`:

```
load: Option<LoadRecord> {
    attempt:      the strategy's reconfiguration whose outcome is not yet
                  recorded, with the progress ratio that motivated a scale-up
    last_cutover: when the strategy last changed the size
    last_scale_up: the lag-motivated scale-up behind that cut-over, with its
                  progress ratio, until it is judged
    backoff:      [{ size, direction, until, failures }]
    hold:         the hold status already audited, if any
}
```

and on the reconfiguration record:

```
origin: User | LoadAutoscaling
trial:  Option<{ allowed_lag, hold_for, max_heap_fraction, ready_since }>
```

### Observability

- **`mz_internal.mz_cluster_load_recommendations`.** One row per managed
  cluster, with or without an `ON LOAD` policy: cluster lag, progress ratio
  and the dataflow with the lowest one, steady-state heap fraction, mean and
  busiest worker utilization, the size the rules would move to, and the
  status. Sizing advice is useful on its own, and it is how we validate the
  policy before it acts.
- **Audit log.** Autoscaler-originated reconfigurations use the existing
  reconfiguration events with their origin. The `started` event also carries
  the reason and the sample values that triggered it. Trial failures, backoffs, and entering or leaving a hold
  status are audited.
- **`mz_cluster_auto_scaling_strategies`** shows the policy and the `load`
  record. **`mz_cluster_reconfigurations`** shows the origin and, for a trial,
  how long the candidate has held.

### Dyncfgs

| Key | Default | Meaning |
|---|---|---|
| `enable_load_autoscaling` | off in production | break-glass for acting |
| `compute_worker_busy_window` | 60s | the replicas' busy-time window, which the strategy samples at |
| `load_autoscaling_lag_tolerance` | 2s | `τ` |
| `load_autoscaling_activation_intervals` | 3 | samples behind before scaling up |
| `load_autoscaling_catch_up` | 15min | `T_catchup` |
| `load_autoscaling_min_progress_ratio` | 0.25 | floor on `ρ` |
| `load_autoscaling_heap_target` | 0.5 | steady-state heap a size is chosen for |
| `load_autoscaling_heap_high_water` | 0.8 | steady-state heap that scales up |
| `load_autoscaling_heap_high_water_duration` | 15min | how long heap must stay above the mark |
| `load_autoscaling_cpu_margin` | 0.8 | share of a candidate's cores peak busy time may use |
| `load_autoscaling_trial_duration` | 15min | how long a candidate must hold |
| `load_autoscaling_default_scale_down_after` | 1h | scale-down window |
| `load_autoscaling_warm_up` | 5min | samples ignored after hydration or a size change |

Samples and recommendations are produced regardless of
`enable_load_autoscaling`.

### Rollout

1. **Shadow.** Signal, samples, and the recommendations relation, with no
   actions. Compare recommendations against the sizes users chose and against
   known freshness incidents.
2. **Scale up only.** The freshness half. It cannot make a cluster staler.
3. **Scale down.** The trial and the memory constraint.
4. **Sources and sinks**, with lag from source statistics.

### Testing

- **Rules.** The decision function is pure. Table-driven tests over sample
  windows cover every rule, the tolerance, the progress floor, the memory
  bands and the sustained high-water period, and backoff.
- **Trial.** Unit tests of the hold in the graceful strategy: stamp, reset on
  a failed check, cut-over after the hold, rollback at the deadline, and no
  change for user reconfigurations.
- **Simulation.** A closed loop against a synthetic replica (capacity
  `cores^α` with `α <= 1`, configurable skew, heap, and hydration time)
  asserting that from every starting size the cluster reaches the smallest
  size that keeps up and fits, that it then stays, and that a failed trial is
  not repeated.
- **mzcompose.** A step-load scenario asserting the size sequence and that lag
  recovers. A hot-key scenario asserting `skew-limited` and no scale-up loop.
  A scenario whose smaller size cannot keep up, asserting the trial fails with
  no staleness visible to queries. A memory scenario for both memory rules.

### Implementation notes

Where the implementation departs from the text above:

- **Memory kill.** The rule ignores an Up backoff, since staying at the
  current size means being killed again. A size that failed instead raises
  the floor: the target needs twice the heap limit of the largest failed size
  above the current one. Without this, a target that is itself killed during
  hydration times out, is backed off, and leaves the cluster at a size that is
  killed in a loop.
- **Heap limit.** A size's heap limit is its memory plus its disk only when
  the size spills to swap with a non-zero disk limit, which is when the
  controller passes `--heap-limit` to the replica. Other sizes use their
  memory limit. A measured heap fraction uses the limit the replica process
  reports, falling back to the size's.
- **Trial lag.** The trial's lag allowance applies even when
  `enable_cluster_reconfiguration_lag_gate` is off.
- **Convergence.** The simulation reaches the smallest fitting size from every
  start only for workloads away from two bands. A size that keeps up with its
  busy time above the CPU margin, or holds its heap between 50% and the
  high-water mark, is kept when the cluster arrives there from below, since no
  rule moves it. From above, busy time measured at a larger size overstates
  what a smaller one needs when capacity grows slower than cores, so the CPU
  condition can stop the descent a step early. In both bands the cluster
  settles within one step of the smallest fitting size, in at most two
  changes, and stays.

## Minimal Viable Prototype

Not built for this revision. The shadow phase is the prototype. Every input
except the busy-time row already exists in the controller, and the scale-up
rule needs none of the new instrumentation, so the first thing to validate is
whether sustained local lag identifies the clusters people have had to resize
by hand.

## Alternatives

**Size from a demand model against a utilization target** (the first revision
of this design, following DS2 and the Flink autoscaler). It measured busy time
per dataflow, inflated it by each dataflow's progress ratio, and chose the
cheapest size that held the result under 70% of its cores. Rejected as the
core of the policy for three reasons. A utilization target is a proxy for the
objective and has no right value: Flink defaults to 0.7 and a large operator
of it runs 0.45. The model assumes capacity scales linearly with cores, and
the Flink documentation itself says the busy-time estimate is least reliable
when it matters most. And its main advantage, sizing each operator separately,
does not apply to a system with one knob per cluster. What survives from it is
the scale-up step, where the replica's saturation makes the estimate simple.

**Threshold rules on CPU and memory utilization.** Simple, and close to what
ran in production elsewhere for years. It detects load without knowing whether
a larger size would help, and it needs a threshold tuned per workload.

**Scale on lag alone, without the trial.** Lag can trigger a scale-up and
cannot justify a scale-down, since a cluster with no lag may be many times too
large. Something has to propose and verify the smaller size.

**Trust the prediction for scale-down and rely on rollback.** The existing
readiness gate already rolls back a target that cannot hydrate or falls 60
seconds behind. The trial tightens that gate to the tolerance and holds it,
which turns a safety net into the test of the objective.

**Scale by adding replicas.** Replicas duplicate work, so this adds cost and
no capacity.

**In-place vertical resize.** Kubernetes can change a pod's memory limit
without a restart, which could make memory-only steps nearly free. It does not
change the worker count. Worth revisiting as a mechanism under this policy.

**Live state migration** ([Megaphone](#references)). It would remove the
rehydration cost. It is a much larger change to compute.

**Predictive scaling.** Complementary. It would address the one thing this
design cannot: when load grows past the current size, data is stale for one
hydration time while the larger replica comes up.

## Open questions

1. **Default of `enable_load_autoscaling` in production.** Decided: off.
2. **Tolerance.** 2 seconds is proposed. It has to exceed the lag a replica
   shows at any size, which depends on the timestamp interval and persist
   latency. The shadow phase should measure that floor per cluster.
3. **How long heap must stay above the high-water mark.** 15 minutes is
   proposed. Longer ignores more transient spikes and leaves less time before
   a kill for a cluster whose memory is growing steadily.
4. **Which replica speaks for the cluster.** The proposal takes the freshest
   replica. A cluster whose replicas differ persistently would then never
   scale for the slow one.
5. **Jumping several sizes down.** The proposal goes straight to the cheapest
   size that passes the filters and lets the trial catch a bad guess. Stepping
   one size at a time is safer and takes a window per step.
6. **Trial length against periodic load.** A 15 minute trial at a quiet time
   says nothing about the next peak. Condition 3 is meant to cover it. Is the
   margin enough, or should the trial be required to span a peak?
7. **Is the busy-time export needed at all?** Process CPU from the existing
   metrics could serve condition 3. Skew would then be discovered by a wasted
   scale-up instead of being recognized first.
8. **Jumpy frontiers.** Dataflows with temporal filters, `REFRESH` schedules,
   or coarse input timestamps advance in steps. Their lag may exceed any fixed
   tolerance without being behind.
9. **Should recommendations be visible to users without the feature?**

## References

- V. Kalavri, J. Liagouris, M. Hoffmann, D. Dimitrova, M. Forshaw, T. Roscoe.
  [Three steps is all you need: fast, accurate, automatic scaling decisions
  for distributed streaming dataflows](https://people.inf.ethz.ch/troscoe/pubs/kalavri_osdi_2018.pdf).
  OSDI 2018.
- [Flink Kubernetes Operator autoscaler](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/autoscaler/)
  and [FLIP-271: Autoscaling](https://cwiki.apache.org/confluence/spaces/FLINK/pages/231117250/FLIP-271%2BAutoscaling).
  The utilization band, scale-down window, capped steps, and effectiveness
  detection that production use added to the DS2 model.
- [Netflix Moves toward Open Source Flink Autoscaler](https://www.infoq.com/news/2026/09/netflix-flink-autoscaler/).
  A cluster-level CPU and lag controller replaced by the Flink autoscaler, run
  at a 0.45 utilization target.
- D. Schmitz, G. Rosinosky, E. Rivière.
  [Justin: Hybrid CPU/Memory Elastic Scaling for Distributed Stream
  Processing](https://arxiv.org/abs/2505.19739). 2025.
- M. Hoffmann, A. Lattuada, F. McSherry, V. Kalavri, J. Liagouris, T. Roscoe.
  [Megaphone: Latency-conscious state migration for distributed streaming
  dataflows](https://arxiv.org/abs/1812.01371). PVLDB 2019.
