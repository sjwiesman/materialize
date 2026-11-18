# Cluster Autoscaling on Load, by Trial

- Associated: (TBD, link epics/issues)
- Builds on: [Cluster Autoscaling and Background Reconfiguration](20260522_cluster_autoscaling.md)

## The Problem

A cluster's `SIZE` is chosen by a human, once, for a workload that changes.
Users size for peak and overpay the rest of the time, or size for the average
and serve stale data during spikes.

The cluster controller can change a cluster's size in the background, with no
downtime, by running the new replicas beside the old ones until they are
ready. What is missing is a policy that decides when to change the size and
to what.

### The objective

A cluster should run at **the smallest size at which hardware is not what
limits its dataflows from keeping up with their inputs**, subject to its
memory fitting.

The workload is not fixed, so neither is the right size. A cluster that needs
a larger size on some days and a smaller one on others should follow that,
as long as it does not change size at short intervals. The policy is a
right-sizer and not a reflex. It may take tens of minutes to act.

### What can be measured, inferred, and not known

The design rests on being strict about these three.

**Measured.** How far each dataflow's output trails its inputs on each
replica. How much heap each replica process uses, against its limit. How long
each worker was not parked. Whether a replica was killed for memory.

**Inferred, and unreliable.** That a busy cluster would be faster with more
workers. That an idle cluster would be as fast with fewer. That a dataflow
which is behind lacks hardware. None of these follow from utilization. A
dataflow limited by one hot key, by work that does not parallelize, or by a
cost it pays on every tick at any size is behind on a cluster of any size.
A cluster whose workers are mostly parked can still be at the smallest size
that keeps up, because the busy part does not parallelize.

**Not known without trying.** Whether a specific other size would keep up
with this workload.

Materialize can try. During a graceful reconfiguration the old and new
replicas run the same dataflows on the same inputs at the same time, and
their lag can be compared before anything is committed. So the policy uses
measurements to decide what is worth trying, and commits to a size only on
what a trial showed. Utilization never justifies a size change by itself.

## Success Criteria

- A user gives a cluster a size range and stops thinking about size.
- **Sufficient.** When dataflows are behind and a larger size in range makes
  them fresher, the cluster moves to it.
- **Smallest.** When a smaller size in range is as fresh as the current one
  and fits in memory, the cluster moves to it.
- **No futile scaling.** A cluster that is behind for a reason a larger size
  does not fix is never committed to the larger size. The policy says so in
  a status, and it does not try again until its evidence expires.
- **Memory.** The policy never selects a size at which it expects heap above
  80% of the limit. Where the workload allows, the cluster sits between 50%
  and 80%. Keeping up takes priority over that band, so a cluster may sit
  below 50% when it needs the workers.
- **Bounded oscillation.** The time between a scale-up from a size and the
  next scale-down to it is at least `SCALE DOWN AFTER`. It grows when
  descents to that size keep being undone soon after.
- **Bounded experiments.** For an unchanged set of dataflows, the number of
  trials is bounded by the number of sizes in range per evidence lifetime.
- **Explainability.** Every decision and every trial verdict is attributable
  to recorded values a user can query.
- Operators can disable the strategy environment-wide without affecting the
  other strategies.

## Out of Scope

- **Query latency.** The objective is freshness. Queries consume worker time
  and so can cause lag, which the policy sees.
- **Predictive or scheduled scaling.** When load grows past the current size,
  data is stale until the larger replicas have hydrated.
- **Changing `REPLICATION FACTOR`.**
- **Scale to zero.**
- **Fixing skew.** It is detected by trial and reported.
- **Sources and sinks.** Their lag is measured against an external system,
  and their workers are not covered by the busy-time signal. A cluster that
  hosts a source or a sink cannot have an `ON LOAD` policy, and a source or
  sink cannot be created on a cluster that has one. Both are rejected at plan
  time. Without the rejection the policy would see such a cluster as idle and
  shrink it.
- **Combination with `SCHEDULE = ON REFRESH`.** Rejected at plan time.
- **Hydration speed.** A larger replica hydrates faster. `ON HYDRATION`
  covers that and composes with this strategy.

## Solution Proposal

### Summary

The `ON LOAD` strategy keeps three things per cluster: a history of load
summaries, a small table of evidence about sizes it has tried, and a lag
floor for each dataflow that a larger size did not help.

- When dataflows stay behind, the strategy starts an **up-trial**. A larger
  size runs beside the current one. If the behind dataflows are fresher on
  it, the cluster cuts over. If they are not, the candidate is dropped, the
  dataflows are marked as not helped by size, and the cluster stays.
- When the cluster has kept up for a quiet window and the next smaller size
  would fit its memory, the strategy starts a **down-trial**. The smaller
  size runs beside the current one and must be as fresh for the whole hold.
  The trial is evaluated once. A candidate that fails is dropped.
- When memory is the constraint, the cluster moves up without a trial,
  because the benefit of more memory does not need testing.

Every verdict is recorded against the size it concerns. Evidence keeps the
strategy from repeating a trial whose outcome it knows, and it expires, so
the cluster follows a workload that changes.

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

- `MIN SIZE` and `MAX SIZE` are required and belong to the size family of
  `SIZE`, with `MIN SIZE <= SIZE <= MAX SIZE`. The maximum is the user's cost
  bound.
- The candidate sizes are the enabled sizes of that family between the
  bounds, ordered by credits per hour.
- `SIZE` remains the realized size, and the controller moves it.
- A user `ALTER CLUSTER ... SET (SIZE)` inside the range is allowed and wins
  over anything the strategy has in progress.
- `SCALE DOWN AFTER` (`W`, default 1h) is the shortest time the cluster must
  have kept up before a smaller size is tried. It is also the shortest time
  between leaving a size for lack of capacity and trying it again.

### Signals

| Signal | Source | Cadence |
|---|---|---|
| Local lag per dataflow per replica | compute controller frontiers | every lag sample interval, 10s |
| Heap bytes and heap limit per process | replica metrics | 60s |
| Memory-kill status | orchestrator status events | on event |
| Hydration state and time per replica | compute controller | on change |
| Busy time per replica: workers, total, busiest worker | replica introspection | per busy window, 60s |

**Local lag.** For a dataflow on a replica, the gap between the least write
frontier of the dataflow's direct inputs and the replica's own output
frontier for it. An input that is stale upstream contributes nothing, so the
cluster is not scaled for a problem it does not own.

Lag is read from the controller on every sample and does not depend on
anything the replica has to deliver. A replica too overloaded to report its
busy time still has a lag, and that is when the lag matters most. A missing
busy window leaves the busy fields of a summary empty and changes nothing
else.

**Heap.** Memory plus swap of a process, against the limit the replica
reports, which is the size's memory plus its disk where the size spills to
swap. The steady-state heap of a replica is the median over an evaluation
window on its fullest process, taken outside hydration. The hydration peak is
the largest heap seen between a replica's creation and its hydration.

**Busy time.** Non-parked time per worker per window. It is used only to
avoid proposing a down-trial that cannot pass, and to describe a verdict. It
never triggers a size change and never vetoes an up-trial.

### Load summaries

Every evaluation window (10 minutes) the strategy reduces the samples of the
replicas at the realized size, hydrated and past warm-up, to one summary:

```
summary {
  at, size, fingerprint,
  per dataflow: median lag, progress ratio, behind
  heap:  steady-state heap bytes and fraction
  busy:  mean busy millicores over the window, busiest-worker utilization
  killed: whether a replica at the realized size was killed for memory
}
```

Hydration is a state of the whole replica. A replica on which any dataflow
is hydrating is hydrating, and contributes nothing to a summary: its workers
are busy and its other dataflows may lag, and both are expected. This holds
for a new replica and for a running one on which an object was just created.
A replica qualifies for a window when it qualifies at every sample taken in
it. A window in which no replica qualifies produces no summary, with two
exceptions that are recorded so that later rules can read them:

- A window in which a replica at the realized size was hydrating is recorded
  as a **hydrating window**. It carries no lag, heap, or busy fields.
- A window in which a replica at the realized size was killed for memory is
  recorded with `killed` set, whatever else it carries.

A window needs samples for at least half of its sample intervals, with at
least one in its first third and one in its last third. With fewer it
produces no summary.

Heap and busy time are taken from the replica with the largest steady-state
heap and from the busiest replica, since every replica has to fit.

A materialized view with a refresh schedule advances its output only at its
refreshes, so its lag says nothing about the cluster. Its dataflow is left
out of the summaries. It still counts toward the fingerprint.

The cluster's lag for a dataflow is the smallest among its replicas, since
replicas do the same work and the freshest one answers.

The **progress ratio** of a dataflow over an interval is how far its output
frontier advanced divided by how far its inputs advanced. It says whether lag
is growing, steady, or shrinking. It does not say how large the lag is. A
dataflow at a steady 30 seconds of lag has a ratio of 1.

The **fingerprint** identifies the set of dataflows installed on the cluster.
It is a hash of the sorted identifiers of the collections the cluster's
dataflows export.

Summaries are appended to two relations and read back at startup, so a
restart of `environmentd` does not empty the quiet window.
`mz_internal.mz_cluster_load_history` has one row per cluster per evaluation
window, with the heap, busy, and kill fields, and the cluster's last hydration
time at its size, so a restart does not lose it either.
`mz_internal.mz_cluster_load_dataflow_history` has one row per dataflow per
evaluation window, with its lag, progress ratio, and whether it was behind.
Both are retained for the longest window any rule reads, 7 days.

### When a dataflow is behind

A dataflow is **behind** in a summary when all of these hold:

1. Its median lag over the window exceeds its threshold. The threshold is the
   tolerance `τ` (default 2s), or 1.5 times the dataflow's lag floor when it
   has one and that is larger.
2. It is not draining. A dataflow drains when the median lag of the last
   third of the window is below half that of the first third, or when its
   progress ratio over the window is at least 1.1. The second test covers a
   backlog too large to halve within one window. A dataflow working off a
   backlog needs time and not hardware.

The median over ten minutes of 10 second samples is what makes this robust.
A dataflow whose frontier advances in steps, or that pauses for a merge, has
a lag that swings. Its median does not depend on where a sample boundary
falls.

A **lag floor** is the lag a dataflow showed on a larger size during an
up-trial that did not help it. It is the best lag hardware was seen to buy
that dataflow. A dataflow at its floor is not behind in any sense a size
change addresses. Floors are what let one dataflow with a hot key coexist
with a policy for the rest of the cluster.

### Evidence

Durable, per size, for the current fingerprint:

```
evidence {
  size,
  verdict:  Insufficient | NoGain,
  at:       when the verdict was reached,
  interval: how long the verdict blocks its trial
}
```

- `Insufficient(S)`: the cluster did not keep up at `S`, or did not fit. It
  blocks a down-trial to `S` or anything smaller until `at + interval`.
- `NoGain(S)`: an up-trial from `S` found nothing fresher on the larger size.
  It blocks an up-trial from `S` until `at + interval`.

The interval adapts to how the verdict recurs. The first verdict against a
size gets the base interval, `W` for `Insufficient` and 24h for `NoGain`. A
verdict that recurs within 4 times its interval of the previous one doubles
the interval, up to 7 days. A verdict that recurs later than that gets the
base interval again.

This is the whole oscillation policy. A workload that spikes once a day with
`W` of an hour scales up for each spike and down after it. That is the
cluster following its workload. A workload that spikes every three hours
doubles the interval at each recurrence until descents become rare, and the
cluster stays up. Nothing has to be configured to tell the two apart.

There is at most one entry per size and verdict. An entry is kept after it
stops blocking, because the next verdict against that size needs its `at` and
`interval` to decide whether to double. A new verdict replaces the entry.

Evidence and floors are cleared when the fingerprint changes or the user
changes the policy. They describe sizes for a workload that no longer exists.

### Scale up: the up-trial

**Trigger.** Some dataflow is behind in the latest summary, the cluster is
below `MAX SIZE`, and no `NoGain` verdict blocks the current size.

**Candidate.** At least the next size up. When behind dataflows have a
progress ratio below 1, their backlog is growing and one step may be too
little, so the candidate is the cheapest size with at least
`cores(current) / ρ` cores, where `ρ` is the lowest progress ratio among the
behind dataflows, floored at 0.25. The estimate only chooses what to try. An
overshoot is corrected by the scale-down path.

**Procedure.** The strategy writes a reconfiguration record for the candidate
with an up-trial. The graceful reconfiguration strategy creates the candidate
replicas beside the current ones. From the moment they are hydrated the trial
holds for the up-trial duration (default 5 minutes) and then reaches a
verdict, once.

**Verdict.** Let `B` be the dataflows that were behind at the trigger. Over
the hold, a dataflow in `B` **gained** on the candidate when any of these
holds:

- *Fresh.* Its median lag on the candidate is within the tolerance and it is
  not falling behind there.
- *Faster.* Its progress ratio on the candidate exceeds its ratio on the
  current replicas by at least 0.1. This is the case of a backlog that grows
  on the current size.
- *Closer.* Its progress ratio is within 0.05 of 1 on both, and its median
  lag on the candidate is at most half its median lag on the current
  replicas. This is the case of a dataflow that holds a steady lag at each
  size, where only the level differs.

Three cases are needed because overload has two shapes. A dataflow can fall
further behind without bound, or it can settle at a lag where larger batches
make each update cheap enough to keep pace. A test on the ratio alone misses
the second shape, and a test on the level alone is fooled in the first,
where the candidate's lag is lower only because it started later.

- **Some dataflow in `B` gained.** The cluster cuts over. `Insufficient` is
  recorded for the size it left. Each dataflow in `B` that did not gain gets
  a lag floor, its median lag on the candidate.
- **No dataflow in `B` gained.** The candidate is dropped and the cluster
  never changed size. `NoGain` is recorded for the current size. Every
  dataflow in `B` gets a lag floor, its median lag on the current replicas.
  The status is `not-scalable`.

A cluster that cut over and still has dataflows behind without a floor is
eligible for another up-trial at the next summary. A cluster that needs four
times its size gets there in one or two trials.

The current replicas keep serving throughout. While the candidate is fresher
it also answers queries, so users see the improvement during the hold.

### Scale down: the down-trial

A down-trial to the next smaller size `S` is **proposed** when all of these
held over the quiet window. Its length is `W`, or twice the cluster's last
hydration time if that is longer.

Hydrating windows do not count toward the quiet window. It is made of the
most recent evaluation windows that are not hydrating windows, taken back
from now until they add up to its length, and reaching back at most twice
its length. A hydration therefore delays a descent and does not void the
observation before it. A cluster that hydrates for more than half of the
time never fills a quiet window, and has not shown that it keeps up.

The quiet window must be observed: summaries exist for at least 90% of the
evaluation windows it is made of.

1. No dataflow was behind in any summary, and no replica was killed for
   memory. A dataflow at its lag floor is not behind, so a cluster that is
   limited by something other than hardware can still descend.
2. No evidence blocks `S`.
3. The largest steady-state heap in the window would be at most 70% of the
   heap limit of `S`. With a known ratio of hydration peak to steady state,
   the projected peak must also fit the limit.
4. The 95th percentile of the summaries' busy time would fit the cores of `S`
   with a margin (default 80%).

Conditions 3 and 4 decide what is worth trying. They commit nothing. The
70% keeps a margin under the 80% bound for the error of projecting heap from
one size to another. The percentile keeps one burst from a query or a merge
from blocking a descent for a whole window.

The descent is one size at a time. A cluster far above its right size
descends once per quiet window. Each step is a hydration, and a trial two
sizes down that fails costs the same hydration and teaches less.

**Procedure.** The candidate replicas are created beside the current ones.
The trial has a deadline that is set when it starts: a hydration budget of 4
times the cluster's last hydration time, at least 30 minutes, plus the hold
(default 15 minutes). An up-trial uses the same budget with its own hold. The hold begins when the candidate is hydrated. At its
end the trial reaches a verdict, once.

**Verdict.** The candidate **passes** when all of these hold over the hold:

- for every dataflow, its median lag on the candidate is within the
  tolerance of its median lag on the current replicas,
- the same is true of the last third of the hold, so a candidate that is
  slowly falling behind does not pass on its early samples,
- its steady-state heap stayed below 80%,
- it was not killed for memory.

A candidate that passes is cut over to. A candidate that fails, is killed, or
has not hydrated when its hydration budget ends is dropped, and
`Insufficient(S)` is recorded. There is no retry within a trial. A trial that could restart its
hold until it passed would test the quietest quarter hour of the day.

**Probation.** For `W` after a descent, measured from `last_cutover`, the
size it left is known to have been sufficient for this workload. If a dataflow goes behind during
probation, the cluster returns to that size with a plain reconfiguration and
no trial, and `Insufficient` is recorded for the smaller size. If the
dataflow is still behind after the return, the ordinary up-trial applies.

### Memory

Memory rules move the cluster up without a trial. They ignore `NoGain`
evidence, which is about lag.

- **High heap.** The steady-state heap of every summary over the high-heap
  period (default 20 minutes, two summaries) is above 80%. The cluster moves
  to the smallest size at which the heap would be at most 70%.
  `Insufficient` is recorded for the size it left.
- **Killed.** A replica at the realized size was killed for memory. A killed
  replica cannot report what it needed, so the cluster moves at once to the
  next size with at least twice the heap limit. `Insufficient` is recorded
  for the size it left. A kill at `MAX SIZE` is reported as
  `out-of-memory-at-max`.

The two memory thresholds cannot chase each other. A cluster moves up above
80% of a size, which on a ladder that doubles is above 40% of the next. It is
proposed back down only at or below 70% of the smaller size, which is 35% of
the current one.

The resulting band is the requirement. A descent happens only to a size
where the heap is expected at or below 70% and observed below 80%. A
cluster stays above its smallest memory fit only when lag evidence says it
needs the workers.

### Statuses

`steady`, `trial-up`, `trial-down`, `scaling-up` (memory or probation
return), `not-scalable`, `waiting` (a descent is otherwise due and evidence
blocks it), `at-max`, `at-min`, `out-of-memory-at-max`, and `warming-up`.

### Convergence

For a fixed workload and fingerprint, let `L` be the largest size with live
`Insufficient` evidence and `U` the current size once no dataflow without a
floor is behind.

- Every up-trial either raises the current size or ends in `NoGain`, which
  stops up-trials. So the ascent ends, at a sufficient size, at `MAX SIZE`,
  or at `not-scalable`.
- Every down-trial either lowers `U` by one size or raises `L` to just below
  it. So the descent ends with `L` and `U` adjacent, which is the smallest
  sufficient size.
- The cluster then holds until evidence expires. Each expiry allows one
  trial. If the workload has not changed the trial repeats its verdict and
  the interval doubles, up to 7 days. So a stable workload costs at most one
  repeated trial per week per direction.

For a workload that changes, evidence that has expired is the mechanism by
which the cluster finds the new right size, and the doubling is what keeps a
recurring pattern from costing a round trip each time.

### The strategy

`OnLoadStrategy` implements the `Strategy` trait.

- `signal_request` asks for the load signal when the cluster has an `ON LOAD`
  policy and the break-glass flag is on.
- `update_state` evaluates the rules as a pure function of the cluster
  state, the summaries, the trial statistics, and `now`. It writes
  reconfiguration records and maintains the `load` record.
- `desired_replicas` returns nothing. Graceful reconfiguration contributes
  the candidate replicas.

`GracefulReconfigurationStrategy` carries out a record with a trial. It keeps
the candidate beside the current replicas until the trial's verdict, cuts
over on a pass, and drops the candidate on a fail. Records without a trial
behave as they do today.

Durable state:

```
load: Option<LoadRecord> {
    fingerprint,
    attempt:      the strategy's reconfiguration whose outcome is not yet
                  recorded, with the dataflows behind at its trigger
    last_cutover: when the strategy last changed the size, from which size,
                  and why
    evidence:     [{ size, verdict, at, interval }]
    floors:       [{ dataflow, lag }]
    hydration_peak_ratio: hydration peak over steady-state heap, when seen
}
```

and on the reconfiguration record:

```
origin: User | LoadAutoscaling
trial:  Option<{ kind: Up | Down, hold_for, deadline, hold_started }>
```

A record with a trial has two instants. The record's own `deadline` is the
end of the hydration budget. A candidate that is not hydrated by then is
dropped. A down-trial records `Insufficient` for it, and an up-trial records
nothing. The trial's `deadline` is the end of the hydration budget plus the
hold, and it is the latest instant at which the candidate can exist.

`hold_started` is written when the candidate is hydrated and the hold begins.
The per-sample lag of the candidate and current replicas during a hold is
kept in memory. If `environmentd` restarts during a hold, the samples are
gone, so the hold starts over and `hold_started` is rewritten. A hold that
cannot finish before the trial's deadline is not started again. The trial is
dropped at the deadline without recording evidence, because no verdict was
reached.

The last status is kept in memory. The first evaluation after a restart of
`environmentd` sets it without writing a status change to the audit log.

### Observability

- **`mz_internal.mz_cluster_load_history`** and
  **`mz_internal.mz_cluster_load_dataflow_history`.** The summaries.
- **`mz_internal.mz_cluster_load_recommendations`.** One row per managed
  cluster, with or without a policy: the status, the dataflows behind with
  their lag and floor, heap, busy time, and what the rules would do next.
  Computed regardless of `enable_load_autoscaling`.
- **Audit log.** A trial's start and its verdict, with the per-dataflow lag
  and progress ratio on both sizes and which case decided it. Evidence
  recorded and cleared. Status changes.
- **`mz_cluster_auto_scaling_strategies`** shows the policy, evidence, and
  floors.

### Dyncfgs

| Key | Default | Meaning |
|---|---|---|
| `enable_load_autoscaling` | off in production | break-glass for acting |
| `load_autoscaling_lag_sample_interval` | 10s | how often lag is sampled |
| `load_autoscaling_evaluation_window` | 10min | span of one summary |
| `load_autoscaling_lag_tolerance` | 2s | `τ` |
| `load_autoscaling_floor_factor` | 1.5 | multiple of its floor above which a dataflow is behind |
| `load_autoscaling_draining_progress_ratio` | 1.1 | progress ratio over a window at or above which a dataflow is draining |
| `load_autoscaling_min_progress_ratio` | 0.25 | floor on `ρ` when sizing an up-trial |
| `load_autoscaling_up_trial_duration` | 5min | hold of an up-trial |
| `load_autoscaling_down_trial_duration` | 15min | hold of a down-trial |
| `load_autoscaling_heap_max` | 0.8 | heap a selected size may not exceed |
| `load_autoscaling_heap_proposal_max` | 0.7 | projected heap a size is proposed at |
| `load_autoscaling_heap_high_duration` | 20min | how long heap must stay above the maximum |
| `load_autoscaling_cpu_margin` | 0.8 | share of a candidate's cores busy time may use |
| `load_autoscaling_default_scale_down_after` | 1h | `W` |
| `load_autoscaling_quiet_window_coverage` | 0.9 | share of the quiet window's evaluation windows that must have a summary |
| `load_autoscaling_no_gain_interval` | 24h | base interval of `NoGain` |
| `load_autoscaling_max_evidence_interval` | 7d | cap on an evidence interval |
| `load_autoscaling_warm_up` | 5min | samples ignored after a size change |
| `cluster_load_history_retention_interval` | 7d | how long summaries are retained |

### Rollout

1. **Shadow.** Signals, summaries, and recommendations, with no actions.
   Measure the lag healthy dataflows show at rest, to confirm the tolerance.
2. **Scale up.** Up-trials and the memory rules.
3. **Scale down.** Down-trials and probation.
4. **Sources and sinks**, once they have a lag signal and a busy signal.

## Simulation

The decision function is pure and the controller loop is deterministic, so
the policy is validated in a closed-loop simulator before it runs against a
cluster. The simulator drives the real strategies against synthetic replicas.

### Replica model

Each simulated dataflow `d` on a replica of `n` cores has:

- **Demand** `D_d(t)`, the work arriving per second, as a function of time.
- **Parallel capacity** `n^α`, with `α <= 1` for returns that diminish.
- **Skew** `s_d`, the share of the dataflow's work that lands on one worker.
  That worker's capacity is 1 at any size, so skew caps what size can buy.
- **Batching gain** `g_d >= 1`. Work per update falls as lag grows, down to
  `1/g_d` of its cost when fresh. With `g_d > 1` an overloaded dataflow
  settles at a steady lag when demand is within `g_d` times capacity, and
  falls behind without bound past that.
- **Inherent lag** `f_d`, a lag the dataflow shows at any size.
- **Lag noise**: a sawtooth of a given amplitude and period.

The replica has:

- **Heap** `H(t, n)`: a base that may grow over time, plus an overhead per
  worker.
- **Hydration**: a duration that shrinks with `n`, and a heap peak that is a
  multiple of the steady state.
- **Faults**: a share of busy windows that never arrive, replicas that
  restart, and `environmentd` restarts at given times.

### Invariants checked in every run

- The size is never outside the range.
- No size is selected whose projected heap exceeds 80%, and no down-trial
  passes with observed heap at or above 80%.
- A trial reaches at most one verdict, and its candidate is gone after its
  deadline.
- No trial starts while evidence blocks it.
- After a scale-up from `S`, no scale-down to `S` happens within `W`.
- Lag served to queries never worsens because of a trial that failed.

### Cases

Each case runs from every starting size in range unless it says otherwise.

| # | Workload | Asserts |
|---|---|---|
| 1 | Constant demand, `α = 1`, no skew, `g = 1` | Reaches the smallest size that keeps up and fits, then no further size change. At most one repeated trial per evidence lifetime. |
| 2 | As 1 with `α` of 0.9, 0.8, 0.6 | Same. The CPU filter may skip no descent that would pass by more than one size. |
| 3 | Steady-lag overload: `g = 3`, demand 1.5 times capacity at the start size, lag settles at 30s there and at 8s one size up | The up-trial commits by the *closer* case. A second up-trial follows. The cluster ends at the size that is fresh. It is never moved back down while behind. |
| 4 | Diverging overload: `g = 1`, demand 4 times capacity | The up-trial candidate is sized from the progress ratio. Fresh within two trials. |
| 5 | One dataflow with skew 1 behind at every size, four dataflows behind that scale | The up-trial commits because the four gained. The skewed dataflow gets a floor. No further up-trial for it. The four are fresh at the end. |
| 6 | Only the skewed dataflow of case 5, on 2, 4, and 16 workers | One up-trial, verdict `NoGain`, status `not-scalable`, size unchanged. Next up-trial no sooner than 24h, then 48h. Busy time plays no part: the result is the same on 2 workers. |
| 7 | As 6, started three sizes above the minimum | `not-scalable` and then descends one size per quiet window to the smallest size with the same lag, since a dataflow at its floor does not block descent. |
| 8 | A dataflow with inherent lag of 5s at every size, otherwise idle | One up-trial, `NoGain`, floor of 5s. The dataflow is then not behind. The cluster descends to the minimum. |
| 9 | Demand between the capacities of two sizes for 22h a day, above the larger for 2h, `W = 1h` | One scale-up and one scale-down per day. Interval for the smaller size stays at `W`. Stale time per day is bounded by detection plus hydration. |
| 10 | As 9 with the spike every 3h | The interval of the smaller size doubles at each recurrence. Size changes per day fall over the run. The cluster ends holding the larger size. |
| 11 | A smaller size that keeps up at night and not by day, with the descent first due by day | The down-trial fails once and the candidate is dropped at the verdict. No candidate outlives its deadline. No cut-over to the smaller size during the day. |
| 12 | A smaller size that falls behind by 0.2% of input, too slowly for a 15 minute hold to show | The down-trial passes. Probation returns the cluster within `W`. `Insufficient` is recorded and the interval doubles on the next occurrence. |
| 13 | Step up in demand to 3 times, held | Up within one evaluation window plus hydration plus the hold. No descent afterwards. |
| 14 | Step down in demand to a third, held | One size down per quiet window until the smallest fit. |
| 15 | Lag sawtooth of 1s to 6s around a median of 1.5s, fresh in the median | No up-trial. Descent is not blocked. |
| 16 | Lag sawtooth around a median of 6s, with a period between 4 sample intervals and one evaluation window | Behind in every summary whatever the phase of the sawtooth against the sampling. |
| 17 | Half the busy windows never arrive, and none arrive while a dataflow is behind | Lag decisions are unchanged from the same run with all windows. Descent proposals wait for busy data and no descent happens without it. |
| 18 | Heap fixed at 60% of the next smaller size, CPU idle | Descends. Heap fixed at 75% of the next smaller size: does not descend. |
| 19 | Heap growing 5% an hour | Moves up by the high-heap rule before any kill, to a size at or below 70%. Never proposed back down while growth continues. |
| 20 | Heap that jumps past the limit within one summary | The kill rule moves up to at least twice the limit. `Insufficient` blocks the return. |
| 21 | Heap that grows without bound | Climbs to `MAX SIZE`, reports `out-of-memory-at-max`, starts no trials. |
| 22 | Hydration peak of 2 times steady state, steady state at 60% of the smaller size | With a known peak ratio the descent is not proposed. With an unknown ratio the candidate is killed in the trial, the trial fails, and the cluster never left its size. |
| 23 | CPU needs four sizes above what memory needs | Settles at the CPU size with heap below 50%. No descent. |
| 24 | Heap overhead per worker large enough that heap is higher at the smaller size than projected | The down-trial fails on observed heap at or above 80%. |
| 25 | A dataflow is created on a settled cluster and needs a larger size | Evidence and floors are cleared. Up-trial, cut-over. |
| 26 | The dataflow of case 25 is dropped | Evidence is cleared and the cluster descends, including to sizes that were `Insufficient` before. |
| 27 | `environmentd` restarts every 6h with `W = 1d` | The quiet window survives the restarts and the descent happens on schedule. A restart during a hold restarts the hold and the trial still ends by its deadline. |
| 28 | A replica at the realized size crash-loops for a reason that is not memory | No summaries, so no trial and no size change. Status `warming-up`. |
| 29 | A user alters the size during a trial | The trial is abandoned without evidence. The user's size holds. |
| 30 | Two replicas, one persistently slower | The freshest speaks for the cluster. No up-trial while it is fresh. |
| 31 | An object is created on a settled cluster and takes 30 minutes to hydrate, with workers saturated and other dataflows lagging meanwhile | No summaries during the hydration, so no trial and no size change because of it. The backlog afterwards drains and does not count as behind. |
| 32 | An idle, oversized cluster on which an object is created every hour and hydrates for 10 minutes, `W = 1h` | The hydrating windows are skipped, the quiet window fills from the windows around them, and the cluster descends, later than it would without the hydrations. With an object created every 15 minutes the quiet window never fills and no descent is proposed. |

Cases 3, 5, 11, and 17 fail against a policy that scales up on a progress
ratio alone, tracks only the slowest dataflow, lets a trial restart its
hold, or ties lag to the delivery of busy windows. They are the regression
tests for those four mistakes.

Two sawtooth periods fall outside case 16 and are known limits of the
signal. At a period of exactly twice the sample interval the samples can
land on the same two points of every cycle, and the median reads low. At a
period longer than the evaluation window, a window that contains the reset
reads as draining. In both, the dataflow is behind in some summaries and not
in others.

### Beyond the simulator

The simulator's model is an assumption about replicas. Three mzcompose
scenarios check the parts of it that matter most against a real cluster:

- A hot-key dataflow on a 2-worker and a 4-worker size, asserting `NoGain`.
- A workload sized between two sizes with a load step, asserting the size
  sequence and that a failed down-trial is invisible to queries.
- A dataflow driven to overload, recording whether its lag settles or
  diverges, at two sizes. This measures the batching gain the model assumes.

## Minimal Viable Prototype

The shadow phase. Summaries and recommendations need only the signals, and
the recommendations can be compared with the sizes users chose by hand and
with known freshness incidents before the strategy acts.

## Alternatives

**Decide scale-up from measurements and revert if it did not help.** Scale
up on sustained lag, size the step from the progress ratio, and go back if
the ratio has not improved. A cluster that is not helped pays two hydrations and
serves from a size it then leaves. The ratio cannot see an improvement in a
lag that is steady at both sizes. The up-trial costs one hydration and
compares the sizes on what the user cares about.

**Veto up-trials on low utilization.** A cluster whose workers are mostly
parked and which is behind is usually limited by one worker. Skipping the
trial would save a hydration. It fails on small sizes, where one hot worker
of two is half the cluster, and it assumes what the trial would measure. The
evidence interval already bounds the cost to one trial per day and then per
week.

**A utilization target.** It is a proxy for the objective and has no right
value.

**Descend several sizes at once.** Faster when a cluster is far too large,
and each failed guess costs a hydration without locating the boundary.

**A learned window in place of evidence per size.** One window per cluster
cannot represent a workload for which one size is reliably too small and the
next is reliably enough.

**Predictive scaling.** Complementary. It addresses the staleness during the
hydration of a scale-up, which nothing reactive can.

## Open questions

1. **Does overload settle or diverge?** The *closer* case exists because an
   overloaded dataflow is assumed to settle at a steady lag. The third
   mzcompose scenario measures it. If dataflows only diverge, the case is
   unused and harmless.
2. **What counts as busy.** Whether non-parked time includes idle merge
   effort, peeks, and time in page faults decides how much the CPU filter
   blocks. A breakdown of one busy window on a real replica answers it.
3. **Is heap against memory plus disk a measure of pressure?** At 70% of
   that sum a replica may be deep in swap, depending on the ratio of disk to
   memory at each size. Lag covers the case where swap makes the replica
   slow. Whether it covers it early enough needs lag measured against swap
   use.
4. **Hydration peak.** How far above steady state heap goes while a smaller
   size hydrates. It decides whether 70% is the right proposal bound.
5. **The tolerance.** 2 seconds has to exceed the lag a healthy dataflow
   shows at rest. The shadow phase measures that per dataflow. A dataflow
   with a higher resting lag costs one futile up-trial and then has a floor.
6. **Is a trial representative?** A candidate shares persist writes and peeks
   with the current replicas during a trial and carries them alone after.
   Probation covers a candidate that passes and then cannot. Whether it
   happens often is a question for the shadow phase and the second scenario.
7. **Memory kills caused by a query.** A kill carries no cause. One runaway
   query doubles the cluster, and the descent then takes a quiet window per
   size. Attributing a kill to a peek would need a signal that does not
   exist.
8. **Summaries in memory.** The coordinator holds 7 days of summaries per
   cluster, each with a row per dataflow. Whether that needs a bound in
   environments with many dataflows is a question for the shadow phase.
9. **Up-trial hold.** 5 minutes is short enough to act and may be too short
   to tell *closer* from noise. The simulator's sawtooth cases bound it.
