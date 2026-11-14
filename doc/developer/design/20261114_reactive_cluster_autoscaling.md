# Reactive Cluster Autoscaling

- Associated: (TBD, link epics/issues)
- Builds on: [Cluster Autoscaling and Background Reconfiguration](20260522_cluster_autoscaling.md)

## The Problem

A cluster's `SIZE` is chosen by a human, once, for a workload that changes.
Users size for peak and overpay the rest of the time, or size for the average
and fall behind during spikes. Finding the right size today means reading
`mz_cluster_replica_utilization`, guessing, issuing `ALTER CLUSTER`, waiting
for rehydration, and repeating. Each iteration costs a full hydration, so a
wrong guess is expensive in both directions.

The cluster controller from the [cluster autoscaling design](20260522_cluster_autoscaling.md)
gives us the mechanism: durable, background, zero-downtime size changes with a
readiness gate and rollback. It has strategies for hydration (`ON HYDRATION`)
and refresh windows (`ON REFRESH`). It has no strategy that reacts to load, and
its signal interface carries no load signals.

What is missing is the decision: when to change size, and to which size. That
is the question DS2 answers for streaming dataflows
([Kalavri et al., OSDI 2018](#references)). This document adapts the DS2 model
to Materialize and specifies it as a new strategy in the cluster controller.

### What DS2 says, and what transfers

DS2's core claim is that coarse, externally observed metrics (CPU utilization,
backpressure, observed throughput) cannot tell a controller *how much* to
scale, only that something is wrong. Controllers built on them make small
speculative steps and take many reconfigurations to converge, or oscillate.
DS2 instead measures each operator instance's **useful time**, the time spent
doing work as opposed to waiting for input or output. Records processed divided
by useful time is the operator's **true rate**, its capacity. Given the true
rates and the source rates, the parallelism every operator needs follows in one
traversal of the dataflow graph. Under linear scaling the estimate never
overshoots, and under sub-linear scaling it converges monotonically in a few
steps (three or fewer in every experiment in the paper).

Three properties of Materialize change how the model applies.

1. **Parallelism is global.** Timely workers each run every operator, so there
   is no per-operator parallelism to assign. The paper covers this case
   (§4.3): the number of workers needed is the sum of the per-operator
   optimal parallelism. That sum has a simple closed form, derived
   [below](#the-demand-model), that needs busy time and progress but no
   record counts or selectivities.
2. **Reconfiguration costs a rehydration.** DS2 assumes a savepoint and restart
   measured in tens of seconds. Our mechanism builds a new replica and
   hydrates it from persist, which takes minutes to hours and bills for the
   overlap. The settling time is dominated by the mechanism, which makes
   DS2's one-step accuracy more valuable here than in Flink, and makes every
   unnecessary decision more expensive. Decisions must be few and damped.
3. **Memory is a first-class constraint.** DS2 models CPU only. A Materialize
   replica that runs out of memory does not slow down gracefully, it crash
   loops. Arrangement size does not shrink when workers are added. A size
   decision therefore has to satisfy a CPU demand and a memory demand
   independently, which is the extension Justin makes to DS2 for Flink
   ([Schmitz et al., 2025](#references)).

Actions are also coarser. The only knob is `SIZE`, a discrete ladder where each
step roughly doubles capacity. Adding replicas adds fault tolerance, not
capacity, because every replica does all the work.

## Success Criteria

- A user can give a cluster a size range and stop thinking about size. The
  cluster keeps up with its inputs within that range and does not stay
  oversized for long once load drops.
- **Accuracy.** From a stable workload and an arbitrary starting size, the
  controller reaches the smallest size in range that keeps up within three
  reconfigurations, and usually one.
- **Stability.** A stable workload produces no size changes. The controller
  never alternates between two sizes.
- **Safety.** An autoscaling decision never makes a cluster unavailable. A
  scale-down that cannot hydrate or keep up is rolled back and not retried
  blindly.
- **Bounded cost.** Steady-state spend never exceeds the configured maximum
  size. Transitions bill for overlap, as manual graceful reconfigurations do.
- **Explainability.** Every decision, and every decision *not* taken, is
  attributable to recorded signal values a user can query.
- Operators can disable the strategy environment-wide without affecting the
  other strategies.

## Out of Scope

- **Predictive or scheduled scaling.** This design reacts to observed load. It
  does not forecast diurnal patterns.
- **Changing `REPLICATION FACTOR`.** Replica count is an availability choice
  and stays with the user.
- **Scale to zero.** Turning an idle cluster off and on is a different
  product decision with different availability semantics.
- **Skew mitigation.** A dataflow limited by one hot worker cannot be fixed by
  a larger size. We detect and report this, and do not try to fix it.
- **Finer-grained mechanisms.** In-place pod resizing and live state migration
  would change the cost model. See [Alternatives](#alternatives).
- **Combination with `SCHEDULE = ON REFRESH`.** Rejected at plan time, like
  `ON HYDRATION`.

Storage objects (sources and sinks) are in scope for the design and deferred
in the rollout. See [Rollout](#rollout).

## Solution Proposal

### Summary

Add an `ON LOAD` autoscaling strategy to the cluster controller. Each policy
interval it computes, per cluster, a **CPU demand** from worker busy time and
frontier progress (the DS2 model), and a **memory demand** from peak memory
use. It maps the pair to the cheapest size in the user's range that satisfies
both. When that recommendation differs from the current size and has held for
long enough, the strategy writes the same durable `reconfiguration` record that
`ALTER CLUSTER ... SET (SIZE)` writes. The existing graceful reconfiguration
strategy does the rest: overlap, readiness gate, cut-over, and rollback on
timeout.

The new parts are a load signal, a pure decision function, a small durable
record for backoff, and a recommendations relation. Nothing about replica
lifecycle changes.

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
- `SIZE` remains the realized size, as in the existing design. With `ON LOAD`
  set, the controller moves it. `mz_clusters.size` and `SHOW CREATE CLUSTER`
  show what is running now.
- A user `ALTER CLUSTER ... SET (SIZE)` is still allowed inside the range. It
  acts as a manual override that the autoscaler continues from. A user-written
  reconfiguration always wins over an autoscaler-written one, and the
  autoscaler never overwrites an in-progress user record.
- `ON LOAD` composes with `ON HYDRATION`. They are complementary: burst gets a
  cluster hydrated quickly, and load scaling fixes the case the burst design
  accepts as a limitation, where the steady size cannot keep up and the burst
  replica never retires.

All other tuning lives in dyncfgs. The paper's lesson about threshold tuning
applies to our users as well. A policy that needs per-cluster tuning to be
correct has failed.

### The demand model

Take a replica with `W` workers observed over a window of length `T`.

For operator `i`, DS2 defines the true processing rate per instance as records
processed over useful time, and the required parallelism as the target input
rate over that true rate (Eq. 7). Write `B_i` for the operator's busy time
summed over workers, `R_i` for the records it processed, and `r_i` for the
rate it would see if everything upstream kept up. Then

```
λ_i = R_i / B_i                                  (true rate per instance)
π_i = r_i / λ_i = (r_i · T / R_i) · (B_i / T)    (instances needed)
```

The second factor, `B_i / T`, is the number of cores the operator used. The
first factor is the ratio of the rate it should have processed to the rate it
did process. With shared workers the cluster needs `Σ π_i` workers.

The ratio `r_i · T / R_i` is where DS2 needs source rates and operator
selectivities. Materialize has a more direct measurement. Every dataflow's
work is indexed by timestamp, and both its input frontier and its output
frontier are known to the controller. Define the **progress ratio** of dataflow
`d` over the window as

```
ρ_d = Δ(output frontier) / Δ(input frontier)
```

A dataflow that keeps up has `ρ_d = 1`. A dataflow that advanced 30 seconds of
timestamps while its inputs advanced 60 has `ρ_d = 0.5`: it processed half of
the work that arrived, so every operator in it has `r_i · T / R_i = 1 / ρ_d`.
Operators in one dataflow share one ratio, so we only need busy time per
dataflow. The CPU demand in cores is

```
D_cpu = Σ_d (B_d / T) / ρ_d  +  overhead
```

where overhead is worker time not attributed to any operator (progress
tracking, communication), scaled by the same aggregate factor. When everything
keeps up, `D_cpu` is simply the busy cores. When the cluster is falling
behind, each lagging dataflow's share is inflated by how far short it fell.
This is DS2's Eq. 7 summed over operators, with the frontier doing the job of
the rate instrumentation.

Three refinements:

- **Clamp.** `ρ_d` is clamped to `[ρ_min, 1]`, default `ρ_min = 0.25`. A
  dataflow whose output frontier did not move at all gives no information
  about how much capacity it needs, only that it needs more. The clamp bounds
  a single step at 4x capacity and lets the next step refine, as DS2 does
  when its first estimate is censored.
- **Local progress only.** The input frontier is the minimum over the
  dataflow's imports. A cluster whose inputs are stalled upstream has
  `Δ(input) = 0`, is treated as `ρ_d = 1`, and is not scaled for a problem it
  does not own.
- **Catch-up.** A dataflow that is behind by local lag `L` needs extra
  capacity to recover. Its term is multiplied by `1 + L / T_catchup`, the
  rule the Flink autoscaler added on top of DS2. Default `T_catchup = 15min`.

The memory demand is the peak, over the scale-down window, of heap plus swap
bytes on the busiest process, scaled to the whole replica:

```
D_mem = max over window (heap_bytes + swap_bytes)
```

Memory is taken as independent of worker count. That is the conservative
direction for scale-up and is checked by the readiness gate on scale-down.

### From demand to a size

Each size on the ladder has a CPU capacity `cores(s)`, the effective cores
across its processes, and a memory capacity `mem(s)`, both already in
`mz_cluster_replica_sizes`. The recommendation is

```
recommend = cheapest s in [MIN SIZE, MAX SIZE] such that
              cores(s) · u_target >= D_cpu   and
              mem(s)   · m_target >= D_mem
```

clamped to `MAX SIZE` if none fits. `u_target` (default 0.7) is headroom:
latency in a timely worker grows sharply as it approaches saturation, and the
model's linear scaling assumption is optimistic across process boundaries.
`m_target` (default 0.8) keeps steady state in memory and leaves swap as a
buffer for bursts.

Because each ladder step roughly doubles capacity, hysteresis falls out of the
discretization. A cluster at `u_target` after a downsize was at half of
`u_target` before it. Scale-down therefore triggers at about 35% utilization
and scale-up at saturation or lag, with no separately tuned low-water mark.
This is also DS2's "ignore minor changes" rule for free.

### Decision rules

The strategy evaluates one **sample** per policy interval (default 60s) and
keeps a window of samples. A sample is only taken from replicas that are at
the realized size, hydrated, and past a warm-up period after hydration. Rates
measured during hydration describe hydration, not the workload.

**Scale up** when the median recommendation over the activation window
(default 3 intervals) is larger than the current size. Using the median over a
few intervals follows DS2's activation time and filters single-interval
spikes.

**Scale up immediately** when a realized-size replica is OOM-killed. The
measurement is censored, so the target is the next size with at least twice
the memory, and the activation window is skipped.

**Scale down** when the *maximum* recommendation over the scale-down window
(`SCALE DOWN AFTER`, default 1h) is smaller than the current size, every
sample in the window had all dataflows keeping up, and no reconfiguration is
in flight. Taking the maximum means one busy interval in the window blocks
the downsize. The asymmetry is deliberate: being too small costs freshness
and being too large costs money for an hour. The window is floored at twice
the cluster's last observed hydration time, so we never spend more time
transitioning than we spend at the result.

**Do nothing, and say why,** in these cases:

- *Skew-limited.* A dataflow is falling behind, the busiest worker is
  saturated, and the recommendation is not larger than the current size. Mean
  utilization is low because one worker holds the hot key. More workers will
  not help. DS2 behaves the same way under skew (§4.2.3): it will not
  over-provision, and it does not reach the target.
- *At bound.* The recommendation is clamped by `MIN SIZE` or `MAX SIZE`.
- *Backing off.* See below.

**Backoff.** Two outcomes mark a target size as recently failed:

- An autoscaler-written reconfiguration ends `TimedOut` or
  `ResourceExhausted`. For a scale-down this is the expected failure mode of a
  target that cannot hold the hydration peak or cannot meet the lag gate. The
  existing rollback returns the cluster to the old size with no downtime.
- A scale-up completes and the dataflows that motivated it are not
  progressing faster after warm-up. This is DS2's "consecutive decisions with
  very small improvements" case and the Flink autoscaler's ineffective scaling
  detection. The strategy scales back to the previous size and blocks
  scale-ups past it.

A failed target is not retried until its backoff expires. Backoff starts at 1h
and doubles per consecutive failure up to 24h. It is cleared when the user
changes the policy or the set of objects on the cluster changes.

### SASO properties

DS2 is evaluated against stability, accuracy, short settling time, and no
overshoot. How they hold here:

- **No overshoot on scale-up.** With sub-linear scaling the linear estimate is
  a lower bound on the capacity needed (DS2 Property 1). `u_target < 1` adds
  deliberate headroom on top, which is overshoot by design and bounded by
  `1 / u_target`.
- **No undershoot on scale-down** is DS2 Property 2 under the same assumption.
  We do not rely on it. A scale-down only cuts over after the target replica
  is hydrated and within the allowed lag of the outgoing one, so an
  undershoot is caught before it serves alone.
- **Stability.** Discretization gives a 2x dead band. The scale-down window
  takes a maximum. Failed targets back off. Samples are excluded during
  transitions, so the controller never reacts to its own reconfiguration.
- **Settling time** is one to three hydrations. That is the floor for this
  mechanism.

### Signals

The strategy needs four inputs per replica. Two exist in the controllers and
two are new.

| Signal | Source | Status |
|---|---|---|
| Per-dataflow input and output frontiers | compute and storage controllers | exists in process |
| Heap, swap, memory limit per process | `record_replica_metrics`, 60s poll | exists in process |
| OOM-killed status | orchestrator status events | exists in process |
| Busy time per worker and per dataflow | replica introspection | **new** |

Busy time is the one piece of instrumentation DS2 requires. Timely already
logs every operator schedule start and stop and every worker park, and these
feed `mz_scheduling_elapsed` and `mz_scheduling_parks_histogram`. Those
relations are only readable by targeting a replica, the elapsed log retracts
when a dataflow is dropped, and the parks log is bucketed. None of that suits
a controller taking deltas.

We add one compute log, `mz_introspection.mz_worker_busy_raw`, with monotone
counters: non-parked nanoseconds per worker, and scheduled nanoseconds per
`(worker, dataflow)`. A unified introspection subscribe exports it as
`mz_internal.mz_cluster_replica_load_raw`, one row per replica and worker and
one per replica and dataflow, the path hydration times already take. The
overhead is a counter increment on events the logger already handles. For
reference, DS2 measured its Timely instrumentation at under 20% on per-epoch
latency with a 10s interval, and it had to add event logging we already do.

The adapter-side signal provider differences the counters each policy
interval, joins frontiers and memory, and keeps a ring buffer of samples per
cluster. The strategy interface gains one request and one signal:

```rust
pub struct SignalRequest {
    // ...
    /// Fetch the window of load samples for the cluster.
    pub load: bool,
}

pub struct LiveSignals {
    // ...
    /// Load samples, oldest first. Empty when not requested.
    pub load: Vec<LoadSample>,
}
```

The sample window is live-signal state, not durable state. After an
`environmentd` restart the window is empty and refills before any decision can
activate. A restart can therefore delay a decision and can never cause one,
which keeps the controller's "no in-memory state" rule intact where it
matters.

Using process CPU from the existing metrics poll instead was considered and
rejected. It hides per-worker saturation, which is the skew case. It includes
threads whose work does not scale with timely workers. And DS2's central
argument applies: a utilization number says a replica is busy, not how much
capacity the work needs once it stops falling behind.

### The strategy

`OnLoadStrategy` implements the existing `Strategy` trait.

- `signal_request` asks for `load`, and for nothing when the cluster has no
  `ON LOAD` policy or the break-glass flag is off.
- `update_state` evaluates the decision rules as a pure function of the
  cluster state, the sample window, and `now`. When a rule fires it writes a
  `reconfiguration` record with `status = InProgress`, the target size, the
  default deadline, `on_timeout = ROLLBACK`, and a new `origin` field naming
  the autoscaler and the decision. It also maintains the `load` record
  described below.
- `desired_replicas` returns nothing. Graceful reconfiguration contributes the
  target replicas and the baseline holds the realized set.

Routing through the reconfiguration record, and not contributing replicas
directly as `ON HYDRATION` does, is the central design choice. A size change
decided by a model needs exactly the guarantees a size change typed by a human
gets: overlap, the lag-gated readiness check, a deadline, rollback, compare
and append against concurrent `ALTER`s, and audit events. The record already
provides them, and `mz_cluster_reconfigurations` and `SHOW CLUSTERS` already
display it.

One addition to the durable cluster state, alongside `reconfiguration` and
`burst`:

```
load: Option<LoadScalingState> {
    last_cutover: timestamp,
    previous_size: size,              // for ineffective-scale-up revert
    backoff: [{ target, until, failures, reason }],
}
```

Standard catalog migration with a `None` default.

### Observability

- **`mz_internal.mz_cluster_load_recommendations`.** One row per cluster with
  the current sample summary: CPU demand, memory demand, mean and maximum
  worker utilization, the minimum progress ratio and which dataflow has it,
  the recommended size, and a `status` (`steady`, `scaling-up`,
  `scaling-down`, `skew-limited`, `at-max`, `at-min`, `backing-off`,
  `warming-up`). Populated for every managed cluster, with or without an `ON
  LOAD` policy. Sizing advice is useful on its own, the Console can show it,
  and it is how we validate the model before it acts.
- **Audit log.** Autoscaler-originated reconfigurations use the existing
  reconfiguration events with the `origin` and the sample that triggered
  them. Entering and leaving `skew-limited` and `backing-off` are audited.
- **`mz_cluster_auto_scaling_strategies`** shows the policy and the `load`
  record.

### Dyncfgs

| Key | Default | Meaning |
|---|---|---|
| `enable_load_autoscaling` | see Open Questions | break-glass for the strategy |
| `load_autoscaling_policy_interval` | 60s | sample period |
| `load_autoscaling_target_utilization` | 0.7 | `u_target` |
| `load_autoscaling_target_memory` | 0.8 | `m_target` |
| `load_autoscaling_activation_intervals` | 3 | scale-up activation |
| `load_autoscaling_default_scale_down_after` | 1h | scale-down window |
| `load_autoscaling_warm_up` | 5min | samples ignored after hydration |
| `load_autoscaling_catch_up` | 15min | `T_catchup` |
| `load_autoscaling_min_progress_ratio` | 0.25 | `ρ_min` |

The numeric defaults are starting points to be calibrated in the shadow phase.

### Storage objects

The model applies to sources with one substitution. A source's progress ratio
is `Δ(offset_committed) / Δ(offset_known)` from source statistics, which is
literally DS2's observed source rate over target source rate. Sinks use
frontiers like compute dataflows. The busy counters must also be emitted by
the storage timely runtime, which is additional work, so storage follows
compute in the rollout. Until then a cluster with sources or sinks accepts
`ON LOAD`, and its storage dataflows contribute busy time with `ρ = 1`.

### Rollout

1. **Shadow.** Ship the busy-time log, the signal, and the recommendations
   relation. No actions. Compare recommendations against the sizes users
   chose and against known incidents across the fleet. Calibrate defaults.
2. **Compute clusters.** Enable `ON LOAD` behind a feature flag, scale-up
   only, then scale-down.
3. **Storage.** Add storage busy time and source progress ratios.

### Testing

- **Strategy unit tests.** The decision function is pure. Table-driven tests
  over sample windows cover every rule, the clamp, and backoff.
- **Simulation.** A closed-loop test drives the strategy against a synthetic
  replica model (`capacity = cores^α`, `α <= 1`, configurable skew and
  hydration time) and asserts the SASO properties: convergence in three steps
  or fewer from every starting size, and no decisions under stable load.
- **mzcompose.** A load generator with a step profile (up, hold, down) against
  a cluster with `ON LOAD`. Asserts the size sequence and that wallclock lag
  recovers. A second scenario with a single hot key asserts `skew-limited` and
  no scale-up loop. A third forces a scale-down target that OOMs during
  hydration and asserts rollback and backoff.

## Minimal Viable Prototype

Not built yet. The shadow phase is the prototype: the recommendation can be
computed today, without code changes, by a script that targets a replica and
reads `mz_scheduling_elapsed`, `mz_compute_frontiers`,
`mz_compute_import_frontiers`, and `mz_cluster_replica_metrics`. The
experiment that would validate the design is DS2's §5.4 on our size ladder:
for a few representative dataflows and fixed input rates, start at every size
and record the sequence of recommended sizes. The design holds if every
sequence ends at the smallest size that keeps up, in three steps or fewer.

## Alternatives

**Threshold rules on CPU and memory utilization** (Kubernetes HPA, most cloud
warehouses). Simple and needs no new instrumentation. It is the approach DS2
argues against: it detects a problem without sizing the fix, hides per-worker
saturation behind a process average, and needs thresholds tuned per workload.
With a rehydration per step, a controller that takes six steps to converge is
not usable.

**Scale on wallclock lag alone** (in the spirit of Dhalion's backpressure
symptom, or Dataflow's backlog). Lag is the user-visible symptom and we do use
it, as the progress ratio. Alone it cannot trigger a scale-down, since a
cluster with zero lag may be ten times too large, and it cannot size a
scale-up. Google Dataflow pairs backlog with CPU for this reason.

**Full per-operator DS2 with record counts.** Faithful to the paper and
unnecessary. Per-operator parallelism is not a knob we have, and the progress
ratio replaces rate and selectivity bookkeeping. Per-operator busy time is
still available in introspection for a human diagnosing a `skew-limited`
cluster.

**Contribute replicas directly from the strategy.** The pattern `ON HYDRATION`
uses. It would require the strategy to re-implement cut-over, readiness, and
rollback, and would leave `cluster.size` disagreeing with what is running.

**Scale by adding replicas.** Replicas duplicate work, so this adds cost and
no capacity.

**In-place vertical resize.** Kubernetes can change a pod's CPU and memory
limits without a restart. That could make memory-only steps nearly free. The
timely worker count is fixed at process start, so it does not help CPU, and it
does not move a replica across node shapes. Worth revisiting as a mechanism
under this policy, which is mechanism-agnostic in the way DS2 is.

**Live state migration** ([Megaphone](#references), from several of the DS2
authors and implemented on timely). It would remove the rehydration cost and
let the policy run on DS2's timescales of seconds. It is a much larger change
to compute and is not required for this design to be useful.

**Predictive scaling.** Complementary. A forecast would adjust the target the
reactive controller converges to. It needs the same signals, so this design
is a prerequisite.

## Open questions

1. **Default of `enable_load_autoscaling` in production.** A product decision.
2. **Is the progress ratio well behaved for all dataflows?** Dataflows with
   temporal filters, `REFRESH` schedules, or coarse input timestamps advance
   their frontiers in jumps. The window may need to be longer for them, or
   they may need to be excluded from the ratio. The shadow phase should
   answer this with fleet data.
3. **Hydration memory peak.** Steady-state memory under-predicts what a
   smaller replica needs to hydrate. The design relies on rollback and
   backoff to absorb a wrong downsize, at the cost of one wasted hydration.
   Recording peak-during-hydration per cluster would let us skip targets that
   cannot hydrate. Is the wasted hydration acceptable for v1?
4. **Swap in the memory target.** Should a cluster be allowed to sit in swap
   at steady state if it keeps up? That is cheaper and slower. The proposal
   says no (`m_target` applies to memory, not memory plus disk). Justin's
   observation that slow state access shows up as CPU busyness suggests the
   CPU term would partly account for it anyway.
5. **Signal path.** The proposal uses a unified introspection subscribe
   tapped in the adapter. The alternative is to carry the counters on the
   existing replica metrics poll. The first reuses an established path for
   replica logs. The second avoids a dependency on introspection being
   enabled for the cluster. Which do the compute and adapter owners prefer?
6. **Interaction with multiple replicas.** Replicas do the same work, so the
   proposal takes the maximum demand across measured replicas. Replicas in
   different availability zones can differ in performance. Is maximum right,
   or should it be the median?
7. **Peek-heavy clusters.** Serving load appears as busy time with a progress
   ratio of 1, so a cluster saturated by queries scales on utilization alone
   and only once it is past `u_target`. Is query latency needed as a third
   signal?
8. **Should recommendations be visible to users without the feature?** The
   relation is proposed for all managed clusters. That is a product
   question, as it advertises that a cluster is oversized.

## References

- V. Kalavri, J. Liagouris, M. Hoffmann, D. Dimitrova, M. Forshaw, T. Roscoe.
  [Three steps is all you need: fast, accurate, automatic scaling decisions
  for distributed streaming dataflows](https://people.inf.ethz.ch/troscoe/pubs/kalavri_osdi_2018.pdf).
  OSDI 2018.
- A. Floratou et al. Dhalion: Self-regulating stream processing in Heron.
  PVLDB 2017. The symptom-and-heuristic baseline DS2 compares against.
- [FLIP-271: Autoscaling](https://cwiki.apache.org/confluence/spaces/FLINK/pages/231117250/FLIP-271%2BAutoscaling)
  and the [Flink Kubernetes Operator autoscaler](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/autoscaler/).
  DS2 in production: adds target utilization, catch-up duration,
  stabilization intervals, and ineffective-scaling detection.
- D. Schmitz, G. Rosinosky, E. Rivière.
  [Justin: Hybrid CPU/Memory Elastic Scaling for Distributed Stream
  Processing](https://arxiv.org/abs/2505.19739). 2025. Extends DS2 with a
  memory dimension.
- M. Hoffmann, A. Lattuada, F. McSherry, V. Kalavri, J. Liagouris, T. Roscoe.
  [Megaphone: Latency-conscious state migration for distributed streaming
  dataflows](https://arxiv.org/abs/1812.01371). PVLDB 2019.
- [Google Cloud Dataflow autoscaling](https://cloud.google.com/dataflow/docs/guides/autoscaling-metrics).
  Backlog time plus a CPU utilization target.
