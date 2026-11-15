// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The load signal of the `ON LOAD` autoscaling strategy.
//!
//! Once per busy-time window, `compute_worker_busy_window`, the coordinator samples every managed
//! cluster. It builds a [`ReplicaLoad`] for every replica representative of the cluster's workload
//! from the replica's latest closed busy window, the frontiers at the window's boundaries, and the
//! replica's heap, and appends the resulting [`LoadSample`] to the cluster's window. The window
//! answers [`ClusterControllerCtx::load_signals`], and its evaluation is written to
//! `mz_internal.mz_cluster_load_recommendations`. Neither depends on `enable_load_autoscaling`,
//! which only gates the strategy acting on them.
//!
//! Replicas measure their busy time in windows that end at multiples of the window length since the
//! Unix epoch, and report each closed window through the `ReplicaBusyHistory` introspection
//! subscribe, whose latest windows are mirrored here as its batches arrive. The coordinator ticks
//! at the same boundaries. At each tick it snapshots the frontiers, and after
//! [`WINDOW_DELIVERY_GRACE`] it takes the sample, pairing each replica's new window with the
//! frontier snapshots at the window's start and end. A window delivered after the grace is paired
//! with the snapshots of its own boundaries at the next sample, if it is still the replica's
//! latest. A window whose boundaries have no snapshot is not measured.
//!
//! Everything in this module is in memory and starts empty after a restart. The strategy
//! tolerates that by design: an empty window can delay a decision but not cause one.
//!
//! [`ClusterControllerCtx::load_signals`]: mz_cluster_controller::ctx::ClusterControllerCtx::load_signals

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use mz_catalog::builtin::MZ_CLUSTER_LOAD_RECOMMENDATIONS;
use mz_catalog::memory::objects::{Cluster, ClusterReplica, ClusterVariant};
use mz_cluster_client::ReplicaId;
use mz_cluster_controller::load::{
    DataflowLoad, DataflowProgress, LoadBounds, LoadEvaluation, LoadSample, LoadSignals,
    ReplicaLoad, evaluate, measure,
};
use mz_compute_client::controller::LoadFrontiers;
use mz_compute_types::dyncfgs::worker_busy_window;
use mz_controller::clusters::ReplicaLocation;
use mz_controller_types::ClusterId;
use mz_orchestrator::ServiceProcessMetrics;
use mz_ore::cast::CastFrom;
use mz_ore::now::EpochMillis;
use mz_ore::task;
use mz_repr::adt::interval::Interval;
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_storage_client::controller::IntrospectionType;

use crate::catalog::BuiltinTableUpdate;
use crate::catalog::cluster_state::{
    process_heap_limit, project_expected, size_capacity, size_ladder,
};
use crate::coord::{Coordinator, Message};

/// The most samples kept per cluster, whatever the scale-down window. At the default policy
/// interval of 60s this is 68 hours, which covers a scale-down window of a day twice over.
const MAX_SAMPLES: usize = 4096;

/// How long after a window boundary a sample is taken, so that the windows replicas closed at
/// the boundary have arrived. A replica emits a window one logging interval after its end, and
/// the subscribe forwards it once the log's frontier passes that, which takes up to another
/// logging interval. 5s covers the default logging interval of 1s with room for delivery. Capped
/// at half the window length, so a sample precedes the next boundary.
pub(crate) const WINDOW_DELIVERY_GRACE: Duration = Duration::from_secs(5);

/// How many boundaries' frontier snapshots are kept per replica, which bounds how late a window
/// can arrive and still be measured.
const FRONTIER_HISTORY: usize = 8;

/// One replica's last closed busy window.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ReplicaWindow {
    pub start: EpochMillis,
    pub end: EpochMillis,
    /// How many workers reported the window.
    pub workers: u64,
    /// Non-parked time summed over workers.
    pub total_busy: Duration,
    pub max_worker_busy: Duration,
}

/// Frontiers of one export on one replica, for exports whose frontiers are not empty.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ExportFrontiers {
    /// The least write frontier of the export's direct dependencies.
    pub input: Timestamp,
    /// The replica's output frontier of the export.
    pub output: Timestamp,
}

/// The frontiers of a replica's exports at one window boundary.
pub(crate) type BoundaryFrontiers = BTreeMap<GlobalId, ExportFrontiers>;

/// What the coordinator knows about one replica at one sample.
#[derive(Clone, Debug)]
pub(crate) struct ReplicaObservation {
    /// The replica's latest closed window, unless an earlier sample used it.
    pub window: Option<ReplicaWindow>,
    /// The frontier snapshots at the window's start and end, when both were taken.
    pub frontiers: Option<(BoundaryFrontiers, BoundaryFrontiers)>,
    /// How many workers the replica runs.
    pub workers: u64,
    /// Whether the replica runs at the cluster's realized size.
    pub at_size: bool,
    /// Since when every object on the replica has been hydrated. `None` while one is not.
    pub hydrated_since: Option<EpochMillis>,
    /// Whether the replica was killed for exceeding its memory since the previous sample.
    pub oom_killed: bool,
    pub cpu_millicores: u64,
    /// `None` until a process reported metrics.
    pub heap: Option<ReplicaHeap>,
}

/// A replica's heap, from its processes' latest metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReplicaHeap {
    /// The busiest process's heap times the number of processes. Processes of a replica do the
    /// same kind of work, so the busiest one stands for all.
    pub bytes: u64,
    /// The largest heap of a process as a fraction of its heap limit, in permille.
    pub fraction: u32,
}

/// The heap of a replica of `processes` processes whose size gives each process
/// `process_heap_limit`, from the metrics its processes reported.
///
/// A process's heap is its `heap_bytes`, or its `memory_bytes` where the orchestrator does not
/// report the heap. Its heap limit is the one the process reported, else the size's.
pub(crate) fn replica_heap(
    metrics: &[ServiceProcessMetrics],
    processes: u64,
    process_heap_limit: u64,
) -> Option<ReplicaHeap> {
    let mut busiest: Option<ReplicaHeap> = None;
    for process in metrics {
        let Some(bytes) = process.heap_bytes.or(process.memory_bytes) else {
            continue;
        };
        let limit = process.heap_limit.unwrap_or(process_heap_limit).max(1);
        let fraction = u128::from(bytes) * 1000 / u128::from(limit);
        let fraction = u32::try_from(fraction).unwrap_or(u32::MAX);
        let heap = busiest.get_or_insert(ReplicaHeap { bytes, fraction });
        heap.bytes = heap.bytes.max(bytes);
        heap.fraction = heap.fraction.max(fraction);
    }
    busiest.map(|heap| ReplicaHeap {
        bytes: heap.bytes.saturating_mul(processes),
        ..heap
    })
}

/// The load of one replica over its window. `None` when the replica is not representative of the
/// cluster's workload, or the window cannot be measured.
pub(crate) fn replica_load(
    observation: &ReplicaObservation,
    warm_up: Duration,
) -> Option<ReplicaLoad> {
    let window = observation.window.as_ref()?;
    // Workers that disagree on the window's bounds report it as several windows, each from only
    // some of the workers.
    if window.workers != observation.workers {
        return None;
    }
    let (start_frontiers, end_frontiers) = observation.frontiers.as_ref()?;
    // Rates measured during hydration describe hydration, not the workload. The window has to
    // start after hydration and end after the warm-up.
    let hydrated_since = observation.hydrated_since?;
    let warmed_up = hydrated_since.saturating_add(u64::try_from(warm_up.as_millis()).ok()?);
    if !observation.at_size || window.start < hydrated_since || window.end < warmed_up {
        return None;
    }
    let interval = Duration::from_millis(window.end.checked_sub(window.start)?);
    if interval.is_zero() {
        return None;
    }

    let heap = observation.heap?;
    // An export missing from the start snapshot started within the window.
    let slowest = end_frontiers
        .iter()
        .filter_map(|(id, after)| {
            let before = start_frontiers.get(id)?;
            Some(DataflowLoad {
                id: *id,
                progress: progress(before, after),
            })
        })
        .max_by_key(|d| (d.progress.local_lag, d.id));

    Some(ReplicaLoad {
        cpu_millicores: observation.cpu_millicores,
        interval,
        workers: usize::cast_from(window.workers),
        total_busy: window.total_busy,
        max_worker_busy: window.max_worker_busy,
        slowest,
        heap_fraction: heap.fraction,
        heap_bytes: heap.bytes,
    })
}

fn progress(before: &ExportFrontiers, after: &ExportFrontiers) -> DataflowProgress {
    let advance = |from: Timestamp, to: Timestamp| {
        Duration::from_millis(u64::from(to).saturating_sub(u64::from(from)))
    };
    DataflowProgress {
        input_advance: advance(before.input, after.input),
        output_advance: advance(before.output, after.output),
        local_lag: advance(after.output, after.input),
    }
}

/// The first multiple of `length` after `after`, in milliseconds since the Unix epoch.
pub(crate) fn next_boundary(after: EpochMillis, length: Duration) -> EpochMillis {
    let length = u64::try_from(length.as_millis()).unwrap_or(u64::MAX).max(1);
    (after / length).saturating_add(1).saturating_mul(length)
}

/// One sample of a cluster at its realized `size`.
pub(crate) fn sample(
    size: &str,
    at: Timestamp,
    observations: &BTreeMap<ReplicaId, ReplicaObservation>,
    warm_up: Duration,
) -> LoadSample {
    let loads: Vec<_> = observations
        .values()
        .filter_map(|observation| replica_load(observation, warm_up))
        .collect();
    LoadSample {
        at,
        size: size.to_string(),
        measurement: measure(&loads),
        oom_killed: observations
            .values()
            .any(|observation| observation.at_size && observation.oom_killed),
    }
}

/// Appends `sample` to `window` and drops the samples no decision can use anymore: those older
/// than twice the longest scale-down window in force, which [`evaluate`] floors at twice the
/// hydration time.
fn push_sample(
    window: &mut VecDeque<LoadSample>,
    sample: LoadSample,
    scale_down_after: Duration,
    hydration_time: Option<Duration>,
) {
    window.push_back(sample);
    let hydration = hydration_time.unwrap_or_default();
    let retention = scale_down_after
        .max(hydration.saturating_mul(2))
        .saturating_mul(2);
    let retention = u64::try_from(retention.as_millis()).unwrap_or(u64::MAX);
    let latest = window.back().map_or(0, |s| u64::from(s.at));
    let horizon = latest.saturating_sub(retention);
    while window.len() > MAX_SAMPLES || window.front().is_some_and(|s| u64::from(s.at) < horizon) {
        window.pop_front();
    }
}

/// A row of the busy history subscribe, without its replica ID.
fn parse_window(row: &Row) -> Option<ReplicaWindow> {
    let datums: Vec<_> = row.iter().collect();
    let [
        Datum::TimestampTz(start),
        Datum::TimestampTz(end),
        Datum::UInt64(workers),
        Datum::UInt64(busy),
        Datum::UInt64(max_busy),
    ] = datums[..]
    else {
        return None;
    };
    Some(ReplicaWindow {
        start: u64::try_from(start.timestamp_millis()).ok()?,
        end: u64::try_from(end.timestamp_millis()).ok()?,
        workers,
        total_busy: Duration::from_nanos(busy),
        max_worker_busy: Duration::from_nanos(max_busy),
    })
}

/// The in-memory state of the load signal.
#[derive(Debug, Default)]
pub(crate) struct ClusterLoad {
    /// Contents of each replica's load and hydration-time introspection subscribes.
    mirrors: BTreeMap<ReplicaId, ReplicaMirror>,
    /// The end of each replica's latest window a sample used or rejected.
    consumed: BTreeMap<ReplicaId, EpochMillis>,
    /// Each replica's frontier snapshots by boundary, the latest [`FRONTIER_HISTORY`] of them.
    boundary_frontiers: BTreeMap<ReplicaId, BTreeMap<EpochMillis, BoundaryFrontiers>>,
    /// Since when each hydrated replica has been hydrated, by sample time.
    hydrated_since: BTreeMap<ReplicaId, EpochMillis>,
    /// Replicas killed for exceeding their memory since the previous sample.
    oom_killed: BTreeSet<ReplicaId>,
    /// Each managed cluster's samples, oldest first.
    windows: BTreeMap<ClusterId, VecDeque<LoadSample>>,
    /// The rows last written to `mz_cluster_load_recommendations`.
    recommendations: BTreeMap<ClusterId, Row>,
}

#[derive(Debug, Default)]
struct ReplicaMirror {
    /// The rows of the latest batch of the busy history subscribe that inserted any.
    // TODO: Read the windows back from `mz_cluster_replica_busy_history` instead, so they survive
    // a restart.
    load: BTreeSet<Row>,
    hydration: BTreeMap<Row, Diff>,
}

impl ClusterLoad {
    /// Applies a batch of an introspection subscribe's updates, each row prefixed with the
    /// replica ID. `reset` drops what the subscribe reported before, for a reinstalled subscribe
    /// whose new snapshot replaces the old one.
    pub(crate) fn observe_introspection(
        &mut self,
        introspection_type: IntrospectionType,
        replica_id: ReplicaId,
        reset: bool,
        updates: &[(Row, Diff)],
    ) {
        let mirror = self.mirrors.entry(replica_id).or_default();
        let contents = match introspection_type {
            // The subscribe reports each window once, as an insertion. Its retractions are of
            // windows a previous batch reported, or, for a new subscribe, one it never reported.
            IntrospectionType::ReplicaBusyHistory => {
                let inserted: BTreeSet<_> = updates
                    .iter()
                    .filter(|(_, diff)| diff.is_positive())
                    .map(|(row, _)| Row::pack(row.iter().skip(1)))
                    .collect();
                if !inserted.is_empty() {
                    mirror.load = inserted;
                }
                return;
            }
            IntrospectionType::ComputeHydrationTimes => &mut mirror.hydration,
            _ => return,
        };
        if reset {
            contents.clear();
        }
        for (row, diff) in updates {
            *contents.entry(Row::pack(row.iter().skip(1))).or_default() += *diff;
        }
        contents.retain(|_, diff| *diff != Diff::ZERO);
    }

    pub(crate) fn forget_replica(&mut self, replica_id: ReplicaId) {
        self.mirrors.remove(&replica_id);
        self.consumed.remove(&replica_id);
        self.boundary_frontiers.remove(&replica_id);
        self.hydrated_since.remove(&replica_id);
        self.oom_killed.remove(&replica_id);
    }

    pub(crate) fn note_oom_killed(&mut self, replica_id: ReplicaId) {
        self.oom_killed.insert(replica_id);
    }

    /// A restarted replica rehydrates. Its warm-up starts over once it is hydrated again, even
    /// when no sample saw it unhydrated.
    pub(crate) fn note_restarted(&mut self, replica_id: ReplicaId) {
        self.hydrated_since.remove(&replica_id);
    }

    fn samples(&self, cluster_id: ClusterId) -> Vec<LoadSample> {
        self.windows
            .get(&cluster_id)
            .map(|window| window.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// The latest window a replica's subscribe reports. `None` while it reports none, or more
    /// than one with the latest end.
    fn window(&self, replica_id: ReplicaId) -> Option<ReplicaWindow> {
        let mirror = self.mirrors.get(&replica_id)?;
        let windows: Vec<_> = mirror
            .load
            .iter()
            .map(parse_window)
            .collect::<Option<_>>()?;
        let end = windows.iter().map(|w| w.end).max()?;
        let mut latest = windows.into_iter().filter(|w| w.end == end);
        let window = latest.next()?;
        latest.next().is_none().then_some(window)
    }

    /// The replica's latest window, unless an earlier call returned it. Marks it as used.
    fn take_new_window(&mut self, replica_id: ReplicaId) -> Option<ReplicaWindow> {
        let window = self.window(replica_id)?;
        if self
            .consumed
            .get(&replica_id)
            .is_some_and(|end| *end >= window.end)
        {
            return None;
        }
        self.consumed.insert(replica_id, window.end);
        Some(window)
    }

    /// Records the frontiers of every replica in `frontiers`, taken at `boundary`.
    fn record_frontiers(
        &mut self,
        boundary: EpochMillis,
        frontiers: &BTreeMap<ClusterId, LoadFrontiers>,
    ) {
        for cluster in frontiers.values() {
            for (replica_id, replica) in &cluster.replicas {
                let snapshot = replica
                    .output
                    .iter()
                    .filter_map(|(id, output)| {
                        let input = cluster.input.get(id)?.as_option()?;
                        let output = output.as_option()?;
                        let frontiers = ExportFrontiers {
                            input: *input,
                            output: *output,
                        };
                        Some((*id, frontiers))
                    })
                    .collect();
                let history = self.boundary_frontiers.entry(*replica_id).or_default();
                history.insert(boundary, snapshot);
                while history.len() > FRONTIER_HISTORY {
                    history.pop_first();
                }
            }
        }
    }

    /// The replica's frontier snapshots at the start and end of `window`, when both were taken.
    fn window_frontiers(
        &self,
        replica_id: ReplicaId,
        window: &ReplicaWindow,
    ) -> Option<(BoundaryFrontiers, BoundaryFrontiers)> {
        let history = self.boundary_frontiers.get(&replica_id)?;
        let start = history.get(&window.start)?;
        let end = history.get(&window.end)?;
        Some((start.clone(), end.clone()))
    }

    /// The longest time any object took to hydrate on the replica, once all have.
    fn hydration_time(&self, replica_id: ReplicaId) -> Option<Duration> {
        let mirror = self.mirrors.get(&replica_id)?;
        let mut longest = None;
        for row in mirror.hydration.keys() {
            match row.iter().nth(1) {
                Some(Datum::UInt64(ns)) => {
                    longest = longest.max(Some(Duration::from_nanos(ns)));
                }
                _ => return None,
            }
        }
        longest
    }

    /// The longest any replica of `cluster` at its realized `size` took to hydrate, among those
    /// whose hydration time is known.
    fn cluster_hydration_time(&self, cluster: &Cluster, size: &str) -> Option<Duration> {
        cluster
            .replicas()
            .filter(|replica| match &replica.config.location {
                ReplicaLocation::Managed(location) => location.size == size,
                ReplicaLocation::Unmanaged(_) => false,
            })
            .filter_map(|replica| self.hydration_time(replica.replica_id))
            .max()
    }
}

impl Coordinator {
    /// Schedules the next load tick at the first window boundary after `after`.
    pub(crate) fn schedule_cluster_load_sample(&self, after: EpochMillis) {
        let length = worker_busy_window(self.catalog().system_config().dyncfgs());
        let boundary = next_boundary(after.max(self.now()), length);
        let delay = Duration::from_millis(boundary.saturating_sub(self.now()));
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        task::spawn(|| "cluster_load_schedule", async move {
            tokio::time::sleep(delay).await;
            let _ = internal_cmd_tx.send(Message::ClusterLoadSample { boundary });
        });
    }

    /// Requests the frontiers of every managed cluster at window `boundary`, and schedules the
    /// next tick. The replies arrive as [`Message::ClusterLoadFrontiers`], once the windows that
    /// closed at `boundary` have had time to arrive, which completes the sample.
    pub(crate) fn cluster_load_sample(&self, boundary: EpochMillis) {
        let mut pending = Vec::new();
        for cluster in self.catalog().clusters().filter(|c| c.is_managed()) {
            if let Ok(rx) = self.controller.compute.load_frontiers(cluster.id) {
                pending.push((cluster.id, rx));
            }
        }
        let length = worker_busy_window(self.catalog().system_config().dyncfgs());
        let grace = WINDOW_DELIVERY_GRACE.min(length / 2);
        let record_at = tokio::time::Instant::now() + grace;
        // The replies come from the compute instance tasks and must not be awaited on the
        // coordinator loop.
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        task::spawn(|| "cluster_load_frontiers", async move {
            let mut frontiers = BTreeMap::new();
            for (cluster_id, rx) in pending {
                if let Ok(reply) = rx.await {
                    frontiers.insert(cluster_id, reply);
                }
            }
            tokio::time::sleep_until(record_at).await;
            let _ = internal_cmd_tx.send(Message::ClusterLoadFrontiers {
                boundary,
                frontiers,
            });
        });
        self.schedule_cluster_load_sample(boundary);
    }

    /// Takes one sample of every cluster in `frontiers`, the frontiers at window `boundary`, and
    /// updates the recommendations.
    pub(crate) fn cluster_load_record(
        &mut self,
        boundary: EpochMillis,
        frontiers: BTreeMap<ClusterId, LoadFrontiers>,
    ) {
        let catalog = self.owned_catalog();
        let params = mz_cluster_controller::load_params(catalog.system_config().dyncfgs());
        let now = self.now();
        let oom_killed = std::mem::take(&mut self.cluster_load.oom_killed);
        self.cluster_load.record_frontiers(boundary, &frontiers);

        let mut rows = BTreeMap::new();
        for (cluster_id, frontiers) in frontiers {
            let Some(cluster) = catalog.try_get_cluster(cluster_id) else {
                continue;
            };
            let ClusterVariant::Managed(managed) = &cluster.config.variant else {
                continue;
            };
            let size = managed.size.clone();
            let expected = project_expected(managed);
            let ladder = size_ladder(catalog.cluster_replica_sizes(), &size);

            let observations: BTreeMap<_, _> = cluster
                .replicas()
                .filter_map(|replica| {
                    let observation = self.observe_replica(
                        cluster,
                        replica,
                        &size,
                        &frontiers,
                        now,
                        &oom_killed,
                    )?;
                    Some((replica.replica_id, observation))
                })
                .collect();

            let sample = sample(&size, Timestamp::from(now), &observations, params.warm_up);

            let bounds = expected
                .auto_scaling_policy
                .as_ref()
                .and_then(|policy| policy.on_load.as_ref())
                .map(LoadBounds::from)
                .unwrap_or_default();
            let hydration_time = self.cluster_load.cluster_hydration_time(cluster, &size);
            let window = self.cluster_load.windows.entry(cluster_id).or_default();
            push_sample(
                window,
                sample,
                bounds
                    .scale_down_after
                    .unwrap_or(params.default_scale_down_after),
                hydration_time,
            );
            let signals = LoadSignals {
                ladder,
                samples: window.iter().cloned().collect(),
                hydration_time,
            };
            let evaluation = evaluate(
                &size,
                bounds,
                &signals,
                expected.load.as_ref(),
                &params,
                Timestamp::from(now),
            );
            if let Some(evaluation) = evaluation {
                rows.insert(cluster_id, recommendation_row(cluster_id, &evaluation, now));
            }
        }

        self.cluster_load
            .windows
            .retain(|id, _| catalog.try_get_cluster(*id).is_some_and(|c| c.is_managed()));
        self.write_recommendations(rows);
    }

    /// Observes one replica of `cluster`, whose realized size is `size`, at `now`, and takes its
    /// new window. `None` for an unmanaged replica, or one the compute controller does not know.
    fn observe_replica(
        &mut self,
        cluster: &Cluster,
        replica: &ClusterReplica,
        size: &str,
        frontiers: &LoadFrontiers,
        now: EpochMillis,
        oom_killed: &BTreeSet<ReplicaId>,
    ) -> Option<ReplicaObservation> {
        let ReplicaLocation::Managed(location) = &replica.config.location else {
            return None;
        };
        let replica_id = replica.replica_id;
        let replica_frontiers = frontiers.replicas.get(&replica_id)?;

        let hydrated = replica_frontiers.hydrated
            && self
                .replica_hydration_gate(cluster.id, replica_id)
                .is_some();
        let hydrated_since = if hydrated {
            Some(
                *self
                    .cluster_load
                    .hydrated_since
                    .entry(replica_id)
                    .or_insert(now),
            )
        } else {
            self.cluster_load.hydrated_since.remove(&replica_id);
            None
        };

        let window = self.cluster_load.take_new_window(replica_id);
        let window_frontiers = window
            .as_ref()
            .and_then(|window| self.cluster_load.window_frontiers(replica_id, window));

        let capacity = size_capacity(&location.size, &location.allocation);

        let observation = ReplicaObservation {
            window,
            frontiers: window_frontiers,
            workers: u64::cast_from(replica.config.location.workers()?),
            at_size: location.size == size,
            hydrated_since,
            oom_killed: oom_killed.contains(&replica_id),
            cpu_millicores: capacity.cpu_millicores,
            heap: self.replica_heap(cluster.id, replica_id),
        };
        Some(observation)
    }

    /// The heap of a managed replica, from its latest metrics. `None` for an unmanaged replica or
    /// before its processes reported metrics.
    pub(crate) fn replica_heap(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> Option<ReplicaHeap> {
        let replica = self
            .catalog()
            .try_get_cluster_replica(cluster_id, replica_id)?;
        let ReplicaLocation::Managed(location) = &replica.config.location else {
            return None;
        };
        let processes = u64::from(location.allocation.scale.get());
        let process_heap_limit = process_heap_limit(&location.allocation);
        let metrics = self.controller.replica_metrics(replica_id)?;
        replica_heap(metrics, processes, process_heap_limit)
    }

    /// Replaces the contents of `mz_cluster_load_recommendations` with `rows`.
    fn write_recommendations(&mut self, rows: BTreeMap<ClusterId, Row>) {
        // Builtin tables are not writable in read-only mode. Writing the rows on promotion would
        // need no catch-up: the next sample replaces them anyway.
        if self.controller.read_only() {
            return;
        }
        let table = self
            .catalog()
            .resolve_builtin_table(&MZ_CLUSTER_LOAD_RECOMMENDATIONS);
        let previous = std::mem::replace(&mut self.cluster_load.recommendations, rows);
        let current = &self.cluster_load.recommendations;
        let retractions = previous
            .iter()
            .filter(|(id, row)| current.get(id) != Some(*row))
            .map(|(_, row)| BuiltinTableUpdate::row(table, row.clone(), Diff::MINUS_ONE));
        let insertions = current
            .iter()
            .filter(|(id, row)| previous.get(id) != Some(*row))
            .map(|(_, row)| BuiltinTableUpdate::row(table, row.clone(), Diff::ONE));
        let updates: Vec<_> = retractions.chain(insertions).collect();
        if !updates.is_empty() {
            // The write needs no confirmation: a lost one is replaced by the next sample.
            let _ = self.builtin_table_update().background(updates);
        }
    }

    /// The load signal of a managed cluster, for [`ClusterControllerCtx::load_signals`].
    ///
    /// [`ClusterControllerCtx::load_signals`]: mz_cluster_controller::ctx::ClusterControllerCtx::load_signals
    pub(crate) fn cluster_load_signals(&self, cluster_id: ClusterId) -> Option<LoadSignals> {
        let cluster = self.catalog().try_get_cluster(cluster_id)?;
        let ClusterVariant::Managed(managed) = &cluster.config.variant else {
            return None;
        };
        Some(LoadSignals {
            ladder: size_ladder(self.catalog().cluster_replica_sizes(), &managed.size),
            samples: self.cluster_load.samples(cluster_id),
            hydration_time: self
                .cluster_load
                .cluster_hydration_time(cluster, &managed.size),
        })
    }
}

fn recommendation_row(
    cluster_id: ClusterId,
    evaluation: &LoadEvaluation,
    sampled_at: EpochMillis,
) -> Row {
    let cluster_id = cluster_id.to_string();
    let latest = evaluation.latest.as_ref();
    let ratio = |permille: u32| Datum::Float64((f64::from(permille) / 1000.0).into());
    let slowest = latest
        .and_then(|m| m.slowest_dataflow)
        .map(|id| id.to_string());
    let lag = latest.map(|m| {
        let micros = i64::try_from(m.lag.as_micros()).unwrap_or(i64::MAX);
        Interval::new(0, 0, micros)
    });
    Row::pack_slice(&[
        Datum::String(&cluster_id),
        Datum::TimestampTz(
            mz_ore::now::to_datetime(sampled_at)
                .try_into()
                .expect("must fit"),
        ),
        lag.map_or(Datum::Null, Datum::Interval),
        latest.map_or(Datum::Null, |m| ratio(m.progress())),
        Datum::from(slowest.as_deref()),
        latest.map_or(Datum::Null, |m| ratio(m.heap_fraction)),
        latest.map_or(Datum::Null, |m| ratio(m.mean_utilization)),
        latest.map_or(Datum::Null, |m| ratio(m.max_worker_utilization)),
        Datum::from(evaluation.recommended_size.as_deref()),
        Datum::String(evaluation.status.as_str()),
    ])
}

#[cfg(test)]
mod tests;
