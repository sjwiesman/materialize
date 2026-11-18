// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Gathers the load signal of every managed cluster and keeps its load summaries.
//!
//! Every `load_autoscaling_lag_sample_interval` the coordinator asks the compute controller for
//! the frontiers of every managed cluster's dataflows on each replica, adds each replica's latest
//! heap, and appends a [`LoadSample`] per cluster. Busy windows arrive through the
//! `ReplicaBusyHistory` introspection subscribe, and memory kills through cluster events. At the
//! first sample of each evaluation window, the previous window is summarized by
//! [`mz_cluster_controller::load::summarize`]. Every managed cluster is sampled, with or without
//! an `ON LOAD` policy.
//!
//! Summaries are appended to `mz_internal.mz_cluster_load_history` and
//! `mz_internal.mz_cluster_load_dataflow_history`, and read back from them at startup, so the
//! in-memory history survives a restart of `environmentd`. Samples, busy windows, kills, and
//! warm-up state are in memory only. After a restart, a replica's warm-up starts over and the
//! window in progress yields no summary, since it lacks its early samples.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use mz_adapter_types::dyncfgs::{
    LOAD_AUTOSCALING_EVALUATION_WINDOW, LOAD_AUTOSCALING_FLOOR_FACTOR,
    LOAD_AUTOSCALING_LAG_SAMPLE_INTERVAL, LOAD_AUTOSCALING_LAG_TOLERANCE, LOAD_AUTOSCALING_WARM_UP,
};
use mz_catalog::builtin::{MZ_CLUSTER_LOAD_DATAFLOW_HISTORY, MZ_CLUSTER_LOAD_HISTORY};
use mz_catalog::memory::objects::ClusterVariant;
use mz_cluster_client::ReplicaId;
use mz_cluster_controller::ctx::LoadRecord;
use mz_cluster_controller::load::{
    BusySummary, BusyWindow, DataflowFrontiers, DataflowSummary, HeapSummary, LoadSample,
    LoadSummary, MemoryKill, ProcessHeap, ReplicaSample, SummaryParams, summarize, window_start,
};
use mz_compute_client::controller::LoadFrontiers;
use mz_controller::clusters::{ReplicaAllocation, ReplicaLocation};
use mz_controller_types::ClusterId;
use mz_dyncfg::ConfigSet;
use mz_orchestrator::ServiceProcessMetrics;
use mz_ore::now::EpochMillis;
use mz_ore::task;
use mz_repr::adt::interval::Interval;
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_storage_client::controller::IntrospectionType;
use mz_storage_types::dyncfgs::CLUSTER_LOAD_HISTORY_RETENTION_INTERVAL;
use timely::progress::Antichain;
use tracing::warn;

use crate::catalog::cluster_state::project_expected;
use crate::coord::{Coordinator, Message};

/// What the coordinator observed of one cluster at one sample.
#[derive(Clone, Debug)]
pub(crate) struct ClusterObservation {
    /// The cluster's realized size.
    pub size: String,
    /// The cluster's `ON LOAD` state, whose lag floors the summaries apply.
    pub record: Option<LoadRecord>,
    /// The collections the cluster's dataflows export.
    pub exports: BTreeSet<GlobalId>,
    pub replicas: BTreeMap<ReplicaId, ReplicaObservation>,
}

/// What the coordinator observed of one replica at one sample.
#[derive(Clone, Debug)]
pub(crate) struct ReplicaObservation {
    pub size: String,
    pub hydrated: bool,
    pub dataflows: BTreeMap<GlobalId, DataflowFrontiers>,
    pub heap: Option<ProcessHeap>,
}

/// Per-replica state carried across samples.
#[derive(Clone, Debug, Default)]
struct ReplicaTracker {
    /// See [`ReplicaSample::eligible_since`].
    eligible_since: Option<Timestamp>,
    /// Whether the replica has been hydrated since its creation or last restart.
    hydrated_once: bool,
    /// The largest heap seen before the replica first hydrated.
    hydration_peak: Option<ProcessHeap>,
}

/// The in-memory state of the load signal.
#[derive(Debug, Default)]
pub(crate) struct ClusterLoad {
    replicas: BTreeMap<ReplicaId, ReplicaTracker>,
    /// Each cluster's samples of the evaluation windows not yet summarized, oldest first.
    samples: BTreeMap<ClusterId, Vec<LoadSample>>,
    /// Each replica's busy windows that end after the last summarized evaluation window.
    busy: BTreeMap<ReplicaId, Vec<BusyWindow>>,
    /// Each cluster's memory kills after the last summarized evaluation window.
    kills: BTreeMap<ClusterId, Vec<MemoryKill>>,
    /// Each cluster's summaries within the retention interval, by window end.
    summaries: BTreeMap<ClusterId, BTreeMap<Timestamp, LoadSummary>>,
    /// The time of the latest recorded sample.
    last_sample: Option<Timestamp>,
}

impl ClusterLoad {
    /// The load summaries of a cluster within the retention interval, oldest first.
    // TODO: Read by the `ON LOAD` strategy and the recommendations.
    #[allow(dead_code)]
    pub(crate) fn summaries(
        &self,
        cluster_id: ClusterId,
    ) -> impl DoubleEndedIterator<Item = &LoadSummary> {
        self.summaries
            .get(&cluster_id)
            .into_iter()
            .flat_map(|summaries| summaries.values())
    }

    /// The largest heap of a replica's fullest process seen between its creation, or its last
    /// restart, and its first hydration. `None` before any heap was reported then.
    // TODO: Read by the `ON LOAD` strategy.
    #[allow(dead_code)]
    pub(crate) fn hydration_peak(&self, replica_id: ReplicaId) -> Option<ProcessHeap> {
        self.replicas.get(&replica_id)?.hydration_peak
    }

    /// Records the busy windows in a batch of the `ReplicaBusyHistory` introspection subscribe.
    /// Each row is prefixed with the replica ID.
    pub(crate) fn observe_busy_rows(&mut self, replica_id: ReplicaId, updates: &[(Row, Diff)]) {
        let windows = self.busy.entry(replica_id).or_default();
        for (row, diff) in updates {
            if !diff.is_positive() {
                continue;
            }
            match parse_busy_row(replica_id, row) {
                Some(window) => windows.push(window),
                None => warn!(?row, "unexpected busy history row"),
            }
        }
    }

    /// Records that a replica of a cluster was killed for exceeding its memory.
    pub(crate) fn note_memory_kill(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        size: String,
        at: Timestamp,
    ) {
        self.kills.entry(cluster_id).or_default().push(MemoryKill {
            replica_id,
            size,
            at,
        });
    }

    /// Records that a replica's process went offline or restarted. It rehydrates from scratch,
    /// so its warm-up and hydration peak start over.
    pub(crate) fn note_restarted(&mut self, replica_id: ReplicaId) {
        self.replicas.remove(&replica_id);
    }

    /// Records one sample of the observed clusters at `at`, and summarizes every evaluation
    /// window that ended at or before `at`. Returns the new summaries.
    ///
    /// A sample no later than the latest recorded one is ignored. Its window may have been
    /// summarized already.
    pub(crate) fn record(
        &mut self,
        at: Timestamp,
        observations: BTreeMap<ClusterId, ClusterObservation>,
        params: &SummaryParams,
    ) -> Vec<(ClusterId, LoadSummary)> {
        if self.last_sample.is_some_and(|last| at <= last) {
            return Vec::new();
        }
        self.last_sample = Some(at);
        let current_window = window_start(at, params.window);
        let mut new = Vec::new();
        for (cluster_id, observation) in observations {
            let sample = self.sample(at, observation.clone());
            let samples = self.samples.entry(cluster_id).or_default();
            samples.push(sample);

            let closed: Vec<_> = samples
                .iter()
                .filter(|sample| sample.at < current_window)
                .cloned()
                .collect();
            samples.retain(|sample| sample.at >= current_window);

            let windows: BTreeSet<_> = closed
                .iter()
                .map(|sample| window_start(sample.at, params.window))
                .collect();
            let replicas: BTreeSet<_> = closed
                .iter()
                .flat_map(|sample| sample.replicas.keys().copied())
                .collect();
            let busy: Vec<_> = replicas
                .iter()
                .filter_map(|id| self.busy.get(id))
                .flatten()
                .cloned()
                .collect();
            let kills = self.kills.remove(&cluster_id).unwrap_or_default();
            for start in windows {
                let window = u64::try_from(params.window.as_millis()).unwrap_or(u64::MAX);
                let end = Timestamp::new(u64::from(start).saturating_add(window));
                if let Some(summary) = summarize(
                    start,
                    end,
                    &closed,
                    &busy,
                    &kills,
                    observation.record.as_ref(),
                    params,
                ) {
                    new.push((cluster_id, summary));
                }
            }
            // Kills reported in the window in progress belong to its summary.
            let pending: Vec<_> = kills
                .into_iter()
                .filter(|kill| kill.at >= current_window)
                .collect();
            if !pending.is_empty() {
                self.kills.insert(cluster_id, pending);
            }
        }
        for windows in self.busy.values_mut() {
            windows.retain(|window| window.end > current_window);
        }
        for (cluster_id, summary) in &new {
            self.summaries
                .entry(*cluster_id)
                .or_default()
                .insert(summary.at, summary.clone());
        }
        new
    }

    /// Builds the sample of one cluster, and updates the state of its replicas.
    fn sample(&mut self, at: Timestamp, observation: ClusterObservation) -> LoadSample {
        let ClusterObservation {
            size,
            record: _,
            exports,
            replicas,
        } = observation;
        let replicas = replicas
            .into_iter()
            .map(|(replica_id, replica)| {
                let tracker = self.replicas.entry(replica_id).or_default();
                if replica.size != size {
                    tracker.eligible_since = None;
                } else if replica.hydrated && tracker.eligible_since.is_none() {
                    tracker.eligible_since = Some(at);
                }
                if !tracker.hydrated_once {
                    if replica.hydrated {
                        tracker.hydrated_once = true;
                    } else if let Some(heap) = replica.heap {
                        let peak = tracker.hydration_peak.get_or_insert(heap);
                        if heap.bytes > peak.bytes {
                            *peak = heap;
                        }
                    }
                }
                let sample = ReplicaSample {
                    size: replica.size,
                    hydrated: replica.hydrated,
                    eligible_since: tracker.eligible_since,
                    dataflows: replica.dataflows,
                    heap: replica.heap,
                };
                (replica_id, sample)
            })
            .collect();
        LoadSample {
            at,
            size,
            exports,
            replicas,
        }
    }

    /// Adds summaries read back from the history relations. A summary already in memory wins.
    pub(crate) fn restore(&mut self, summaries: Vec<(ClusterId, LoadSummary)>) {
        for (cluster_id, summary) in summaries {
            self.summaries
                .entry(cluster_id)
                .or_default()
                .entry(summary.at)
                .or_insert(summary);
        }
    }

    /// Drops the state of clusters and replicas that no longer exist, and summaries that ended
    /// before `cutoff`.
    pub(crate) fn retain(
        &mut self,
        clusters: &BTreeSet<ClusterId>,
        replicas: &BTreeSet<ReplicaId>,
        cutoff: Timestamp,
    ) {
        self.replicas.retain(|id, _| replicas.contains(id));
        self.busy.retain(|id, _| replicas.contains(id));
        self.samples.retain(|id, _| clusters.contains(id));
        self.kills.retain(|id, _| clusters.contains(id));
        self.summaries.retain(|id, summaries| {
            summaries.retain(|at, _| *at >= cutoff);
            clusters.contains(id) && !summaries.is_empty()
        });
    }
}

/// The parameters of the summaries, from the dyncfgs.
fn summary_params(dyncfgs: &ConfigSet) -> SummaryParams {
    SummaryParams {
        window: LOAD_AUTOSCALING_EVALUATION_WINDOW
            .get(dyncfgs)
            .max(Duration::from_millis(1)),
        sample_interval: LOAD_AUTOSCALING_LAG_SAMPLE_INTERVAL
            .get(dyncfgs)
            .max(Duration::from_millis(1)),
        warm_up: LOAD_AUTOSCALING_WARM_UP.get(dyncfgs),
        lag_tolerance: LOAD_AUTOSCALING_LAG_TOLERANCE.get(dyncfgs),
        floor_factor: LOAD_AUTOSCALING_FLOOR_FACTOR.get(dyncfgs),
    }
}

/// The first multiple of `interval` after `after`, in milliseconds since the Unix epoch.
fn next_boundary(after: EpochMillis, interval: Duration) -> EpochMillis {
    let interval = u64::try_from(interval.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    (after / interval)
        .saturating_add(1)
        .saturating_mul(interval)
}

/// The heap limit of one process of a size: its memory limit plus its disk limit when the size
/// spills to swap, the sum the controller passes to `clusterd` as `--heap-limit`. `None` when
/// the size has no memory limit.
fn process_heap_limit(allocation: &ReplicaAllocation) -> Option<u64> {
    let memory = allocation.memory_limit?.0.as_u64();
    // A disk limit of zero requests unlimited swap, and then `clusterd` gets no heap limit.
    match allocation.disk_limit {
        Some(disk) if allocation.swap_enabled && disk.0.as_u64() != 0 => {
            Some(memory.saturating_add(disk.0.as_u64()))
        }
        _ => Some(memory),
    }
}

/// The heap of a replica's fullest process, from the latest metrics of its processes. A
/// process's heap is its `heap_bytes`, or its `memory_bytes` where the orchestrator does not
/// report the heap. Its limit is the one it reported, else `size_limit`.
fn fullest_process(
    metrics: &[ServiceProcessMetrics],
    size_limit: Option<u64>,
) -> Option<ProcessHeap> {
    metrics
        .iter()
        .filter_map(|process| {
            let bytes = process.heap_bytes.or(process.memory_bytes)?;
            let limit = process.heap_limit.or(size_limit);
            Some(ProcessHeap { bytes, limit })
        })
        .max_by(|a, b| {
            a.fraction()
                .unwrap_or(0.0)
                .total_cmp(&b.fraction().unwrap_or(0.0))
                .then(a.bytes.cmp(&b.bytes))
        })
}

/// The frontiers of a replica's dataflows whose input and output frontiers are not empty.
fn dataflow_frontiers(
    frontiers: &BTreeMap<GlobalId, mz_compute_client::controller::DataflowFrontiers>,
) -> BTreeMap<GlobalId, DataflowFrontiers> {
    let single = |frontier: &Antichain<Timestamp>| frontier.as_option().copied();
    frontiers
        .iter()
        .filter_map(|(id, frontiers)| {
            let frontiers = DataflowFrontiers {
                input: single(&frontiers.input)?,
                output: single(&frontiers.output)?,
            };
            Some((*id, frontiers))
        })
        .collect()
}

/// A row of the busy history subscribe, prefixed with the replica ID.
fn parse_busy_row(replica_id: ReplicaId, row: &Row) -> Option<BusyWindow> {
    let datums: Vec<_> = row.iter().collect();
    let [
        _,
        Datum::TimestampTz(start),
        Datum::TimestampTz(end),
        Datum::UInt64(workers),
        Datum::UInt64(busy),
        Datum::UInt64(max_worker_busy),
    ] = datums[..]
    else {
        return None;
    };
    Some(BusyWindow {
        replica_id,
        start: Timestamp::new(u64::try_from(start.timestamp_millis()).ok()?),
        end: Timestamp::new(u64::try_from(end.timestamp_millis()).ok()?),
        workers,
        busy: Duration::from_nanos(busy),
        max_worker_busy: Duration::from_nanos(max_worker_busy),
    })
}

fn timestamptz(at: Timestamp) -> Datum<'static> {
    Datum::TimestampTz(
        mz_ore::now::to_datetime(at.into())
            .try_into()
            .expect("must fit"),
    )
}

fn interval(duration: Duration) -> Interval {
    Interval::new(
        0,
        0,
        i64::try_from(duration.as_micros()).unwrap_or(i64::MAX),
    )
}

/// The rows of `mz_cluster_load_history` and `mz_cluster_load_dataflow_history` for a summary.
pub(crate) fn summary_rows(cluster_id: ClusterId, summary: &LoadSummary) -> (Row, Vec<Row>) {
    let cluster_id = cluster_id.to_string();
    let LoadSummary {
        start,
        at,
        size,
        fingerprint,
        dataflows,
        heap,
        busy,
        killed,
    } = summary;
    let cluster_row = Row::pack_slice(&[
        Datum::String(&cluster_id),
        timestamptz(*start),
        timestamptz(*at),
        Datum::String(size),
        Datum::UInt64(*fingerprint),
        heap.map_or(Datum::Null, |heap| Datum::UInt64(heap.bytes)),
        heap.and_then(|heap| heap.fraction)
            .map_or(Datum::Null, |fraction| Datum::Float64(fraction.into())),
        busy.map_or(Datum::Null, |busy| Datum::UInt64(busy.mean_millicores)),
        busy.map_or(Datum::Null, |busy| {
            Datum::Float64(busy.busiest_worker_utilization.into())
        }),
        Datum::from(*killed),
    ]);
    let dataflow_rows = dataflows
        .iter()
        .map(|(id, dataflow)| {
            let id = id.to_string();
            Row::pack_slice(&[
                Datum::String(&cluster_id),
                timestamptz(*at),
                Datum::String(&id),
                Datum::Interval(interval(dataflow.median_lag)),
                dataflow
                    .progress_ratio
                    .map_or(Datum::Null, |ratio| Datum::Float64(ratio.into())),
                Datum::from(dataflow.behind),
            ])
        })
        .collect();
    (cluster_row, dataflow_rows)
}

fn datum_timestamp(datum: Datum) -> Option<Timestamp> {
    match datum {
        Datum::TimestampTz(at) => Some(Timestamp::new(u64::try_from(at.timestamp_millis()).ok()?)),
        _ => None,
    }
}

fn datum_f64(datum: Datum) -> Option<Option<f64>> {
    match datum {
        Datum::Null => Some(None),
        Datum::Float64(value) => Some(Some(value.into_inner())),
        _ => None,
    }
}

fn datum_u64(datum: Datum) -> Option<Option<u64>> {
    match datum {
        Datum::Null => Some(None),
        Datum::UInt64(value) => Some(Some(value)),
        _ => None,
    }
}

/// The summaries in the rows of `mz_cluster_load_history` and
/// `mz_cluster_load_dataflow_history`. Rows that do not decode are skipped, and so are dataflow
/// rows without a cluster row.
pub(crate) fn decode_summaries(
    cluster_rows: &[Row],
    dataflow_rows: &[Row],
) -> Vec<(ClusterId, LoadSummary)> {
    let mut summaries: BTreeMap<(ClusterId, Timestamp), LoadSummary> = BTreeMap::new();
    for row in cluster_rows {
        let decoded = (|| {
            let datums: Vec<_> = row.iter().collect();
            let [
                Datum::String(cluster_id),
                start,
                at,
                Datum::String(size),
                Datum::UInt64(fingerprint),
                heap_bytes,
                heap_fraction,
                busy_millicores,
                busiest_worker_utilization,
                killed,
            ] = datums[..]
            else {
                return None;
            };
            let cluster_id: ClusterId = cluster_id.parse().ok()?;
            let at = datum_timestamp(at)?;
            let heap = datum_u64(heap_bytes)?.map(|bytes| -> Option<HeapSummary> {
                Some(HeapSummary {
                    bytes,
                    fraction: datum_f64(heap_fraction)?,
                })
            });
            let busy = match (
                datum_u64(busy_millicores)?,
                datum_f64(busiest_worker_utilization)?,
            ) {
                (Some(mean_millicores), Some(busiest_worker_utilization)) => Some(BusySummary {
                    mean_millicores,
                    busiest_worker_utilization,
                }),
                _ => None,
            };
            let summary = LoadSummary {
                start: datum_timestamp(start)?,
                at,
                size: size.to_string(),
                fingerprint,
                dataflows: BTreeMap::new(),
                heap: heap.flatten(),
                busy,
                killed: killed == Datum::True,
            };
            Some(((cluster_id, at), summary))
        })();
        match decoded {
            Some((key, summary)) => {
                summaries.insert(key, summary);
            }
            None => warn!(?row, "unexpected cluster load history row"),
        }
    }
    for row in dataflow_rows {
        let decoded = (|| {
            let datums: Vec<_> = row.iter().collect();
            let [
                Datum::String(cluster_id),
                at,
                Datum::String(id),
                Datum::Interval(median_lag),
                progress_ratio,
                behind,
            ] = datums[..]
            else {
                return None;
            };
            let key = (cluster_id.parse().ok()?, datum_timestamp(at)?);
            let id: GlobalId = id.parse().ok()?;
            let dataflow = DataflowSummary {
                median_lag: median_lag.duration().ok()?,
                progress_ratio: datum_f64(progress_ratio)?,
                behind: behind == Datum::True,
            };
            Some((key, id, dataflow))
        })();
        match decoded {
            Some((key, id, dataflow)) => {
                if let Some(summary) = summaries.get_mut(&key) {
                    summary.dataflows.insert(id, dataflow);
                }
            }
            None => warn!(?row, "unexpected cluster load dataflow history row"),
        }
    }
    summaries
        .into_iter()
        .map(|((cluster_id, _), summary)| (cluster_id, summary))
        .collect()
}

impl Coordinator {
    /// Schedules the next load sample at the next multiple of the sample interval.
    pub(crate) fn schedule_cluster_load_sample(&self) {
        let interval =
            LOAD_AUTOSCALING_LAG_SAMPLE_INTERVAL.get(self.catalog().system_config().dyncfgs());
        let now = self.now();
        let delay = Duration::from_millis(next_boundary(now, interval).saturating_sub(now));
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        task::spawn(|| "cluster_load_schedule", async move {
            tokio::time::sleep(delay).await;
            let _ = internal_cmd_tx.send(Message::ClusterLoadSample);
        });
    }

    /// Requests the frontiers of every managed cluster, and schedules the next sample. The
    /// replies arrive as [`Message::ClusterLoadFrontiers`].
    pub(crate) fn cluster_load_sample(&self) {
        let at = Timestamp::new(self.now());
        let mut pending = Vec::new();
        for cluster in self.catalog().clusters().filter(|c| c.is_managed()) {
            if let Ok(rx) = self.controller.compute.load_frontiers(cluster.id) {
                pending.push((cluster.id, rx));
            }
        }
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
            let _ = internal_cmd_tx.send(Message::ClusterLoadFrontiers { at, frontiers });
        });
        self.schedule_cluster_load_sample();
    }

    /// Records the sample taken at `at`, and persists the summaries of the evaluation windows it
    /// closes.
    pub(crate) fn cluster_load_record(
        &mut self,
        at: Timestamp,
        frontiers: BTreeMap<ClusterId, LoadFrontiers>,
    ) {
        let dyncfgs = self.catalog().system_config().dyncfgs();
        let params = summary_params(dyncfgs);
        let retention = CLUSTER_LOAD_HISTORY_RETENTION_INTERVAL.get(dyncfgs);

        let mut observations = BTreeMap::new();
        for (cluster_id, frontiers) in frontiers {
            if let Some(observation) = self.observe_cluster(cluster_id, frontiers) {
                observations.insert(cluster_id, observation);
            }
        }
        let new = self.cluster_load.record(at, observations, &params);

        let clusters: BTreeSet<_> = self
            .catalog()
            .clusters()
            .filter(|c| c.is_managed())
            .map(|c| c.id)
            .collect();
        let replicas: BTreeSet<_> = self
            .catalog()
            .clusters()
            .flat_map(|c| c.replicas().map(|r| r.replica_id))
            .collect();
        let cutoff = Timestamp::new(
            u64::from(at).saturating_sub(u64::try_from(retention.as_millis()).unwrap_or(u64::MAX)),
        );
        self.cluster_load.retain(&clusters, &replicas, cutoff);

        if new.is_empty() || self.controller.read_only() {
            return;
        }
        let mut cluster_updates = Vec::new();
        let mut dataflow_updates = Vec::new();
        for (cluster_id, summary) in &new {
            let (cluster_row, dataflow_rows) = summary_rows(*cluster_id, summary);
            cluster_updates.push((cluster_row, Diff::ONE));
            dataflow_updates.extend(dataflow_rows.into_iter().map(|row| (row, Diff::ONE)));
        }
        self.controller
            .storage
            .append_introspection_updates(IntrospectionType::ClusterLoadHistory, cluster_updates);
        if !dataflow_updates.is_empty() {
            self.controller.storage.append_introspection_updates(
                IntrospectionType::ClusterLoadDataflowHistory,
                dataflow_updates,
            );
        }
    }

    /// What the coordinator observes of a managed cluster, from its frontiers and the latest
    /// metrics of its replicas. Replicas the compute controller did not report are left out.
    fn observe_cluster(
        &self,
        cluster_id: ClusterId,
        frontiers: LoadFrontiers,
    ) -> Option<ClusterObservation> {
        let cluster = self.catalog().try_get_cluster(cluster_id)?;
        let ClusterVariant::Managed(managed) = &cluster.config.variant else {
            return None;
        };
        let LoadFrontiers {
            exports,
            replicas: mut replica_frontiers,
        } = frontiers;
        let replicas = cluster
            .replicas()
            .filter_map(|replica| {
                let ReplicaLocation::Managed(location) = &replica.config.location else {
                    return None;
                };
                let frontiers = replica_frontiers.remove(&replica.replica_id)?;
                let heap = self
                    .controller
                    .replica_metrics(replica.replica_id)
                    .and_then(|metrics| {
                        fullest_process(metrics, process_heap_limit(&location.allocation))
                    });
                let observation = ReplicaObservation {
                    size: location.size.clone(),
                    hydrated: frontiers.hydrated,
                    dataflows: dataflow_frontiers(&frontiers.dataflows),
                    heap,
                };
                Some((replica.replica_id, observation))
            })
            .collect();
        let record = project_expected(managed).load;
        Some(ClusterObservation {
            size: managed.size.clone(),
            record,
            exports,
            replicas,
        })
    }

    /// Reads the summaries within the retention interval back from the history relations. They
    /// arrive as [`Message::ClusterLoadHistoryRestored`].
    pub(crate) fn restore_cluster_load_history(&self) {
        let catalog = self.catalog();
        let collection = |builtin| {
            let item_id = catalog.resolve_builtin_storage_collection(builtin);
            catalog.get_entry(&item_id).latest_global_id()
        };
        let cluster_id = collection(&MZ_CLUSTER_LOAD_HISTORY);
        let dataflow_id = collection(&MZ_CLUSTER_LOAD_DATAFLOW_HISTORY);
        let retention =
            CLUSTER_LOAD_HISTORY_RETENTION_INTERVAL.get(catalog.system_config().dyncfgs());
        let cutoff = Timestamp::new(
            self.now()
                .saturating_sub(u64::try_from(retention.as_millis()).unwrap_or(u64::MAX)),
        );
        let storage_collections = std::sync::Arc::clone(&self.controller.storage_collections);
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        task::spawn(|| "cluster_load_restore", async move {
            // The histories are append-only, and a summary appended twice, by an old and a new
            // leader around a zero-downtime deployment, consolidates to a diff of 2. So read at
            // the latest readable time and keep every row with a positive diff, which
            // `snapshot_latest` would reject.
            let read = |id: GlobalId| {
                let storage_collections = std::sync::Arc::clone(&storage_collections);
                async move {
                    let upper = storage_collections
                        .collection_frontiers(id)
                        .ok()?
                        .write_frontier;
                    let as_of = upper.as_option()?.step_back()?;
                    let mut rows = storage_collections.snapshot(id, as_of).await.ok()?;
                    differential_dataflow::consolidation::consolidate(&mut rows);
                    Some(
                        rows.into_iter()
                            .filter(|(_, diff)| *diff > 0)
                            .map(|(row, _)| row)
                            .collect::<Vec<_>>(),
                    )
                }
            };
            let (Some(cluster_rows), Some(dataflow_rows)) =
                (read(cluster_id).await, read(dataflow_id).await)
            else {
                warn!("cannot read the cluster load histories, starting without them");
                return;
            };
            let summaries = decode_summaries(&cluster_rows, &dataflow_rows)
                .into_iter()
                .filter(|(_, summary)| summary.at >= cutoff)
                .collect();
            let _ = internal_cmd_tx.send(Message::ClusterLoadHistoryRestored(summaries));
        });
    }
}

#[cfg(test)]
mod tests;
