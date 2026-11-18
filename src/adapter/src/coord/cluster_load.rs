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
//! in-memory history survives a restart of `environmentd`. Each summary row also carries the
//! cluster's last hydration time at its size, which restores [`ClusterLoad::last_hydration`].
//! Samples, busy windows, kills, and warm-up state are in memory only. After a restart, a
//! replica's warm-up starts over and the window in progress yields no summary, since it lacks
//! its early samples.
//!
//! After every sample, [`load_rules_inputs`] assembles what the `ON LOAD` rules read of each
//! managed cluster, the rules are evaluated without applying the decision, and the rows of
//! `mz_internal.mz_cluster_load_recommendations` that changed are rewritten.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use mz_adapter_types::cluster_state::{LoadCutoverReason, LoadVerdict};
use mz_adapter_types::dyncfgs::{
    LOAD_AUTOSCALING_DRAINING_PROGRESS_RATIO, LOAD_AUTOSCALING_EVALUATION_WINDOW,
    LOAD_AUTOSCALING_FLOOR_FACTOR, LOAD_AUTOSCALING_LAG_SAMPLE_INTERVAL,
    LOAD_AUTOSCALING_LAG_TOLERANCE, LOAD_AUTOSCALING_WARM_UP,
};
use mz_catalog::builtin::{
    MZ_CLUSTER_LOAD_DATAFLOW_HISTORY, MZ_CLUSTER_LOAD_HISTORY, MZ_CLUSTER_LOAD_RECOMMENDATIONS,
};
use mz_catalog::config::ClusterReplicaSizeMap;
use mz_catalog::memory::objects::{CatalogItem, ClusterVariant, ClusterVariantManaged};
use mz_cluster_client::ReplicaId;
use mz_cluster_controller::ctx::{LoadRecord, OnLoadPolicy, ReconfigurationRecord, TrialKind};
use mz_cluster_controller::load::rules::{
    DropReason, LoadAction, LoadDecision, RuleParams, RulesInput, SizeCapacity, TrialObservation,
    evaluate,
};
use mz_cluster_controller::load::{
    BusySummary, BusyWindow, DataflowFrontiers, DataflowSummary, HeapSummary, LoadSample,
    LoadSummary, MemoryKill, ProcessHeap, ReplicaSample, SummaryParams, fingerprint, summarize,
    window_start,
};
use mz_compute_client::controller::LoadFrontiers;
use mz_controller::clusters::ReplicaLocation;
use mz_controller_types::ClusterId;
use mz_dyncfg::ConfigSet;
use mz_orchestrator::ServiceProcessMetrics;
use mz_ore::now::EpochMillis;
use mz_ore::task;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::jsonb::JsonbPacker;
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_storage_client::controller::IntrospectionType;
use mz_storage_types::dyncfgs::CLUSTER_LOAD_HISTORY_RETENTION_INTERVAL;
use timely::progress::Antichain;
use tracing::warn;

use crate::catalog::BuiltinTableUpdate;
use crate::catalog::cluster_state::{process_heap_limit, project_expected, size_ladder};
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
    /// The first sample of the replica since its creation or last restart. `None` when the
    /// replica was already running at the first sample after a restart of `environmentd`, so
    /// that its hydration started at an unknown time.
    first_seen: Option<Timestamp>,
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
    /// Each cluster's summaries within the retention interval, ordered by window end, with at
    /// most one per window end.
    summaries: BTreeMap<ClusterId, Vec<LoadSummary>>,
    /// Per cluster and replica size, how long the replicas at that size most recently took to
    /// hydrate.
    hydrations: BTreeMap<ClusterId, BTreeMap<String, Duration>>,
    /// The time of the latest recorded sample.
    last_sample: Option<Timestamp>,
}

impl ClusterLoad {
    /// The load summaries of a cluster within the retention interval, oldest first.
    pub(crate) fn summaries(&self, cluster_id: ClusterId) -> &[LoadSummary] {
        self.summaries
            .get(&cluster_id)
            .map_or(&[], |summaries| summaries.as_slice())
    }

    /// The fingerprint of the dataflows on a cluster at its latest sample. `None` before the
    /// cluster's first sample since `environmentd` started.
    pub(crate) fn fingerprint(&self, cluster_id: ClusterId) -> Option<u64> {
        let sample = self.samples.get(&cluster_id)?.last()?;
        Some(fingerprint(&sample.exports))
    }

    /// How long the replicas of a cluster at `size` most recently took from their creation, or
    /// their last restart, to hydrated. Measured to the sample interval, and restored from
    /// `mz_cluster_load_history` after a restart of `environmentd`. `None` when no such
    /// hydration was seen within the retention interval.
    pub(crate) fn last_hydration(&self, cluster_id: ClusterId, size: &str) -> Option<Duration> {
        self.hydrations.get(&cluster_id)?.get(size).copied()
    }

    /// The largest heap of the fullest process of a cluster's replicas at `size` in its latest
    /// sample, between each replica's creation, or last restart, and its first hydration. `None`
    /// when no such replica reported a heap then.
    pub(crate) fn hydration_peak(&self, cluster_id: ClusterId, size: &str) -> Option<u64> {
        let sample = self.samples.get(&cluster_id)?.last()?;
        sample
            .replicas
            .iter()
            .filter(|(_, replica)| replica.size == size)
            .filter_map(|(id, _)| self.replicas.get(id)?.hydration_peak)
            .map(|heap| heap.bytes)
            .max()
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
        let first = self.last_sample.is_none();
        self.last_sample = Some(at);
        let current_window = window_start(at, params.window);
        let mut new = Vec::new();
        for (cluster_id, observation) in observations {
            let sample = self.sample(cluster_id, at, observation.clone(), first);
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
            insert_summary(
                self.summaries.entry(*cluster_id).or_default(),
                summary.clone(),
                true,
            );
        }
        new
    }

    /// Builds the sample of one cluster, and updates the state of its replicas and its
    /// hydration times. `first` is whether this is the first sample since `environmentd`
    /// started.
    fn sample(
        &mut self,
        cluster_id: ClusterId,
        at: Timestamp,
        observation: ClusterObservation,
        first: bool,
    ) -> LoadSample {
        let ClusterObservation {
            size,
            record: _,
            exports,
            replicas,
        } = observation;
        let replicas = replicas
            .into_iter()
            .map(|(replica_id, replica)| {
                let tracker = self
                    .replicas
                    .entry(replica_id)
                    .or_insert_with(|| ReplicaTracker {
                        first_seen: (!first).then_some(at),
                        ..Default::default()
                    });
                if replica.size != size {
                    tracker.eligible_since = None;
                } else if replica.hydrated && tracker.eligible_since.is_none() {
                    tracker.eligible_since = Some(at);
                }
                if !tracker.hydrated_once {
                    if replica.hydrated {
                        tracker.hydrated_once = true;
                        if let Some(first_seen) = tracker.first_seen {
                            let took = Duration::from_millis(
                                u64::from(at).saturating_sub(u64::from(first_seen)),
                            );
                            self.hydrations
                                .entry(cluster_id)
                                .or_default()
                                .insert(replica.size.clone(), took);
                        }
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

    /// Adds summaries read back from the history relations, each with the hydration time at its
    /// size as of its window. A summary or a hydration time already in memory wins.
    pub(crate) fn restore(&mut self, summaries: Vec<RestoredSummary>) {
        let mut hydrations: BTreeMap<(ClusterId, String), (Timestamp, Duration)> = BTreeMap::new();
        for RestoredSummary {
            cluster_id,
            summary,
            hydration_time,
        } in summaries
        {
            if let Some(took) = hydration_time {
                let latest = hydrations
                    .entry((cluster_id, summary.size.clone()))
                    .or_insert((summary.at, took));
                if summary.at >= latest.0 {
                    *latest = (summary.at, took);
                }
            }
            insert_summary(
                self.summaries.entry(cluster_id).or_default(),
                summary,
                false,
            );
        }
        for ((cluster_id, size), (_, took)) in hydrations {
            self.hydrations
                .entry(cluster_id)
                .or_default()
                .entry(size)
                .or_insert(took);
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
        self.hydrations.retain(|id, _| clusters.contains(id));
        self.summaries.retain(|id, summaries| {
            summaries.retain(|summary| summary.at >= cutoff);
            clusters.contains(id) && !summaries.is_empty()
        });
    }
}

/// A summary read back from the history relations.
#[derive(Clone, Debug, PartialEq)]
pub struct RestoredSummary {
    pub cluster_id: ClusterId,
    pub summary: LoadSummary,
    /// See [`ClusterLoad::last_hydration`], at the summary's size as of its window.
    pub hydration_time: Option<Duration>,
}

/// Inserts `summary` into `summaries`, which is ordered by window end. A summary with the same
/// window end is replaced when `replace` is set, and kept otherwise.
fn insert_summary(summaries: &mut Vec<LoadSummary>, summary: LoadSummary, replace: bool) {
    match summaries.binary_search_by_key(&summary.at, |s| s.at) {
        Ok(i) if replace => summaries[i] = summary,
        Ok(_) => {}
        Err(i) => summaries.insert(i, summary),
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
        draining_progress_ratio: LOAD_AUTOSCALING_DRAINING_PROGRESS_RATIO.get(dyncfgs),
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

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn rfc3339(at: Timestamp) -> String {
    mz_ore::now::to_datetime(at.into()).to_rfc3339()
}

fn cutover_reason(reason: LoadCutoverReason) -> &'static str {
    match reason {
        LoadCutoverReason::UpTrial => "up-trial",
        LoadCutoverReason::DownTrial => "down-trial",
        LoadCutoverReason::HighHeap => "high-heap",
        LoadCutoverReason::OutOfMemory => "out-of-memory",
        LoadCutoverReason::ProbationReturn => "probation-return",
    }
}

fn interval(duration: Duration) -> Interval {
    Interval::new(
        0,
        0,
        i64::try_from(duration.as_micros()).unwrap_or(i64::MAX),
    )
}

/// The rows of `mz_cluster_load_history` and `mz_cluster_load_dataflow_history` for a summary,
/// with the cluster's hydration time at the summary's size as of its window.
pub(crate) fn summary_rows(
    cluster_id: ClusterId,
    summary: &LoadSummary,
    hydration_time: Option<Duration>,
) -> (Row, Vec<Row>) {
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
        hydrating,
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
        Datum::from(*hydrating),
        hydration_time.map_or(Datum::Null, |took| Datum::Interval(interval(took))),
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
) -> Vec<RestoredSummary> {
    let mut summaries: BTreeMap<(ClusterId, Timestamp), (LoadSummary, Option<Duration>)> =
        BTreeMap::new();
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
                hydrating,
                hydration_time,
            ] = datums[..]
            else {
                return None;
            };
            let cluster_id: ClusterId = cluster_id.parse().ok()?;
            let hydration_time = match hydration_time {
                Datum::Null => None,
                Datum::Interval(took) => Some(took.duration().ok()?),
                _ => return None,
            };
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
                hydrating: hydrating == Datum::True,
            };
            Some(((cluster_id, at), (summary, hydration_time)))
        })();
        match decoded {
            Some((key, decoded)) => {
                summaries.insert(key, decoded);
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
                if let Some((summary, _)) = summaries.get_mut(&key) {
                    summary.dataflows.insert(id, dataflow);
                }
            }
            None => warn!(?row, "unexpected cluster load dataflow history row"),
        }
    }
    summaries
        .into_iter()
        .map(
            |((cluster_id, _), (summary, hydration_time))| RestoredSummary {
                cluster_id,
                summary,
                hydration_time,
            },
        )
        .collect()
}

/// What the `ON LOAD` rules read of one managed cluster, apart from a trial in flight and the
/// time of the evaluation. See [`load_rules_inputs`].
#[derive(Clone, Debug)]
pub(crate) struct LoadRulesInputs<'a> {
    /// The cluster's `ON LOAD` policy. For a cluster without one, a policy whose range spans its
    /// whole size ladder, so that the rules consider every size of its family.
    pub policy: OnLoadPolicy,
    /// See [`size_ladder`].
    pub ladder: Vec<SizeCapacity>,
    /// The realized size.
    pub size: String,
    pub reconfiguration: Option<ReconfigurationRecord>,
    pub record: Option<LoadRecord>,
    /// See [`ClusterLoad::fingerprint`].
    pub fingerprint: u64,
    pub summaries: &'a [LoadSummary],
    /// See [`ClusterLoad::last_hydration`].
    pub last_hydration: Option<Duration>,
    /// See [`ClusterLoad::hydration_peak`].
    pub hydration_peak: Option<u64>,
    pub params: RuleParams,
}

impl LoadRulesInputs<'_> {
    pub(crate) fn rules_input<'b>(
        &'b self,
        trial: Option<&'b TrialObservation>,
        now: Timestamp,
    ) -> RulesInput<'b> {
        RulesInput {
            policy: &self.policy,
            ladder: &self.ladder,
            size: &self.size,
            reconfiguration: self.reconfiguration.as_ref(),
            record: self.record.as_ref(),
            fingerprint: self.fingerprint,
            summaries: self.summaries,
            trial,
            last_hydration: self.last_hydration,
            hydration_peak: self.hydration_peak,
            now,
        }
    }

    /// Evaluates the rules. `None` when the realized size or a bound of the policy is not on
    /// the ladder.
    pub(crate) fn evaluate(
        &self,
        trial: Option<&TrialObservation>,
        now: Timestamp,
    ) -> Option<LoadDecision> {
        evaluate(&self.rules_input(trial, now), &self.params)
    }
}

/// Assembles what the `ON LOAD` rules read of a managed cluster from its catalog config, the
/// replica sizes, the load signal, and the dyncfgs. `None` when the cluster's size is unknown,
/// or when the cluster has not been sampled since `environmentd` started.
pub(crate) fn load_rules_inputs<'a>(
    cluster_id: ClusterId,
    managed: &ClusterVariantManaged,
    sizes: &ClusterReplicaSizeMap,
    load: &'a ClusterLoad,
    dyncfgs: &ConfigSet,
) -> Option<LoadRulesInputs<'a>> {
    let expected = project_expected(managed);
    let ladder = size_ladder(sizes, &expected.size);
    let policy = match expected
        .auto_scaling_policy
        .and_then(|policy| policy.on_load)
    {
        Some(policy) => policy,
        None => OnLoadPolicy {
            min_size: ladder.first()?.name.clone(),
            max_size: ladder.last()?.name.clone(),
            scale_down_after: None,
        },
    };
    Some(LoadRulesInputs {
        policy,
        fingerprint: load.fingerprint(cluster_id)?,
        last_hydration: load.last_hydration(cluster_id, &expected.size),
        hydration_peak: load.hydration_peak(cluster_id, &expected.size),
        summaries: load.summaries(cluster_id),
        ladder,
        size: expected.size,
        reconfiguration: expected.reconfiguration,
        record: expected.load,
        params: RuleParams::from_config(dyncfgs),
    })
}

/// The row of `mz_cluster_load_recommendations` for a cluster at `size`, from the inputs and
/// the decision of the rules when they could be evaluated.
pub(crate) fn recommendation_row(
    cluster_id: ClusterId,
    size: &str,
    evaluated: Option<(&LoadRulesInputs, &LoadDecision)>,
) -> Row {
    let cluster_id = cluster_id.to_string();
    let mut row = Row::default();
    let mut packer = row.packer();
    packer.push(Datum::String(&cluster_id));
    packer.push(Datum::String(size));
    let Some((inputs, decision)) = evaluated else {
        // `min_size` to `window_end`, an empty `behind`, and `heap_bytes` to `last_hydration`.
        packer.extend(std::iter::repeat_n(Datum::Null, 4));
        JsonbPacker::new(&mut packer)
            .pack_serde_json(serde_json::Value::Array(Vec::new()))
            .expect("valid JSON");
        packer.extend(std::iter::repeat_n(Datum::Null, 10));
        return row;
    };
    let facts = &decision.facts;
    packer.push(Datum::String(&inputs.policy.min_size));
    packer.push(Datum::String(&inputs.policy.max_size));
    packer.push(Datum::String(decision.status.as_str()));
    packer.push(facts.summary_at.map_or(Datum::Null, timestamptz));
    let behind: Vec<_> = facts
        .behind
        .iter()
        .map(|dataflow| {
            serde_json::json!({
                "dataflow_id": dataflow.id.to_string(),
                "lag_ms": millis(dataflow.median_lag),
                "progress_ratio": dataflow.progress_ratio,
                "floor_ms": dataflow.floor.map(millis),
            })
        })
        .collect();
    JsonbPacker::new(&mut packer)
        .pack_serde_json(serde_json::Value::Array(behind))
        .expect("valid JSON");
    packer.push(
        facts
            .heap
            .map_or(Datum::Null, |heap| Datum::UInt64(heap.bytes)),
    );
    packer.push(
        facts
            .heap
            .and_then(|heap| heap.fraction)
            .map_or(Datum::Null, |fraction| Datum::Float64(fraction.into())),
    );
    packer.push(
        facts
            .busy
            .map_or(Datum::Null, |busy| Datum::UInt64(busy.mean_millicores)),
    );
    packer.push(facts.busy.map_or(Datum::Null, |busy| {
        Datum::Float64(busy.busiest_worker_utilization.into())
    }));
    let in_flight_target = inputs
        .reconfiguration
        .as_ref()
        .map(|reconfiguration| reconfiguration.target.size.as_str());
    let (action, target, reason) = match &decision.action {
        None => (None, None, None),
        Some(LoadAction::StartTrial(start)) => {
            let action = match start.kind {
                TrialKind::Up => "up-trial",
                TrialKind::Down => "down-trial",
            };
            (Some(action), Some(start.target.as_str()), None)
        }
        Some(LoadAction::Reconfigure { target, reason, .. }) => (
            Some("scale-up"),
            Some(target.as_str()),
            Some(cutover_reason(*reason)),
        ),
        Some(LoadAction::CutOver(_)) => (Some("cut-over"), in_flight_target, None),
        Some(LoadAction::Drop(reason)) => {
            let reason = match reason {
                DropReason::Failed(_) => "failed",
                DropReason::Killed => "killed",
                DropReason::NotHydrated => "not-hydrated",
                DropReason::Deadline => "deadline",
            };
            (Some("drop"), in_flight_target, Some(reason))
        }
    };
    for value in [action, target, reason] {
        packer.push(value.map_or(Datum::Null, Datum::String));
    }
    match &facts.blocking {
        Some(evidence) => {
            let verdict = match evidence.verdict {
                LoadVerdict::Insufficient => "insufficient",
                LoadVerdict::NoGain => "no-gain",
            };
            let expires_at =
                Timestamp::new(u64::from(evidence.at).saturating_add(millis(evidence.interval)));
            let evidence = serde_json::json!({
                "size": evidence.size,
                "verdict": verdict,
                "recorded_at": rfc3339(evidence.at),
                "expires_at": rfc3339(expires_at),
            });
            JsonbPacker::new(&mut packer)
                .pack_serde_json(evidence)
                .expect("valid JSON");
        }
        None => packer.push(Datum::Null),
    }
    packer.push(
        facts
            .down_block
            .map_or(Datum::Null, |block| Datum::String(block.as_str())),
    );
    packer.push(
        inputs
            .last_hydration
            .map_or(Datum::Null, |took| Datum::Interval(interval(took))),
    );
    row
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
        self.update_cluster_load_recommendations(at);

        if new.is_empty() || self.controller.read_only() {
            return;
        }
        let mut cluster_updates = Vec::new();
        let mut dataflow_updates = Vec::new();
        for (cluster_id, summary) in &new {
            let hydration_time = self.cluster_load.last_hydration(*cluster_id, &summary.size);
            let (cluster_row, dataflow_rows) = summary_rows(*cluster_id, summary, hydration_time);
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

    /// What the `ON LOAD` rules read of a managed cluster. See [`load_rules_inputs`].
    pub(crate) fn load_rules_inputs(&self, cluster_id: ClusterId) -> Option<LoadRulesInputs<'_>> {
        let catalog = self.catalog();
        let cluster = catalog.try_get_cluster(cluster_id)?;
        let ClusterVariant::Managed(managed) = &cluster.config.variant else {
            return None;
        };
        load_rules_inputs(
            cluster_id,
            managed,
            catalog.cluster_replica_sizes(),
            &self.cluster_load,
            catalog.system_config().dyncfgs(),
        )
    }

    /// Evaluates the rules for every managed cluster at `at`, without applying the decisions,
    /// and writes the rows of `mz_cluster_load_recommendations` that changed.
    fn update_cluster_load_recommendations(&mut self, at: Timestamp) {
        let mut rows = BTreeMap::new();
        for cluster in self.catalog().clusters() {
            let ClusterVariant::Managed(managed) = &cluster.config.variant else {
                continue;
            };
            let inputs = self.load_rules_inputs(cluster.id);
            let decision = inputs.as_ref().and_then(|inputs| inputs.evaluate(None, at));
            let evaluated = inputs.as_ref().zip(decision.as_ref());
            let row = recommendation_row(cluster.id, &managed.size, evaluated);
            rows.insert(cluster.id, row);
        }
        let item_id = self
            .catalog()
            .resolve_builtin_table(&MZ_CLUSTER_LOAD_RECOMMENDATIONS);
        let previous = std::mem::replace(&mut self.cluster_load_recommendations, rows);
        let current = &self.cluster_load_recommendations;
        let retractions = previous
            .iter()
            .filter(|(id, row)| current.get(id) != Some(row))
            .map(|(_, row)| BuiltinTableUpdate::row(item_id, row.clone(), Diff::MINUS_ONE));
        let additions = current
            .iter()
            .filter(|(id, row)| previous.get(id) != Some(row))
            .map(|(_, row)| BuiltinTableUpdate::row(item_id, row.clone(), Diff::ONE));
        let updates: Vec<_> = retractions.chain(additions).collect();
        if !updates.is_empty() {
            // The rows are advisory, so they ride along with the next group commit.
            drop(self.builtin_table_update().background(updates));
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
        // A materialized view with a refresh schedule advances its output only at its refreshes,
        // so its lag says nothing about the cluster. It stays in `exports`, the fingerprint.
        let scheduled: BTreeSet<GlobalId> = cluster
            .bound_objects
            .iter()
            .filter_map(|id| match self.catalog().get_entry(id).item() {
                CatalogItem::MaterializedView(mv) if mv.refresh_schedule.is_some() => {
                    Some(mv.global_id_writes())
                }
                _ => None,
            })
            .collect();
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
                let mut dataflows = dataflow_frontiers(&frontiers.dataflows);
                dataflows.retain(|id, _| !scheduled.contains(id));
                let observation = ReplicaObservation {
                    size: location.size.clone(),
                    hydrated: frontiers.hydrated,
                    dataflows,
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
                .filter(|restored| restored.summary.at >= cutoff)
                .collect();
            let _ = internal_cmd_tx.send(Message::ClusterLoadHistoryRestored(summaries));
        });
    }
}

#[cfg(test)]
mod tests;
