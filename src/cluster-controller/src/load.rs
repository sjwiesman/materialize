// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The load signal of the `ON LOAD` strategy: samples in, load summaries out.
//!
//! The environment samples every managed cluster at a fixed interval. A [`LoadSample`] holds,
//! per replica, the input and output frontiers of each dataflow, whether the replica is
//! hydrated, and the heap of its fullest process. Replicas also report their busy time per busy
//! window ([`BusyWindow`]), and the orchestrator reports memory kills ([`MemoryKill`]).
//!
//! At the end of each evaluation window, [`summarize`] reduces the window's samples to one
//! [`LoadSummary`]. Only replicas that qualify at every sample of the window contribute: they
//! run at the cluster's realized size, every collection they host is hydrated, and they are past
//! their warm-up. Hydration is a state of the whole replica, so a replica on which any dataflow
//! is hydrating contributes nothing.
//!
//! Everything here is a pure function of its arguments. Time is the [`Timestamp`] of the
//! `EpochMilliseconds` timeline, the unit of the frontiers.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use mz_adapter_types::cluster_state::LoadRecord;
use mz_controller_types::ReplicaId;
use mz_ore::cast::CastLossy;
use mz_repr::{GlobalId, Timestamp};

/// The frontiers of one dataflow on one replica at one sample.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DataflowFrontiers {
    /// The least write frontier of the dataflow's direct inputs.
    pub input: Timestamp,
    /// The replica's output frontier of the dataflow.
    pub output: Timestamp,
}

impl DataflowFrontiers {
    /// The local lag: how far the output trails the inputs.
    pub fn lag(&self) -> Duration {
        Duration::from_millis(u64::from(self.input).saturating_sub(u64::from(self.output)))
    }
}

/// The heap of a replica's fullest process at one sample.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessHeap {
    /// Memory plus swap of the process.
    pub bytes: u64,
    /// The heap limit of the process. `None` when neither the process nor its size has one.
    pub limit: Option<u64>,
}

impl ProcessHeap {
    /// The heap as a fraction of its limit.
    pub fn fraction(&self) -> Option<f64> {
        let limit = self.limit.filter(|limit| *limit > 0)?;
        Some(f64::cast_lossy(self.bytes) / f64::cast_lossy(limit))
    }
}

/// One replica at one sample.
#[derive(Clone, Debug, PartialEq)]
pub struct ReplicaSample {
    /// The replica's size.
    pub size: String,
    /// Whether every non-transient collection the replica hosts is hydrated.
    pub hydrated: bool,
    /// Since when the replica has been at the cluster's realized size and hydrated, without a
    /// restart in between. Its warm-up runs from here. `None` while it is not.
    pub eligible_since: Option<Timestamp>,
    /// The frontiers of every dataflow on the replica whose frontiers are not empty.
    pub dataflows: BTreeMap<GlobalId, DataflowFrontiers>,
    /// `None` until a process of the replica reported its heap.
    pub heap: Option<ProcessHeap>,
}

/// One sample of a cluster.
#[derive(Clone, Debug, PartialEq)]
pub struct LoadSample {
    pub at: Timestamp,
    /// The cluster's realized size.
    pub size: String,
    /// The collections the cluster's dataflows export.
    pub exports: BTreeSet<GlobalId>,
    pub replicas: BTreeMap<ReplicaId, ReplicaSample>,
}

/// One closed busy window of a replica, from the workers that agree on its bounds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BusyWindow {
    pub replica_id: ReplicaId,
    pub start: Timestamp,
    pub end: Timestamp,
    pub workers: u64,
    /// Non-parked time, summed over workers.
    pub busy: Duration,
    /// The non-parked time of the busiest worker.
    pub max_worker_busy: Duration,
}

/// A replica killed for exceeding its memory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryKill {
    pub replica_id: ReplicaId,
    /// The replica's size.
    pub size: String,
    pub at: Timestamp,
}

/// The parameters of [`summarize`].
#[derive(Clone, Debug, PartialEq)]
pub struct SummaryParams {
    /// The length of an evaluation window.
    pub window: Duration,
    /// The interval between samples.
    pub sample_interval: Duration,
    /// How long after its `eligible_since` a replica's samples are ignored.
    pub warm_up: Duration,
    /// `τ`: the median lag within which a dataflow without a floor is not behind.
    pub lag_tolerance: Duration,
    /// The multiple of its floor above which a dataflow with a floor is behind.
    pub floor_factor: f64,
}

/// The load of a cluster over one evaluation window.
#[derive(Clone, Debug, PartialEq)]
pub struct LoadSummary {
    /// The start of the window.
    pub start: Timestamp,
    /// The end of the window, exclusive.
    pub at: Timestamp,
    /// The cluster's realized size.
    pub size: String,
    /// See [`fingerprint`].
    pub fingerprint: u64,
    /// Per dataflow with lag samples on the replicas that contributed. Empty when no replica
    /// qualified.
    pub dataflows: BTreeMap<GlobalId, DataflowSummary>,
    /// The largest steady-state heap among the replicas that contributed. `None` when none
    /// reported a heap, or none qualified.
    pub heap: Option<HeapSummary>,
    /// The busiest of the replicas that contributed. `None` when none delivered a busy window
    /// within the evaluation window.
    pub busy: Option<BusySummary>,
    /// Whether a replica at the realized size was killed for memory within the window.
    pub killed: bool,
    /// Whether this is a hydrating window: no replica qualified, and a replica at the realized
    /// size was hydrating within the window. A hydrating window carries no lag, heap, or busy
    /// fields.
    pub hydrating: bool,
}

/// One dataflow in a [`LoadSummary`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DataflowSummary {
    /// The median over the window of the cluster's lag, the smallest lag among the contributing
    /// replicas at each sample.
    pub median_lag: Duration,
    /// See [`DataflowStats::progress_ratio`].
    pub progress_ratio: Option<f64>,
    /// See [`is_behind`].
    pub behind: bool,
}

/// The steady-state heap of a replica: the medians over a window of its fullest process.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HeapSummary {
    /// The median heap of the fullest process, in bytes.
    pub bytes: u64,
    /// The median heap of the fullest process as a fraction of its limit. `None` when the limit
    /// is unknown.
    pub fraction: Option<f64>,
}

/// The busy time of a replica over a window.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BusySummary {
    /// Non-parked time over the busy windows, summed over workers, divided by their length.
    pub mean_millicores: u64,
    /// The mean over the busy windows of the busiest worker's share of the window.
    pub busiest_worker_utilization: f64,
}

/// The lag statistics of one dataflow over an interval, on a set of replicas.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DataflowStats {
    /// The median of the smallest lag among the replicas at each sample.
    pub median_lag: Duration,
    /// The median lag of the samples in the first third of the interval. `None` when it holds
    /// none.
    pub first_third_median_lag: Option<Duration>,
    /// The median lag of the samples in the last third of the interval. `None` when it holds
    /// none.
    pub last_third_median_lag: Option<Duration>,
    /// How far the furthest output frontier among the replicas advanced, divided by how far the
    /// input frontier advanced, between the first and the last sample of the dataflow. A
    /// dataflow at a steady lag has a ratio of 1. `None` when the input did not advance.
    pub progress_ratio: Option<f64>,
}

/// The fingerprint of the set of dataflows installed on a cluster, from the collections they
/// export.
///
/// The hash is SeaHash, whose output is fixed across platforms and releases of the crate, over
/// the identifiers' string forms, sorted and joined by newlines. It does not depend on the
/// order of `exports`, or on the in-memory representation or `Ord` of [`GlobalId`], so it is
/// stable across restarts and releases.
pub fn fingerprint<'a>(exports: impl IntoIterator<Item = &'a GlobalId>) -> u64 {
    let ids: BTreeSet<String> = exports.into_iter().map(|id| id.to_string()).collect();
    let mut bytes = Vec::new();
    for id in ids {
        bytes.extend_from_slice(id.as_bytes());
        bytes.push(b'\n');
    }
    seahash::hash(&bytes)
}

/// The start of the evaluation window of length `window` that contains `at`. Windows start at
/// multiples of their length since the Unix epoch.
pub fn window_start(at: Timestamp, window: Duration) -> Timestamp {
    let length = u64::try_from(window.as_millis()).unwrap_or(u64::MAX).max(1);
    let at = u64::from(at);
    Timestamp::new(at - at % length)
}

/// The median lag above which a dataflow is behind: `tolerance`, or `floor_factor` times its
/// lag floor when it has one and that is larger.
pub fn behind_threshold(
    tolerance: Duration,
    floor: Option<Duration>,
    floor_factor: f64,
) -> Duration {
    let from_floor = floor.map_or(Duration::ZERO, |floor| {
        Duration::try_from_secs_f64(floor.as_secs_f64() * floor_factor).unwrap_or(Duration::MAX)
    });
    tolerance.max(from_floor)
}

/// Whether a dataflow is working off a backlog: the median lag of the last third of the
/// interval is below half that of the first third. `false` when either third has no samples.
pub fn is_draining(stats: &DataflowStats) -> bool {
    match (stats.first_third_median_lag, stats.last_third_median_lag) {
        (Some(first), Some(last)) => last < first / 2,
        _ => false,
    }
}

/// Whether a dataflow is behind: its median lag exceeds `threshold`, see [`behind_threshold`],
/// and it is not draining, see [`is_draining`].
pub fn is_behind(stats: &DataflowStats, threshold: Duration) -> bool {
    stats.median_lag > threshold && !is_draining(stats)
}

/// The evaluation windows a down-trial proposal reads, see [`quiet_window`].
#[derive(Clone, Debug, PartialEq)]
pub struct QuietWindow<'a> {
    /// The summaries of the windows the quiet window is made of, newest first.
    pub summaries: Vec<&'a LoadSummary>,
    /// How many evaluation windows the quiet window is made of, with or without a summary.
    pub windows: usize,
    /// Whether those windows add up to the quiet window's length.
    pub filled: bool,
}

impl QuietWindow<'_> {
    /// The share of the quiet window's evaluation windows that have a summary. 1 when it is made
    /// of no windows.
    pub fn coverage(&self) -> f64 {
        if self.windows == 0 {
            return 1.0;
        }
        f64::cast_lossy(self.summaries.len()) / f64::cast_lossy(self.windows)
    }
}

/// The quiet window of length `length` before `now`, over evaluation windows of length
/// `window`.
///
/// It is made of the most recent evaluation windows that ended by `now` and are not hydrating
/// windows, taken back until they add up to `length`, and reaching back at most `2 * length`
/// from `now`. A window without a summary is part of it. A hydrating window in which a replica
/// was also killed for memory is part of it too, so that the kill is seen.
pub fn quiet_window<'a>(
    summaries: impl IntoIterator<Item = &'a LoadSummary>,
    now: Timestamp,
    length: Duration,
    window: Duration,
) -> QuietWindow<'a> {
    let by_end: BTreeMap<u64, &LoadSummary> = summaries
        .into_iter()
        .map(|summary| (u64::from(summary.at), summary))
        .collect();
    let window = u64::try_from(window.as_millis()).unwrap_or(u64::MAX).max(1);
    let length = u64::try_from(length.as_millis()).unwrap_or(u64::MAX);
    let now = u64::from(now);
    let reach = now.saturating_sub(length.saturating_mul(2));

    let mut quiet = QuietWindow {
        summaries: Vec::new(),
        windows: 0,
        filled: false,
    };
    let mut covered = 0u64;
    let mut end = now - now % window;
    while covered < length && end >= window && end - window >= reach {
        let summary = by_end.get(&end).copied();
        let skipped = summary.is_some_and(|summary| summary.hydrating && !summary.killed);
        if !skipped {
            quiet.windows += 1;
            covered = covered.saturating_add(window);
            quiet.summaries.extend(summary);
        }
        end -= window;
    }
    quiet.filled = covered >= length;
    quiet
}

/// The lag statistics of every dataflow over the samples in `[start, end)`, on the `replicas`
/// that have it.
///
/// The cluster's lag at a sample is the smallest among the replicas, since replicas do the same
/// work and the freshest one answers. A dataflow that none of the replicas has at some sample
/// is measured over the samples where they do.
pub fn dataflow_stats(
    samples: &[LoadSample],
    replicas: &BTreeSet<ReplicaId>,
    start: Timestamp,
    end: Timestamp,
) -> BTreeMap<GlobalId, DataflowStats> {
    let length = u64::from(end).saturating_sub(u64::from(start));
    let first_third_end = u64::from(start).saturating_add(length / 3);
    let last_third_start = u64::from(end).saturating_sub(length / 3);

    // Per dataflow, in sample order: the sample time, the cluster's lag, its input frontier,
    // and the furthest output frontier.
    let mut series: BTreeMap<GlobalId, Vec<(u64, Duration, Timestamp, Timestamp)>> =
        BTreeMap::new();
    for sample in samples
        .iter()
        .filter(|sample| sample.at >= start && sample.at < end)
    {
        let mut at_sample: BTreeMap<GlobalId, (Duration, Timestamp, Timestamp)> = BTreeMap::new();
        for (replica_id, replica) in &sample.replicas {
            if !replicas.contains(replica_id) {
                continue;
            }
            for (id, frontiers) in &replica.dataflows {
                let lag = frontiers.lag();
                let entry =
                    at_sample
                        .entry(*id)
                        .or_insert((lag, frontiers.input, frontiers.output));
                entry.0 = entry.0.min(lag);
                entry.1 = entry.1.max(frontiers.input);
                entry.2 = entry.2.max(frontiers.output);
            }
        }
        for (id, (lag, input, output)) in at_sample {
            series
                .entry(id)
                .or_default()
                .push((u64::from(sample.at), lag, input, output));
        }
    }

    series
        .into_iter()
        .filter_map(|(id, points)| {
            let lags: Vec<_> = points.iter().map(|(_, lag, _, _)| *lag).collect();
            let first_third: Vec<_> = points
                .iter()
                .filter(|(at, ..)| *at < first_third_end)
                .map(|(_, lag, _, _)| *lag)
                .collect();
            let last_third: Vec<_> = points
                .iter()
                .filter(|(at, ..)| *at >= last_third_start)
                .map(|(_, lag, _, _)| *lag)
                .collect();
            let (_, _, first_input, first_output) = points.first()?;
            let (_, _, last_input, last_output) = points.last()?;
            let input_advance = u64::from(*last_input).saturating_sub(u64::from(*first_input));
            let output_advance = u64::from(*last_output).saturating_sub(u64::from(*first_output));
            let progress_ratio = (input_advance > 0)
                .then(|| f64::cast_lossy(output_advance) / f64::cast_lossy(input_advance));
            let stats = DataflowStats {
                median_lag: median(lags)?,
                first_third_median_lag: median(first_third),
                last_third_median_lag: median(last_third),
                progress_ratio,
            };
            Some((id, stats))
        })
        .collect()
}

/// Reduces the samples, busy windows, and memory kills of the evaluation window `[start, end)`
/// to one summary.
///
/// A replica contributes when it qualifies at every sample of the window: it is at the realized
/// size, hydrated, and past its warm-up. A window yields no summary when it has fewer than half
/// the samples its length calls for, none in its first or last third, or no contributing
/// replica, with two exceptions that carry no lag, heap, or busy fields. A window with enough
/// samples in which a replica at the realized size was hydrating is a hydrating window. A
/// window in which a replica at the realized size was killed for memory has `killed` set,
/// whatever else it carries, so the kill is not lost to the restart it causes.
///
/// `record` provides the lag floors. They apply only when its fingerprint is the summary's.
pub fn summarize(
    start: Timestamp,
    end: Timestamp,
    samples: &[LoadSample],
    busy: &[BusyWindow],
    kills: &[MemoryKill],
    record: Option<&LoadRecord>,
    params: &SummaryParams,
) -> Option<LoadSummary> {
    let window: Vec<_> = samples
        .iter()
        .filter(|sample| sample.at >= start && sample.at < end)
        .cloned()
        .collect();
    let last = window.last()?;
    let size = last.size.clone();
    let fingerprint = fingerprint(&last.exports);
    let killed = kills
        .iter()
        .any(|kill| kill.at >= start && kill.at < end && kill.size == size);
    let without_contributors = |hydrating: bool| {
        (killed || hydrating).then(|| LoadSummary {
            start,
            at: end,
            size: size.clone(),
            fingerprint,
            dataflows: BTreeMap::new(),
            heap: None,
            busy: None,
            killed,
            hydrating,
        })
    };

    if !enough_samples(&window, start, end, params) {
        return without_contributors(false);
    }
    let contributing = contributing_replicas(&window, &size, params.warm_up);
    if contributing.is_empty() {
        let hydrating = window.iter().any(|sample| {
            sample
                .replicas
                .values()
                .any(|replica| replica.size == sample.size && !replica.hydrated)
        });
        return without_contributors(hydrating);
    }

    let floors: BTreeMap<GlobalId, Duration> = record
        .filter(|record| record.fingerprint == fingerprint)
        .map(|record| {
            record
                .floors
                .iter()
                .map(|floor| (floor.dataflow, floor.lag))
                .collect()
        })
        .unwrap_or_default();
    let dataflows = dataflow_stats(&window, &contributing, start, end)
        .into_iter()
        .map(|(id, stats)| {
            let threshold = behind_threshold(
                params.lag_tolerance,
                floors.get(&id).copied(),
                params.floor_factor,
            );
            let summary = DataflowSummary {
                median_lag: stats.median_lag,
                progress_ratio: stats.progress_ratio,
                behind: is_behind(&stats, threshold),
            };
            (id, summary)
        })
        .collect();

    Some(LoadSummary {
        start,
        at: end,
        size,
        fingerprint,
        dataflows,
        heap: steady_state_heap(&window, &contributing),
        busy: busy_summary(busy, &contributing, start, end),
        killed,
        hydrating: false,
    })
}

/// Whether the window holds at least half the samples its length calls for, and samples in its
/// first and last thirds.
fn enough_samples(
    window: &[LoadSample],
    start: Timestamp,
    end: Timestamp,
    params: &SummaryParams,
) -> bool {
    let length = u64::from(end).saturating_sub(u64::from(start));
    let interval = u64::try_from(params.sample_interval.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    let expected = (length / interval).max(1);
    let count = u64::try_from(window.len()).unwrap_or(u64::MAX);
    let first_third_end = u64::from(start).saturating_add(length / 3);
    let last_third_start = u64::from(end).saturating_sub(length / 3);
    count.saturating_mul(2) >= expected
        && window.iter().any(|s| u64::from(s.at) < first_third_end)
        && window.iter().any(|s| u64::from(s.at) >= last_third_start)
}

/// The replicas that qualify at every sample of `window`.
fn contributing_replicas(
    window: &[LoadSample],
    size: &str,
    warm_up: Duration,
) -> BTreeSet<ReplicaId> {
    let warm_up = u64::try_from(warm_up.as_millis()).unwrap_or(u64::MAX);
    let qualifying = |sample: &LoadSample| -> BTreeSet<ReplicaId> {
        sample
            .replicas
            .iter()
            .filter(|(_, replica)| {
                replica.size == size
                    && sample.size == size
                    && replica.hydrated
                    && replica.eligible_since.is_some_and(|since| {
                        u64::from(sample.at) >= u64::from(since).saturating_add(warm_up)
                    })
            })
            .map(|(id, _)| *id)
            .collect()
    };
    let mut samples = window.iter();
    let Some(first) = samples.next() else {
        return BTreeSet::new();
    };
    let mut contributing = qualifying(first);
    for sample in samples {
        let at_sample = qualifying(sample);
        contributing.retain(|id| at_sample.contains(id));
    }
    contributing
}

/// The largest steady-state heap among `replicas`, by fraction of the limit, then by bytes.
fn steady_state_heap(window: &[LoadSample], replicas: &BTreeSet<ReplicaId>) -> Option<HeapSummary> {
    replicas
        .iter()
        .filter_map(|replica_id| {
            let heaps: Vec<_> = window
                .iter()
                .filter_map(|sample| sample.replicas.get(replica_id)?.heap)
                .collect();
            let bytes = median(heaps.iter().map(|heap| heap.bytes).collect())?;
            let fractions: Vec<_> = heaps.iter().filter_map(ProcessHeap::fraction).collect();
            Some(HeapSummary {
                bytes,
                fraction: median_f64(fractions),
            })
        })
        .max_by(|a, b| {
            a.fraction
                .unwrap_or(0.0)
                .total_cmp(&b.fraction.unwrap_or(0.0))
                .then(a.bytes.cmp(&b.bytes))
        })
}

/// The busiest of `replicas` over the busy windows that lie within `[start, end)`.
fn busy_summary(
    busy: &[BusyWindow],
    replicas: &BTreeSet<ReplicaId>,
    start: Timestamp,
    end: Timestamp,
) -> Option<BusySummary> {
    replicas
        .iter()
        .filter_map(|replica_id| {
            // Workers that disagree on a window's bounds report it in separate rows, which
            // together cover the replica's workers.
            let mut windows: BTreeMap<(Timestamp, Timestamp), (Duration, Duration)> =
                BTreeMap::new();
            for window in busy.iter().filter(|window| {
                window.replica_id == *replica_id
                    && window.start >= start
                    && window.end <= end
                    && window.end > window.start
            }) {
                let entry = windows.entry((window.start, window.end)).or_default();
                entry.0 += window.busy;
                entry.1 = entry.1.max(window.max_worker_busy);
            }
            if windows.is_empty() {
                return None;
            }
            let mut total_busy = Duration::ZERO;
            let mut total_length = Duration::ZERO;
            let mut busiest_shares = Vec::new();
            for ((window_start, window_end), (busy, max_worker_busy)) in &windows {
                let length = Duration::from_millis(
                    u64::from(*window_end).saturating_sub(u64::from(*window_start)),
                );
                total_busy += *busy;
                total_length += length;
                busiest_shares.push(max_worker_busy.as_secs_f64() / length.as_secs_f64());
            }
            let millicores = total_busy.as_nanos().saturating_mul(1000) / total_length.as_nanos();
            let busiest_worker_utilization =
                busiest_shares.iter().sum::<f64>() / f64::cast_lossy(busiest_shares.len());
            Some(BusySummary {
                mean_millicores: u64::try_from(millicores).unwrap_or(u64::MAX),
                busiest_worker_utilization,
            })
        })
        .max_by_key(|busy| busy.mean_millicores)
}

/// The lower median. `None` for no values.
fn median<T: Ord + Copy>(mut values: Vec<T>) -> Option<T> {
    if values.is_empty() {
        return None;
    }
    values.sort();
    Some(values[(values.len() - 1) / 2])
}

/// The lower median. `None` for no values.
fn median_f64(mut values: Vec<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    Some(values[(values.len() - 1) / 2])
}

#[cfg(test)]
mod tests;
