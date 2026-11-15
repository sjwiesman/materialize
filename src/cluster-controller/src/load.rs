// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The `ON LOAD` autoscaling strategy and the rules behind it.
//!
//! The policy keeps a cluster at the smallest size at which a larger size
//! would not make its data fresher, subject to fitting in memory. Three
//! layers, each a pure function of the one before:
//!
//! - [`measure`] turns the frontier progress, busy time, and heap of a
//!   cluster's representative replicas over one interval into a
//!   [`LoadMeasurement`].
//! - [`evaluate`] turns a window of [`LoadSample`]s into a [`LoadEvaluation`]:
//!   the size the rules would move to and whether to act on it.
//! - [`OnLoadStrategy`] applies an evaluation by writing a reconfiguration
//!   record. It contributes no replicas. The graceful reconfiguration strategy
//!   carries out the size change. A scale-down record carries a
//!   [`ReconfigurationTrial`], so the smaller size has to match the current
//!   one's freshness for a while before the cluster cuts over to it.
//!
//! The sample window is a live signal and is empty after a restart. Every rule
//! needs a minimum number of samples, so a restart can delay a decision and
//! cannot cause one.

use std::time::Duration;

use mz_adapter_types::cluster_state::{
    LoadAttempt, LoadAudit, LoadBackoff, LoadDirection, LoadHold, LoadRecord, LoadScaleDecision,
    LoadScaleReason, LoadScaleSample, LoadScaleUp, OnLoadPolicy, ReconfigurationOrigin,
    ReconfigurationTrial,
};
use mz_ore::cast::CastFrom;
use mz_repr::{GlobalId, Timestamp};

use crate::ctx::{
    AvailabilityZones, ClusterSchedule, ClusterState, LoadWrite, OnTimeout, ReconfigurationAudit,
    ReconfigurationRecord, ReconfigurationStatus, ReconfigurationTarget, ReconfigurationWrite,
    StateWrite,
};
use crate::strategy::{ConfigSignals, DesiredReplica, LiveSignals, SignalRequest, Strategy};

/// The median mean worker utilization, in permille, below which a cluster
/// that is behind is not scaled up: most of its worker time is idle, so more
/// workers would idle too.
const IDLE_UTILIZATION: u32 = 500;

/// The busiest worker's utilization, in permille, from which an idle cluster
/// that is behind counts as skew-limited. Below it the cluster is stalled.
const SATURATED_UTILIZATION: u32 = 900;

/// The progress-ratio gain, in permille, a lag-motivated scale-up must show to
/// count as effective.
const EFFECTIVE_PROGRESS_GAIN: u32 = 100;

/// The backoff after a target's first failure. Doubles per consecutive
/// failure up to [`BACKOFF_MAX`].
const BACKOFF_BASE: Duration = Duration::from_secs(60 * 60);
const BACKOFF_MAX: Duration = Duration::from_secs(24 * 60 * 60);

/// The capacity of one replica size.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SizeCapacity {
    pub size: String,
    /// CPU across all processes of a replica of this size.
    pub cpu_millicores: u64,
    /// Heap limit across all processes of a replica of this size.
    pub heap_limit_bytes: u64,
}

/// The load signal for one cluster.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LoadSignals {
    /// The sizes the cluster may be moved between, cheapest first. Covers the
    /// size family of the cluster's current size.
    pub ladder: Vec<SizeCapacity>,
    /// Load samples, oldest first, one per busy-time window.
    pub samples: Vec<LoadSample>,
    /// The longest any current replica at the realized size took to hydrate,
    /// when known.
    pub hydration_time: Option<Duration>,
}

/// One busy-time window's observation of a cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadSample {
    pub at: Timestamp,
    /// The cluster's realized size when the sample was taken.
    pub size: String,
    /// `None` when no replica at the realized size was hydrated and past its
    /// warm-up, so there was nothing representative to measure.
    pub measurement: Option<LoadMeasurement>,
    /// Whether a replica at the realized size was killed for exceeding its
    /// memory during the interval.
    pub oom_killed: bool,
}

/// What a cluster's replicas showed over one interval. Ratios are in permille
/// so the type stays `Eq`.
///
/// Lag and progress come from the replica that speaks for the cluster, the
/// one with the least replica lag, since replicas do the same work and the
/// freshest one answers queries. Utilization comes from that replica too.
/// Busy time and heap are the maximum over the measured replicas.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadMeasurement {
    /// Cluster lag at the end of the interval.
    pub lag: Duration,
    /// How far the inputs of the speaking replica's most lagging dataflow
    /// advanced over the interval.
    pub input_advance: Duration,
    /// How far that dataflow's output advanced over the interval.
    pub output_advance: Duration,
    /// The speaking replica's most lagging dataflow, if it hosts any.
    pub slowest_dataflow: Option<GlobalId>,
    /// Heap of the busiest process as a fraction of its heap limit.
    pub heap_fraction: u32,
    /// Heap of the busiest process times the number of processes, the heap
    /// the replica would need at any size.
    pub heap_bytes: u64,
    pub mean_utilization: u32,
    pub max_worker_utilization: u32,
    /// Non-parked worker time per unit of time, in millicores.
    pub busy_millicores: u64,
}

impl LoadMeasurement {
    /// The output advance over the input advance, `1000` when the inputs did
    /// not advance. Above `1000` while the dataflow catches up.
    pub fn progress(&self) -> u32 {
        if self.input_advance.is_zero() {
            return 1000;
        }
        let ratio = self.output_advance.as_secs_f64() / self.input_advance.as_secs_f64();
        // Float-to-int `as` saturates.
        (ratio * 1000.0) as u32
    }

    /// Over the tolerance, and the lag did not shrink over the interval. A
    /// cluster that is catching up on its own is not behind.
    pub fn behind(&self, tolerance: Duration) -> bool {
        self.lag > tolerance && self.output_advance <= self.input_advance
    }

    fn scale_sample(&self) -> LoadScaleSample {
        LoadScaleSample {
            lag: self.lag,
            progress: self.progress(),
            heap_fraction: self.heap_fraction,
            busy_millicores: self.busy_millicores,
        }
    }
}

/// The rules' tunables, latched from dyncfgs once per tick. Ratios are in
/// permille.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LoadParams {
    /// The sample period, which is the replicas' busy-time window length.
    pub policy_interval: Duration,
    pub lag_tolerance: Duration,
    pub activation_intervals: usize,
    pub catch_up: Duration,
    /// Floor on the progress ratio a scale-up is sized from.
    pub min_progress: u32,
    pub heap_target: u32,
    pub heap_high_water: u32,
    pub heap_high_water_duration: Duration,
    /// The share of a candidate's cores the peak busy time may use.
    pub cpu_margin: u32,
    pub trial_duration: Duration,
    pub default_scale_down_after: Duration,
    pub warm_up: Duration,
}

/// Busy time, progress, and heap of one replica over one interval.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaLoad {
    /// The replica's CPU capacity across all processes.
    pub cpu_millicores: u64,
    pub interval: Duration,
    pub workers: usize,
    /// Non-parked time over the interval, summed over workers.
    pub total_busy: Duration,
    /// The longest non-parked time of any one worker over the interval.
    pub max_worker_busy: Duration,
    /// Of the hydrated, non-transient dataflows with frontiers at both ends
    /// of the interval, the one with the largest local lag. Ties go to the
    /// largest ID.
    pub slowest: Option<DataflowLoad>,
    /// See [`LoadMeasurement::heap_fraction`].
    pub heap_fraction: u32,
    /// See [`LoadMeasurement::heap_bytes`].
    pub heap_bytes: u64,
}

/// The progress of one dataflow on one replica, keyed by an export.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataflowLoad {
    pub id: GlobalId,
    pub progress: DataflowProgress,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DataflowProgress {
    /// How far the least write frontier of the dataflow's direct inputs
    /// advanced over the interval.
    pub input_advance: Duration,
    /// How far the replica's output frontier advanced over the interval.
    pub output_advance: Duration,
    /// How far the output frontier trails the input frontier at the end of
    /// the interval.
    pub local_lag: Duration,
}

/// The measurement of a cluster over one interval, from the replicas that
/// were representative of its workload. `None` when there were none.
pub fn measure(replicas: &[ReplicaLoad]) -> Option<LoadMeasurement> {
    let measurable = replicas
        .iter()
        .filter(|r| r.workers > 0 && !r.interval.is_zero());
    let replica_lag = |replica: &ReplicaLoad| {
        replica
            .slowest
            .as_ref()
            .map_or(Duration::ZERO, |d| d.progress.local_lag)
    };
    let speaker = measurable.clone().min_by_key(|r| replica_lag(*r))?;
    let slowest = speaker.slowest.as_ref();
    let interval = speaker.interval.as_secs_f64();
    let permille = |busy: Duration, of: f64| ((busy.as_secs_f64() / of).min(1.0) * 1000.0) as u32;
    let busy_millicores = |replica: &ReplicaLoad| {
        // Busy time is wall-clock time on a worker. On a size with less than
        // one core per worker a fully busy worker used only its share of a
        // core.
        let workers = replica.workers as f64;
        let core_share = (replica.cpu_millicores as f64 / workers).min(1000.0);
        let busy_workers = replica.total_busy.as_secs_f64() / replica.interval.as_secs_f64();
        (busy_workers * core_share).ceil() as u64
    };
    Some(LoadMeasurement {
        lag: replica_lag(speaker),
        input_advance: slowest.map_or(Duration::ZERO, |d| d.progress.input_advance),
        output_advance: slowest.map_or(Duration::ZERO, |d| d.progress.output_advance),
        slowest_dataflow: slowest.map(|d| d.id),
        heap_fraction: measurable.clone().map(|r| r.heap_fraction).max()?,
        heap_bytes: measurable.clone().map(|r| r.heap_bytes).max()?,
        mean_utilization: permille(speaker.total_busy, interval * speaker.workers as f64),
        max_worker_utilization: permille(speaker.max_worker_busy, interval),
        busy_millicores: measurable.map(busy_millicores).max()?,
    })
}

/// What the rules conclude about a cluster from its sample window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadEvaluation {
    pub status: LoadStatus,
    /// The size the rules would move to, within the allowed range. `None`
    /// until there are enough samples.
    pub recommended_size: Option<String>,
    /// The size change to start now, if a rule fired.
    pub action: Option<LoadAction>,
    /// The most recent measurement at the current size.
    pub latest: Option<LoadMeasurement>,
    /// Whether this evaluation judged the record's `last_scale_up`, which
    /// happens once the activation window at the current size is full. The
    /// action is then the revert of an ineffective scale-up, if any.
    pub judged_scale_up: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoadStatus {
    /// At the recommended size.
    Steady,
    ScalingUp,
    ScalingDown,
    /// See [`LoadHold::SkewLimited`].
    SkewLimited,
    /// See [`LoadHold::Stalled`].
    Stalled,
    /// A rule calls for a larger size than the largest allowed one.
    AtMax,
    /// The load would fit a size below the smallest allowed one.
    AtMin,
    /// See [`LoadHold::BackingOff`].
    BackingOff,
    /// Not enough representative samples at the current size yet.
    WarmingUp,
}

impl LoadStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            LoadStatus::Steady => "steady",
            LoadStatus::ScalingUp => "scaling-up",
            LoadStatus::ScalingDown => "scaling-down",
            LoadStatus::SkewLimited => "skew-limited",
            LoadStatus::Stalled => "stalled",
            LoadStatus::AtMax => "at-max",
            LoadStatus::AtMin => "at-min",
            LoadStatus::BackingOff => "backing-off",
            LoadStatus::WarmingUp => "warming-up",
        }
    }

    fn hold(&self) -> Option<LoadHold> {
        match self {
            LoadStatus::SkewLimited => Some(LoadHold::SkewLimited),
            LoadStatus::Stalled => Some(LoadHold::Stalled),
            LoadStatus::BackingOff => Some(LoadHold::BackingOff),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadAction {
    pub target_size: String,
    pub reason: LoadScaleReason,
    /// The sample values the decision was taken on, for the audit log. A
    /// lag-motivated scale-up records the median progress ratio of its
    /// activation window here.
    pub sample: Option<LoadScaleSample>,
}

/// The allowed sizes and scale-down window of a cluster, from its `ON LOAD`
/// policy. A cluster without a policy is evaluated against the whole ladder,
/// for recommendations only.
#[derive(Clone, Copy, Debug, Default)]
pub struct LoadBounds<'a> {
    pub min_size: Option<&'a str>,
    pub max_size: Option<&'a str>,
    pub scale_down_after: Option<Duration>,
}

impl<'a> From<&'a OnLoadPolicy> for LoadBounds<'a> {
    fn from(policy: &'a OnLoadPolicy) -> Self {
        LoadBounds {
            min_size: Some(&policy.min_size),
            max_size: Some(&policy.max_size),
            scale_down_after: policy.scale_down_after,
        }
    }
}

/// Evaluate the decision rules for a cluster at `current_size`.
///
/// Returns `None` when the ladder does not contain the current size or the
/// bounds, in which case there is no meaningful recommendation.
pub fn evaluate(
    current_size: &str,
    bounds: LoadBounds<'_>,
    signals: &LoadSignals,
    record: Option<&LoadRecord>,
    params: &LoadParams,
    now: Timestamp,
) -> Option<LoadEvaluation> {
    let ladder = Ladder::new(&signals.ladder, bounds)?;
    let current = ladder.index_of(current_size)?;

    let now_ms = u64::from(now);
    let interval = millis(params.policy_interval).max(1);
    let activation = params.activation_intervals.max(1);
    // Samples older than this cannot justify a decision now. Twice the
    // nominal span tolerates a missed interval.
    let recent_horizon =
        now_ms.saturating_sub(interval.saturating_mul(2 * u64::cast_from(activation)));
    let last_cutover = record.and_then(|r| r.last_cutover).map(u64::from);
    let warmed_up = last_cutover.map_or(0, |at| at.saturating_add(millis(params.warm_up)));

    let at_size = || {
        signals
            .samples
            .iter()
            .filter(|s| s.size == current_size && last_cutover.is_none_or(|c| u64::from(s.at) > c))
    };
    let measured: Vec<(u64, &LoadMeasurement)> = at_size()
        .filter(|s| u64::from(s.at) >= warmed_up)
        .filter_map(|s| s.measurement.as_ref().map(|m| (u64::from(s.at), m)))
        .collect();
    let latest = measured.last().map(|(_, m)| *m);
    // The samples of the last `len`, when they cover it: they reach back to
    // its start and cover at least half of its intervals. A window that was
    // not observed, as after a restart, justifies nothing.
    let window = |len: Duration| -> Option<(u64, Vec<&LoadMeasurement>)> {
        let len = millis(len);
        let start = now_ms.saturating_sub(len);
        let in_window: Vec<&LoadMeasurement> = measured
            .iter()
            .filter(|(at, _)| *at >= start)
            .map(|(_, m)| *m)
            .collect();
        let covered = !in_window.is_empty()
            && measured
                .first()
                .is_some_and(|(at, _)| *at <= start.saturating_add(interval.saturating_mul(2)))
            && u64::cast_from(in_window.len()).saturating_mul(2) >= len / interval;
        covered.then_some((start, in_window))
    };

    let recent: Vec<&LoadMeasurement> = measured
        .iter()
        .rev()
        .take(activation)
        .filter(|(at, _)| *at >= recent_horizon)
        .map(|(_, m)| *m)
        .collect();
    let last_scale_up = record
        .and_then(|r| r.last_scale_up.as_ref())
        .filter(|_| recent.len() >= activation);

    let evaluation = |status, recommended: Option<usize>, action| LoadEvaluation {
        status,
        recommended_size: recommended.map(|i| ladder.size(i).to_string()),
        action,
        latest: latest.cloned(),
        judged_scale_up: last_scale_up.is_some(),
    };
    let scale_up = |wanted: usize, reason, sample| {
        if current >= ladder.hi {
            return evaluation(LoadStatus::AtMax, Some(ladder.hi), None);
        }
        match ladder.allow_up(current, wanted, record, now) {
            Some(target) => evaluation(
                LoadStatus::ScalingUp,
                Some(wanted),
                Some(LoadAction {
                    target_size: ladder.size(target).to_string(),
                    reason,
                    sample,
                }),
            ),
            None => evaluation(LoadStatus::BackingOff, Some(wanted), None),
        }
    };

    // A killed replica cannot report what it needed, so this rule needs no
    // measurement and skips the activation window. It does not honor an Up
    // backoff, since staying means being killed again. A size that failed
    // instead raises the floor: a target killed during hydration times out
    // and is backed off, and the next attempt doubles past it.
    if at_size().any(|s| s.oom_killed && u64::from(s.at) >= recent_horizon) {
        if current >= ladder.hi {
            return Some(evaluation(LoadStatus::AtMax, Some(ladder.hi), None));
        }
        let failed = ladder
            .blocked(record, LoadDirection::Up, now)
            .filter(|i| *i > current)
            .map(|i| ladder.heap_limit(i))
            .max()
            .unwrap_or(0);
        let floor = ladder.heap_limit(current).max(failed).saturating_mul(2);
        let target = ladder.first_above(current, |c| c.heap_limit_bytes >= floor);
        return Some(evaluation(
            LoadStatus::ScalingUp,
            Some(target),
            Some(LoadAction {
                target_size: ladder.size(target).to_string(),
                reason: LoadScaleReason::OutOfMemory,
                sample: None,
            }),
        ));
    }

    // A transient spike, from a large transaction or a merge, breaks the run.
    if let Some((_, samples)) = window(params.heap_high_water_duration) {
        if samples
            .iter()
            .all(|m| m.heap_fraction > params.heap_high_water)
        {
            let peak_heap = samples.iter().map(|m| m.heap_bytes).max().unwrap_or(0);
            let wanted = ladder.first_above(current, |c| {
                fits(peak_heap, c.heap_limit_bytes, params.heap_target)
            });
            return Some(scale_up(
                wanted,
                LoadScaleReason::HeapHighWater,
                latest.map(LoadMeasurement::scale_sample),
            ));
        }
    }

    if recent.len() < activation {
        return Some(evaluation(LoadStatus::WarmingUp, None, None));
    }
    let behind = |m: &&LoadMeasurement| m.behind(params.lag_tolerance);

    if recent.iter().all(behind) {
        // The median over the activation window filters a single-interval
        // outlier.
        let median_by = |key: &dyn Fn(&LoadMeasurement) -> u64| {
            let mut values: Vec<u64> = recent.iter().map(|m| key(m)).collect();
            values.sort_unstable();
            values[(values.len() - 1) / 2]
        };
        let progress = u32::try_from(median_by(&|m| u64::from(m.progress()))).unwrap_or(u32::MAX);
        // A scale-up that did not raise the progress ratio shows that workers
        // were not the bottleneck, so by the objective the smaller size was
        // right. This precedes the idle check: a scale-up that did not help
        // can leave its added workers idle, which reads as a hold.
        if let Some(scale_up) = last_scale_up {
            let from = ladder.index_of(&scale_up.from_size);
            if let Some(from) = from.filter(|_| {
                scale_up.target_size == current_size
                    && progress < scale_up.progress.saturating_add(EFFECTIVE_PROGRESS_GAIN)
            }) {
                return Some(evaluation(
                    LoadStatus::ScalingDown,
                    Some(from),
                    Some(LoadAction {
                        target_size: scale_up.from_size.clone(),
                        reason: LoadScaleReason::IneffectiveScaleUp,
                        sample: Some(LoadScaleSample {
                            progress,
                            ..recent[0].scale_sample()
                        }),
                    }),
                ));
            }
        }
        if median_by(&|m| u64::from(m.mean_utilization)) < u64::from(IDLE_UTILIZATION) {
            let status = if median_by(&|m| u64::from(m.max_worker_utilization))
                >= u64::from(SATURATED_UTILIZATION)
            {
                LoadStatus::SkewLimited
            } else {
                LoadStatus::Stalled
            };
            return Some(evaluation(status, Some(current), None));
        }
        // A cluster that is behind saturates its current size, so the current
        // size's cores are what it achieved. A dataflow whose output did not
        // move says only that it needs more, so the floor bounds one step
        // and lets the next one refine.
        let rho = f64::from(progress.max(params.min_progress).clamp(1, 1000)) / 1000.0;
        let catch_up = if params.catch_up.is_zero() {
            1.0
        } else {
            1.0 + recent[0].lag.as_secs_f64() / params.catch_up.as_secs_f64()
        };
        let needed = ladder.cpu(current) as f64 / rho * catch_up;
        let wanted = ladder.first_above(current, |c| c.cpu_millicores as f64 >= needed);
        return Some(scale_up(
            wanted,
            LoadScaleReason::ScaleUp,
            Some(LoadScaleSample {
                progress,
                ..recent[0].scale_sample()
            }),
        ));
    }
    if recent.iter().any(behind) {
        return Some(evaluation(LoadStatus::Steady, Some(current), None));
    }

    // The window is at least twice the hydration time so the cluster never
    // spends longer transitioning than at the result.
    let hydration = signals.hydration_time.unwrap_or_default();
    let window_len = bounds
        .scale_down_after
        .unwrap_or(params.default_scale_down_after)
        .max(hydration.saturating_mul(2));
    let Some((window_start, samples)) = window(window_len) else {
        return Some(evaluation(LoadStatus::Steady, Some(current), None));
    };
    let quiet = !samples.iter().any(behind)
        && !at_size().any(|s| s.oom_killed && u64::from(s.at) >= window_start);
    if !quiet {
        return Some(evaluation(LoadStatus::Steady, Some(current), None));
    }
    let peak_heap = samples.iter().map(|m| m.heap_bytes).max().unwrap_or(0);
    let peak_busy = samples.iter().map(|m| m.busy_millicores).max().unwrap_or(0);
    // The heap condition is the requirement on the result. The CPU condition
    // only avoids trials that cannot pass.
    let candidate = |i: usize| {
        let c = &ladder.sizes[i];
        fits(peak_heap, c.heap_limit_bytes, params.heap_target)
            && fits(peak_busy, c.cpu_millicores, params.cpu_margin)
    };
    let Some(wanted) = (ladder.lo..current).find(|i| candidate(*i)) else {
        let status = if current == ladder.lo && current > 0 && candidate(current - 1) {
            LoadStatus::AtMin
        } else {
            LoadStatus::Steady
        };
        return Some(evaluation(status, Some(current), None));
    };
    let floor = ladder
        .blocked(record, LoadDirection::Down, now)
        .max()
        .map_or(wanted, |blocked| wanted.max(blocked + 1));
    Some(match (floor..current).find(|i| candidate(*i)) {
        Some(target) => evaluation(
            LoadStatus::ScalingDown,
            Some(wanted),
            Some(LoadAction {
                target_size: ladder.size(target).to_string(),
                reason: LoadScaleReason::ScaleDown,
                sample: Some(LoadScaleSample {
                    lag: samples.iter().map(|m| m.lag).max().unwrap_or_default(),
                    progress: samples.iter().map(|m| m.progress()).min().unwrap_or(1000),
                    heap_fraction: samples.iter().map(|m| m.heap_fraction).max().unwrap_or(0),
                    busy_millicores: peak_busy,
                }),
            }),
        ),
        None => evaluation(LoadStatus::BackingOff, Some(wanted), None),
    })
}

/// Whether `load` is at most `ratio` permille of `capacity`.
fn fits(load: u64, capacity: u64, ratio: u32) -> bool {
    u128::from(load) * 1000 <= u128::from(capacity) * u128::from(ratio)
}

/// The size ladder with the allowed range resolved to indexes.
struct Ladder<'a> {
    sizes: &'a [SizeCapacity],
    lo: usize,
    hi: usize,
}

impl<'a> Ladder<'a> {
    fn new(sizes: &'a [SizeCapacity], bounds: LoadBounds<'_>) -> Option<Self> {
        let index = |size: &str| sizes.iter().position(|s| s.size == size);
        let lo = match bounds.min_size {
            Some(size) => index(size)?,
            None => 0,
        };
        let hi = match bounds.max_size {
            Some(size) => index(size)?,
            None => sizes.len().checked_sub(1)?,
        };
        (lo <= hi).then_some(Ladder { sizes, lo, hi })
    }

    fn index_of(&self, size: &str) -> Option<usize> {
        self.sizes.iter().position(|s| s.size == size)
    }

    fn size(&self, index: usize) -> &str {
        &self.sizes[index].size
    }

    fn cpu(&self, index: usize) -> u64 {
        self.sizes[index].cpu_millicores
    }

    fn heap_limit(&self, index: usize) -> u64 {
        self.sizes[index].heap_limit_bytes
    }

    /// The cheapest size above `current`, within range, that satisfies
    /// `pred`. The largest allowed size when none does.
    fn first_above(&self, current: usize, pred: impl Fn(&SizeCapacity) -> bool) -> usize {
        (current + 1..=self.hi)
            .find(|i| pred(&self.sizes[*i]))
            .unwrap_or(self.hi)
    }

    /// The scale-up target given active backoff: `wanted`, or the largest
    /// size below the lowest blocked one. `None` when that leaves nothing
    /// above `current`.
    fn allow_up(
        &self,
        current: usize,
        wanted: usize,
        record: Option<&LoadRecord>,
        now: Timestamp,
    ) -> Option<usize> {
        let ceiling = self
            .blocked(record, LoadDirection::Up, now)
            .min()
            .unwrap_or(usize::MAX);
        let target = wanted.min(ceiling.checked_sub(1)?);
        (target > current).then_some(target)
    }

    fn blocked<'r>(
        &'r self,
        record: Option<&'r LoadRecord>,
        direction: LoadDirection,
        now: Timestamp,
    ) -> impl Iterator<Item = usize> + 'r {
        record
            .into_iter()
            .flat_map(|r| &r.backoff)
            .filter(move |b| b.direction == direction && b.until > now)
            .filter_map(|b| self.index_of(&b.size))
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn after(now: Timestamp, duration: Duration) -> Timestamp {
    Timestamp::from(u64::from(now).saturating_add(millis(duration)))
}

/// The `ON LOAD` strategy. See the module docs.
///
/// Engaged for MANUAL clusters that are on, carry an `ON LOAD` policy, and
/// while the break-glass flag is set. It writes two things: a reconfiguration
/// record when [`evaluate`] calls for a size change, and its own `load` record
/// to follow that reconfiguration to its outcome.
#[derive(Clone, Copy, Debug, Default)]
pub struct OnLoadStrategy;

impl OnLoadStrategy {
    fn active_policy<'a>(
        &self,
        state: &'a ClusterState,
        config: &ConfigSignals,
    ) -> Option<&'a OnLoadPolicy> {
        if !config.load_enabled
            || state.replication_factor == 0
            || !matches!(state.schedule, ClusterSchedule::Manual)
        {
            return None;
        }
        state.auto_scaling_policy.as_ref()?.on_load.as_ref()
    }

    /// The write that starts a reconfiguration to `action.target_size` and
    /// records the attempt.
    fn start(
        &self,
        state: &ClusterState,
        mut record: LoadRecord,
        action: &LoadAction,
        config: &ConfigSignals,
        now: Timestamp,
        mut audit: Vec<LoadAudit>,
    ) -> StateWrite {
        let deadline = after(now, config.default_reconfiguration_timeout);
        record.attempt = Some(LoadAttempt {
            from_size: state.size.clone(),
            target_size: action.target_size.clone(),
            deadline,
            progress_before: action
                .sample
                .filter(|_| action.reason == LoadScaleReason::ScaleUp)
                .map(|sample| sample.progress),
        });
        record.last_scale_up = None;
        if record.hold.take().is_some() {
            audit.push(LoadAudit::HoldChanged { hold: None });
        }
        // A scale-down is the only decision taken on a prediction. The others
        // follow an observed lack of capacity or memory. So only it is tested
        // before it is committed.
        let trial = (action.reason == LoadScaleReason::ScaleDown).then(|| ReconfigurationTrial {
            allowed_lag: config.load.lag_tolerance,
            hold_for: config.load.trial_duration,
            max_heap_fraction: config.load.heap_high_water,
            ready_since: None,
        });
        StateWrite {
            reconfiguration: Some(ReconfigurationWrite {
                record: Some(ReconfigurationRecord {
                    target: ReconfigurationTarget {
                        size: action.target_size.clone(),
                        replication_factor: state.replication_factor,
                        availability_zones: AvailabilityZones(state.availability_zones.clone()),
                        logging: state.logging.clone(),
                        arrangement_compression: state.arrangement_compression,
                    },
                    deadline,
                    // A target that cannot hydrate or keep up must leave the
                    // cluster serving at its old size.
                    on_timeout: OnTimeout::Rollback,
                    status: ReconfigurationStatus::InProgress,
                    origin: ReconfigurationOrigin::LoadAutoscaling,
                    trial,
                }),
                audit: Some(ReconfigurationAudit::Started {
                    load: Some(LoadScaleDecision {
                        reason: action.reason,
                        sample: action.sample,
                    }),
                }),
            }),
            load: Some(LoadWrite {
                record: Some(record),
                audit,
            }),
            ..Default::default()
        }
    }
}

fn load_write(record: LoadRecord, audit: Vec<LoadAudit>) -> StateWrite {
    StateWrite {
        load: Some(LoadWrite {
            record: Some(record),
            audit,
        }),
        ..Default::default()
    }
}

/// Record a failed target, backing off for longer on each consecutive
/// failure. Returns the audit for the new backoff.
fn back_off(
    record: &mut LoadRecord,
    size: &str,
    direction: LoadDirection,
    now: Timestamp,
) -> LoadAudit {
    let index = record
        .backoff
        .iter()
        .position(|b| b.size == size && b.direction == direction);
    let failures = index
        .map_or(0, |i| record.backoff[i].failures)
        .saturating_add(1);
    let duration = BACKOFF_BASE
        .saturating_mul(1u32.checked_shl(failures - 1).unwrap_or(u32::MAX))
        .min(BACKOFF_MAX);
    let entry = LoadBackoff {
        size: size.to_string(),
        direction,
        until: after(now, duration),
        failures,
    };
    let audit = LoadAudit::BackoffStarted {
        size: entry.size.clone(),
        direction,
        until: entry.until,
    };
    match index {
        Some(i) => record.backoff[i] = entry,
        None => record.backoff.push(entry),
    }
    audit
}

impl Strategy for OnLoadStrategy {
    fn signal_request(&self, state: &ClusterState, config: &ConfigSignals) -> SignalRequest {
        SignalRequest {
            load: self.active_policy(state, config).is_some(),
            ..Default::default()
        }
    }

    fn update_state(
        &self,
        state: &ClusterState,
        signals: &LiveSignals,
        config: &ConfigSignals,
        now: Timestamp,
    ) -> StateWrite {
        let Some(policy) = self.active_policy(state, config) else {
            // Catalog writes retire a record they invalidate. This covers the
            // break-glass flag switching off.
            return match state.load {
                Some(_) => StateWrite {
                    load: Some(LoadWrite {
                        record: None,
                        audit: Vec::new(),
                    }),
                    ..Default::default()
                },
                None => StateWrite::default(),
            };
        };
        let Some(load) = &signals.load else {
            return StateWrite::default();
        };
        let params = &config.load;
        let mut record = state.load.clone().unwrap_or_default();
        let in_progress = state
            .reconfiguration
            .as_ref()
            .is_some_and(|r| r.is_in_progress());
        let ladder_index = |size: &str| load.ladder.iter().position(|s| s.size == size);

        if let Some(attempt) = record.attempt.take() {
            // A user `ALTER` may have written over the attempt's record. Only
            // the record this attempt wrote speaks for its outcome.
            let status = state
                .reconfiguration
                .as_ref()
                .filter(|r| {
                    r.origin == ReconfigurationOrigin::LoadAutoscaling
                        && r.deadline == attempt.deadline
                        && r.target.size == attempt.target_size
                })
                .map(|r| r.status);
            let mut audit = Vec::new();
            match status {
                Some(ReconfigurationStatus::InProgress) => return StateWrite::default(),
                Some(ReconfigurationStatus::Finalized) => {
                    record.last_cutover = Some(now);
                    record.last_scale_up = attempt.progress_before.map(|progress| LoadScaleUp {
                        from_size: attempt.from_size,
                        target_size: attempt.target_size,
                        progress,
                    });
                }
                Some(
                    ReconfigurationStatus::TimedOut | ReconfigurationStatus::ResourceExhausted,
                ) => {
                    // For a scale-down this is a trial that did not hold.
                    // Without the backoff the same target would be retried
                    // every window.
                    let direction = match (
                        ladder_index(&attempt.target_size),
                        ladder_index(&attempt.from_size),
                    ) {
                        (Some(target), Some(from)) if target < from => LoadDirection::Down,
                        _ => LoadDirection::Up,
                    };
                    audit.push(back_off(&mut record, &attempt.target_size, direction, now));
                }
                Some(ReconfigurationStatus::Cancelled) | None => {}
            }
            return load_write(record, audit);
        }

        // One transition at a time, and a user's reconfiguration is never
        // overwritten.
        if in_progress {
            return StateWrite::default();
        }
        let Some(evaluation) =
            evaluate(&state.size, policy.into(), load, Some(&record), params, now)
        else {
            return StateWrite::default();
        };
        // The tick that judges a lag scale-up starts nothing but its revert
        // and the memory rules, so a scale-up that helped gets one more
        // interval before the next lag decision.
        let action = evaluation.action.as_ref().filter(|action| {
            !evaluation.judged_scale_up
                || !matches!(
                    action.reason,
                    LoadScaleReason::ScaleUp | LoadScaleReason::ScaleDown
                )
        });
        if let Some(action) = action {
            let mut audit = Vec::new();
            if action.reason == LoadScaleReason::IneffectiveScaleUp {
                // Stop scaling up past the size the revert goes back to.
                audit.push(back_off(&mut record, &state.size, LoadDirection::Up, now));
            }
            return self.start(state, record, action, config, now, audit);
        }
        if evaluation.judged_scale_up {
            record.last_scale_up = None;
            return load_write(record, Vec::new());
        }
        let hold = evaluation.status.hold();
        if hold == record.hold {
            return StateWrite::default();
        }
        record.hold = hold;
        load_write(record, vec![LoadAudit::HoldChanged { hold }])
    }

    fn desired_replicas(
        &self,
        _state: &ClusterState,
        _signals: &LiveSignals,
        _config: &ConfigSignals,
        _now: Timestamp,
    ) -> Vec<DesiredReplica> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests;
