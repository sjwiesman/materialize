// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The `ON LOAD` autoscaling strategy and the demand model behind it.
//!
//! The model follows DS2 (Kalavri et al., OSDI 2018) adapted to shared timely
//! workers. Three layers, each a pure function of the one before:
//!
//! - [`measure`] turns raw busy time and frontier progress for one interval
//!   into a [`LoadMeasurement`]: the CPU and memory the workload would need if
//!   every dataflow kept up.
//! - [`evaluate`] turns a window of [`LoadSample`]s into a [`LoadEvaluation`]:
//!   the recommended size and whether to act on it.
//! - [`OnLoadStrategy`] applies an evaluation by writing a reconfiguration
//!   record. It contributes no replicas. The graceful reconfiguration strategy
//!   carries out the size change, so a load-driven resize gets the same
//!   overlap, readiness gate, and rollback as a user's `ALTER CLUSTER`.
//!
//! The sample window is a live signal and is empty after a restart. Every rule
//! needs a minimum number of samples, so a restart can delay a decision and
//! cannot cause one.

use std::time::Duration;

use mz_adapter_types::cluster_state::{
    LoadAttempt, LoadAttemptPhase, LoadAudit, LoadBackoff, LoadDirection, LoadHold, LoadRecord,
    LoadScaleReason, OnLoadPolicy, ReconfigurationOrigin,
};
use mz_ore::cast::CastFrom;
use mz_repr::{GlobalId, Timestamp};

use crate::ctx::{
    AvailabilityZones, ClusterSchedule, ClusterState, LoadWrite, OnTimeout, ReconfigurationAudit,
    ReconfigurationRecord, ReconfigurationStatus, ReconfigurationTarget, ReconfigurationWrite,
    StateWrite,
};
use crate::strategy::{ConfigSignals, DesiredReplica, LiveSignals, SignalRequest, Strategy};

/// Frontiers advance in ticks of the timestamp interval, 1s by default. A
/// dataflow whose output trails its input by at most one tick over an interval
/// is keeping up, not lagging.
const FRONTIER_JITTER: Duration = Duration::from_secs(1);

/// The busiest worker's utilization, in permille, from which a replica counts
/// as saturated on one worker.
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
    /// Memory across all processes of a replica of this size.
    pub memory_bytes: u64,
}

/// The load signal for one cluster.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LoadSignals {
    /// The sizes the cluster may be moved between, cheapest first, with
    /// capacity non-decreasing. Covers the size family of the cluster's
    /// current size.
    pub ladder: Vec<SizeCapacity>,
    /// Load samples, oldest first, one per policy interval.
    pub samples: Vec<LoadSample>,
}

/// One policy interval's observation of a cluster.
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

/// What a cluster's workload needed during one interval. Ratios are in
/// permille so the type stays `Eq`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadMeasurement {
    /// The CPU the workload would use if every dataflow kept up.
    pub cpu_demand_millicores: u64,
    pub memory_demand_bytes: u64,
    /// The busiest worker's busy fraction.
    pub max_worker_utilization: u32,
    /// The lowest progress ratio among the cluster's dataflows. `1000` when
    /// every dataflow kept up.
    pub min_progress: u32,
    /// The dataflow with the lowest progress ratio, when one is lagging.
    pub slowest_dataflow: Option<GlobalId>,
    /// How long the measured replicas took to hydrate, when known.
    pub hydration_time: Option<Duration>,
}

impl LoadMeasurement {
    fn lagging(&self) -> bool {
        self.min_progress < 1000
    }
}

/// The model's tunables, latched from dyncfgs once per tick.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LoadParams {
    pub policy_interval: Duration,
    /// Permille of a size's CPU that demand is sized to.
    pub target_utilization: u32,
    /// Permille of a size's memory that demand is sized to.
    pub target_memory: u32,
    pub activation_intervals: usize,
    pub default_scale_down_after: Duration,
    pub warm_up: Duration,
    pub catch_up: Duration,
    /// Permille floor on a dataflow's progress ratio.
    pub min_progress: u32,
}

/// Raw busy time and progress of one replica over one interval.
#[derive(Clone, Debug)]
pub struct ReplicaLoad {
    /// The replica's CPU capacity across all processes.
    pub cpu_millicores: u64,
    pub interval: Duration,
    /// Non-parked time per worker over the interval.
    pub worker_busy: Vec<Duration>,
    pub dataflows: Vec<DataflowLoad>,
    /// Heap plus swap, scaled to the whole replica.
    pub memory_bytes: u64,
    pub hydration_time: Option<Duration>,
}

/// Raw busy time and progress of one dataflow on one replica.
#[derive(Clone, Debug)]
pub struct DataflowLoad {
    pub id: Option<GlobalId>,
    /// Scheduled time summed over workers.
    pub busy: Duration,
    /// `None` for a dataflow without frontiers to compare, such as a peek. It
    /// counts as keeping up.
    pub progress: Option<DataflowProgress>,
}

#[derive(Clone, Copy, Debug)]
pub struct DataflowProgress {
    /// How far the minimum input frontier advanced over the interval.
    pub input_advance: Duration,
    /// How far the output frontier advanced over the interval.
    pub output_advance: Duration,
    /// How far the output frontier trails the input frontier at the end of
    /// the interval.
    pub local_lag: Duration,
}

/// The demand of a cluster over one interval, from the replicas that were
/// representative of its workload. `None` when there were none.
///
/// Replicas of a cluster do the same work, so the cluster's demand is the
/// maximum over replicas.
pub fn measure(replicas: &[ReplicaLoad], params: &LoadParams) -> Option<LoadMeasurement> {
    replicas
        .iter()
        .filter_map(|replica| measure_replica(replica, params))
        .reduce(|a, b| {
            let (min_progress, slowest_dataflow) = if b.min_progress < a.min_progress {
                (b.min_progress, b.slowest_dataflow)
            } else {
                (a.min_progress, a.slowest_dataflow)
            };
            LoadMeasurement {
                cpu_demand_millicores: a.cpu_demand_millicores.max(b.cpu_demand_millicores),
                memory_demand_bytes: a.memory_demand_bytes.max(b.memory_demand_bytes),
                max_worker_utilization: a.max_worker_utilization.max(b.max_worker_utilization),
                min_progress,
                slowest_dataflow,
                hydration_time: a.hydration_time.max(b.hydration_time),
            }
        })
}

fn measure_replica(replica: &ReplicaLoad, params: &LoadParams) -> Option<LoadMeasurement> {
    let interval = replica.interval.as_secs_f64();
    if replica.worker_busy.is_empty() || interval <= 0.0 {
        return None;
    }
    let min_progress = f64::from(params.min_progress.clamp(1, 1000)) / 1000.0;

    // DS2 Eq. 7 summed over operators. An operator's required parallelism is
    // its busy cores times the ratio of the rate it should have processed to
    // the rate it did. All operators of a dataflow share that ratio, the
    // inverse of the dataflow's progress ratio, so busy time per dataflow is
    // all the model needs.
    let mut attributed = 0.0;
    let mut adjusted = 0.0;
    let mut slowest: Option<(f64, Option<GlobalId>)> = None;
    for dataflow in &replica.dataflows {
        let busy = dataflow.busy.as_secs_f64();
        let (ratio, catch_up) = match dataflow.progress {
            Some(progress) => {
                let deficit = progress.input_advance.saturating_sub(progress.output_advance);
                if deficit <= FRONTIER_JITTER {
                    (1.0, 1.0)
                } else {
                    let ratio = progress.output_advance.as_secs_f64()
                        / progress.input_advance.as_secs_f64();
                    // Extra capacity to work off the backlog within the
                    // catch-up time, on top of keeping pace.
                    let catch_up = if params.catch_up.is_zero() {
                        1.0
                    } else {
                        1.0 + progress.local_lag.as_secs_f64() / params.catch_up.as_secs_f64()
                    };
                    // A stalled output frontier bounds demand from below
                    // without sizing it. The floor bounds one step and lets
                    // the next sample, at the larger size, refine.
                    (ratio.max(min_progress), catch_up)
                }
            }
            None => (1.0, 1.0),
        };
        attributed += busy;
        adjusted += busy * catch_up / ratio;
        if ratio < 1.0 && slowest.is_none_or(|(lowest, _)| ratio < lowest) {
            slowest = Some((ratio, dataflow.id));
        }
    }

    // Worker time outside any operator (progress tracking, communication)
    // grows with the work, so it scales by the same aggregate factor.
    let total_busy: f64 = replica.worker_busy.iter().map(Duration::as_secs_f64).sum();
    let overhead = (total_busy - attributed).max(0.0);
    let factor = if attributed > 0.0 {
        adjusted / attributed
    } else {
        1.0
    };

    // Busy time is wall-clock time on a worker. On a size with less than one
    // core per worker a fully busy worker used only its share of a core.
    let workers = replica.worker_busy.len() as f64;
    let core_share = (replica.cpu_millicores as f64 / 1000.0 / workers).min(1.0);
    let demand_cores = (adjusted + overhead * factor) / interval * core_share;

    let max_busy = replica
        .worker_busy
        .iter()
        .map(Duration::as_secs_f64)
        .fold(0.0, f64::max);

    Some(LoadMeasurement {
        // Float-to-int `as` saturates, which is the right behavior for a
        // demand too large to represent.
        cpu_demand_millicores: (demand_cores * 1000.0).ceil() as u64,
        memory_demand_bytes: replica.memory_bytes,
        max_worker_utilization: ((max_busy / interval).min(1.0) * 1000.0) as u32,
        min_progress: slowest.map_or(1000, |(ratio, _)| (ratio * 1000.0) as u32),
        slowest_dataflow: slowest.and_then(|(_, id)| id),
        hydration_time: replica.hydration_time,
    })
}

/// What the model concludes about a cluster from its sample window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadEvaluation {
    pub status: LoadStatus,
    /// The size the recent load calls for, within the allowed range. `None`
    /// until there are enough samples.
    pub recommended_size: Option<String>,
    /// The size change to start now, if a rule fired.
    pub action: Option<LoadAction>,
    /// The most recent measurement at the current size.
    pub latest: Option<LoadMeasurement>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoadStatus {
    /// At the recommended size.
    Steady,
    ScalingUp,
    ScalingDown,
    /// See [`LoadHold::SkewLimited`].
    SkewLimited,
    /// The load calls for more than the largest allowed size.
    AtMax,
    /// The load would fit less than the smallest allowed size.
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
            LoadStatus::AtMax => "at-max",
            LoadStatus::AtMin => "at-min",
            LoadStatus::BackingOff => "backing-off",
            LoadStatus::WarmingUp => "warming-up",
        }
    }

    fn hold(&self) -> Option<LoadHold> {
        match self {
            LoadStatus::SkewLimited => Some(LoadHold::SkewLimited),
            LoadStatus::BackingOff => Some(LoadHold::BackingOff),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadAction {
    pub target_size: String,
    pub reason: LoadScaleReason,
    /// The demand the target was sized from. Zero for an out-of-memory
    /// scale-up, which has no usable measurement.
    pub cpu_demand_millicores: u64,
    pub memory_demand_bytes: u64,
    /// The lowest progress ratio among the samples behind a scale-up, when
    /// lag motivated it.
    pub progress_before: Option<u32>,
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
    let ladder = Ladder::new(&signals.ladder, bounds, params)?;
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
    let latest = measured.last().map(|(_, m)| (*m).clone());

    let evaluation = |status, recommended: Option<usize>, action| LoadEvaluation {
        status,
        recommended_size: recommended.map(|i| ladder.size(i).to_string()),
        action,
        latest: latest.clone(),
    };

    // An out-of-memory kill needs no measurement and cannot provide one: the
    // replica died before it could report what it needed. Double the memory.
    if at_size().any(|s| s.oom_killed && u64::from(s.at) >= recent_horizon) {
        if current >= ladder.hi {
            return Some(evaluation(LoadStatus::AtMax, Some(ladder.hi), None));
        }
        let wanted = ladder.first_with_memory(current, ladder.memory(current).saturating_mul(2));
        return Some(match ladder.allow_up(current, wanted, record, now) {
            Some(target) => evaluation(
                LoadStatus::ScalingUp,
                Some(wanted),
                Some(LoadAction {
                    target_size: ladder.size(target).to_string(),
                    reason: LoadScaleReason::OutOfMemory,
                    cpu_demand_millicores: 0,
                    memory_demand_bytes: 0,
                    progress_before: None,
                }),
            ),
            None => evaluation(LoadStatus::BackingOff, Some(wanted), None),
        });
    }

    let recent: Vec<&LoadMeasurement> = measured
        .iter()
        .rev()
        .take(activation)
        .filter(|(at, _)| *at >= recent_horizon)
        .map(|(_, m)| *m)
        .collect();
    if recent.len() < activation {
        return Some(evaluation(LoadStatus::WarmingUp, None, None));
    }

    // The median over the activation window filters a single-interval spike
    // (DS2's activation time).
    let median_by = |key: &dyn Fn(&LoadMeasurement) -> u64| {
        let mut values: Vec<u64> = recent.iter().map(|m| key(m)).collect();
        values.sort_unstable();
        values[(values.len() - 1) / 2]
    };
    let wanted = usize::cast_from(median_by(&|m| u64::cast_from(ladder.fit(m))));
    let recommended = ladder.clamp(wanted);
    let cpu_demand = median_by(&|m| m.cpu_demand_millicores);
    let memory_demand = median_by(&|m| m.memory_demand_bytes);
    let lagging = median_by(&|m| u64::from(m.min_progress)) < 1000;
    let saturated = median_by(&|m| u64::from(m.max_worker_utilization))
        >= u64::from(SATURATED_UTILIZATION);

    if recommended > current {
        return Some(match ladder.allow_up(current, recommended, record, now) {
            Some(target) => evaluation(
                LoadStatus::ScalingUp,
                Some(recommended),
                Some(LoadAction {
                    target_size: ladder.size(target).to_string(),
                    reason: LoadScaleReason::ScaleUp,
                    cpu_demand_millicores: cpu_demand,
                    memory_demand_bytes: memory_demand,
                    progress_before: lagging
                        .then(|| recent.iter().map(|m| m.min_progress).min())
                        .flatten(),
                }),
            ),
            None => evaluation(LoadStatus::BackingOff, Some(recommended), None),
        });
    }
    if wanted > ladder.hi {
        return Some(evaluation(LoadStatus::AtMax, Some(recommended), None));
    }
    if lagging {
        // Behind, yet the load does not call for a larger size. With one
        // worker saturated that is skew: the mean is low because one worker
        // holds the hot keys, and more workers would not help (DS2 §4.2.3).
        // Either way a lagging cluster is never scaled down.
        let status = if saturated {
            LoadStatus::SkewLimited
        } else {
            LoadStatus::Steady
        };
        return Some(evaluation(status, Some(recommended), None));
    }

    // Scale down on the maximum over a long window: one busy interval blocks
    // it. Being too small costs freshness, being too large costs money for
    // one window. The window is at least twice the hydration time so the
    // cluster never spends longer transitioning than at the result.
    let hydration = measured
        .iter()
        .filter_map(|(_, m)| m.hydration_time)
        .max()
        .unwrap_or_default();
    let window = millis(
        bounds
            .scale_down_after
            .unwrap_or(params.default_scale_down_after)
            .max(hydration.saturating_mul(2)),
    );
    let window_start = now_ms.saturating_sub(window);
    let in_window: Vec<&LoadMeasurement> = measured
        .iter()
        .filter(|(at, _)| *at >= window_start)
        .map(|(_, m)| *m)
        .collect();
    // The window must be observed, not assumed: samples have to reach back to
    // its start and cover at least half of its intervals.
    let covered = measured
        .first()
        .is_some_and(|(at, _)| *at <= window_start.saturating_add(interval.saturating_mul(2)))
        && u64::cast_from(in_window.len()).saturating_mul(2) >= window / interval;
    let quiet = in_window.iter().all(|m| !m.lagging())
        && !at_size().any(|s| s.oom_killed && u64::from(s.at) >= window_start);
    if !covered || !quiet {
        return Some(evaluation(LoadStatus::Steady, Some(recommended), None));
    }
    let wanted_down = in_window
        .iter()
        .map(|m| ladder.fit(m))
        .max()
        .unwrap_or(current);
    let down = ladder.clamp(wanted_down);
    if down >= current {
        let status = if wanted_down < ladder.lo && current == ladder.lo {
            LoadStatus::AtMin
        } else {
            LoadStatus::Steady
        };
        return Some(evaluation(status, Some(recommended), None));
    }
    Some(match ladder.allow_down(current, down, record, now) {
        Some(target) => evaluation(
            LoadStatus::ScalingDown,
            Some(down),
            Some(LoadAction {
                target_size: ladder.size(target).to_string(),
                reason: LoadScaleReason::ScaleDown,
                cpu_demand_millicores: in_window
                    .iter()
                    .map(|m| m.cpu_demand_millicores)
                    .max()
                    .unwrap_or(0),
                memory_demand_bytes: in_window
                    .iter()
                    .map(|m| m.memory_demand_bytes)
                    .max()
                    .unwrap_or(0),
                progress_before: None,
            }),
        ),
        None => evaluation(LoadStatus::BackingOff, Some(down), None),
    })
}

/// The size ladder with the allowed range resolved to indexes.
struct Ladder<'a> {
    sizes: &'a [SizeCapacity],
    lo: usize,
    hi: usize,
    params: &'a LoadParams,
}

impl<'a> Ladder<'a> {
    fn new(
        sizes: &'a [SizeCapacity],
        bounds: LoadBounds<'_>,
        params: &'a LoadParams,
    ) -> Option<Self> {
        let index = |size: &str| sizes.iter().position(|s| s.size == size);
        let lo = match bounds.min_size {
            Some(size) => index(size)?,
            None => 0,
        };
        let hi = match bounds.max_size {
            Some(size) => index(size)?,
            None => sizes.len().checked_sub(1)?,
        };
        (lo <= hi).then_some(Ladder {
            sizes,
            lo,
            hi,
            params,
        })
    }

    fn index_of(&self, size: &str) -> Option<usize> {
        self.sizes.iter().position(|s| s.size == size)
    }

    fn size(&self, index: usize) -> &str {
        &self.sizes[index].size
    }

    fn memory(&self, index: usize) -> u64 {
        self.sizes[index].memory_bytes
    }

    fn clamp(&self, index: usize) -> usize {
        index.clamp(self.lo, self.hi)
    }

    /// The cheapest size whose capacity, at the target utilization, covers
    /// the measurement. `sizes.len()` when none does.
    fn fit(&self, m: &LoadMeasurement) -> usize {
        let covers = |capacity: u64, target: u32, demand: u64| {
            u128::from(capacity) * u128::from(target) >= u128::from(demand) * 1000
        };
        self.sizes
            .iter()
            .position(|s| {
                covers(
                    s.cpu_millicores,
                    self.params.target_utilization,
                    m.cpu_demand_millicores,
                ) && covers(
                    s.memory_bytes,
                    self.params.target_memory,
                    m.memory_demand_bytes,
                )
            })
            .unwrap_or(self.sizes.len())
    }

    /// The first size above `current`, within range, with at least `memory`.
    /// The largest allowed size when none has.
    fn first_with_memory(&self, current: usize, memory: u64) -> usize {
        (current + 1..=self.hi)
            .find(|i| self.memory(*i) >= memory)
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

    /// The scale-down target given active backoff: `wanted`, or the smallest
    /// size above the highest blocked one. `None` when that leaves nothing
    /// below `current`.
    fn allow_down(
        &self,
        current: usize,
        wanted: usize,
        record: Option<&LoadRecord>,
        now: Timestamp,
    ) -> Option<usize> {
        let target = match self.blocked(record, LoadDirection::Down, now).max() {
            Some(floor) => wanted.max(floor + 1),
            None => wanted,
        };
        (target < current).then_some(target)
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
            progress_before: action.progress_before,
            phase: LoadAttemptPhase::Reconfiguring,
        });
        if record.hold.take().is_some() {
            audit.push(LoadAudit::HoldChanged { hold: None });
        }
        audit.push(LoadAudit::ScaleStarted {
            from_size: state.size.clone(),
            target_size: action.target_size.clone(),
            reason: action.reason,
            cpu_demand_millicores: action.cpu_demand_millicores,
            memory_demand_bytes: action.memory_demand_bytes,
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
                }),
                audit: Some(ReconfigurationAudit::Started),
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
    let failures = index.map_or(0, |i| record.backoff[i].failures).saturating_add(1);
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

        if let Some(attempt) = record.attempt.clone() {
            match attempt.phase {
                LoadAttemptPhase::Reconfiguring => {
                    // A user `ALTER` may have written over the attempt's
                    // record. Only the record this attempt wrote speaks for
                    // its outcome.
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
                            record.attempt = attempt.progress_before.map(|_| LoadAttempt {
                                phase: LoadAttemptPhase::Evaluating,
                                ..attempt
                            });
                        }
                        Some(
                            ReconfigurationStatus::TimedOut
                            | ReconfigurationStatus::ResourceExhausted,
                        ) => {
                            // For a scale-down this is the expected failure
                            // of a target that cannot hold the hydration peak
                            // or meet the lag gate. Without the backoff the
                            // same target would be retried every window.
                            let direction = match (
                                ladder_index(&attempt.target_size),
                                ladder_index(&attempt.from_size),
                            ) {
                                (Some(target), Some(from)) if target < from => LoadDirection::Down,
                                _ => LoadDirection::Up,
                            };
                            audit.push(back_off(&mut record, &attempt.target_size, direction, now));
                            record.attempt = None;
                        }
                        Some(ReconfigurationStatus::Cancelled) | None => record.attempt = None,
                    }
                    return load_write(record, audit);
                }
                LoadAttemptPhase::Evaluating => {
                    if state.size != attempt.target_size {
                        record.attempt = None;
                        return load_write(record, Vec::new());
                    }
                    if in_progress {
                        return StateWrite::default();
                    }
                    let warmed_up = record
                        .last_cutover
                        .map_or(Timestamp::MIN, |at| after(at, params.warm_up));
                    let mut progress: Vec<u32> = load
                        .samples
                        .iter()
                        .filter(|s| s.size == state.size && s.at >= warmed_up)
                        .filter_map(|s| s.measurement.as_ref())
                        .map(|m| m.min_progress)
                        .collect();
                    if progress.len() < params.activation_intervals.max(1) {
                        return StateWrite::default();
                    }
                    progress.sort_unstable();
                    let after_scale_up = progress[(progress.len() - 1) / 2];
                    let before = attempt.progress_before.unwrap_or(1000);
                    let ineffective = after_scale_up < 1000
                        && after_scale_up < before.saturating_add(EFFECTIVE_PROGRESS_GAIN)
                        && ladder_index(&attempt.from_size).is_some();
                    record.attempt = None;
                    if !ineffective {
                        return load_write(record, Vec::new());
                    }
                    // The larger size did not make the lagging dataflows
                    // progress faster, so its cost buys nothing. Go back and
                    // stop scaling up past the old size (DS2 §4.2.2).
                    let audit = vec![back_off(
                        &mut record,
                        &attempt.target_size,
                        LoadDirection::Up,
                        now,
                    )];
                    let action = LoadAction {
                        target_size: attempt.from_size.clone(),
                        reason: LoadScaleReason::IneffectiveScaleUp,
                        cpu_demand_millicores: 0,
                        memory_demand_bytes: 0,
                        progress_before: None,
                    };
                    return self.start(state, record, &action, config, now, audit);
                }
            }
        }

        // One transition at a time, and a user's reconfiguration is never
        // overwritten.
        if in_progress {
            return StateWrite::default();
        }
        let Some(evaluation) = evaluate(
            &state.size,
            policy.into(),
            load,
            Some(&record),
            params,
            now,
        ) else {
            return StateWrite::default();
        };
        if let Some(action) = &evaluation.action {
            return self.start(state, record, action, config, now, Vec::new());
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
