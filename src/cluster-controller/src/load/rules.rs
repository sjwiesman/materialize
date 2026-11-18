// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The decision rules of the `ON LOAD` strategy.
//!
//! [`evaluate`] is a pure function of a cluster's policy, its size ladder, its durable state,
//! its load summaries, the statistics of a trial whose hold has ended, and `now`. It returns a
//! [`LoadDecision`]: what to write and the cluster's [`LoadStatus`]. It acts on nothing, so the
//! strategy can apply a decision and the recommendations can show one.
//!
//! A decision moves the cluster in one of three ways. A trial runs a candidate size beside the
//! current one and commits to it only on a verdict ([`LoadAction::StartTrial`], then
//! [`LoadAction::CutOver`] or [`LoadAction::Drop`]). A plain reconfiguration moves the cluster
//! up without a trial, for memory or at the end of a probation ([`LoadAction::Reconfigure`]).
//! The durable [`LoadRecord`] carries what the rules learned: per-size evidence, lag floors,
//! the attempt in flight, and the last cut-over.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use mz_adapter_types::cluster_state::{
    LoadAttempt, LoadCutover, LoadCutoverReason, LoadEvidence, LoadFloor, LoadRecord, LoadVerdict,
    OnLoadPolicy, ReconfigurationOrigin, ReconfigurationRecord, TrialKind,
};
use mz_adapter_types::dyncfgs::{
    LOAD_AUTOSCALING_CPU_MARGIN, LOAD_AUTOSCALING_DEFAULT_SCALE_DOWN_AFTER,
    LOAD_AUTOSCALING_DOWN_TRIAL_DURATION, LOAD_AUTOSCALING_EVALUATION_WINDOW,
    LOAD_AUTOSCALING_FLOOR_FACTOR, LOAD_AUTOSCALING_HEAP_HIGH_DURATION, LOAD_AUTOSCALING_HEAP_MAX,
    LOAD_AUTOSCALING_HEAP_PROPOSAL_MAX, LOAD_AUTOSCALING_LAG_TOLERANCE,
    LOAD_AUTOSCALING_MAX_EVIDENCE_INTERVAL, LOAD_AUTOSCALING_MIN_PROGRESS_RATIO,
    LOAD_AUTOSCALING_NO_GAIN_INTERVAL, LOAD_AUTOSCALING_QUIET_WINDOW_COVERAGE,
    LOAD_AUTOSCALING_UP_TRIAL_DURATION,
};
use mz_dyncfg::ConfigSet;
use mz_ore::cast::CastLossy;
use mz_repr::{GlobalId, Timestamp};

use crate::load::{
    BusySummary, DataflowStats, HeapSummary, LoadSummary, behind_threshold, quiet_window,
};

/// A trial's hydration budget is this multiple of the cluster's last hydration time...
const HYDRATION_BUDGET_FACTOR: u32 = 4;
/// ...and at least this long.
const MIN_HYDRATION_BUDGET: Duration = Duration::from_secs(30 * 60);
/// A verdict that recurs within this multiple of the previous one's interval doubles it.
const EVIDENCE_RECURRENCE_FACTOR: u32 = 4;
/// The *faster* gain: the candidate's progress ratio exceeds the current one's by this much.
const FASTER_RATIO_GAIN: f64 = 0.1;
/// The *closer* gain: both progress ratios are within this distance of 1...
const CLOSER_RATIO_DISTANCE: f64 = 0.05;
/// ...and the candidate's median lag is at most this share of the current one's.
const CLOSER_LAG_SHARE: f64 = 0.5;
/// The percentile of the quiet window's busy time that has to fit a down-trial's candidate.
const BUSY_PERCENTILE: f64 = 0.95;
/// A hydration peak ratio is rewritten when it moves by more than this share of the recorded
/// one, so that a ratio that wobbles with the steady-state heap is not written every window.
const PEAK_RATIO_REWRITE_SHARE: f64 = 0.1;

/// One size of the cluster's size family.
#[derive(Clone, Debug, PartialEq)]
pub struct SizeCapacity {
    pub name: String,
    /// Cores per replica.
    pub cores: f64,
    /// Memory per replica, in bytes.
    pub memory_bytes: u64,
    /// The heap limit of a replica's process, in bytes. See [`crate::load::ProcessHeap`].
    pub heap_limit: u64,
}

/// The parameters of the rules, from the `load_autoscaling_*` dyncfgs.
#[derive(Clone, Debug, PartialEq)]
pub struct RuleParams {
    pub evaluation_window: Duration,
    /// `τ`.
    pub lag_tolerance: Duration,
    pub floor_factor: f64,
    /// The floor on `ρ`, the lowest progress ratio among the behind dataflows, when sizing an
    /// up-trial.
    pub min_progress_ratio: f64,
    pub up_trial_duration: Duration,
    pub down_trial_duration: Duration,
    /// The heap fraction a selected size may not reach.
    pub heap_max: f64,
    /// The projected heap fraction at which a size is proposed.
    pub heap_proposal_max: f64,
    pub heap_high_duration: Duration,
    /// The share of a down-trial candidate's cores that busy time may use.
    pub cpu_margin: f64,
    /// `W` for a policy without `SCALE DOWN AFTER`.
    pub default_scale_down_after: Duration,
    pub quiet_window_coverage: f64,
    pub no_gain_interval: Duration,
    pub max_evidence_interval: Duration,
}

impl RuleParams {
    pub fn from_config(config: &ConfigSet) -> Self {
        RuleParams {
            evaluation_window: LOAD_AUTOSCALING_EVALUATION_WINDOW
                .get(config)
                .max(Duration::from_millis(1)),
            lag_tolerance: LOAD_AUTOSCALING_LAG_TOLERANCE.get(config),
            floor_factor: LOAD_AUTOSCALING_FLOOR_FACTOR.get(config),
            min_progress_ratio: LOAD_AUTOSCALING_MIN_PROGRESS_RATIO.get(config),
            up_trial_duration: LOAD_AUTOSCALING_UP_TRIAL_DURATION.get(config),
            down_trial_duration: LOAD_AUTOSCALING_DOWN_TRIAL_DURATION.get(config),
            heap_max: LOAD_AUTOSCALING_HEAP_MAX.get(config),
            heap_proposal_max: LOAD_AUTOSCALING_HEAP_PROPOSAL_MAX.get(config),
            heap_high_duration: LOAD_AUTOSCALING_HEAP_HIGH_DURATION.get(config),
            cpu_margin: LOAD_AUTOSCALING_CPU_MARGIN.get(config),
            default_scale_down_after: LOAD_AUTOSCALING_DEFAULT_SCALE_DOWN_AFTER.get(config),
            quiet_window_coverage: LOAD_AUTOSCALING_QUIET_WINDOW_COVERAGE.get(config),
            no_gain_interval: LOAD_AUTOSCALING_NO_GAIN_INTERVAL.get(config),
            max_evidence_interval: LOAD_AUTOSCALING_MAX_EVIDENCE_INTERVAL.get(config),
        }
    }
}

/// What the strategy observed of a trial in flight.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TrialObservation {
    /// Whether a candidate replica was killed for memory since the trial started.
    pub candidate_killed: bool,
    /// The statistics over the hold, once the hold has ended. `None` while it runs, and when
    /// the samples of the hold were lost.
    pub stats: Option<TrialStats>,
}

/// The lag and heap of the candidate and the current replicas over a trial's hold.
#[derive(Clone, Debug, PartialEq)]
pub struct TrialStats {
    /// Per dataflow, on the current replicas. See [`crate::load::dataflow_stats`].
    pub current: BTreeMap<GlobalId, DataflowStats>,
    /// Per dataflow, on the candidate replicas.
    pub candidate: BTreeMap<GlobalId, DataflowStats>,
    /// The candidate's steady-state heap over the hold.
    pub candidate_heap: Option<HeapSummary>,
}

/// Everything [`evaluate`] reads.
#[derive(Clone, Debug)]
pub struct RulesInput<'a> {
    pub policy: &'a OnLoadPolicy,
    /// The sizes of the cluster's size family, ordered by credits per hour.
    pub ladder: &'a [SizeCapacity],
    /// The realized size.
    pub size: &'a str,
    /// The latest reconfiguration record, of any origin and status.
    pub reconfiguration: Option<&'a ReconfigurationRecord>,
    pub record: Option<&'a LoadRecord>,
    /// The fingerprint of the dataflows installed on the cluster now. See
    /// [`crate::load::fingerprint`].
    pub fingerprint: u64,
    /// The cluster's load summaries, in any order.
    pub summaries: &'a [LoadSummary],
    /// What was observed of the trial in flight, if any.
    pub trial: Option<&'a TrialObservation>,
    /// How long the replicas at the realized size took to hydrate, most recently.
    pub last_hydration: Option<Duration>,
    /// The largest heap of the realized size's replicas between their creation and their
    /// hydration, in bytes.
    pub hydration_peak: Option<u64>,
    pub now: Timestamp,
}

/// The status of a cluster under the `ON LOAD` policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LoadStatus {
    /// Keeping up at a size between the bounds, with nothing due.
    Steady,
    TrialUp,
    TrialDown,
    /// A plain reconfiguration up, for memory or at the end of a probation.
    ScalingUp,
    /// A `NoGain` verdict blocks an up-trial from the realized size.
    NotScalable,
    /// A descent is otherwise due, and evidence blocks it.
    Waiting,
    /// A dataflow is behind at `MAX SIZE`.
    AtMax,
    /// At `MIN SIZE`, with nothing due.
    AtMin,
    /// A replica at `MAX SIZE` was killed for memory.
    OutOfMemoryAtMax,
    /// No summary with lag at the realized size covers the latest evaluation window.
    WarmingUp,
}

impl LoadStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            LoadStatus::Steady => "steady",
            LoadStatus::TrialUp => "trial-up",
            LoadStatus::TrialDown => "trial-down",
            LoadStatus::ScalingUp => "scaling-up",
            LoadStatus::NotScalable => "not-scalable",
            LoadStatus::Waiting => "waiting",
            LoadStatus::AtMax => "at-max",
            LoadStatus::AtMin => "at-min",
            LoadStatus::OutOfMemoryAtMax => "out-of-memory-at-max",
            LoadStatus::WarmingUp => "warming-up",
        }
    }
}

/// A dataflow that is behind in the latest summary.
#[derive(Clone, Debug, PartialEq)]
pub struct BehindDataflow {
    pub id: GlobalId,
    pub median_lag: Duration,
    pub progress_ratio: Option<f64>,
    pub floor: Option<Duration>,
}

/// The values a status rests on.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StatusFacts {
    /// The end of the latest summary with lag at the realized size, which the other facts are
    /// read from. `None` when there is none.
    pub summary_at: Option<Timestamp>,
    /// The dataflows behind in the latest summary at the realized size.
    pub behind: Vec<BehindDataflow>,
    pub heap: Option<HeapSummary>,
    pub busy: Option<BusySummary>,
    /// The evidence that blocks the trial the status names, for `waiting` and `not-scalable`.
    pub blocking: Option<LoadEvidence>,
    /// Why no down-trial is proposed, when the decision has no action, nothing is in flight,
    /// and the realized size is above `MIN SIZE`.
    pub down_block: Option<DownBlock>,
}

/// A trial to start.
#[derive(Clone, Debug, PartialEq)]
pub struct TrialStart {
    pub kind: TrialKind,
    pub target: String,
    pub hold_for: Duration,
    /// The end of the hydration budget: the reconfiguration record's deadline.
    pub hydration_deadline: Timestamp,
    /// The latest instant at which the candidate can exist: the trial's deadline.
    pub deadline: Timestamp,
}

/// Which of the gains an up-trial's candidate showed for a dataflow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Gain {
    /// Its median lag on the candidate is within the tolerance and not rising past it.
    Fresh,
    /// Its progress ratio on the candidate exceeds the current one's by at least 0.1.
    Faster,
    /// Its progress ratio is near 1 on both, and its lag on the candidate at most half.
    Closer,
}

/// One dataflow in a trial verdict.
#[derive(Clone, Debug, PartialEq)]
pub struct DataflowComparison {
    pub current: Option<DataflowStats>,
    pub candidate: Option<DataflowStats>,
    /// For an up-trial, the gain the candidate showed. For a down-trial, `Some(Fresh)` when the
    /// candidate was as fresh as the current replicas.
    pub gain: Option<Gain>,
}

/// The verdict of a trial whose hold ended.
#[derive(Clone, Debug, PartialEq)]
pub struct Verdict {
    pub kind: TrialKind,
    pub passed: bool,
    /// For an up-trial, the dataflows behind at its trigger. For a down-trial, every dataflow.
    pub dataflows: BTreeMap<GlobalId, DataflowComparison>,
    pub candidate_heap: Option<HeapSummary>,
}

/// Why a trial's candidate is dropped.
#[derive(Clone, Debug, PartialEq)]
pub enum DropReason {
    /// The verdict was a fail.
    Failed(Verdict),
    /// A candidate replica was killed for memory.
    Killed,
    /// The candidate was not hydrated by the end of its hydration budget.
    NotHydrated,
    /// The trial's deadline passed without a verdict.
    Deadline,
}

/// What a decision does to the cluster's size.
#[derive(Clone, Debug, PartialEq)]
pub enum LoadAction {
    /// Write a reconfiguration record with a trial. The candidate runs beside the current
    /// replicas until the verdict.
    StartTrial(TrialStart),
    /// Write a reconfiguration record without a trial, superseding any in flight.
    Reconfigure {
        target: String,
        reason: LoadCutoverReason,
        hydration_deadline: Timestamp,
    },
    /// The trial's candidate passed. Cut over to it.
    CutOver(Verdict),
    /// Drop the trial's candidate. The cluster stays at its size.
    Drop(DropReason),
}

/// A change to the evidence.
#[derive(Clone, Debug, PartialEq)]
pub enum EvidenceChange {
    Recorded(LoadEvidence),
    /// Everything was cleared, because the fingerprint changed.
    Cleared {
        fingerprint: u64,
    },
}

/// The outcome of [`evaluate`].
#[derive(Clone, Debug, PartialEq)]
pub struct LoadDecision {
    pub status: LoadStatus,
    pub facts: StatusFacts,
    pub action: Option<LoadAction>,
    /// The load record after the decision. `None` when it is unchanged.
    pub record: Option<LoadRecord>,
    pub evidence: Vec<EvidenceChange>,
}

/// The record a policy change leaves: the same fingerprint, without evidence or floors.
/// Evidence and floors describe sizes for a policy and a workload that no longer exist.
pub fn cleared_record(record: &LoadRecord, fingerprint: u64) -> LoadRecord {
    LoadRecord {
        fingerprint,
        evidence: Vec::new(),
        floors: Vec::new(),
        ..record.clone()
    }
}

/// Whether `evidence` blocks its trial at `now`.
pub fn blocks(evidence: &LoadEvidence, now: Timestamp) -> bool {
    u64::from(now) < add(evidence.at, evidence.interval).into()
}

/// The interval of a new verdict against a size: the base interval, doubled up to `max` when
/// the previous verdict of the same kind against that size was within
/// [`EVIDENCE_RECURRENCE_FACTOR`] times its interval.
pub fn evidence_interval(
    previous: Option<&LoadEvidence>,
    base: Duration,
    max: Duration,
    now: Timestamp,
) -> Duration {
    let recurs = previous.is_some_and(|previous| {
        u64::from(now)
            <= u64::from(add(
                previous.at,
                previous.interval * EVIDENCE_RECURRENCE_FACTOR,
            ))
    });
    match previous {
        Some(previous) if recurs => previous.interval.saturating_mul(2).min(max),
        _ => base.min(max),
    }
}

/// The gain an up-trial's candidate showed for a dataflow, if any.
pub fn gain(
    current: &DataflowStats,
    candidate: &DataflowStats,
    tolerance: Duration,
) -> Option<Gain> {
    let fresh = candidate.median_lag <= tolerance
        && candidate
            .last_third_median_lag
            .is_none_or(|last| last <= tolerance);
    if fresh {
        return Some(Gain::Fresh);
    }
    if let (Some(cur), Some(cand)) = (current.progress_ratio, candidate.progress_ratio) {
        if cand - cur >= FASTER_RATIO_GAIN {
            return Some(Gain::Faster);
        }
        let steady = |ratio: f64| (ratio - 1.0).abs() <= CLOSER_RATIO_DISTANCE;
        if steady(cur)
            && steady(cand)
            && candidate.median_lag.as_secs_f64()
                <= current.median_lag.as_secs_f64() * CLOSER_LAG_SHARE
        {
            return Some(Gain::Closer);
        }
    }
    None
}

/// Whether a down-trial's candidate was as fresh as the current replicas for a dataflow: its
/// median lag over the hold, and over the hold's last third, is within the tolerance of theirs.
pub fn as_fresh(current: &DataflowStats, candidate: &DataflowStats, tolerance: Duration) -> bool {
    let within = |cand: Duration, cur: Duration| cand <= cur.saturating_add(tolerance);
    within(candidate.median_lag, current.median_lag)
        && match (
            candidate.last_third_median_lag,
            current.last_third_median_lag,
        ) {
            (Some(cand), Some(cur)) => within(cand, cur),
            (Some(cand), None) => within(cand, current.median_lag),
            (None, _) => false,
        }
}

/// Evaluates the rules for one cluster. `None` when the realized size or a bound of the policy
/// is not on the ladder.
pub fn evaluate(input: &RulesInput, params: &RuleParams) -> Option<LoadDecision> {
    Rules::new(input, params).map(|rules| rules.evaluate())
}

/// One evaluation.
struct Rules<'a> {
    input: &'a RulesInput<'a>,
    params: &'a RuleParams,
    /// The sizes between the bounds.
    range: &'a [SizeCapacity],
    /// The realized size's index in `range`.
    current: usize,
    /// `W`.
    scale_down_after: Duration,
    /// The record as the decision leaves it.
    record: LoadRecord,
    evidence: Vec<EvidenceChange>,
    /// The cluster's summaries at the realized size, newest first.
    at_size: Vec<&'a LoadSummary>,
}

impl<'a> Rules<'a> {
    fn new(input: &'a RulesInput<'a>, params: &'a RuleParams) -> Option<Self> {
        let index = |name: &str| input.ladder.iter().position(|size| size.name == name);
        let min = index(&input.policy.min_size)?;
        let max = index(&input.policy.max_size)?;
        let range = input.ladder.get(min..=max)?;
        let current = range.iter().position(|size| size.name == input.size)?;

        let mut evidence = Vec::new();
        let record = match input.record {
            Some(record) if record.fingerprint == input.fingerprint => record.clone(),
            Some(record) => {
                if !record.evidence.is_empty() || !record.floors.is_empty() {
                    evidence.push(EvidenceChange::Cleared {
                        fingerprint: input.fingerprint,
                    });
                }
                cleared_record(record, input.fingerprint)
            }
            None => LoadRecord {
                fingerprint: input.fingerprint,
                attempt: None,
                last_cutover: None,
                evidence: Vec::new(),
                floors: Vec::new(),
                hydration_peak_ratio: None,
            },
        };
        let mut at_size: Vec<_> = input
            .summaries
            .iter()
            .filter(|summary| summary.size == input.size)
            .collect();
        at_size.sort_by(|a, b| b.at.cmp(&a.at));
        Some(Rules {
            input,
            params,
            range,
            current,
            scale_down_after: input
                .policy
                .scale_down_after
                .unwrap_or(params.default_scale_down_after),
            record,
            evidence,
            at_size,
        })
    }

    fn evaluate(mut self) -> LoadDecision {
        self.update_peak_ratio();
        let facts = self.facts();
        let (status, action) = self.decide(&facts);
        let mut facts = facts;
        let in_flight = self
            .input
            .reconfiguration
            .is_some_and(|r| r.is_in_progress());
        if action.is_none() && !in_flight {
            facts.down_block = self.down_target().and_then(|target| {
                self.down_conditions(target)
                    .err()
                    .or_else(|| self.blocking_down(target).map(|_| DownBlock::Evidence))
            });
        }
        if matches!(status, LoadStatus::Waiting | LoadStatus::NotScalable) {
            facts.blocking = match status {
                LoadStatus::NotScalable => self.blocking_up().cloned(),
                _ => self
                    .down_target()
                    .and_then(|t| self.blocking_down(t).cloned()),
            };
        }
        let record = (self.input.record != Some(&self.record)).then_some(self.record);
        LoadDecision {
            status,
            facts,
            action,
            record,
            evidence: self.evidence,
        }
    }

    fn decide(&mut self, facts: &StatusFacts) -> (LoadStatus, Option<LoadAction>) {
        let reconfiguration = self.input.reconfiguration.filter(|r| r.is_in_progress());
        match reconfiguration {
            Some(r) if r.origin == ReconfigurationOrigin::User => {
                // A user's reconfiguration wins over the strategy's. An attempt it superseded
                // never reaches a verdict.
                self.record.attempt = None;
                return (self.idle_status(facts), None);
            }
            Some(r) => {
                let Some(trial) = &r.trial else {
                    return (LoadStatus::ScalingUp, None);
                };
                // The memory rules supersede a trial. The trial's outcome is not recorded.
                if let Some((status, Some(action))) = self.memory_rules() {
                    self.record.attempt = None;
                    return (status, Some(action));
                }
                return self.trial_in_flight(r, trial.kind);
            }
            None => {
                // An attempt whose record finished without a verdict, cut over by its deadline
                // or rolled back, has no outcome to record.
                self.record.attempt = None;
            }
        }
        if let Some(memory) = self.memory_rules() {
            return memory;
        }
        if let Some(probation) = self.probation() {
            return probation;
        }
        if let Some(up) = self.up_trial(facts) {
            return up;
        }
        if let Some(down) = self.down_trial() {
            return down;
        }
        (self.idle_status(facts), None)
    }

    /// The status when nothing is in flight and no rule acts.
    fn idle_status(&self, facts: &StatusFacts) -> LoadStatus {
        let at_max = self.current + 1 == self.range.len();
        if at_max && self.latest_window().is_some_and(|s| s.killed) {
            return LoadStatus::OutOfMemoryAtMax;
        }
        if self.latest().is_none() {
            return LoadStatus::WarmingUp;
        }
        if self.blocking_up().is_some() {
            return LoadStatus::NotScalable;
        }
        if at_max && !facts.behind.is_empty() {
            return LoadStatus::AtMax;
        }
        if let Some(target) = self.down_target() {
            if self.blocking_down(target).is_some() && self.down_conditions(target).is_ok() {
                return LoadStatus::Waiting;
            }
        }
        if self.current == 0 {
            return LoadStatus::AtMin;
        }
        LoadStatus::Steady
    }

    fn facts(&self) -> StatusFacts {
        let latest = self.latest();
        StatusFacts {
            summary_at: latest.map(|s| s.at),
            behind: latest.map(|s| self.behind(s)).unwrap_or_default(),
            heap: latest.and_then(|s| s.heap),
            busy: latest.and_then(|s| s.busy),
            blocking: None,
            down_block: None,
        }
    }

    /// The summary of the latest closed evaluation window at the realized size, of any kind.
    /// The summary of the window before stands in while the latest one is being produced.
    fn latest_window(&self) -> Option<&'a LoadSummary> {
        let window = millis(self.params.evaluation_window);
        let now = u64::from(self.input.now);
        let fresh_since = now.saturating_sub(window.saturating_mul(2));
        self.at_size
            .first()
            .copied()
            .filter(|summary| u64::from(summary.at) > fresh_since)
    }

    /// The latest summary at the realized size with lag, if it is fresh. See
    /// [`Self::latest_window`].
    fn latest(&self) -> Option<&'a LoadSummary> {
        self.latest_window()
            .filter(|summary| !summary.hydrating && !summary.dataflows.is_empty())
    }

    fn floor(&self, id: GlobalId) -> Option<Duration> {
        self.record
            .floors
            .iter()
            .find(|floor| floor.dataflow == id)
            .map(|floor| floor.lag)
    }

    /// The dataflows behind in a summary, against the current floors. A summary's own `behind`
    /// was decided against the floors of its time. A floor set since then raises the threshold,
    /// so the current threshold narrows it. Whether the dataflow was draining is not in the
    /// summary, so a flag that was clear stays clear.
    fn behind(&self, summary: &LoadSummary) -> Vec<BehindDataflow> {
        summary
            .dataflows
            .iter()
            .filter_map(|(id, dataflow)| {
                let floor = self.floor(*id);
                let threshold =
                    behind_threshold(self.params.lag_tolerance, floor, self.params.floor_factor);
                (dataflow.behind && dataflow.median_lag > threshold).then_some(BehindDataflow {
                    id: *id,
                    median_lag: dataflow.median_lag,
                    progress_ratio: dataflow.progress_ratio,
                    floor,
                })
            })
            .collect()
    }

    fn update_peak_ratio(&mut self) {
        let Some(peak) = self.input.hydration_peak else {
            return;
        };
        let Some(steady) = self.latest().and_then(|s| s.heap).filter(|h| h.bytes > 0) else {
            return;
        };
        let ratio = f64::cast_lossy(peak) / f64::cast_lossy(steady.bytes);
        let permille = u32::cast_lossy((ratio * 1000.0).round());
        let rewrite = match self.record.hydration_peak_ratio {
            None => true,
            Some(recorded) => {
                let recorded = f64::from(recorded);
                (f64::from(permille) - recorded).abs() > recorded * PEAK_RATIO_REWRITE_SHARE
            }
        };
        if rewrite {
            self.record.hydration_peak_ratio = Some(permille);
        }
    }

    fn hydration_budget(&self) -> Duration {
        self.input
            .last_hydration
            .map_or(Duration::ZERO, |h| {
                h.saturating_mul(HYDRATION_BUDGET_FACTOR)
            })
            .max(MIN_HYDRATION_BUDGET)
    }

    fn record_evidence(&mut self, size: &str, verdict: LoadVerdict) {
        let base = match verdict {
            LoadVerdict::Insufficient => self.scale_down_after,
            LoadVerdict::NoGain => self.params.no_gain_interval,
        };
        let previous = self
            .record
            .evidence
            .iter()
            .find(|e| e.size == size && e.verdict == verdict);
        let interval = evidence_interval(
            previous,
            base,
            self.params.max_evidence_interval,
            self.input.now,
        );
        let evidence = LoadEvidence {
            size: size.to_string(),
            verdict,
            at: self.input.now,
            interval,
        };
        self.record
            .evidence
            .retain(|e| !(e.size == size && e.verdict == verdict));
        self.record.evidence.push(evidence.clone());
        self.evidence.push(EvidenceChange::Recorded(evidence));
    }

    fn set_floor(&mut self, id: GlobalId, lag: Duration) {
        self.record.floors.retain(|floor| floor.dataflow != id);
        self.record.floors.push(LoadFloor { dataflow: id, lag });
        self.record.floors.sort_by_key(|floor| floor.dataflow);
    }

    fn cut_over(&mut self, reason: LoadCutoverReason) {
        self.record.last_cutover = Some(LoadCutover {
            at: self.input.now,
            from_size: self.input.size.to_string(),
            reason,
        });
    }

    fn size_name(&self, index: usize) -> String {
        self.range[index].name.clone()
    }

    /// A plain reconfiguration up to `target`, recording that the realized size was
    /// insufficient.
    fn reconfigure(&mut self, target: usize, reason: LoadCutoverReason) -> LoadAction {
        let from = self.input.size.to_string();
        self.record_evidence(&from, LoadVerdict::Insufficient);
        self.cut_over(reason);
        LoadAction::Reconfigure {
            target: self.size_name(target),
            reason,
            hydration_deadline: add(self.input.now, self.hydration_budget()),
        }
    }

    /// The kill and high-heap rules.
    fn memory_rules(&mut self) -> Option<(LoadStatus, Option<LoadAction>)> {
        let at_max = self.current + 1 == self.range.len();
        let killed = self.latest_window().is_some_and(|summary| summary.killed);
        if killed {
            if at_max {
                return Some((LoadStatus::OutOfMemoryAtMax, None));
            }
            let limit = self.range[self.current].heap_limit;
            let target = (self.current + 1..self.range.len())
                .find(|i| self.range[*i].heap_limit >= limit.saturating_mul(2))
                .unwrap_or(self.range.len() - 1);
            let action = self.reconfigure(target, LoadCutoverReason::OutOfMemory);
            return Some((LoadStatus::ScalingUp, Some(action)));
        }

        // Every summary over the high-heap period has a heap above the maximum.
        let period = self.params.heap_high_duration;
        let window = self.params.evaluation_window;
        let needed = usize::try_from(period.as_millis().div_ceil(window.as_millis().max(1)))
            .unwrap_or(usize::MAX)
            .max(1);
        let since = u64::from(self.input.now).saturating_sub(millis(period) + millis(window));
        let recent: Vec<_> = self
            .at_size
            .iter()
            .filter(|summary| u64::from(summary.at) > since && !summary.hydrating)
            .take(needed)
            .collect();
        let high = recent.len() == needed
            && recent.iter().all(|summary| {
                summary
                    .heap
                    .and_then(|heap| heap.fraction)
                    .is_some_and(|fraction| fraction > self.params.heap_max)
            });
        if !high || at_max {
            return None;
        }
        let bytes = recent
            .iter()
            .filter_map(|summary| summary.heap.map(|heap| heap.bytes))
            .max()
            .unwrap_or(0);
        let target = (self.current + 1..self.range.len())
            .find(|i| heap_fraction(bytes, &self.range[*i]) <= self.params.heap_proposal_max)
            .unwrap_or(self.range.len() - 1);
        let action = self.reconfigure(target, LoadCutoverReason::HighHeap);
        Some((LoadStatus::ScalingUp, Some(action)))
    }

    /// The return to the size a descent left, when a dataflow goes behind within `W` of it.
    fn probation(&mut self) -> Option<(LoadStatus, Option<LoadAction>)> {
        let cutover = self.record.last_cutover.as_ref()?;
        if cutover.reason != LoadCutoverReason::DownTrial
            || u64::from(self.input.now) >= u64::from(add(cutover.at, self.scale_down_after))
        {
            return None;
        }
        let latest = self.latest()?;
        if u64::from(latest.start) < u64::from(cutover.at) || self.behind(latest).is_empty() {
            return None;
        }
        let target = self
            .range
            .iter()
            .position(|size| size.name == cutover.from_size)?;
        let action = self.reconfigure(target, LoadCutoverReason::ProbationReturn);
        Some((LoadStatus::ScalingUp, Some(action)))
    }

    /// The live `NoGain` evidence against the realized size.
    fn blocking_up(&self) -> Option<&LoadEvidence> {
        self.record.evidence.iter().find(|e| {
            e.verdict == LoadVerdict::NoGain
                && e.size == self.input.size
                && blocks(e, self.input.now)
        })
    }

    fn up_trial(&mut self, facts: &StatusFacts) -> Option<(LoadStatus, Option<LoadAction>)> {
        if facts.behind.is_empty()
            || self.current + 1 == self.range.len()
            || self.blocking_up().is_some()
        {
            return None;
        }
        let rho = facts
            .behind
            .iter()
            .filter_map(|d| d.progress_ratio)
            .fold(f64::INFINITY, f64::min);
        let next = self.current + 1;
        let target = if rho < 1.0 {
            let rho = rho.max(self.params.min_progress_ratio);
            let cores = self.range[self.current].cores / rho;
            (next..self.range.len())
                .find(|i| self.range[*i].cores >= cores)
                .unwrap_or(self.range.len() - 1)
        } else {
            next
        };
        let behind = facts.behind.iter().map(|d| d.id).collect();
        let start = self.start_trial(TrialKind::Up, target, self.params.up_trial_duration, behind);
        Some((LoadStatus::TrialUp, Some(start)))
    }

    fn start_trial(
        &mut self,
        kind: TrialKind,
        target: usize,
        hold_for: Duration,
        behind: Vec<GlobalId>,
    ) -> LoadAction {
        let hydration_deadline = add(self.input.now, self.hydration_budget());
        let target = self.size_name(target);
        self.record.attempt = Some(LoadAttempt {
            from_size: self.input.size.to_string(),
            target_size: target.clone(),
            behind,
        });
        LoadAction::StartTrial(TrialStart {
            kind,
            target,
            hold_for,
            hydration_deadline,
            deadline: add(hydration_deadline, hold_for),
        })
    }

    fn down_target(&self) -> Option<usize> {
        self.current.checked_sub(1)
    }

    /// The live `Insufficient` evidence against `target` or a larger size.
    fn blocking_down(&self, target: usize) -> Option<&LoadEvidence> {
        self.record.evidence.iter().find(|e| {
            e.verdict == LoadVerdict::Insufficient
                && blocks(e, self.input.now)
                && self
                    .range
                    .iter()
                    .position(|size| size.name == e.size)
                    .is_some_and(|i| i >= target)
        })
    }

    /// Whether the quiet window shows that a down-trial to `target` is worth trying, apart from
    /// evidence. Never [`DownBlock::Evidence`].
    fn down_conditions(&self, target: usize) -> Result<(), DownBlock> {
        let length = self.scale_down_after.max(
            self.input
                .last_hydration
                .unwrap_or_default()
                .saturating_mul(2),
        );
        let quiet = quiet_window(
            self.at_size.iter().copied(),
            self.input.now,
            length,
            self.params.evaluation_window,
        );
        if !quiet.filled || quiet.coverage() < self.params.quiet_window_coverage {
            return Err(DownBlock::NotObserved);
        }
        if quiet
            .summaries
            .iter()
            .any(|s| s.killed || !self.behind(s).is_empty())
        {
            return Err(DownBlock::Behind);
        }
        let candidate = &self.range[target];
        let heaps: Vec<_> = quiet.summaries.iter().filter_map(|s| s.heap).collect();
        let Some(bytes) = heaps.iter().map(|heap| heap.bytes).max() else {
            return Err(DownBlock::Heap);
        };
        if heap_fraction(bytes, candidate) > self.params.heap_proposal_max {
            return Err(DownBlock::Heap);
        }
        if let Some(ratio) = self.record.hydration_peak_ratio {
            let peak = f64::cast_lossy(bytes) * f64::from(ratio) / 1000.0;
            if peak > f64::cast_lossy(candidate.heap_limit) {
                return Err(DownBlock::Heap);
            }
        }
        let busy: Option<Vec<u64>> = quiet
            .summaries
            .iter()
            .map(|s| s.busy.map(|busy| busy.mean_millicores))
            .collect();
        let Some(busy) = busy else {
            return Err(DownBlock::Busy);
        };
        let p95 = percentile(busy, BUSY_PERCENTILE).unwrap_or(0);
        if f64::cast_lossy(p95) > candidate.cores * 1000.0 * self.params.cpu_margin {
            return Err(DownBlock::Busy);
        }
        Ok(())
    }

    fn down_trial(&mut self) -> Option<(LoadStatus, Option<LoadAction>)> {
        let target = self.down_target()?;
        if self.blocking_down(target).is_some() || self.down_conditions(target).is_err() {
            return None;
        }
        let start = self.start_trial(
            TrialKind::Down,
            target,
            self.params.down_trial_duration,
            Vec::new(),
        );
        Some((LoadStatus::TrialDown, Some(start)))
    }

    /// A trial's deadlines, kills, and verdict.
    fn trial_in_flight(
        &mut self,
        reconfiguration: &ReconfigurationRecord,
        kind: TrialKind,
    ) -> (LoadStatus, Option<LoadAction>) {
        let status = match kind {
            TrialKind::Up => LoadStatus::TrialUp,
            TrialKind::Down => LoadStatus::TrialDown,
        };
        let Some(trial) = &reconfiguration.trial else {
            return (status, None);
        };
        let target = reconfiguration.target.size.clone();
        let now = u64::from(self.input.now);
        let observation = self.input.trial.cloned().unwrap_or_default();

        if observation.candidate_killed {
            if kind == TrialKind::Down {
                self.record_evidence(&target, LoadVerdict::Insufficient);
            }
            self.record.attempt = None;
            return (status, Some(LoadAction::Drop(DropReason::Killed)));
        }
        let hold_end = trial
            .hold_started
            .map(|started| u64::from(add(started, trial.hold_for)));
        if let (Some(end), Some(stats)) = (hold_end, &observation.stats) {
            if now >= end && now <= u64::from(trial.deadline) {
                return (status, Some(self.verdict(kind, &target, stats)));
            }
        }
        if now >= u64::from(trial.deadline) {
            // No verdict was reached, so nothing is learned.
            self.record.attempt = None;
            return (status, Some(LoadAction::Drop(DropReason::Deadline)));
        }
        if trial.hold_started.is_none() && now >= u64::from(reconfiguration.deadline) {
            if kind == TrialKind::Down {
                self.record_evidence(&target, LoadVerdict::Insufficient);
            }
            self.record.attempt = None;
            return (status, Some(LoadAction::Drop(DropReason::NotHydrated)));
        }
        (status, None)
    }

    fn verdict(&mut self, kind: TrialKind, target: &str, stats: &TrialStats) -> LoadAction {
        let tolerance = self.params.lag_tolerance;
        let from = self.input.size.to_string();
        let attempt = self.record.attempt.take();
        match kind {
            TrialKind::Up => {
                let behind = attempt.map(|a| a.behind).unwrap_or_default();
                let dataflows: BTreeMap<_, _> = behind
                    .iter()
                    .map(|id| {
                        let current = stats.current.get(id).copied();
                        let candidate = stats.candidate.get(id).copied();
                        let gained = match (&current, &candidate) {
                            (Some(cur), Some(cand)) => gain(cur, cand, tolerance),
                            _ => None,
                        };
                        let comparison = DataflowComparison {
                            current,
                            candidate,
                            gain: gained,
                        };
                        (*id, comparison)
                    })
                    .collect();
                let passed = dataflows.values().any(|d| d.gain.is_some());
                let verdict = Verdict {
                    kind,
                    passed,
                    dataflows,
                    candidate_heap: stats.candidate_heap,
                };
                if passed {
                    self.record_evidence(&from, LoadVerdict::Insufficient);
                    for (id, d) in &verdict.dataflows {
                        if let (None, Some(candidate)) = (d.gain, d.candidate) {
                            self.set_floor(*id, candidate.median_lag);
                        }
                    }
                    self.cut_over(LoadCutoverReason::UpTrial);
                    LoadAction::CutOver(verdict)
                } else {
                    self.record_evidence(&from, LoadVerdict::NoGain);
                    for (id, d) in &verdict.dataflows {
                        if let Some(current) = d.current {
                            self.set_floor(*id, current.median_lag);
                        }
                    }
                    LoadAction::Drop(DropReason::Failed(verdict))
                }
            }
            TrialKind::Down => {
                let ids: BTreeSet<_> = stats.current.keys().copied().collect();
                let dataflows: BTreeMap<_, _> = ids
                    .iter()
                    .map(|id| {
                        let current = stats.current.get(id).copied();
                        let candidate = stats.candidate.get(id).copied();
                        let fresh = match (&current, &candidate) {
                            (Some(cur), Some(cand)) => as_fresh(cur, cand, tolerance),
                            _ => false,
                        };
                        let comparison = DataflowComparison {
                            current,
                            candidate,
                            gain: fresh.then_some(Gain::Fresh),
                        };
                        (*id, comparison)
                    })
                    .collect();
                let heap_fits = stats.candidate_heap.is_some_and(|heap| {
                    let fraction = heap.fraction.unwrap_or_else(|| {
                        self.range
                            .iter()
                            .find(|size| size.name == target)
                            .map_or(f64::INFINITY, |size| heap_fraction(heap.bytes, size))
                    });
                    fraction < self.params.heap_max
                });
                let passed = heap_fits && dataflows.values().all(|d| d.gain.is_some());
                let verdict = Verdict {
                    kind,
                    passed,
                    dataflows,
                    candidate_heap: stats.candidate_heap,
                };
                if passed {
                    self.cut_over(LoadCutoverReason::DownTrial);
                    LoadAction::CutOver(verdict)
                } else {
                    self.record_evidence(target, LoadVerdict::Insufficient);
                    LoadAction::Drop(DropReason::Failed(verdict))
                }
            }
        }
    }
}

/// Why a down-trial to the next smaller size is not proposed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DownBlock {
    /// The quiet window did not fill, or has too few summaries.
    NotObserved,
    /// A dataflow was behind or a replica killed within it.
    Behind,
    /// The heap would not fit the candidate, or is unknown.
    Heap,
    /// The busy time would not fit the candidate's cores, or is missing from a summary.
    Busy,
    /// The quiet window shows the descent is worth trying, and evidence blocks it.
    Evidence,
}

impl DownBlock {
    pub fn as_str(&self) -> &'static str {
        match self {
            DownBlock::NotObserved => "not-observed",
            DownBlock::Behind => "behind",
            DownBlock::Heap => "heap",
            DownBlock::Busy => "busy",
            DownBlock::Evidence => "evidence",
        }
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn add(at: Timestamp, duration: Duration) -> Timestamp {
    Timestamp::new(u64::from(at).saturating_add(millis(duration)))
}

/// `bytes` as a fraction of the heap limit of `size`.
fn heap_fraction(bytes: u64, size: &SizeCapacity) -> f64 {
    if size.heap_limit == 0 {
        return f64::INFINITY;
    }
    f64::cast_lossy(bytes) / f64::cast_lossy(size.heap_limit)
}

/// The nearest-rank percentile `p` of `values`.
fn percentile(mut values: Vec<u64>, p: f64) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    values.sort();
    let rank = (p * f64::cast_lossy(values.len())).ceil();
    let index = usize::cast_lossy(rank)
        .saturating_sub(1)
        .min(values.len() - 1);
    Some(values[index])
}

#[cfg(test)]
mod tests;
