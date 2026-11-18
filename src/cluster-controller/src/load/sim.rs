// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A closed-loop simulator of the `ON LOAD` rules against synthetic replicas.
//!
//! Time is virtual and advances in sample intervals of 10s. At each interval every replica's
//! model advances, the cluster is sampled into a [`LoadSample`], and the real
//! [`summarize`](crate::load::summarize) closes an evaluation window when one ends. Once a
//! minute the [`Autoscaler`] ticks: it reads the durable state, the summaries, and the samples,
//! and returns the durable state after its writes and the sizes of the replicas the cluster
//! should run. The harness then creates and drops replicas to match, and checks the invariants.
//!
//! The [`Autoscaler`] is the seam between the cases and what decides. [`RulesAutoscaler`] is a
//! small trial executor around [`rules::evaluate`]. The real strategies can stand behind the
//! same seam, since it speaks in durable state and desired replicas.
//!
//! All randomness comes from a seeded [`Rng`], so every run is reproducible.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use mz_adapter_types::cluster_state::{
    AvailabilityZones, LoadCutoverReason, LoadRecord, LoadVerdict, OnLoadPolicy, OnTimeout,
    ReconfigurationOrigin, ReconfigurationRecord, ReconfigurationStatus, ReconfigurationTarget,
    ReconfigurationTrial, TrialKind,
};
use mz_compute_types::config::ComputeReplicaLogging;
use mz_controller_types::ReplicaId;
use mz_ore::cast::CastLossy;
use mz_repr::{GlobalId, Timestamp};

use crate::load::rules::{
    self, DropReason, LoadAction, LoadDecision, LoadStatus, RuleParams, RulesInput, SizeCapacity,
    TrialObservation, TrialStats, blocks,
};
use crate::load::{
    BusyWindow, DataflowFrontiers, LoadSample, LoadSummary, MemoryKill, ProcessHeap, ReplicaSample,
    SummaryParams, dataflow_stats, fingerprint, steady_state_heap, summarize, window_start,
};

pub const SECOND: u64 = 1000;
pub const MINUTE: u64 = 60 * SECOND;
pub const HOUR: u64 = 60 * MINUTE;
pub const GIB: f64 = 1073741824.0;
const SAMPLE: u64 = 10 * SECOND;
const TICK: u64 = MINUTE;
const BUSY_WINDOW: u64 = MINUTE;
/// The start of every run: far from the epoch, and aligned to the evaluation windows.
pub const T0: u64 = 1000 * HOUR;
/// The lag, in seconds, at which batching has halved the cost of an update.
const BATCHING_SCALE: f64 = 60.0;

/// A deterministic SplitMix64 generator.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    /// A float in `[0, 1)`.
    pub fn unit(&mut self) -> f64 {
        f64::cast_lossy(self.next_u64() >> 11) / f64::cast_lossy(1u64 << 53)
    }
}

/// A function of the hours since the start of the run.
pub type Curve = Box<dyn Fn(f64) -> f64>;

pub fn constant(value: f64) -> Curve {
    Box::new(move |_| value)
}

/// Lag noise: a sawtooth from `min` to `max` seconds over `period` seconds, rising as the
/// `shape`th power of its phase, so that its median is `min + (max - min) * 0.5^shape`.
#[derive(Clone, Copy, Debug)]
pub struct Sawtooth {
    pub min: f64,
    pub max: f64,
    pub period: f64,
    pub shape: f64,
    pub phase: f64,
}

impl Sawtooth {
    fn at(&self, seconds: f64) -> f64 {
        let x = ((seconds + self.phase) / self.period).fract();
        self.min + (self.max - self.min) * x.powf(self.shape)
    }
}

/// One dataflow of the workload.
pub struct DataflowModel {
    pub id: GlobalId,
    /// The work arriving per second, in cores of capacity at the cost of a fresh update.
    pub demand: Curve,
    /// The share of the dataflow's work that lands on one worker.
    pub skew: f64,
    /// `g`: the cost of an update falls as its lag grows, down to `1/g` of its fresh cost.
    pub batching_gain: f64,
    /// A lag, in seconds, the dataflow shows at any size.
    pub inherent_lag: f64,
    pub noise: Option<Sawtooth>,
    /// Hours after the start at which the dataflow is created. Created before the run when 0.
    pub created_at: f64,
    /// How long the dataflow takes to hydrate on a running replica. `None` for the replica's
    /// own hydration time.
    pub hydration: Option<Duration>,
}

impl DataflowModel {
    pub fn new(id: u64, demand: Curve) -> Self {
        DataflowModel {
            id: GlobalId::User(id),
            demand,
            skew: 0.0,
            batching_gain: 1.0,
            inherent_lag: 0.0,
            noise: None,
            created_at: 0.0,
            hydration: None,
        }
    }
}

/// Faults injected into a run.
#[derive(Clone, Debug, Default)]
pub struct Faults {
    /// The share of busy windows that never arrive.
    pub busy_loss: f64,
    /// Whether no busy window arrives from a replica on which a dataflow is behind.
    pub busy_loss_while_behind: bool,
    /// Every replica restarts, not for memory, at this interval after its creation.
    pub replica_restart_every: Option<Duration>,
    /// Hours after the start at which `environmentd` restarts.
    pub envd_restarts: Vec<f64>,
}

/// What the cluster runs.
pub struct Workload {
    pub dataflows: Vec<DataflowModel>,
    /// The parallel capacity of `n` cores is `n^alpha`.
    pub alpha: f64,
    /// The steady-state heap of a replica, in bytes, without its per-core overhead.
    pub heap: Curve,
    pub heap_per_core: f64,
    /// The heap while hydrating, as a multiple of the steady state.
    pub peak_multiple: f64,
    /// How long a replica of one core takes to hydrate. It shrinks with the square root of the
    /// cores.
    pub hydration_one_core: Duration,
    /// The share of a replica's capacity a hydrating dataflow takes from the others.
    pub hydration_share: f64,
    pub faults: Faults,
}

impl Workload {
    pub fn new(dataflows: Vec<DataflowModel>) -> Self {
        Workload {
            dataflows,
            alpha: 1.0,
            heap: constant(GIB),
            heap_per_core: 0.0,
            peak_multiple: 1.2,
            hydration_one_core: Duration::from_secs(8 * 60),
            hydration_share: 1.0,
            faults: Faults::default(),
        }
    }
}

/// `c1` to `c32`, doubling cores, with 8 GiB of heap per core.
pub fn ladder() -> Vec<SizeCapacity> {
    [1u64, 2, 4, 8, 16, 32]
        .into_iter()
        .map(|cores| SizeCapacity {
            name: format!("c{cores}"),
            cores: f64::cast_lossy(cores),
            memory_bytes: cores * (8 << 30),
            heap_limit: cores * (8 << 30),
        })
        .collect()
}

pub fn rule_params() -> RuleParams {
    RuleParams {
        evaluation_window: Duration::from_secs(600),
        lag_tolerance: Duration::from_secs(2),
        floor_factor: 1.5,
        min_progress_ratio: 0.25,
        up_trial_duration: Duration::from_secs(300),
        down_trial_duration: Duration::from_secs(900),
        heap_max: 0.8,
        heap_proposal_max: 0.7,
        heap_high_duration: Duration::from_secs(1200),
        cpu_margin: 0.8,
        default_scale_down_after: Duration::from_secs(3600),
        quiet_window_coverage: 0.9,
        no_gain_interval: Duration::from_secs(24 * 3600),
        max_evidence_interval: Duration::from_secs(7 * 24 * 3600),
    }
}

pub fn summary_params() -> SummaryParams {
    SummaryParams {
        window: Duration::from_secs(600),
        sample_interval: Duration::from_millis(SAMPLE),
        warm_up: Duration::from_secs(300),
        lag_tolerance: Duration::from_secs(2),
        floor_factor: 1.5,
    }
}

/// The durable state of the simulated cluster.
#[derive(Clone, Debug, PartialEq)]
pub struct DurableCluster {
    /// The realized size.
    pub size: String,
    pub reconfiguration: Option<ReconfigurationRecord>,
    pub load: Option<LoadRecord>,
}

/// A replica as the controller sees it.
#[derive(Clone, Debug, PartialEq)]
pub struct ReplicaView {
    pub id: ReplicaId,
    pub size: String,
    pub hydrated: bool,
}

/// What an [`Autoscaler`] reads at a tick.
pub struct TickInput<'a> {
    pub now: Timestamp,
    pub policy: &'a OnLoadPolicy,
    pub ladder: &'a [SizeCapacity],
    pub params: &'a RuleParams,
    pub durable: &'a DurableCluster,
    pub replicas: &'a [ReplicaView],
    /// The samples taken since the previous tick.
    pub samples: &'a [LoadSample],
    /// The memory kills since the previous tick.
    pub kills: &'a [MemoryKill],
    pub summaries: &'a [LoadSummary],
    pub fingerprint: u64,
    pub last_hydration: Option<Duration>,
    pub hydration_peak: Option<u64>,
}

/// What an [`Autoscaler`] decided at a tick.
pub struct TickOutput {
    /// The durable state after the tick's writes.
    pub durable: DurableCluster,
    /// The sizes of the replicas the cluster should run, one replica per entry.
    pub desired: Vec<String>,
    /// The rules' decision, when the autoscaler exposes one.
    pub decision: Option<LoadDecision>,
}

/// What decides the cluster's size and replicas.
pub trait Autoscaler {
    fn tick(&mut self, input: &TickInput) -> TickOutput;
    /// `environmentd` restarted, and every piece of in-memory state is gone.
    fn restart(&mut self);
}

/// A trial executor around [`rules::evaluate`]: it keeps a trial's candidate beside the current
/// replicas, runs the hold, gathers the hold's samples in memory, and applies each decision.
#[derive(Default)]
pub struct RulesAutoscaler {
    /// The `hold_started` the samples in `hold` belong to.
    hold_since: Option<Timestamp>,
    hold: Vec<LoadSample>,
    candidate_killed: bool,
}

impl Autoscaler for RulesAutoscaler {
    fn restart(&mut self) {
        *self = RulesAutoscaler::default();
    }

    fn tick(&mut self, input: &TickInput) -> TickOutput {
        let mut durable = input.durable.clone();
        let now = input.now;
        let in_progress = durable
            .reconfiguration
            .clone()
            .filter(|r| r.is_in_progress());

        let mut observation = None;
        if let Some(record) = &in_progress {
            if let Some(trial) = &record.trial {
                let target = &record.target.size;
                self.candidate_killed |= input.kills.iter().any(|kill| &kill.size == target);
                let candidates: Vec<_> = input
                    .replicas
                    .iter()
                    .filter(|r| &r.size == target)
                    .collect();
                let hydrated = !candidates.is_empty() && candidates.iter().all(|r| r.hydrated);
                let can_hold = u64::from(now) + millis(trial.hold_for) <= u64::from(trial.deadline);
                // A hold whose samples were lost to a restart starts over.
                let running = trial.hold_started.is_some() && trial.hold_started == self.hold_since;
                if hydrated && !running && can_hold {
                    let mut written = record.clone();
                    written.trial.as_mut().expect("trial").hold_started = Some(now);
                    durable.reconfiguration = Some(written);
                    self.hold_since = Some(now);
                    self.hold.clear();
                } else if running {
                    let since = u64::from(self.hold_since.expect("running"));
                    self.hold.extend(
                        input
                            .samples
                            .iter()
                            .filter(|s| u64::from(s.at) >= since)
                            .cloned(),
                    );
                }
                let trial = durable
                    .reconfiguration
                    .as_ref()
                    .and_then(|r| r.trial.clone())
                    .expect("trial");
                let stats = match (trial.hold_started, self.hold_since) {
                    (Some(started), Some(since))
                        if started == since
                            && u64::from(now) >= u64::from(started) + millis(trial.hold_for) =>
                    {
                        Some(self.trial_stats(&input.durable.size, target, started, now))
                    }
                    _ => None,
                };
                observation = Some(TrialObservation {
                    candidate_killed: self.candidate_killed,
                    stats,
                });
            }
        }

        let rules_input = RulesInput {
            policy: input.policy,
            ladder: input.ladder,
            size: &durable.size,
            reconfiguration: durable.reconfiguration.as_ref(),
            record: durable.load.as_ref(),
            fingerprint: input.fingerprint,
            summaries: input.summaries,
            trial: observation.as_ref(),
            last_hydration: input.last_hydration,
            hydration_peak: input.hydration_peak,
            now,
        };
        let decision = rules::evaluate(&rules_input, input.params);

        if let Some(decision) = &decision {
            if let Some(record) = &decision.record {
                durable.load = Some(record.clone());
            }
            match &decision.action {
                Some(LoadAction::StartTrial(start)) => {
                    durable.reconfiguration = Some(ReconfigurationRecord {
                        target: target(&start.target),
                        deadline: start.hydration_deadline,
                        on_timeout: OnTimeout::Rollback,
                        status: ReconfigurationStatus::InProgress,
                        origin: ReconfigurationOrigin::LoadAutoscaling,
                        trial: Some(ReconfigurationTrial {
                            kind: start.kind,
                            hold_for: start.hold_for,
                            deadline: start.deadline,
                            hold_started: None,
                        }),
                    });
                    self.restart();
                }
                Some(LoadAction::Reconfigure {
                    target: size,
                    hydration_deadline,
                    ..
                }) => {
                    durable.reconfiguration = Some(ReconfigurationRecord {
                        target: target(size),
                        deadline: *hydration_deadline,
                        on_timeout: OnTimeout::Rollback,
                        status: ReconfigurationStatus::InProgress,
                        origin: ReconfigurationOrigin::LoadAutoscaling,
                        trial: None,
                    });
                    self.restart();
                }
                Some(LoadAction::CutOver(_)) => {
                    let record = durable.reconfiguration.as_mut().expect("a trial");
                    record.status = ReconfigurationStatus::Finalized;
                    durable.size = record.target.size.clone();
                    self.restart();
                }
                Some(LoadAction::Drop(_)) => {
                    let record = durable.reconfiguration.as_mut().expect("a trial");
                    record.status = ReconfigurationStatus::TimedOut;
                    self.restart();
                }
                None => {}
            }
        }

        // A reconfiguration without a trial cuts over once its target is hydrated, and rolls
        // back at its deadline.
        if let Some(record) = durable
            .reconfiguration
            .as_mut()
            .filter(|r| r.is_in_progress() && r.trial.is_none())
        {
            let targets: Vec<_> = input
                .replicas
                .iter()
                .filter(|r| r.size == record.target.size)
                .collect();
            if !targets.is_empty() && targets.iter().all(|r| r.hydrated) {
                record.status = ReconfigurationStatus::Finalized;
                durable.size = record.target.size.clone();
            } else if now >= record.deadline {
                record.status = ReconfigurationStatus::TimedOut;
            }
        }

        let mut desired = vec![durable.size.clone()];
        if let Some(record) = durable
            .reconfiguration
            .as_ref()
            .filter(|r| r.is_in_progress())
        {
            if record.target.size != durable.size {
                desired.push(record.target.size.clone());
            }
        }
        TickOutput {
            durable,
            desired,
            decision,
        }
    }
}

impl RulesAutoscaler {
    fn trial_stats(
        &self,
        current: &str,
        candidate: &str,
        start: Timestamp,
        end: Timestamp,
    ) -> TrialStats {
        let ids = |size: &str| -> BTreeSet<ReplicaId> {
            self.hold
                .iter()
                .flat_map(|s| s.replicas.iter())
                .filter(|(_, r)| r.size == size)
                .map(|(id, _)| *id)
                .collect()
        };
        let (current, candidate) = (ids(current), ids(candidate));
        TrialStats {
            current: dataflow_stats(&self.hold, &current, start, end),
            candidate: dataflow_stats(&self.hold, &candidate, start, end),
            candidate_heap: steady_state_heap(&self.hold, &candidate),
        }
    }
}

fn target(size: &str) -> ReconfigurationTarget {
    ReconfigurationTarget {
        size: size.to_string(),
        replication_factor: 1,
        availability_zones: AvailabilityZones::default(),
        logging: ComputeReplicaLogging::default(),
        arrangement_compression: false,
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).expect("fits")
}

/// One simulated replica.
struct SimReplica {
    id: ReplicaId,
    /// Its index on the ladder.
    size: usize,
    created_at: u64,
    /// Per dataflow, when it is hydrated on this replica.
    ready_at: BTreeMap<GlobalId, u64>,
    /// Per dataflow, the backlog in seconds of input, apart from inherent lag and noise.
    backlog: BTreeMap<GlobalId, f64>,
    /// Busy core-seconds and the busiest worker's busy seconds in the open busy window.
    busy: (f64, f64),
    /// When its current or most recent hydration began.
    hydration_began: u64,
    /// The end of its most recent hydration, and how long it took.
    last_hydration: Option<(u64, Duration)>,
}

impl SimReplica {
    fn hydrated(&self, now: u64) -> bool {
        self.ready_at.values().all(|ready| *ready <= now)
    }
}

/// The in-memory load signal of `environmentd`, as the coordinator keeps it.
#[derive(Default)]
struct Signal {
    open: Vec<LoadSample>,
    /// Per replica: when it became eligible, whether it hydrated since its creation, and the
    /// largest heap before that.
    trackers: BTreeMap<ReplicaId, (Option<Timestamp>, bool, Option<u64>)>,
    busy: Vec<BusyWindow>,
    kills: Vec<MemoryKill>,
}

impl Signal {
    fn record(
        &mut self,
        mut sample: LoadSample,
        record: Option<&LoadRecord>,
        params: &SummaryParams,
    ) -> Option<LoadSummary> {
        let at = sample.at;
        for (id, replica) in &mut sample.replicas {
            let tracker = self.trackers.entry(*id).or_default();
            if replica.size != sample.size {
                tracker.0 = None;
            } else if replica.hydrated && tracker.0.is_none() {
                tracker.0 = Some(at);
            }
            if !tracker.1 {
                if replica.hydrated {
                    tracker.1 = true;
                } else if let Some(heap) = replica.heap {
                    tracker.2 = Some(tracker.2.unwrap_or(0).max(heap.bytes));
                }
            }
            replica.eligible_since = tracker.0;
        }
        let current = window_start(at, params.window);
        let mut summary = None;
        if let Some(first) = self.open.first() {
            let start = window_start(first.at, params.window);
            if start < current {
                let end = Timestamp::new(u64::from(start) + millis(params.window));
                summary = summarize(
                    start,
                    end,
                    &self.open,
                    &self.busy,
                    &self.kills,
                    record,
                    params,
                );
                self.open.clear();
                self.busy.retain(|w| w.end > current);
                self.kills.retain(|k| k.at >= current);
            }
        }
        self.open.push(sample);
        summary
    }
}

/// A case's setup.
pub struct Scenario {
    pub workload: Workload,
    pub start: &'static str,
    pub min: &'static str,
    pub max: &'static str,
    pub scale_down_after: Option<Duration>,
    pub hours: f64,
    pub seed: u64,
}

impl Scenario {
    pub fn new(workload: Workload, start: &'static str, hours: f64) -> Self {
        Scenario {
            workload,
            start,
            min: "c1",
            max: "c32",
            scale_down_after: None,
            hours,
            seed: 1,
        }
    }
}

/// Something that happened in a run.
#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    TrialStarted {
        at: u64,
        kind: TrialKind,
        from: String,
        target: String,
    },
    Verdict {
        at: u64,
        kind: TrialKind,
        passed: bool,
        target: String,
    },
    Dropped {
        at: u64,
        reason: String,
    },
    Reconfigured {
        at: u64,
        target: String,
        reason: LoadCutoverReason,
    },
    SizeChanged {
        at: u64,
        from: String,
        to: String,
    },
    Killed {
        at: u64,
        size: String,
    },
}

/// The outcome of a run.
pub struct Run {
    pub events: Vec<Event>,
    /// The status at each tick.
    pub statuses: Vec<(u64, LoadStatus)>,
    pub summaries: Vec<LoadSummary>,
    pub durable: DurableCluster,
}

impl Run {
    pub fn trials(&self) -> Vec<(u64, TrialKind, String, String)> {
        self.events
            .iter()
            .filter_map(|e| match e {
                Event::TrialStarted {
                    at,
                    kind,
                    from,
                    target,
                } => Some((*at, *kind, from.clone(), target.clone())),
                _ => None,
            })
            .collect()
    }

    pub fn trials_of(&self, kind: TrialKind) -> usize {
        self.trials().iter().filter(|t| t.1 == kind).count()
    }

    pub fn size_changes(&self) -> Vec<(u64, String, String)> {
        self.events
            .iter()
            .filter_map(|e| match e {
                Event::SizeChanged { at, from, to } => Some((*at, from.clone(), to.clone())),
                _ => None,
            })
            .collect()
    }

    pub fn kills(&self) -> usize {
        self.events
            .iter()
            .filter(|e| matches!(e, Event::Killed { .. }))
            .count()
    }

    pub fn final_status(&self) -> LoadStatus {
        self.statuses.last().expect("a tick").1
    }
}

/// Runs a scenario with [`RulesAutoscaler`].
pub fn run(scenario: Scenario) -> Run {
    Sim::new(scenario, Box::new(RulesAutoscaler::default())).run()
}

struct Sim {
    scenario: Scenario,
    autoscaler: Box<dyn Autoscaler>,
    ladder: Vec<SizeCapacity>,
    policy: OnLoadPolicy,
    params: RuleParams,
    summary_params: SummaryParams,
    rng: Rng,
    now: u64,
    durable: DurableCluster,
    replicas: Vec<SimReplica>,
    next_replica: u64,
    signal: Signal,
    summaries: Vec<LoadSummary>,
    /// Samples and kills since the last tick.
    pending_samples: Vec<LoadSample>,
    pending_kills: Vec<MemoryKill>,
    events: Vec<Event>,
    statuses: Vec<(u64, LoadStatus)>,
    /// When the cluster last moved up from each size.
    scaled_up_from: BTreeMap<String, u64>,
    /// The verdicts reached by each trial, by its start.
    verdicts: BTreeMap<u64, usize>,
    trial_started: Option<u64>,
}

impl Sim {
    fn new(scenario: Scenario, autoscaler: Box<dyn Autoscaler>) -> Self {
        let ladder = ladder();
        let policy = OnLoadPolicy {
            min_size: scenario.min.into(),
            max_size: scenario.max.into(),
            scale_down_after: scenario.scale_down_after,
        };
        let rng = Rng::new(scenario.seed);
        let mut sim = Sim {
            durable: DurableCluster {
                size: scenario.start.into(),
                reconfiguration: None,
                load: None,
            },
            scenario,
            autoscaler,
            ladder,
            policy,
            params: rule_params(),
            summary_params: summary_params(),
            rng,
            now: T0,
            replicas: Vec::new(),
            next_replica: 1,
            signal: Signal::default(),
            summaries: Vec::new(),
            pending_samples: Vec::new(),
            pending_kills: Vec::new(),
            events: Vec::new(),
            statuses: Vec::new(),
            scaled_up_from: BTreeMap::new(),
            verdicts: BTreeMap::new(),
            trial_started: None,
        };
        // The initial replica was hydrated long before the run.
        let size = sim.index(sim.scenario.start);
        let mut replica = sim.new_replica(size, T0);
        for ready in replica.ready_at.values_mut() {
            *ready = T0 - HOUR;
        }
        replica.created_at = T0 - 2 * HOUR;
        replica.last_hydration = None;
        sim.replicas.push(replica);
        sim
    }

    fn index(&self, size: &str) -> usize {
        self.ladder
            .iter()
            .position(|s| s.name == size)
            .expect("on the ladder")
    }

    fn hours(&self, at: u64) -> f64 {
        f64::cast_lossy(at.saturating_sub(T0)) / f64::cast_lossy(HOUR)
    }

    fn created(&self, dataflow: &DataflowModel, at: u64) -> bool {
        self.hours(at) >= dataflow.created_at
    }

    fn hydration_time(&self, size: usize) -> Duration {
        let cores = self.ladder[size].cores;
        Duration::from_secs_f64(
            self.scenario.workload.hydration_one_core.as_secs_f64() / cores.sqrt(),
        )
    }

    fn new_replica(&mut self, size: usize, now: u64) -> SimReplica {
        let id = ReplicaId::User(self.next_replica);
        self.next_replica += 1;
        let hydration = millis(self.hydration_time(size));
        let ready_at = self
            .scenario
            .workload
            .dataflows
            .iter()
            .filter(|d| self.created(d, now))
            .map(|d| (d.id, now + hydration))
            .collect();
        SimReplica {
            id,
            size,
            created_at: now,
            ready_at,
            backlog: BTreeMap::new(),
            busy: (0.0, 0.0),
            hydration_began: now,
            last_hydration: None,
        }
    }

    fn steady_heap(&self, size: usize, at: u64) -> f64 {
        let workload = &self.scenario.workload;
        (workload.heap)(self.hours(at)) + workload.heap_per_core * self.ladder[size].cores
    }

    fn heap(&self, replica: &SimReplica, at: u64) -> f64 {
        let steady = self.steady_heap(replica.size, at);
        if replica.hydrated(at) {
            steady
        } else {
            steady * self.scenario.workload.peak_multiple
        }
    }

    /// The lag a replica shows for a dataflow, in seconds.
    fn observed_lag(&self, replica: &SimReplica, dataflow: &DataflowModel, at: u64) -> f64 {
        let backlog = replica.backlog.get(&dataflow.id).copied().unwrap_or(0.0);
        let noise = dataflow
            .noise
            .map_or(0.0, |n| n.at(f64::cast_lossy(at) / 1000.0));
        backlog + dataflow.inherent_lag + noise
    }

    fn run(mut self) -> Run {
        let end = T0 + u64::cast_lossy(self.scenario.hours * f64::cast_lossy(HOUR));
        let mut envd_restarts: Vec<u64> = self
            .scenario
            .workload
            .faults
            .envd_restarts
            .iter()
            .map(|h| T0 + u64::cast_lossy(h * f64::cast_lossy(HOUR)))
            .collect();
        envd_restarts.sort();
        while self.now < end {
            self.now += SAMPLE;
            if envd_restarts.first().is_some_and(|at| *at <= self.now) {
                envd_restarts.remove(0);
                self.signal = Signal::default();
                self.pending_samples.clear();
                self.pending_kills.clear();
                self.autoscaler.restart();
            }
            self.new_objects();
            self.advance();
            self.memory_and_restarts();
            if self.now % BUSY_WINDOW == 0 {
                self.close_busy_windows();
            }
            self.sample();
            if self.now % TICK == 0 {
                self.tick();
            }
            self.check_served_lag();
        }
        Run {
            events: self.events,
            statuses: self.statuses,
            summaries: self.summaries,
            durable: self.durable,
        }
    }

    /// Starts the hydration of dataflows created in the last interval on the running replicas.
    fn new_objects(&mut self) {
        let now = self.now;
        let created: Vec<_> = self
            .scenario
            .workload
            .dataflows
            .iter()
            .filter(|d| self.created(d, now) && !self.created(d, now - SAMPLE))
            .map(|d| (d.id, d.hydration))
            .collect();
        for (id, hydration) in created {
            for i in 0..self.replicas.len() {
                let duration =
                    hydration.unwrap_or_else(|| self.hydration_time(self.replicas[i].size));
                let replica = &mut self.replicas[i];
                if replica.hydrated(now) {
                    replica.hydration_began = now;
                }
                replica.ready_at.insert(id, now + millis(duration));
            }
        }
    }

    /// Advances every replica's backlog and busy time over one sample interval.
    fn advance(&mut self) {
        let dt = f64::cast_lossy(SAMPLE) / 1000.0;
        let now = self.now;
        let hours = self.hours(now);
        let workload = &self.scenario.workload;
        for replica in &mut self.replicas {
            let cores = self.ladder[replica.size].cores;
            if !replica.hydrated(now) && replica.ready_at.values().all(|r| *r > now) {
                // Hydrating from scratch: nothing to serve yet.
                replica.busy.0 += cores * dt;
                replica.busy.1 += dt;
                continue;
            }
            let hydrating = replica.ready_at.values().any(|r| *r > now);
            let capacity = cores.powf(workload.alpha)
                * if hydrating {
                    1.0 - workload.hydration_share
                } else {
                    1.0
                };
            let active: Vec<_> = workload
                .dataflows
                .iter()
                .filter(|d| replica.ready_at.get(&d.id).is_some_and(|r| *r <= now))
                .collect();
            let needs: Vec<f64> = active
                .iter()
                .map(|d| {
                    let backlog = replica.backlog.get(&d.id).copied().unwrap_or(0.0);
                    let cost = (1.0 / (1.0 + backlog / BATCHING_SCALE)).max(1.0 / d.batching_gain);
                    (d.demand)(hours) * cost
                })
                .collect();
            let total: f64 = needs.iter().sum();
            let mut used = 0.0;
            let mut hot_worker: f64 = 0.0;
            for (i, d) in active.iter().enumerate() {
                let need = &needs[i];
                let backlog = replica.backlog.entry(d.id).or_insert(0.0);
                if *need <= 0.0 {
                    *backlog = 0.0;
                    continue;
                }
                let mut allot = if total > 0.0 {
                    capacity * need / total
                } else {
                    capacity
                };
                if d.skew > 0.0 {
                    allot = allot.min(1.0 / d.skew);
                }
                let rate = allot / need;
                let consumed = if *backlog <= 0.0 && rate >= 1.0 {
                    *need
                } else {
                    allot
                };
                *backlog = (*backlog + dt * (1.0 - rate)).max(0.0);
                used += consumed;
                hot_worker = hot_worker.max(d.skew * consumed);
            }
            // Busy real cores: capacity units cost `cores / capacity` real cores each.
            let full = cores.powf(workload.alpha);
            let busy_cores = if hydrating {
                cores
            } else {
                (used * cores / full).min(cores)
            };
            let busiest = (busy_cores / cores).max(hot_worker).min(1.0);
            replica.busy.0 += busy_cores * dt;
            replica.busy.1 += busiest * dt;
        }
        for replica in &mut self.replicas {
            let last = replica.ready_at.values().max().copied().unwrap_or(0);
            if last > now - SAMPLE && last <= now {
                let duration = Duration::from_millis(last - replica.hydration_began);
                replica.last_hydration = Some((last, duration));
            }
        }
    }

    fn memory_and_restarts(&mut self) {
        let now = self.now;
        let restart_every = self
            .scenario
            .workload
            .faults
            .replica_restart_every
            .map(millis);
        let mut restarted = Vec::new();
        let mut kills = Vec::new();
        for (i, replica) in self.replicas.iter().enumerate() {
            let limit = f64::cast_lossy(self.ladder[replica.size].heap_limit);
            let killed = self.heap(replica, now) > limit;
            let restart = restart_every.is_some_and(|every| now >= replica.created_at + every);
            if killed {
                kills.push(MemoryKill {
                    replica_id: replica.id,
                    size: self.ladder[replica.size].name.clone(),
                    at: Timestamp::new(now),
                });
            }
            if killed || restart {
                restarted.push(i);
            }
        }
        for kill in kills {
            self.events.push(Event::Killed {
                at: now,
                size: kill.size.clone(),
            });
            self.signal.kills.push(kill.clone());
            self.pending_kills.push(kill);
        }
        for i in restarted {
            let size = self.replicas[i].size;
            let id = self.replicas[i].id;
            let mut fresh = self.new_replica(size, now);
            fresh.id = id;
            self.next_replica -= 1;
            self.replicas[i] = fresh;
            self.signal.trackers.remove(&id);
        }
    }

    fn close_busy_windows(&mut self) {
        let now = self.now;
        let faults = self.scenario.workload.faults.clone();
        let tolerance = self.params.lag_tolerance.as_secs_f64();
        for i in 0..self.replicas.len() {
            let (busy, busiest) = std::mem::take(&mut self.replicas[i].busy);
            let replica = &self.replicas[i];
            if replica.created_at > now - BUSY_WINDOW {
                continue;
            }
            let behind = self
                .scenario
                .workload
                .dataflows
                .iter()
                .filter(|d| replica.ready_at.get(&d.id).is_some_and(|r| *r <= now))
                .any(|d| self.observed_lag(replica, d, now) > tolerance);
            let (id, size) = (replica.id, replica.size);
            let lost =
                self.rng.unit() < faults.busy_loss || (faults.busy_loss_while_behind && behind);
            if lost {
                continue;
            }
            let replica_id = id;
            let cores = self.ladder[size].cores;
            self.signal.busy.push(BusyWindow {
                replica_id,
                start: Timestamp::new(now - BUSY_WINDOW),
                end: Timestamp::new(now),
                workers: u64::cast_lossy(cores.ceil()),
                busy: Duration::from_secs_f64(busy),
                max_worker_busy: Duration::from_secs_f64(busiest),
            });
        }
    }

    fn exports(&self) -> BTreeSet<GlobalId> {
        self.scenario
            .workload
            .dataflows
            .iter()
            .filter(|d| self.created(d, self.now))
            .map(|d| d.id)
            .collect()
    }

    fn sample(&mut self) {
        let now = self.now;
        let replicas = self
            .replicas
            .iter()
            .map(|replica| {
                let dataflows = self
                    .scenario
                    .workload
                    .dataflows
                    .iter()
                    .filter(|d| replica.ready_at.get(&d.id).is_some_and(|r| *r <= now))
                    .map(|d| {
                        let lag = u64::cast_lossy(self.observed_lag(replica, d, now) * 1000.0);
                        let frontiers = DataflowFrontiers {
                            input: Timestamp::new(now),
                            output: Timestamp::new(now.saturating_sub(lag)),
                        };
                        (d.id, frontiers)
                    })
                    .collect();
                let sample = ReplicaSample {
                    size: self.ladder[replica.size].name.clone(),
                    hydrated: replica.hydrated(now),
                    eligible_since: None,
                    dataflows,
                    heap: Some(ProcessHeap {
                        bytes: u64::cast_lossy(self.heap(replica, now)),
                        limit: Some(self.ladder[replica.size].heap_limit),
                    }),
                };
                (replica.id, sample)
            })
            .collect();
        let sample = LoadSample {
            at: Timestamp::new(now),
            size: self.durable.size.clone(),
            exports: self.exports(),
            replicas,
        };
        self.pending_samples.push(sample.clone());
        if let Some(summary) =
            self.signal
                .record(sample, self.durable.load.as_ref(), &self.summary_params)
        {
            self.summaries.push(summary);
        }
    }

    fn tick(&mut self) {
        let now = self.now;
        let views: Vec<_> = self
            .replicas
            .iter()
            .map(|r| ReplicaView {
                id: r.id,
                size: self.ladder[r.size].name.clone(),
                hydrated: r.hydrated(now),
            })
            .collect();
        let realized = self.index(&self.durable.size);
        let current: Vec<_> = self
            .replicas
            .iter()
            .filter(|r| r.size == realized)
            .collect();
        let last_hydration = current
            .iter()
            .filter_map(|r| r.last_hydration)
            .max_by_key(|(at, _)| *at)
            .map(|(_, d)| d);
        let hydration_peak = current
            .iter()
            .filter_map(|r| self.signal.trackers.get(&r.id).and_then(|t| t.2))
            .max();
        let fingerprint = fingerprint(&self.exports());
        let samples = std::mem::take(&mut self.pending_samples);
        let kills = std::mem::take(&mut self.pending_kills);
        let input = TickInput {
            now: Timestamp::new(now),
            policy: &self.policy,
            ladder: &self.ladder,
            params: &self.params,
            durable: &self.durable,
            replicas: &views,
            samples: &samples,
            kills: &kills,
            summaries: &self.summaries,
            fingerprint,
            last_hydration,
            hydration_peak,
        };
        let output = self.autoscaler.tick(&input);
        if let Some(decision) = &output.decision {
            self.statuses.push((now, decision.status));
            self.check_decision(decision);
        }
        self.record_events(&output);
        self.durable = output.durable;
        self.reconcile(&output.desired);
        self.check_state();
    }

    fn record_events(&mut self, output: &TickOutput) {
        let now = self.now;
        if let Some(decision) = &output.decision {
            match &decision.action {
                Some(LoadAction::StartTrial(start)) => {
                    self.trial_started = Some(now);
                    self.events.push(Event::TrialStarted {
                        at: now,
                        kind: start.kind,
                        from: self.durable.size.clone(),
                        target: start.target.clone(),
                    });
                }
                Some(LoadAction::Reconfigure { target, reason, .. }) => {
                    self.events.push(Event::Reconfigured {
                        at: now,
                        target: target.clone(),
                        reason: *reason,
                    });
                }
                Some(LoadAction::CutOver(verdict))
                | Some(LoadAction::Drop(DropReason::Failed(verdict))) => {
                    let started = self.trial_started.expect("a trial");
                    let count = self.verdicts.entry(started).or_default();
                    *count += 1;
                    assert_eq!(*count, 1, "a trial reached a second verdict at {now}");
                    let target = self
                        .durable
                        .reconfiguration
                        .as_ref()
                        .map(|r| r.target.size.clone())
                        .unwrap_or_default();
                    self.events.push(Event::Verdict {
                        at: now,
                        kind: verdict.kind,
                        passed: verdict.passed,
                        target,
                    });
                }
                Some(LoadAction::Drop(reason)) => {
                    self.events.push(Event::Dropped {
                        at: now,
                        reason: format!("{reason:?}"),
                    });
                }
                None => {}
            }
        }
        if output.durable.size != self.durable.size {
            let (from, to) = (self.durable.size.clone(), output.durable.size.clone());
            let (from_index, to_index) = (self.index(&from), self.index(&to));
            if to_index > from_index {
                self.scaled_up_from.insert(from.clone(), now);
            } else if let Some(up) = self.scaled_up_from.get(&to) {
                let w = millis(
                    self.policy
                        .scale_down_after
                        .unwrap_or(self.params.default_scale_down_after),
                );
                assert!(
                    now >= up + w,
                    "scaled down to {to} at {now} within W of scaling up from it at {up}"
                );
            }
            self.events.push(Event::SizeChanged { at: now, from, to });
        }
    }

    /// Creates and drops replicas so that the cluster runs one replica per desired size.
    fn reconcile(&mut self, desired: &[String]) {
        let now = self.now;
        let desired: BTreeSet<usize> = desired.iter().map(|s| self.index(s)).collect();
        self.replicas.retain(|r| desired.contains(&r.size));
        let present: BTreeSet<usize> = self.replicas.iter().map(|r| r.size).collect();
        for size in desired {
            if !present.contains(&size) {
                let replica = self.new_replica(size, now);
                self.replicas.push(replica);
            }
        }
    }

    /// The invariants on a decision, against the state it was made from.
    fn check_decision(&self, decision: &LoadDecision) {
        let now = Timestamp::new(self.now);
        let record = self.durable.load.as_ref();
        let latest_heap = self
            .summaries
            .iter()
            .rev()
            .filter(|s| s.size == self.durable.size)
            .find_map(|s| s.heap)
            .map(|h| f64::cast_lossy(h.bytes));
        let check_heap = |target: &str| {
            if let Some(bytes) = latest_heap {
                let limit = f64::cast_lossy(self.ladder[self.index(target)].heap_limit);
                assert!(
                    bytes / limit <= self.params.heap_max,
                    "selected {target} at a projected heap of {} at {now}",
                    bytes / limit
                );
            }
        };
        match &decision.action {
            Some(LoadAction::StartTrial(start)) => {
                check_heap(&start.target);
                let blocked = record.is_some_and(|record| {
                    record.evidence.iter().any(|e| {
                        blocks(e, now)
                            && match start.kind {
                                TrialKind::Up => {
                                    e.verdict == LoadVerdict::NoGain && e.size == self.durable.size
                                }
                                TrialKind::Down => {
                                    e.verdict == LoadVerdict::Insufficient
                                        && self.index(&e.size) >= self.index(&start.target)
                                }
                            }
                    })
                });
                assert!(
                    !blocked,
                    "a trial started while evidence blocks it at {now}"
                );
            }
            Some(LoadAction::Reconfigure { target, .. }) => check_heap(target),
            Some(LoadAction::CutOver(verdict)) if verdict.kind == TrialKind::Down => {
                let heap = verdict
                    .candidate_heap
                    .and_then(|h| h.fraction)
                    .unwrap_or(1.0);
                assert!(
                    heap < self.params.heap_max,
                    "a down-trial passed with heap {heap} at {now}"
                );
            }
            _ => {}
        }
    }

    /// The invariants on the state after a tick.
    fn check_state(&self) {
        let now = self.now;
        let (min, max) = (
            self.index(&self.policy.min_size),
            self.index(&self.policy.max_size),
        );
        let size = self.index(&self.durable.size);
        assert!(
            min <= size && size <= max,
            "size {size} out of range at {now}"
        );
        let in_flight = self
            .durable
            .reconfiguration
            .as_ref()
            .filter(|r| r.is_in_progress());
        for replica in &self.replicas {
            if replica.size == size {
                continue;
            }
            let record = in_flight.expect("a candidate belongs to a reconfiguration");
            assert_eq!(self.ladder[replica.size].name, record.target.size);
            if let Some(trial) = &record.trial {
                assert!(
                    now < u64::from(trial.deadline),
                    "a candidate outlived its trial's deadline at {now}"
                );
            }
        }
    }

    /// The lag served to queries, from the freshest replica, is never worse than the realized
    /// size's replicas alone would serve: a candidate only adds a replica that may answer.
    fn check_served_lag(&self) {
        let now = self.now;
        let size = self.index(&self.durable.size);
        for d in &self.scenario.workload.dataflows {
            let lag = |filter: &dyn Fn(&SimReplica) -> bool| {
                self.replicas
                    .iter()
                    .filter(|r| filter(r) && r.ready_at.get(&d.id).is_some_and(|t| *t <= now))
                    .map(|r| self.observed_lag(r, d, now))
                    .fold(f64::INFINITY, f64::min)
            };
            let served = lag(&|_| true);
            let current = lag(&|r| r.size == size);
            assert!(
                served <= current,
                "a trial worsened the lag of {} at {now}",
                d.id
            );
        }
    }
}

#[cfg(test)]
mod cases;
