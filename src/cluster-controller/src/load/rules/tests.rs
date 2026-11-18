// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::time::Duration;

use mz_adapter_types::cluster_state::{
    AvailabilityZones, LoadAttempt, LoadCutover, LoadCutoverReason, LoadEvidence, LoadFloor,
    LoadRecord, LoadVerdict, OnLoadPolicy, OnTimeout, ReconfigurationOrigin, ReconfigurationRecord,
    ReconfigurationStatus, ReconfigurationTarget, ReconfigurationTrial, TrialKind,
};
use mz_compute_types::config::ComputeReplicaLogging;
use mz_repr::{GlobalId, Timestamp};

use super::*;
use crate::load::{BusySummary, DataflowStats, DataflowSummary, HeapSummary, LoadSummary};

const MINUTE: u64 = 60_000;
const HOUR: u64 = 60 * MINUTE;
const GIB: u64 = 1 << 30;
const D1: GlobalId = GlobalId::User(1);
const D2: GlobalId = GlobalId::User(2);
const FP: u64 = 7;
/// A time aligned to the evaluation windows, far from the epoch.
const NOW: u64 = 1000 * HOUR;

fn ts(ms: u64) -> Timestamp {
    Timestamp::new(ms)
}

fn secs(s: f64) -> Duration {
    Duration::from_secs_f64(s)
}

/// `c1` to `c32`: cores double, with 8 GiB of heap per core.
fn ladder() -> Vec<SizeCapacity> {
    [1u64, 2, 4, 8, 16, 32]
        .into_iter()
        .map(|cores| SizeCapacity {
            name: format!("c{cores}"),
            cores: f64::cast_lossy(cores),
            memory_bytes: cores * 8 * GIB,
            heap_limit: cores * 8 * GIB,
        })
        .collect()
}

fn policy() -> OnLoadPolicy {
    OnLoadPolicy {
        min_size: "c1".into(),
        max_size: "c32".into(),
        scale_down_after: None,
    }
}

fn params() -> RuleParams {
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

fn empty_record() -> LoadRecord {
    LoadRecord {
        fingerprint: FP,
        attempt: None,
        last_cutover: None,
        evidence: Vec::new(),
        floors: Vec::new(),
        hydration_peak_ratio: None,
    }
}

fn evidence(size: &str, verdict: LoadVerdict, at: u64, interval: Duration) -> LoadEvidence {
    LoadEvidence {
        size: size.into(),
        verdict,
        at: ts(at),
        interval,
    }
}

/// A quiet summary of the window ending at `end` at `size`: D1 at 1s, the heap at `heap_gib`,
/// and a busy time of `busy_cores`.
fn quiet(size: &str, end: u64, heap_gib: f64, busy_cores: f64) -> LoadSummary {
    let limit = ladder().iter().find(|s| s.name == size).unwrap().heap_limit;
    let bytes = u64::cast_lossy(heap_gib * f64::cast_lossy(GIB));
    LoadSummary {
        start: ts(end - 10 * MINUTE),
        at: ts(end),
        size: size.into(),
        fingerprint: FP,
        dataflows: BTreeMap::from([(
            D1,
            DataflowSummary {
                median_lag: secs(1.0),
                progress_ratio: Some(1.0),
                behind: false,
            },
        )]),
        heap: Some(HeapSummary {
            bytes,
            fraction: Some(f64::cast_lossy(bytes) / f64::cast_lossy(limit)),
        }),
        busy: Some(BusySummary {
            mean_millicores: u64::cast_lossy(busy_cores * 1000.0),
            busiest_worker_utilization: 0.1,
        }),
        killed: false,
        hydrating: false,
    }
}

/// `quiet` with D1 behind at `lag` and `ratio`.
fn behind(size: &str, end: u64, lag: f64, ratio: f64) -> LoadSummary {
    let mut summary = quiet(size, end, 1.0, 0.5);
    summary.dataflows.insert(
        D1,
        DataflowSummary {
            median_lag: secs(lag),
            progress_ratio: Some(ratio),
            behind: true,
        },
    );
    summary
}

/// Quiet summaries at `size` for the `hours` before `NOW`.
fn quiet_history(size: &str, hours: u64, heap_gib: f64, busy_cores: f64) -> Vec<LoadSummary> {
    (0..hours * 6)
        .map(|i| quiet(size, NOW - i * 10 * MINUTE, heap_gib, busy_cores))
        .collect()
}

struct Case {
    size: &'static str,
    summaries: Vec<LoadSummary>,
    record: Option<LoadRecord>,
    reconfiguration: Option<ReconfigurationRecord>,
    trial: Option<TrialObservation>,
    last_hydration: Option<Duration>,
    hydration_peak: Option<u64>,
    fingerprint: u64,
    policy: OnLoadPolicy,
    now: u64,
}

impl Case {
    fn new(size: &'static str, summaries: Vec<LoadSummary>) -> Self {
        Case {
            size,
            summaries,
            record: Some(empty_record()),
            reconfiguration: None,
            trial: None,
            last_hydration: Some(Duration::from_secs(60)),
            hydration_peak: None,
            fingerprint: FP,
            policy: policy(),
            now: NOW,
        }
    }

    fn eval(&self) -> LoadDecision {
        let ladder = ladder();
        let input = RulesInput {
            policy: &self.policy,
            ladder: &ladder,
            size: self.size,
            reconfiguration: self.reconfiguration.as_ref(),
            record: self.record.as_ref(),
            fingerprint: self.fingerprint,
            summaries: &self.summaries,
            trial: self.trial.as_ref(),
            last_hydration: self.last_hydration,
            hydration_peak: self.hydration_peak,
            now: ts(self.now),
        };
        evaluate(&input, &params()).expect("on the ladder")
    }
}

fn record_of(decision: &LoadDecision) -> LoadRecord {
    decision.record.clone().expect("a record write")
}

fn trial_target(decision: &LoadDecision) -> (TrialKind, String) {
    match &decision.action {
        Some(LoadAction::StartTrial(start)) => (start.kind, start.target.clone()),
        other => panic!("expected a trial, got {other:?}"),
    }
}

fn stats(median: f64, last_third: f64, ratio: Option<f64>) -> DataflowStats {
    DataflowStats {
        median_lag: secs(median),
        first_third_median_lag: Some(secs(median)),
        last_third_median_lag: Some(secs(last_third)),
        progress_ratio: ratio,
    }
}

fn in_flight(
    kind: TrialKind,
    target: &str,
    deadline: u64,
    hold_started: Option<u64>,
) -> ReconfigurationRecord {
    let hold_for = match kind {
        TrialKind::Up => Duration::from_secs(300),
        TrialKind::Down => Duration::from_secs(900),
    };
    ReconfigurationRecord {
        target: ReconfigurationTarget {
            size: target.into(),
            replication_factor: 1,
            availability_zones: AvailabilityZones::default(),
            logging: ComputeReplicaLogging::default(),
            arrangement_compression: false,
        },
        deadline: ts(deadline),
        on_timeout: OnTimeout::Rollback,
        status: ReconfigurationStatus::InProgress,
        origin: ReconfigurationOrigin::LoadAutoscaling,
        trial: Some(ReconfigurationTrial {
            kind,
            hold_for,
            deadline: ts(deadline + u64::try_from(hold_for.as_millis()).unwrap()),
            hold_started: hold_started.map(ts),
        }),
    }
}

#[mz_ore::test]
fn evidence_interval_starts_at_base_doubles_on_recurrence_and_resets() {
    let base = Duration::from_secs(3600);
    let max = Duration::from_secs(7 * 24 * 3600);
    assert_eq!(evidence_interval(None, base, max, ts(NOW)), base);
    let previous = evidence("c2", LoadVerdict::Insufficient, NOW, base);
    // Within four intervals of the previous verdict: doubled.
    assert_eq!(
        evidence_interval(Some(&previous), base, max, ts(NOW + 4 * HOUR)),
        2 * base
    );
    // Later: the base again.
    assert_eq!(
        evidence_interval(Some(&previous), base, max, ts(NOW + 4 * HOUR + 1)),
        base
    );
    // Capped.
    let long = evidence(
        "c2",
        LoadVerdict::Insufficient,
        NOW,
        Duration::from_secs(5 * 24 * 3600),
    );
    assert_eq!(
        evidence_interval(Some(&long), base, max, ts(NOW + HOUR)),
        max
    );
    assert!(blocks(&previous, ts(NOW + HOUR - 1)));
    assert!(!blocks(&previous, ts(NOW + HOUR)));
}

#[mz_ore::test]
fn recorded_evidence_replaces_the_entry_and_doubles_its_interval() {
    // A down-trial to c2 failed an hour and a half ago, and fails again now.
    let mut case = Case::new("c4", Vec::new());
    let mut record = empty_record();
    record.evidence = vec![evidence(
        "c2",
        LoadVerdict::Insufficient,
        NOW - 90 * MINUTE,
        Duration::from_secs(3600),
    )];
    record.attempt = Some(LoadAttempt {
        from_size: "c4".into(),
        target_size: "c2".into(),
        behind: Vec::new(),
    });
    case.record = Some(record);
    case.reconfiguration = Some(in_flight(
        TrialKind::Down,
        "c2",
        NOW,
        Some(NOW - 15 * MINUTE),
    ));
    case.trial = Some(TrialObservation {
        candidate_killed: false,
        stats: Some(TrialStats {
            current: BTreeMap::from([(D1, stats(1.0, 1.0, Some(1.0)))]),
            candidate: BTreeMap::from([(D1, stats(10.0, 20.0, Some(0.9)))]),
            candidate_heap: Some(HeapSummary {
                bytes: GIB,
                fraction: Some(0.1),
            }),
        }),
    });
    let decision = case.eval();
    assert!(matches!(
        decision.action,
        Some(LoadAction::Drop(DropReason::Failed(_)))
    ));
    let record = record_of(&decision);
    assert_eq!(
        record.evidence,
        vec![evidence(
            "c2",
            LoadVerdict::Insufficient,
            NOW,
            Duration::from_secs(7200)
        )]
    );
    assert_eq!(record.attempt, None);
}

#[mz_ore::test]
fn fingerprint_change_clears_evidence_and_floors() {
    let mut case = Case::new("c4", vec![quiet("c4", NOW, 1.0, 0.1)]);
    let mut record = empty_record();
    record.evidence = vec![evidence(
        "c2",
        LoadVerdict::Insufficient,
        NOW,
        Duration::from_secs(3600),
    )];
    record.floors = vec![LoadFloor {
        dataflow: D1,
        lag: secs(5.0),
    }];
    case.record = Some(record);
    case.fingerprint = FP + 1;
    let decision = case.eval();
    let record = record_of(&decision);
    assert_eq!(record.fingerprint, FP + 1);
    assert!(record.evidence.is_empty());
    assert!(record.floors.is_empty());
    assert_eq!(
        decision.evidence,
        vec![EvidenceChange::Cleared {
            fingerprint: FP + 1
        }]
    );
    // The policy-change path clears the same way.
    let mut old = case.record.clone().unwrap();
    old.hydration_peak_ratio = Some(1500);
    let cleared = cleared_record(&old, FP);
    assert!(cleared.evidence.is_empty() && cleared.floors.is_empty());
    assert_eq!(cleared.hydration_peak_ratio, Some(1500));
}

#[mz_ore::test]
fn up_trial_starts_one_size_up_when_the_backlog_is_not_growing() {
    let decision = Case::new("c4", vec![behind("c4", NOW, 30.0, 1.0)]).eval();
    assert_eq!(decision.status, LoadStatus::TrialUp);
    assert_eq!(trial_target(&decision), (TrialKind::Up, "c8".into()));
    let record = record_of(&decision);
    assert_eq!(
        record.attempt,
        Some(LoadAttempt {
            from_size: "c4".into(),
            target_size: "c8".into(),
            behind: vec![D1],
        })
    );
    let Some(LoadAction::StartTrial(start)) = decision.action else {
        unreachable!()
    };
    // A budget of four hydrations, at least 30 minutes, then the hold.
    assert_eq!(start.hydration_deadline, ts(NOW + 30 * MINUTE));
    assert_eq!(start.deadline, ts(NOW + 35 * MINUTE));
    assert_eq!(start.hold_for, Duration::from_secs(300));
}

#[mz_ore::test]
fn up_trial_candidate_is_sized_from_the_progress_ratio() {
    // A ratio of 0.3 asks for c4 / 0.3 = 13.3 cores: c16.
    let decision = Case::new("c4", vec![behind("c4", NOW, 30.0, 0.3)]).eval();
    assert_eq!(trial_target(&decision).1, "c16");
    // A ratio of 0.05 is floored at 0.25: 16 cores.
    let decision = Case::new("c4", vec![behind("c4", NOW, 30.0, 0.05)]).eval();
    assert_eq!(trial_target(&decision).1, "c16");
    // Past the maximum, the maximum.
    let decision = Case::new("c16", vec![behind("c16", NOW, 30.0, 0.05)]).eval();
    assert_eq!(trial_target(&decision).1, "c32");
}

#[mz_ore::test]
fn up_trial_is_blocked_by_no_gain_at_max_and_by_floors() {
    let mut case = Case::new("c4", vec![behind("c4", NOW, 30.0, 1.0)]);
    let mut blocked = empty_record();
    blocked.evidence = vec![evidence(
        "c4",
        LoadVerdict::NoGain,
        NOW - HOUR,
        Duration::from_secs(24 * 3600),
    )];
    case.record = Some(blocked);
    let decision = case.eval();
    assert_eq!(decision.action, None);
    assert_eq!(decision.status, LoadStatus::NotScalable);
    assert_eq!(decision.facts.blocking.as_ref().unwrap().size, "c4");

    let decision = Case::new("c32", vec![behind("c32", NOW, 30.0, 1.0)]).eval();
    assert_eq!(decision.action, None);
    assert_eq!(decision.status, LoadStatus::AtMax);

    // A floor of 25s raises the threshold to 37.5s, so 30s is not behind.
    let mut case = Case::new("c4", vec![behind("c4", NOW, 30.0, 1.0)]);
    let mut floored = empty_record();
    floored.floors = vec![LoadFloor {
        dataflow: D1,
        lag: secs(25.0),
    }];
    case.record = Some(floored);
    assert!(!matches!(
        case.eval().action,
        Some(LoadAction::StartTrial(_))
    ));
}

#[mz_ore::test]
fn up_trial_reads_only_a_fresh_summary_at_the_realized_size() {
    // A summary two windows old is not the latest.
    let decision = Case::new("c4", vec![behind("c4", NOW - 20 * MINUTE, 30.0, 1.0)]).eval();
    assert_eq!(decision.action, None);
    assert_eq!(decision.status, LoadStatus::WarmingUp);
    // Nor is a summary at another size.
    let decision = Case::new("c4", vec![behind("c2", NOW, 30.0, 1.0)]).eval();
    assert_eq!(decision.action, None);
}

fn up_verdict(current: DataflowStats, candidate: DataflowStats) -> LoadDecision {
    let mut case = Case::new("c4", Vec::new());
    let mut record = empty_record();
    record.attempt = Some(LoadAttempt {
        from_size: "c4".into(),
        target_size: "c8".into(),
        behind: vec![D1, D2],
    });
    case.record = Some(record);
    case.reconfiguration = Some(in_flight(TrialKind::Up, "c8", NOW, Some(NOW - 5 * MINUTE)));
    case.trial = Some(TrialObservation {
        candidate_killed: false,
        stats: Some(TrialStats {
            // D2 is behind at 6s on both, and gains nothing.
            current: BTreeMap::from([(D1, current), (D2, stats(6.0, 6.0, Some(1.0)))]),
            candidate: BTreeMap::from([(D1, candidate), (D2, stats(6.0, 6.0, Some(1.0)))]),
            candidate_heap: None,
        }),
    });
    case.eval()
}

fn gained(decision: &LoadDecision) -> Option<Gain> {
    let verdict = match &decision.action {
        Some(LoadAction::CutOver(verdict)) => verdict,
        Some(LoadAction::Drop(DropReason::Failed(verdict))) => verdict,
        other => panic!("expected a verdict, got {other:?}"),
    };
    verdict.dataflows[&D1].gain
}

#[mz_ore::test]
fn gain_fresh() {
    let decision = up_verdict(stats(30.0, 30.0, Some(1.0)), stats(1.0, 1.5, Some(1.0)));
    assert_eq!(gained(&decision), Some(Gain::Fresh));
    // Within the tolerance at the median, rising past it by the last third: not fresh.
    let decision = up_verdict(stats(30.0, 30.0, Some(0.5)), stats(1.5, 4.0, Some(0.5)));
    assert_eq!(gained(&decision), None);
}

#[mz_ore::test]
fn gain_faster_is_a_backlog_that_grows_more_slowly_on_the_candidate() {
    let decision = up_verdict(stats(60.0, 80.0, Some(0.5)), stats(20.0, 30.0, Some(0.8)));
    assert_eq!(gained(&decision), Some(Gain::Faster));
    // Ratios 0.05 apart are not.
    let decision = up_verdict(stats(60.0, 80.0, Some(0.5)), stats(20.0, 30.0, Some(0.55)));
    assert_eq!(gained(&decision), None);
}

#[mz_ore::test]
fn gain_closer_is_a_steady_lag_halved() {
    let decision = up_verdict(stats(30.0, 30.0, Some(1.0)), stats(8.0, 8.0, Some(0.98)));
    assert_eq!(gained(&decision), Some(Gain::Closer));
    // A steady lag that only drops by a third is not.
    let decision = up_verdict(stats(30.0, 30.0, Some(1.0)), stats(20.0, 20.0, Some(1.0)));
    assert_eq!(gained(&decision), None);
}

#[mz_ore::test]
fn lower_lag_only_because_the_candidate_started_later_is_no_gain() {
    // Both fall behind at the same rate. The candidate started fresh at its hydration, so its
    // lag is lower, and still growing.
    let decision = up_verdict(
        stats(300.0, 400.0, Some(0.7)),
        stats(60.0, 100.0, Some(0.7)),
    );
    assert_eq!(gained(&decision), None);
    assert_eq!(decision.status, LoadStatus::TrialUp);
    let record = record_of(&decision);
    assert_eq!(
        record.evidence,
        vec![evidence(
            "c4",
            LoadVerdict::NoGain,
            NOW,
            Duration::from_secs(24 * 3600)
        )]
    );
    // Every behind dataflow gets a floor at its lag on the current replicas.
    assert_eq!(
        record.floors,
        vec![
            LoadFloor {
                dataflow: D1,
                lag: secs(300.0)
            },
            LoadFloor {
                dataflow: D2,
                lag: secs(6.0)
            },
        ]
    );
    assert_eq!(record.attempt, None);
    // The next evaluation reports it.
    let mut case = Case::new("c4", vec![behind("c4", NOW, 30.0, 1.0)]);
    case.record = Some(record);
    case.now = NOW + MINUTE;
    assert_eq!(case.eval().status, LoadStatus::NotScalable);
}

#[mz_ore::test]
fn up_cut_over_records_insufficient_and_floors_the_dataflows_that_did_not_gain() {
    let decision = up_verdict(stats(30.0, 30.0, Some(1.0)), stats(1.0, 1.0, Some(1.0)));
    assert!(matches!(decision.action, Some(LoadAction::CutOver(_))));
    let record = record_of(&decision);
    assert_eq!(
        record.evidence,
        vec![evidence(
            "c4",
            LoadVerdict::Insufficient,
            NOW,
            Duration::from_secs(3600)
        )]
    );
    assert_eq!(
        record.floors,
        vec![LoadFloor {
            dataflow: D2,
            lag: secs(6.0)
        }]
    );
    assert_eq!(
        record.last_cutover,
        Some(LoadCutover {
            at: ts(NOW),
            from_size: "c4".into(),
            reason: LoadCutoverReason::UpTrial,
        })
    );
}

#[mz_ore::test]
fn down_trial_after_a_quiet_window() {
    let decision = Case::new("c8", quiet_history("c8", 2, 4.0, 0.5)).eval();
    assert_eq!(decision.status, LoadStatus::TrialDown);
    assert_eq!(trial_target(&decision), (TrialKind::Down, "c4".into()));
    let Some(LoadAction::StartTrial(start)) = decision.action else {
        unreachable!()
    };
    assert_eq!(start.hold_for, Duration::from_secs(900));
    assert_eq!(start.deadline, ts(NOW + 45 * MINUTE));
}

#[mz_ore::test]
fn down_trial_blocked_by_each_condition() {
    let proposed = |case: &Case| matches!(case.eval().action, Some(LoadAction::StartTrial(_)));
    let base = || Case::new("c8", quiet_history("c8", 2, 4.0, 0.5));
    assert!(proposed(&base()));

    // At the minimum.
    let case = Case::new("c1", quiet_history("c1", 2, 1.0, 0.1));
    assert!(!proposed(&case));
    assert_eq!(case.eval().status, LoadStatus::AtMin);

    // A short history does not fill the quiet window.
    let mut case = base();
    case.summaries.truncate(5);
    assert!(!proposed(&case));
    // A long last hydration lengthens it.
    let mut case = base();
    case.last_hydration = Some(Duration::from_secs(70 * 60));
    assert!(!proposed(&case));
    // A missing summary leaves it below 90% observed.
    let mut case = base();
    case.summaries.remove(2);
    assert!(!proposed(&case));
    // A dataflow behind.
    let mut case = base();
    case.summaries[3] = behind("c8", NOW - 30 * MINUTE, 5.0, 1.0);
    assert!(!proposed(&case));
    // A kill.
    let mut case = base();
    case.summaries[3].killed = true;
    assert!(!proposed(&case));
    // Heap: 23 GiB is 72% of c4.
    let case = Case::new("c8", quiet_history("c8", 2, 23.0, 0.5));
    assert!(!proposed(&case));
    // Heap that fits steady, but not with the hydration peak.
    let mut case = Case::new("c8", quiet_history("c8", 2, 20.0, 0.5));
    assert!(proposed(&case));
    let mut record = empty_record();
    record.hydration_peak_ratio = Some(1700);
    case.record = Some(record);
    assert!(!proposed(&case));
    // Busy time: 3.3 cores is over 80% of c4's 4.
    let mut case = base();
    case.summaries[4].busy = Some(BusySummary {
        mean_millicores: 3300,
        busiest_worker_utilization: 1.0,
    });
    assert!(
        !proposed(&case),
        "the 95th percentile of the six windows is their largest"
    );
    // Busy time missing from a summary.
    let mut case = base();
    case.summaries[4].busy = None;
    assert!(!proposed(&case));
    // Evidence against c4, or against c8 itself.
    for size in ["c4", "c8"] {
        let mut case = base();
        let mut record = empty_record();
        record.evidence = vec![evidence(
            size,
            LoadVerdict::Insufficient,
            NOW - 30 * MINUTE,
            Duration::from_secs(3600),
        )];
        case.record = Some(record);
        let decision = case.eval();
        assert_eq!(decision.action, None);
        assert_eq!(decision.status, LoadStatus::Waiting);
        assert_eq!(decision.facts.blocking.unwrap().size, size);
    }
    // Evidence against a smaller size does not block.
    let mut case = base();
    let mut record = empty_record();
    record.evidence = vec![evidence(
        "c2",
        LoadVerdict::Insufficient,
        NOW - 30 * MINUTE,
        Duration::from_secs(3600),
    )];
    case.record = Some(record);
    assert!(proposed(&case));
}

#[mz_ore::test]
fn quiet_window_counts_only_summaries_at_the_realized_size() {
    // Two hours of quiet at c16, then 50 minutes at c8: the descent waits for c8 to fill a
    // window of its own.
    let mut summaries: Vec<_> = (0..5)
        .map(|i| quiet("c8", NOW - i * 10 * MINUTE, 4.0, 0.5))
        .collect();
    summaries.extend((5..17).map(|i| quiet("c16", NOW - i * 10 * MINUTE, 4.0, 0.5)));
    assert_eq!(Case::new("c8", summaries).eval().action, None);
}

fn down_verdict(
    current: DataflowStats,
    candidate: DataflowStats,
    heap_fraction: f64,
) -> LoadDecision {
    let mut case = Case::new("c8", Vec::new());
    let mut record = empty_record();
    record.attempt = Some(LoadAttempt {
        from_size: "c8".into(),
        target_size: "c4".into(),
        behind: Vec::new(),
    });
    case.record = Some(record);
    case.reconfiguration = Some(in_flight(
        TrialKind::Down,
        "c4",
        NOW,
        Some(NOW - 15 * MINUTE),
    ));
    case.trial = Some(TrialObservation {
        candidate_killed: false,
        stats: Some(TrialStats {
            current: BTreeMap::from([(D1, current)]),
            candidate: BTreeMap::from([(D1, candidate)]),
            candidate_heap: Some(HeapSummary {
                bytes: 1,
                fraction: Some(heap_fraction),
            }),
        }),
    });
    case.eval()
}

#[mz_ore::test]
fn down_verdict_passes_when_as_fresh_and_cuts_over() {
    let decision = down_verdict(stats(1.0, 1.0, Some(1.0)), stats(2.5, 2.9, Some(1.0)), 0.5);
    assert!(matches!(decision.action, Some(LoadAction::CutOver(_))));
    let record = record_of(&decision);
    assert!(record.evidence.is_empty());
    assert_eq!(
        record.last_cutover.unwrap().reason,
        LoadCutoverReason::DownTrial
    );
}

#[mz_ore::test]
fn down_verdict_fails_when_the_last_third_falls_behind() {
    // Fresh enough over the hold, but 4s behind the current replicas in its last third.
    let decision = down_verdict(stats(1.0, 1.0, Some(1.0)), stats(2.0, 5.5, Some(0.99)), 0.5);
    assert!(matches!(
        decision.action,
        Some(LoadAction::Drop(DropReason::Failed(_)))
    ));
    assert_eq!(
        record_of(&decision).evidence,
        vec![evidence(
            "c4",
            LoadVerdict::Insufficient,
            NOW,
            Duration::from_secs(3600)
        )]
    );
}

#[mz_ore::test]
fn down_verdict_fails_on_heap_at_the_maximum() {
    let decision = down_verdict(stats(1.0, 1.0, Some(1.0)), stats(1.0, 1.0, Some(1.0)), 0.8);
    assert!(matches!(
        decision.action,
        Some(LoadAction::Drop(DropReason::Failed(_)))
    ));
}

#[mz_ore::test]
fn trial_deadlines() {
    // Not hydrated by the end of the hydration budget: a down-trial records Insufficient, an
    // up-trial nothing.
    for (kind, target, size, recorded) in [
        (TrialKind::Down, "c4", "c8", true),
        (TrialKind::Up, "c16", "c8", false),
    ] {
        let mut case = Case::new(size, Vec::new());
        case.reconfiguration = Some(in_flight(kind, target, NOW, None));
        let decision = case.eval();
        assert_eq!(
            decision.action,
            Some(LoadAction::Drop(DropReason::NotHydrated))
        );
        assert_eq!(
            decision
                .record
                .map(|r| !r.evidence.is_empty())
                .unwrap_or(false),
            recorded
        );
        // Before it, nothing.
        case.now = NOW - 1;
        assert_eq!(case.eval().action, None);
    }
    // A hold without statistics, after a restart, is dropped at the trial's deadline without
    // evidence.
    let mut case = Case::new("c8", Vec::new());
    case.reconfiguration = Some(in_flight(TrialKind::Down, "c4", NOW, Some(NOW + MINUTE)));
    case.now = NOW + 15 * MINUTE;
    case.trial = Some(TrialObservation::default());
    let decision = case.eval();
    assert_eq!(
        decision.action,
        Some(LoadAction::Drop(DropReason::Deadline))
    );
    assert_eq!(decision.record, None);
    // A killed candidate fails a down-trial at once.
    let mut case = Case::new("c8", Vec::new());
    case.reconfiguration = Some(in_flight(TrialKind::Down, "c4", NOW + HOUR, None));
    case.trial = Some(TrialObservation {
        candidate_killed: true,
        stats: None,
    });
    let decision = case.eval();
    assert_eq!(decision.action, Some(LoadAction::Drop(DropReason::Killed)));
    assert_eq!(record_of(&decision).evidence[0].size, "c4");
}

#[mz_ore::test]
fn kill_moves_to_twice_the_heap_limit() {
    let mut killed = quiet("c4", NOW, 1.0, 0.1);
    killed.killed = true;
    killed.dataflows.clear();
    killed.heap = None;
    killed.hydrating = true;
    let decision = Case::new("c4", vec![killed.clone()]).eval();
    assert_eq!(decision.status, LoadStatus::ScalingUp);
    let Some(LoadAction::Reconfigure { target, reason, .. }) = &decision.action else {
        panic!("{decision:?}")
    };
    assert_eq!(
        (target.as_str(), *reason),
        ("c8", LoadCutoverReason::OutOfMemory)
    );
    let record = record_of(&decision);
    assert_eq!(record.evidence[0].size, "c4");
    assert_eq!(
        record.last_cutover.unwrap().reason,
        LoadCutoverReason::OutOfMemory
    );

    // It ignores NoGain.
    let mut case = Case::new("c4", vec![killed.clone()]);
    let mut record = empty_record();
    record.evidence = vec![evidence(
        "c4",
        LoadVerdict::NoGain,
        NOW,
        Duration::from_secs(3600),
    )];
    case.record = Some(record);
    assert!(matches!(
        case.eval().action,
        Some(LoadAction::Reconfigure { .. })
    ));

    // At the maximum it is reported.
    killed.size = "c32".into();
    let decision = Case::new("c32", vec![killed.clone()]).eval();
    assert_eq!(decision.action, None);
    assert_eq!(decision.status, LoadStatus::OutOfMemoryAtMax);

    // While its plain reconfiguration is in flight, it is not repeated.
    killed.size = "c4".into();
    let mut case = Case::new("c4", vec![killed]);
    let mut plain = in_flight(TrialKind::Up, "c8", NOW + HOUR, None);
    plain.trial = None;
    case.reconfiguration = Some(plain);
    let decision = case.eval();
    assert_eq!(
        (decision.status, decision.action),
        (LoadStatus::ScalingUp, None)
    );
}

#[mz_ore::test]
fn high_heap_moves_to_the_smallest_size_at_the_proposal_maximum() {
    // 7 GiB of c8's 8: 87.5% for two summaries.
    let summaries = vec![
        quiet("c1", NOW, 7.0, 0.1),
        quiet("c1", NOW - 10 * MINUTE, 7.0, 0.1),
    ];
    let decision = Case::new("c1", summaries.clone()).eval();
    let Some(LoadAction::Reconfigure { target, reason, .. }) = &decision.action else {
        panic!("{decision:?}")
    };
    // 7 GiB is 44% of c2.
    assert_eq!(
        (target.as_str(), *reason),
        ("c2", LoadCutoverReason::HighHeap)
    );
    assert_eq!(record_of(&decision).evidence[0].size, "c1");

    // 15 GiB at c2 is 94%. c4 holds it at 47%.
    let summaries = vec![
        quiet("c2", NOW, 15.0, 0.1),
        quiet("c2", NOW - 10 * MINUTE, 15.0, 0.1),
    ];
    let Some(LoadAction::Reconfigure { target, .. }) = Case::new("c2", summaries).eval().action
    else {
        panic!()
    };
    assert_eq!(target, "c4");

    // One summary above the maximum is not the high-heap period.
    let summaries = vec![
        quiet("c1", NOW, 7.0, 0.1),
        quiet("c1", NOW - 10 * MINUTE, 6.0, 0.1),
    ];
    assert_eq!(Case::new("c1", summaries).eval().action, None);
}

#[mz_ore::test]
fn probation_returns_without_a_trial() {
    let mut case = Case::new("c4", vec![behind("c4", NOW, 5.0, 1.0)]);
    let mut record = empty_record();
    record.last_cutover = Some(LoadCutover {
        at: ts(NOW - 40 * MINUTE),
        from_size: "c8".into(),
        reason: LoadCutoverReason::DownTrial,
    });
    record.evidence = vec![evidence(
        "c4",
        LoadVerdict::NoGain,
        NOW - HOUR,
        Duration::from_secs(24 * 3600),
    )];
    case.record = Some(record.clone());
    let decision = case.eval();
    let Some(LoadAction::Reconfigure { target, reason, .. }) = &decision.action else {
        panic!("{decision:?}")
    };
    assert_eq!(
        (target.as_str(), *reason),
        ("c8", LoadCutoverReason::ProbationReturn)
    );
    let written = record_of(&decision);
    assert!(
        written
            .evidence
            .iter()
            .any(|e| e.size == "c4" && e.verdict == LoadVerdict::Insufficient)
    );

    // After W, the ordinary rules apply: here the NoGain blocks an up-trial.
    case.now = NOW + 20 * MINUTE;
    case.summaries = vec![behind("c4", NOW + 20 * MINUTE, 5.0, 1.0)];
    let decision = case.eval();
    assert_eq!(decision.action, None);
    assert_eq!(decision.status, LoadStatus::NotScalable);

    // A descent by any other path has no probation.
    let mut case = Case::new("c4", vec![behind("c4", NOW, 5.0, 1.0)]);
    record.last_cutover.as_mut().unwrap().reason = LoadCutoverReason::UpTrial;
    record.evidence.clear();
    case.record = Some(record);
    assert!(matches!(
        case.eval().action,
        Some(LoadAction::StartTrial(_))
    ));
}

#[mz_ore::test]
fn user_reconfiguration_wins_and_abandons_the_attempt() {
    let mut case = Case::new("c4", vec![behind("c4", NOW, 30.0, 1.0)]);
    let mut record = empty_record();
    record.attempt = Some(LoadAttempt {
        from_size: "c4".into(),
        target_size: "c8".into(),
        behind: vec![D1],
    });
    case.record = Some(record);
    let mut user = in_flight(TrialKind::Up, "c2", NOW + HOUR, None);
    user.origin = ReconfigurationOrigin::User;
    user.trial = None;
    case.reconfiguration = Some(user);
    let decision = case.eval();
    assert_eq!(decision.action, None);
    let record = record_of(&decision);
    assert_eq!(record.attempt, None);
    assert!(record.evidence.is_empty());
}

#[mz_ore::test]
fn hydration_peak_ratio_is_recorded_and_rewritten_on_a_large_change() {
    let mut case = Case::new("c4", vec![quiet("c4", NOW, 4.0, 0.1)]);
    case.hydration_peak = Some(6 * GIB);
    let record = record_of(&case.eval());
    assert_eq!(record.hydration_peak_ratio, Some(1500));
    case.record = Some(record.clone());
    case.hydration_peak = Some(6 * GIB + GIB / 4);
    assert_eq!(case.eval().record, None, "within 10% of the recorded ratio");
    case.hydration_peak = Some(8 * GIB);
    assert_eq!(record_of(&case.eval()).hydration_peak_ratio, Some(2000));
}

#[mz_ore::test]
fn statuses_without_action() {
    // No summary yet.
    assert_eq!(
        Case::new("c4", Vec::new()).eval().status,
        LoadStatus::WarmingUp
    );
    // A hydrating window only.
    let mut hydrating = quiet("c4", NOW, 1.0, 0.1);
    hydrating.hydrating = true;
    hydrating.dataflows.clear();
    assert_eq!(
        Case::new("c4", vec![hydrating]).eval().status,
        LoadStatus::WarmingUp
    );
    // Quiet, but not for long enough to descend.
    assert_eq!(
        Case::new("c4", vec![quiet("c4", NOW, 1.0, 0.1)])
            .eval()
            .status,
        LoadStatus::Steady
    );
    // Off the ladder.
    let ladder = ladder();
    let input = RulesInput {
        policy: &policy(),
        ladder: &ladder,
        size: "c64",
        reconfiguration: None,
        record: None,
        fingerprint: FP,
        summaries: &[],
        trial: None,
        last_hydration: None,
        hydration_peak: None,
        now: ts(NOW),
    };
    assert_eq!(evaluate(&input, &params()), None);
}
