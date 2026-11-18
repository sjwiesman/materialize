// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use mz_adapter_types::cluster_state::{LoadFloor, LoadRecord};
use mz_controller_types::ReplicaId;
use mz_repr::{GlobalId, Timestamp};

use super::*;

const SIZE: &str = "100cc";
const R1: ReplicaId = ReplicaId::User(1);
const R2: ReplicaId = ReplicaId::User(2);
const D1: GlobalId = GlobalId::User(1);
const D2: GlobalId = GlobalId::User(2);

/// Windows of 60s sampled every second, starting at 600s.
const START: u64 = 600_000;
const END: u64 = 660_000;

fn params() -> SummaryParams {
    SummaryParams {
        window: Duration::from_secs(60),
        sample_interval: Duration::from_secs(1),
        warm_up: Duration::from_secs(30),
        lag_tolerance: Duration::from_secs(2),
        floor_factor: 1.5,
    }
}

fn ts(ms: u64) -> Timestamp {
    Timestamp::new(ms)
}

/// A replica that has been eligible since long before the window.
fn replica(dataflows: &[(GlobalId, u64, u64)]) -> ReplicaSample {
    ReplicaSample {
        size: SIZE.into(),
        hydrated: true,
        eligible_since: Some(ts(0)),
        dataflows: dataflows
            .iter()
            .map(|(id, input, output)| {
                let frontiers = DataflowFrontiers {
                    input: ts(*input),
                    output: ts(*output),
                };
                (*id, frontiers)
            })
            .collect(),
        heap: None,
    }
}

fn sample(at: u64, replicas: Vec<(ReplicaId, ReplicaSample)>) -> LoadSample {
    let exports = replicas
        .iter()
        .flat_map(|(_, r)| r.dataflows.keys().copied())
        .collect();
    LoadSample {
        at: ts(at),
        size: SIZE.into(),
        exports,
        replicas: replicas.into_iter().collect(),
    }
}

/// One sample per second over the window, with D1's frontiers on R1 given by `frontiers` as a
/// function of the sample time.
fn series(frontiers: impl Fn(u64) -> (u64, u64)) -> Vec<LoadSample> {
    (START..END)
        .step_by(1000)
        .map(|at| {
            let (input, output) = frontiers(at);
            sample(at, vec![(R1, replica(&[(D1, input, output)]))])
        })
        .collect()
}

fn summarize_window(samples: &[LoadSample]) -> Option<LoadSummary> {
    summarize(ts(START), ts(END), samples, &[], &[], None, &params())
}

fn ratio(summary: &LoadSummary, id: GlobalId) -> f64 {
    summary.dataflows[&id]
        .progress_ratio
        .expect("input advanced")
}

#[mz_ore::test]
fn steady_lag_has_ratio_one() {
    let samples = series(|at| (at, at - 30_000));
    let summary = summarize_window(&samples).expect("summary");
    let d1 = summary.dataflows[&D1];
    assert_eq!(d1.median_lag, Duration::from_secs(30));
    assert!((ratio(&summary, D1) - 1.0).abs() < 1e-9);
    assert!(d1.behind, "a steady lag above the tolerance is behind");
    assert_eq!(summary.at, ts(END));
    assert_eq!(summary.start, ts(START));
    assert_eq!(summary.size, SIZE);
    assert!(!summary.killed);
}

#[mz_ore::test]
fn growing_backlog_has_ratio_below_one() {
    // The output advances at half the rate of the input.
    let samples = series(|at| (at, START + (at - START) / 2));
    let summary = summarize_window(&samples).expect("summary");
    let r = ratio(&summary, D1);
    assert!((r - 0.5).abs() < 0.01, "ratio {r}");
    assert!(summary.dataflows[&D1].behind);
}

#[mz_ore::test]
fn draining_backlog_has_ratio_above_one_and_is_not_behind() {
    // The lag falls linearly from 40s to 0 over the window.
    let samples = series(|at| {
        let elapsed = at - START;
        let lag = 40_000u64.saturating_sub(elapsed * 2 / 3);
        (at, at - lag)
    });
    let summary = summarize_window(&samples).expect("summary");
    let d1 = summary.dataflows[&D1];
    assert!(ratio(&summary, D1) > 1.0);
    assert!(d1.median_lag > Duration::from_secs(2));
    assert!(!d1.behind, "a draining dataflow is not behind");
}

#[mz_ore::test]
fn fresh_dataflow_is_not_behind() {
    let samples = series(|at| (at, at - 1_000));
    let summary = summarize_window(&samples).expect("summary");
    assert!(!summary.dataflows[&D1].behind);
}

#[mz_ore::test]
fn median_ignores_spikes() {
    // A lag of 1s with a spike to 60s at every tenth sample.
    let samples = series(|at| {
        let lag = if (at / 1000) % 10 == 0 { 60_000 } else { 1_000 };
        (at, at - lag)
    });
    let summary = summarize_window(&samples).expect("summary");
    assert_eq!(summary.dataflows[&D1].median_lag, Duration::from_secs(1));
    assert!(!summary.dataflows[&D1].behind);
}

#[mz_ore::test]
fn hydrating_replica_contributes_nothing() {
    // The only replica has another dataflow hydrating at one sample in the window.
    let mut samples = series(|at| (at, at - 1_000));
    samples[30].replicas.get_mut(&R1).unwrap().hydrated = false;
    assert_eq!(summarize_window(&samples), None);

    // With a second replica that stays hydrated, the summary comes from it alone.
    let mut samples: Vec<_> = (START..END)
        .step_by(1000)
        .map(|at| {
            sample(
                at,
                vec![
                    (R1, replica(&[(D1, at, at - 1_000)])),
                    (R2, replica(&[(D1, at, at - 20_000)])),
                ],
            )
        })
        .collect();
    samples[30].replicas.get_mut(&R1).unwrap().hydrated = false;
    let summary = summarize_window(&samples).expect("summary");
    assert_eq!(summary.dataflows[&D1].median_lag, Duration::from_secs(20));
}

#[mz_ore::test]
fn replica_in_warm_up_contributes_nothing() {
    let mut samples = series(|at| (at, at - 1_000));
    for sample in &mut samples {
        // Eligible 20s into the window, so warm-up ends after it.
        sample.replicas.get_mut(&R1).unwrap().eligible_since = Some(ts(START + 20_000));
    }
    assert_eq!(summarize_window(&samples), None);

    for sample in &mut samples {
        // Warm-up ended exactly at the window start.
        sample.replicas.get_mut(&R1).unwrap().eligible_since = Some(ts(START - 30_000));
    }
    assert!(summarize_window(&samples).is_some());
}

#[mz_ore::test]
fn replica_at_another_size_contributes_nothing() {
    let mut samples = series(|at| (at, at - 1_000));
    for sample in &mut samples {
        sample.replicas.get_mut(&R1).unwrap().size = "200cc".into();
    }
    assert_eq!(summarize_window(&samples), None);

    // A change of the realized size within the window leaves no replica qualifying throughout.
    let mut samples = series(|at| (at, at - 1_000));
    for sample in &mut samples[..30] {
        sample.size = "50cc".into();
    }
    assert_eq!(summarize_window(&samples), None);
}

#[mz_ore::test]
fn freshest_replica_speaks_for_the_cluster() {
    let samples: Vec<_> = (START..END)
        .step_by(1000)
        .map(|at| {
            let mut r1 = replica(&[(D1, at, at - 10_000), (D2, at, at - 1_000)]);
            r1.heap = Some(ProcessHeap {
                bytes: 600,
                limit: Some(1000),
            });
            let mut r2 = replica(&[(D1, at, at - 3_000), (D2, at, at - 5_000)]);
            r2.heap = Some(ProcessHeap {
                bytes: 300,
                limit: Some(1000),
            });
            sample(at, vec![(R1, r1), (R2, r2)])
        })
        .collect();
    let summary = summarize_window(&samples).expect("summary");
    // Per dataflow, the smaller lag of the two replicas.
    assert_eq!(summary.dataflows[&D1].median_lag, Duration::from_secs(3));
    assert_eq!(summary.dataflows[&D2].median_lag, Duration::from_secs(1));
    // The heap is the larger of the two replicas'.
    let heap = summary.heap.expect("heap");
    assert_eq!(heap.bytes, 600);
    assert_eq!(heap.fraction, Some(0.6));
}

#[mz_ore::test]
fn heap_is_the_median_of_the_fullest_process() {
    let samples: Vec<_> = series(|at| (at, at))
        .into_iter()
        .enumerate()
        .map(|(i, mut sample)| {
            // A short spike does not move the median.
            let bytes = if i < 5 { 900 } else { 500 };
            sample.replicas.get_mut(&R1).unwrap().heap = Some(ProcessHeap {
                bytes,
                limit: Some(1000),
            });
            sample
        })
        .collect();
    let heap = summarize_window(&samples).unwrap().heap.unwrap();
    assert_eq!(heap.bytes, 500);
    assert_eq!(heap.fraction, Some(0.5));

    // Without a limit there is no fraction.
    let samples: Vec<_> = series(|at| (at, at))
        .into_iter()
        .map(|mut sample| {
            sample.replicas.get_mut(&R1).unwrap().heap = Some(ProcessHeap {
                bytes: 500,
                limit: None,
            });
            sample
        })
        .collect();
    let heap = summarize_window(&samples).unwrap().heap.unwrap();
    assert_eq!(heap.fraction, None);
}

#[mz_ore::test]
fn too_few_samples_produce_no_summary() {
    let all = series(|at| (at, at - 1_000));
    // Fewer than half the samples the window calls for.
    let sparse: Vec<_> = all.iter().step_by(3).cloned().collect();
    assert_eq!(summarize_window(&sparse), None);
    // Exactly half suffices.
    let half: Vec<_> = all.iter().step_by(2).cloned().collect();
    assert!(summarize_window(&half).is_some());
    // No sample in the last third.
    assert_eq!(summarize_window(&all[..40]), None);
    // No sample in the first third.
    assert_eq!(summarize_window(&all[20..]), None);
    // No samples at all.
    assert_eq!(summarize_window(&[]), None);
}

#[mz_ore::test]
fn samples_outside_the_window_are_ignored() {
    let mut samples = series(|at| (at, at - 1_000));
    samples.insert(0, sample(START - 1_000, vec![(R1, replica(&[(D1, 0, 0)]))]));
    samples.push(sample(END, vec![(R1, replica(&[(D1, END, 0)]))]));
    let summary = summarize_window(&samples).expect("summary");
    assert_eq!(summary.dataflows[&D1].median_lag, Duration::from_secs(1));
}

#[mz_ore::test]
fn memory_kill_yields_a_summary_without_a_qualifying_replica() {
    let mut samples = series(|at| (at, at));
    for sample in &mut samples {
        sample.replicas.get_mut(&R1).unwrap().hydrated = false;
    }
    let kill = MemoryKill {
        replica_id: R1,
        size: SIZE.into(),
        at: ts(START + 5_000),
    };
    let summary = summarize(
        ts(START),
        ts(END),
        &samples,
        &[],
        std::slice::from_ref(&kill),
        None,
        &params(),
    )
    .expect("a kill is summarized");
    assert!(summary.killed);
    assert!(summary.dataflows.is_empty());
    assert_eq!(summary.heap, None);
    assert_eq!(summary.busy, None);

    // A kill of a replica at another size, or outside the window, is not.
    for kill in [
        MemoryKill {
            size: "200cc".into(),
            ..kill.clone()
        },
        MemoryKill {
            at: ts(END),
            ..kill.clone()
        },
    ] {
        let summary = summarize(ts(START), ts(END), &samples, &[], &[kill], None, &params());
        assert_eq!(summary, None);
    }

    // With a qualifying replica the kill is recorded beside the lag.
    let samples = series(|at| (at, at));
    let summary = summarize(ts(START), ts(END), &samples, &[], &[kill], None, &params()).unwrap();
    assert!(summary.killed);
    assert!(summary.dataflows.contains_key(&D1));
}

#[mz_ore::test]
fn busy_time_combines_rows_and_ignores_windows_outside() {
    let samples = series(|at| (at, at));
    let window = |start: u64, end: u64, workers: u64, busy_ms: u64, max_ms: u64| BusyWindow {
        replica_id: R1,
        start: ts(start),
        end: ts(end),
        workers,
        busy: Duration::from_millis(busy_ms),
        max_worker_busy: Duration::from_millis(max_ms),
    };
    let busy = [
        // Two rows of the same window, from workers that disagree on its bounds elsewhere.
        window(START, START + 30_000, 1, 15_000, 15_000),
        window(START, START + 30_000, 1, 15_000, 30_000),
        window(START + 30_000, END, 2, 30_000, 15_000),
        // Outside the window.
        window(START - 30_000, START, 2, 60_000, 30_000),
        window(END - 10_000, END + 20_000, 2, 60_000, 30_000),
        // Another replica.
        BusyWindow {
            replica_id: R2,
            ..window(START, END, 2, 120_000, 60_000)
        },
    ];
    let summary = summarize(ts(START), ts(END), &samples, &busy, &[], None, &params()).unwrap();
    let busy = summary.busy.expect("busy");
    // 60s of busy time over 60s of windows is one core.
    assert_eq!(busy.mean_millicores, 1000);
    // The busiest worker was busy for the whole first window and half the second.
    assert!((busy.busiest_worker_utilization - 0.75).abs() < 1e-9);

    // Without busy windows the busy fields are empty and nothing else changes.
    let without = summarize(ts(START), ts(END), &samples, &[], &[], None, &params()).unwrap();
    assert_eq!(without.busy, None);
    assert_eq!(without.dataflows, summary.dataflows);
}

#[mz_ore::test]
fn lag_floor_raises_the_threshold_for_the_matching_fingerprint() {
    let samples = series(|at| (at, at - 12_000));
    let exports: BTreeSet<_> = [D1].into_iter().collect();
    let mut record = LoadRecord {
        fingerprint: fingerprint(&exports),
        attempt: None,
        last_cutover: None,
        evidence: Vec::new(),
        floors: vec![LoadFloor {
            dataflow: D1,
            lag: Duration::from_secs(10),
        }],
        hydration_peak_ratio: None,
    };
    let behind = |record: &LoadRecord| {
        summarize(
            ts(START),
            ts(END),
            &samples,
            &[],
            &[],
            Some(record),
            &params(),
        )
        .unwrap()
        .dataflows[&D1]
            .behind
    };
    // 12s is within 1.5 times the floor of 10s.
    assert!(!behind(&record));
    // A floor recorded for another set of dataflows does not apply.
    record.fingerprint ^= 1;
    assert!(behind(&record));
}

#[mz_ore::test]
fn behind_threshold_is_the_larger_of_tolerance_and_floor() {
    let tolerance = Duration::from_secs(2);
    assert_eq!(behind_threshold(tolerance, None, 1.5), tolerance);
    assert_eq!(
        behind_threshold(tolerance, Some(Duration::from_secs(1)), 1.5),
        tolerance
    );
    assert_eq!(
        behind_threshold(tolerance, Some(Duration::from_secs(10)), 1.5),
        Duration::from_secs(15)
    );
}

#[mz_ore::test]
fn fingerprint_is_order_independent_and_stable() {
    let a = fingerprint(&[GlobalId::User(2), GlobalId::System(1), GlobalId::User(10)]);
    let b = fingerprint(&[GlobalId::User(10), GlobalId::User(2), GlobalId::System(1)]);
    assert_eq!(a, b);
    assert_ne!(a, fingerprint(&[GlobalId::User(2), GlobalId::System(1)]));
    // The fingerprint is persisted, so its value must not change across releases.
    assert_eq!(a, FINGERPRINT_S1_U10_U2);
    assert_eq!(fingerprint(&[]), FINGERPRINT_EMPTY);
}

const FINGERPRINT_S1_U10_U2: u64 = 4047128929019290678;
const FINGERPRINT_EMPTY: u64 = 14492805990617963705;

#[mz_ore::test]
fn window_start_aligns_to_multiples_of_the_window() {
    let window = Duration::from_secs(600);
    assert_eq!(window_start(ts(0), window), ts(0));
    assert_eq!(window_start(ts(599_999), window), ts(0));
    assert_eq!(window_start(ts(600_000), window), ts(600_000));
    assert_eq!(window_start(ts(1_234_567), window), ts(1_200_000));
}

#[mz_ore::test]
fn coverage_counts_window_ends_in_the_interval() {
    let window = Duration::from_secs(600);
    let summary = |at: u64| LoadSummary {
        start: ts(at - 600_000),
        at: ts(at),
        size: SIZE.into(),
        fingerprint: 0,
        dataflows: BTreeMap::new(),
        heap: None,
        busy: None,
        killed: false,
    };
    // Windows end at 1.2M, 1.8M, 2.4M, and 3.0M within (0.6M, 3.0M].
    let summaries = [
        summary(600_000),
        summary(1_200_000),
        summary(1_800_000),
        summary(3_000_000),
    ];
    let c = coverage(&summaries, ts(600_000), ts(3_000_000), window);
    assert!((c - 0.75).abs() < 1e-9, "coverage {c}");
    assert_eq!(coverage(&summaries, ts(600_000), ts(600_001), window), 1.0);
    assert_eq!(coverage(&[], ts(0), ts(1_200_000), window), 0.0);
}
