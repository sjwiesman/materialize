// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use mz_adapter_types::cluster_state::{
    AutoScalingPolicy, LoadAudit, LoadBackoff, LoadDirection, LoadHold, LoadRecord,
    LoadScaleReason, LoadScaleUp, OnLoadPolicy, ReconfigurationOrigin,
};
use mz_compute_types::config::ComputeReplicaLogging;
use mz_controller_types::ClusterId;
use mz_repr::GlobalId;

use super::*;
use crate::ctx::{ClusterSchedule, ClusterState, ReconfigurationStatus};
use crate::strategy::{ConfigSignals, LiveSignals, Strategy};
use crate::testing::{GIB, HOUR, MINUTE, apply_write, ladder, load_params, ts};

/// A start time far enough from zero that a window longer than the samples
/// reaches back before the first one.
const T0: u64 = 100 * HOUR;

/// A cluster keeping up: 1s of lag, inputs and output advancing a minute per
/// minute, `busy_millicores` of busy time, and `heap_gib` of heap at a 30%
/// heap fraction.
fn fresh(busy_millicores: u64, heap_gib: u64) -> LoadMeasurement {
    LoadMeasurement {
        lag: Duration::from_secs(1),
        input_advance: Duration::from_secs(60),
        output_advance: Duration::from_secs(60),
        slowest_dataflow: None,
        heap_fraction: 300,
        heap_bytes: heap_gib * GIB,
        mean_utilization: 600,
        max_worker_utilization: 700,
        busy_millicores,
    }
}

/// A saturated cluster `lag_secs` behind whose output advanced `progress`
/// permille of what its inputs did.
fn behind(lag_secs: u64, progress: u64) -> LoadMeasurement {
    LoadMeasurement {
        lag: Duration::from_secs(lag_secs),
        output_advance: Duration::from_millis(60 * progress),
        slowest_dataflow: Some(GlobalId::User(7)),
        mean_utilization: 1000,
        max_worker_utilization: 1000,
        ..fresh(4000, 4)
    }
}

fn sample(at: u64, size: &str, measurement: LoadMeasurement) -> LoadSample {
    LoadSample {
        at: ts(at),
        size: size.to_string(),
        measurement: Some(measurement),
        oom_killed: false,
    }
}

/// One sample per minute at `size` over `[from, to]`, all with `measurement`.
fn steady(from: u64, to: u64, size: &str, measurement: &LoadMeasurement) -> Vec<LoadSample> {
    (from..=to)
        .step_by(usize::cast_from(MINUTE))
        .map(|at| sample(at, size, measurement.clone()))
        .collect()
}

fn signals(samples: Vec<LoadSample>) -> LoadSignals {
    LoadSignals {
        ladder: ladder(),
        samples,
        hydration_time: None,
    }
}

fn bounds<'a>(min: &'a str, max: &'a str) -> LoadBounds<'a> {
    LoadBounds {
        min_size: Some(min),
        max_size: Some(max),
        scale_down_after: None,
    }
}

fn eval(
    size: &str,
    bounds: LoadBounds<'_>,
    samples: Vec<LoadSample>,
    record: Option<&LoadRecord>,
    now: u64,
) -> LoadEvaluation {
    evaluate(
        size,
        bounds,
        &signals(samples),
        record,
        &load_params(),
        ts(now),
    )
    .expect("size and bounds are on the ladder")
}

fn target(evaluation: &LoadEvaluation) -> Option<&str> {
    evaluation.action.as_ref().map(|a| a.target_size.as_str())
}

fn reason(evaluation: &LoadEvaluation) -> Option<LoadScaleReason> {
    evaluation.action.as_ref().map(|a| a.reason)
}

fn backoff(size: &str, direction: LoadDirection, until: u64) -> LoadRecord {
    LoadRecord {
        backoff: vec![LoadBackoff {
            size: size.to_string(),
            direction,
            until: ts(until),
            failures: 1,
        }],
        ..Default::default()
    }
}

/// A replica whose slowest dataflow, if any, is `(id, local lag, input
/// advance, output advance)` in seconds.
fn replica(slowest: Option<(u64, u64, u64, u64)>) -> ReplicaLoad {
    ReplicaLoad {
        cpu_millicores: 4000,
        interval: Duration::from_secs(60),
        workers: 4,
        total_busy: Duration::from_secs(120),
        max_worker_busy: Duration::from_secs(45),
        slowest: slowest.map(|(id, lag, input, output)| DataflowLoad {
            id: GlobalId::User(id),
            progress: DataflowProgress {
                input_advance: Duration::from_secs(input),
                output_advance: Duration::from_secs(output),
                local_lag: Duration::from_secs(lag),
            },
        }),
        heap_fraction: 300,
        heap_bytes: 4 * GIB,
    }
}

#[mz_ore::test]
fn measure_takes_lag_and_progress_from_the_freshest_replica() {
    // Replica lag is the local lag of its slowest dataflow. The freshest
    // replica speaks, and its slowest dataflow gives the progress ratio.
    let slow = replica(Some((0, 30, 60, 10)));
    let mut fast = replica(Some((1, 8, 60, 30)));
    fast.total_busy = Duration::from_secs(60);
    fast.heap_fraction = 200;
    fast.heap_bytes = 6 * GIB;
    let m = measure(&[slow, fast]).expect("measured");
    assert_eq!(m.lag, Duration::from_secs(8));
    assert_eq!(m.progress(), 500);
    assert_eq!(m.slowest_dataflow, Some(GlobalId::User(1)));
    // Utilization of the speaking replica: 60 of 240 worker seconds.
    assert_eq!(m.mean_utilization, 250);
    assert_eq!(m.max_worker_utilization, 750);
    // Busy time and heap are the maxima over replicas.
    assert_eq!(m.busy_millicores, 2000);
    assert_eq!(m.heap_fraction, 300);
    assert_eq!(m.heap_bytes, 6 * GIB);

    // A replica without dataflows has no lag.
    let m = measure(&[replica(None)]).expect("measured");
    assert_eq!(m.lag, Duration::ZERO);
    assert_eq!(m.progress(), 1000);
    assert!(measure(&[]).is_none());
}

#[mz_ore::test]
fn measure_counts_a_throttled_worker_as_its_core_share() {
    // One worker on half a core, busy the whole interval, used half a core.
    let mut load = replica(None);
    load.workers = 1;
    load.cpu_millicores = 500;
    load.total_busy = Duration::from_secs(60);
    assert_eq!(measure(&[load]).expect("measured").busy_millicores, 500);
}

#[mz_ore::test]
fn behind_needs_lag_over_the_tolerance_that_did_not_shrink() {
    // (lag, input advance, output advance) -> behind
    for (lag, input, output, expected) in [
        (2, 60, 30, false),
        (3, 60, 30, true),
        (3, 60, 60, true),
        // Catching up on its own.
        (30, 60, 61, false),
        // Nothing arrived and nothing was processed.
        (30, 0, 0, true),
    ] {
        let m = LoadMeasurement {
            lag: Duration::from_secs(lag),
            input_advance: Duration::from_secs(input),
            output_advance: Duration::from_secs(output),
            ..fresh(0, 1)
        };
        assert_eq!(
            m.behind(Duration::from_secs(2)),
            expected,
            "lag {lag}s, input {input}s, output {output}s"
        );
    }
    let stalled_inputs = LoadMeasurement {
        input_advance: Duration::ZERO,
        output_advance: Duration::ZERO,
        ..fresh(0, 1)
    };
    assert_eq!(stalled_inputs.progress(), 1000);
}

#[mz_ore::test]
fn evaluate_needs_the_size_and_bounds_on_the_ladder() {
    let signals = signals(Vec::new());
    for (size, bounds) in [
        ("c3", bounds("c1", "c32")),
        ("c4", bounds("c3", "c32")),
        ("c4", bounds("c1", "c64")),
        ("c4", bounds("c8", "c2")),
    ] {
        assert!(evaluate(size, bounds, &signals, None, &load_params(), ts(HOUR)).is_none());
    }
}

#[mz_ore::test]
fn evaluate_warms_up_until_the_activation_window_is_full() {
    let e = eval(
        "c4",
        bounds("c1", "c32"),
        steady(0, MINUTE, "c4", &behind(60, 500)),
        None,
        MINUTE,
    );
    assert_eq!(e.status, LoadStatus::WarmingUp);
    assert_eq!(e.action, None);
    assert_eq!(e.recommended_size, None);
}

#[mz_ore::test]
fn evaluate_scales_up_only_when_behind_in_every_activation_sample() {
    // (samples, oldest first) -> scales up
    for (pattern, expected) in [
        (vec![true, true, true], true),
        (vec![false, true, true], false),
        (vec![true, false, true], false),
        (vec![true, true, false], false),
        // Older samples outside the activation window do not matter.
        (vec![false, true, true, true], true),
    ] {
        let samples = pattern
            .iter()
            .enumerate()
            .map(|(i, is_behind)| {
                let m = if *is_behind {
                    behind(60, 500)
                } else {
                    fresh(4000, 4)
                };
                sample(u64::cast_from(i) * MINUTE, "c4", m)
            })
            .collect();
        let now = u64::cast_from(pattern.len() - 1) * MINUTE;
        let e = eval("c4", bounds("c1", "c32"), samples, None, now);
        assert_eq!(e.action.is_some(), expected, "{pattern:?}");
        if !expected {
            assert_eq!(e.status, LoadStatus::Steady, "{pattern:?}");
        }
    }
}

#[mz_ore::test]
fn evaluate_sizes_a_scale_up_from_progress_and_backlog() {
    // (current, progress permille, lag seconds) -> target
    for (current, progress, lag, expected) in [
        // 60% of the input processed: 6.7 cores.
        ("c4", 600, 3, "c8"),
        // Half of it needs 8 cores, and the 3s backlog tips it past c8.
        ("c4", 500, 3, "c16"),
        // A 15 minute backlog with a 15 minute catch-up time doubles it.
        ("c4", 600, 15 * 60, "c16"),
        // Barely behind: still at least one step.
        ("c4", 900, 3, "c8"),
        ("c4", 1000, 3, "c8"),
        // 4 / 0.3 = 13.3 cores.
        ("c4", 300, 3, "c16"),
        // The floor bounds a stalled output at four times the cores.
        ("c2", 0, 3, "c16"),
        ("c2", 100, 3, "c16"),
        ("c2", 250, 3, "c16"),
    ] {
        let context = format!("{current} {progress} {lag}");
        let e = eval(
            current,
            bounds("c1", "c32"),
            steady(0, 2 * MINUTE, current, &behind(lag, progress)),
            None,
            2 * MINUTE,
        );
        assert_eq!(e.status, LoadStatus::ScalingUp, "{context}");
        assert_eq!(target(&e), Some(expected), "{context}");
        assert_eq!(reason(&e), Some(LoadScaleReason::ScaleUp));
        let sample = e.action.expect("action").sample.expect("sample");
        assert_eq!(sample.progress, u32::try_from(progress).unwrap());
        assert_eq!(sample.lag, Duration::from_secs(lag));
    }
}

#[mz_ore::test]
fn evaluate_takes_the_median_progress_and_the_latest_lag() {
    // The oldest sample's lag would call for c16.
    let samples = vec![
        sample(0, "c4", behind(900, 100)),
        sample(MINUTE, "c4", behind(3, 600)),
        sample(2 * MINUTE, "c4", behind(3, 900)),
    ];
    let e = eval("c4", bounds("c1", "c32"), samples, None, 2 * MINUTE);
    assert_eq!(target(&e), Some("c8"));
}

#[mz_ore::test]
fn evaluate_holds_an_idle_cluster_that_is_behind() {
    // (mean, busiest worker utilization) -> status
    for (mean, busiest, expected) in [
        (300, 950, LoadStatus::SkewLimited),
        (300, 900, LoadStatus::SkewLimited),
        (300, 500, LoadStatus::Stalled),
        (499, 899, LoadStatus::Stalled),
        (500, 1000, LoadStatus::ScalingUp),
    ] {
        let m = LoadMeasurement {
            mean_utilization: mean,
            max_worker_utilization: busiest,
            ..behind(60, 500)
        };
        let e = eval(
            "c8",
            bounds("c1", "c32"),
            steady(0, 2 * HOUR, "c8", &m),
            None,
            2 * HOUR,
        );
        assert_eq!(e.status, expected, "{mean} {busiest}");
        assert_eq!(
            e.action.is_some(),
            expected == LoadStatus::ScalingUp,
            "{mean} {busiest}"
        );
    }
}

#[mz_ore::test]
fn evaluate_reports_at_max() {
    let e = eval(
        "c4",
        bounds("c1", "c8"),
        steady(0, 2 * MINUTE, "c4", &behind(60, 100)),
        None,
        2 * MINUTE,
    );
    assert_eq!(target(&e), Some("c8"));

    let e = eval(
        "c8",
        bounds("c1", "c8"),
        steady(0, 2 * MINUTE, "c8", &behind(60, 100)),
        None,
        2 * MINUTE,
    );
    assert_eq!(e.status, LoadStatus::AtMax);
    assert_eq!(e.action, None);
}

#[mz_ore::test]
fn evaluate_scale_up_stops_below_a_backed_off_target() {
    let samples = steady(0, 2 * MINUTE, "c4", &behind(60, 300));

    let record = backoff("c16", LoadDirection::Up, HOUR);
    let e = eval(
        "c4",
        bounds("c1", "c32"),
        samples.clone(),
        Some(&record),
        2 * MINUTE,
    );
    assert_eq!(target(&e), Some("c8"));

    let record = backoff("c8", LoadDirection::Up, HOUR);
    let e = eval(
        "c4",
        bounds("c1", "c32"),
        samples.clone(),
        Some(&record),
        2 * MINUTE,
    );
    assert_eq!(e.status, LoadStatus::BackingOff);
    assert_eq!(e.action, None);

    // An expired entry blocks nothing.
    let record = backoff("c8", LoadDirection::Up, MINUTE);
    let e = eval(
        "c4",
        bounds("c1", "c32"),
        samples,
        Some(&record),
        2 * MINUTE,
    );
    assert_eq!(target(&e), Some("c16"));
}

fn killed(at: u64, size: &str) -> LoadSample {
    LoadSample {
        at: ts(at),
        size: size.to_string(),
        measurement: None,
        oom_killed: true,
    }
}

#[mz_ore::test]
fn evaluate_scales_up_on_a_memory_kill_without_waiting() {
    // One sample, no measurement. The next size with twice the heap limit.
    let e = eval(
        "c4",
        bounds("c1", "c32"),
        vec![killed(MINUTE, "c4")],
        None,
        MINUTE,
    );
    assert_eq!(target(&e), Some("c8"));
    assert_eq!(reason(&e), Some(LoadScaleReason::OutOfMemory));
    assert_eq!(e.action.expect("action").sample, None);

    let e = eval(
        "c4",
        bounds("c1", "c4"),
        vec![killed(MINUTE, "c4")],
        None,
        MINUTE,
    );
    assert_eq!(e.status, LoadStatus::AtMax);

    // A kill at another size, or long ago, does not count.
    let e = eval(
        "c4",
        bounds("c1", "c32"),
        vec![killed(MINUTE, "c2")],
        None,
        MINUTE,
    );
    assert_eq!(e.action, None);
    let e = eval(
        "c4",
        bounds("c1", "c32"),
        vec![killed(MINUTE, "c4")],
        None,
        HOUR,
    );
    assert_eq!(e.action, None);

    // A size that failed raises the floor instead of blocking the rule.
    let e = eval(
        "c4",
        bounds("c1", "c32"),
        vec![killed(MINUTE, "c4")],
        Some(&backoff("c8", LoadDirection::Up, HOUR)),
        MINUTE,
    );
    assert_eq!(target(&e), Some("c16"));

    // Steps that grow the heap limit by less than twice are skipped.
    let ladder = [10, 15, 25]
        .into_iter()
        .enumerate()
        .map(|(i, gib)| SizeCapacity {
            size: format!("m{i}"),
            cpu_millicores: 1000,
            heap_limit_bytes: gib * GIB,
        })
        .collect();
    let signals = LoadSignals {
        ladder,
        samples: vec![killed(MINUTE, "m0")],
        hydration_time: None,
    };
    let e = evaluate(
        "m0",
        LoadBounds::default(),
        &signals,
        None,
        &load_params(),
        ts(MINUTE),
    )
    .expect("on the ladder");
    assert_eq!(target(&e), Some("m2"));
}

fn heavy(heap_fraction: u32, heap_gib: u64) -> LoadMeasurement {
    LoadMeasurement {
        heap_fraction,
        heap_bytes: heap_gib * GIB,
        ..fresh(2000, 0)
    }
}

#[mz_ore::test]
fn evaluate_scales_up_on_sustained_high_heap() {
    // 27 GiB at c4, a 32 GiB limit: c8 holds it at 42%.
    let samples = steady(T0, T0 + 15 * MINUTE, "c4", &heavy(850, 27));
    let e = eval("c4", bounds("c1", "c32"), samples, None, T0 + 15 * MINUTE);
    assert_eq!(target(&e), Some("c8"));
    assert_eq!(reason(&e), Some(LoadScaleReason::HeapHighWater));

    // 40 GiB needs an 80 GiB limit: c16.
    let samples = steady(T0, T0 + 15 * MINUTE, "c4", &heavy(850, 40));
    let e = eval("c4", bounds("c1", "c32"), samples, None, T0 + 15 * MINUTE);
    assert_eq!(target(&e), Some("c16"));

    // At the high-water mark is not above it.
    let samples = steady(T0, T0 + 15 * MINUTE, "c4", &heavy(800, 27));
    let e = eval("c4", bounds("c1", "c32"), samples, None, T0 + 15 * MINUTE);
    assert_eq!(e.action, None);
}

#[mz_ore::test]
fn evaluate_high_heap_resets_on_a_single_dip() {
    let mut samples = steady(T0, T0 + 15 * MINUTE, "c4", &heavy(850, 27));
    samples[7] = sample(T0 + 7 * MINUTE, "c4", heavy(500, 16));
    let e = eval(
        "c4",
        bounds("c1", "c32"),
        samples.clone(),
        None,
        T0 + 15 * MINUTE,
    );
    assert_eq!(e.action, None);

    // Fifteen minutes after the dip it fires.
    samples.extend(steady(
        T0 + 16 * MINUTE,
        T0 + 23 * MINUTE,
        "c4",
        &heavy(850, 27),
    ));
    let e = eval("c4", bounds("c1", "c32"), samples, None, T0 + 23 * MINUTE);
    assert_eq!(target(&e), Some("c8"));
}

#[mz_ore::test]
fn evaluate_high_heap_needs_the_period_observed() {
    // Ten minutes of samples, as after a restart.
    let e = eval(
        "c4",
        bounds("c1", "c32"),
        steady(T0 + 5 * MINUTE, T0 + 15 * MINUTE, "c4", &heavy(850, 27)),
        None,
        T0 + 15 * MINUTE,
    );
    assert_eq!(e.action, None);

    // The period's ends are observed but most of it is missing.
    let mut sparse = steady(T0, T0 + 2 * MINUTE, "c4", &heavy(850, 27));
    sparse.extend(steady(
        T0 + 13 * MINUTE,
        T0 + 15 * MINUTE,
        "c4",
        &heavy(850, 27),
    ));
    let e = eval("c4", bounds("c1", "c32"), sparse, None, T0 + 15 * MINUTE);
    assert_eq!(e.action, None);
}

#[mz_ore::test]
fn evaluate_scales_down_to_the_cheapest_size_that_fits_heap_and_cpu() {
    // (peak busy millicores, peak heap GiB) -> target from c16
    for (busy, heap, expected) in [
        // 1000 <= 0.8 * 2000, 2 GiB <= 0.5 * 16 GiB.
        (1000, 2, Some("c2")),
        (700, 2, Some("c1")),
        // CPU binds: 3000 needs 3750 cores.
        (3000, 2, Some("c4")),
        // Heap binds: 20 GiB needs a 40 GiB limit.
        (700, 20, Some("c8")),
        // Neither smaller size holds the busy time.
        (7000, 2, None),
        // Nor the heap.
        (700, 40, None),
    ] {
        let e = eval(
            "c16",
            bounds("c1", "c32"),
            steady(0, 2 * HOUR, "c16", &fresh(busy, heap)),
            None,
            2 * HOUR,
        );
        assert_eq!(target(&e), expected, "{busy} {heap}");
        match expected {
            Some(_) => {
                assert_eq!(e.status, LoadStatus::ScalingDown);
                assert_eq!(reason(&e), Some(LoadScaleReason::ScaleDown));
            }
            None => assert_eq!(e.status, LoadStatus::Steady),
        }
    }
}

#[mz_ore::test]
fn evaluate_scale_down_takes_the_window_peak() {
    let mut samples = steady(0, 2 * HOUR, "c16", &fresh(1000, 2));
    samples[90] = sample(90 * MINUTE, "c16", fresh(3000, 2));
    samples[100] = sample(100 * MINUTE, "c16", fresh(1000, 20));
    let e = eval("c16", bounds("c1", "c32"), samples, None, 2 * HOUR);
    assert_eq!(target(&e), Some("c8"));
    let sample = e.action.expect("action").sample.expect("sample");
    assert_eq!(sample.busy_millicores, 3000);
}

#[mz_ore::test]
fn evaluate_scale_down_is_blocked_by_each_condition() {
    let now = T0 + 2 * HOUR;
    let quiet = || steady(T0, now, "c16", &fresh(1000, 2));
    let hydrating = |hours: f64, samples: Vec<LoadSample>, bounds: LoadBounds<'_>| {
        let signals = LoadSignals {
            hydration_time: Some(Duration::from_secs_f64(hours * 60.0 * 60.0)),
            ..signals(samples)
        };
        evaluate("c16", bounds, &signals, None, &load_params(), ts(now)).expect("on the ladder")
    };
    let mut cases: Vec<(&str, Vec<LoadSample>, LoadBounds<'_>)> = Vec::new();

    let mut one_behind = quiet();
    one_behind[60] = sample(T0 + 60 * MINUTE, "c16", behind(10, 500));
    cases.push(("one sample behind", one_behind, bounds("c1", "c32")));

    let mut killed_in_window = quiet();
    killed_in_window[90].oom_killed = true;
    cases.push(("a memory kill", killed_in_window, bounds("c1", "c32")));

    cases.push((
        "samples reach back half the window",
        steady(T0 + 90 * MINUTE, now, "c16", &fresh(1000, 2)),
        bounds("c1", "c32"),
    ));

    let mut sparse = steady(T0, T0 + 70 * MINUTE, "c16", &fresh(1000, 2));
    sparse.extend(steady(T0 + 115 * MINUTE, now, "c16", &fresh(1000, 2)));
    cases.push(("most intervals missing", sparse, bounds("c1", "c32")));

    cases.push((
        "the policy's window",
        quiet(),
        LoadBounds {
            scale_down_after: Some(Duration::from_secs(4 * 60 * 60)),
            ..bounds("c1", "c32")
        },
    ));

    for (case, samples, bounds) in cases {
        let e = eval("c16", bounds, samples, None, now);
        assert_eq!(e.action, None, "{case}");
        assert_eq!(e.status, LoadStatus::Steady, "{case}");
    }
    // The window is at least twice the hydration time.
    let e = hydrating(1.5, quiet(), bounds("c1", "c32"));
    assert_eq!(e.action, None);
    assert_eq!(e.status, LoadStatus::Steady);

    // Covered windows do scale down.
    let e = hydrating(1.0, quiet(), bounds("c1", "c32"));
    assert_eq!(target(&e), Some("c2"));
    let e = hydrating(
        1.5,
        steady(T0 - 2 * HOUR, now, "c16", &fresh(1000, 2)),
        bounds("c1", "c32"),
    );
    assert_eq!(target(&e), Some("c2"));
}

#[mz_ore::test]
fn evaluate_reports_at_min() {
    let e = eval(
        "c4",
        bounds("c4", "c32"),
        steady(0, 2 * HOUR, "c4", &fresh(100, 1)),
        None,
        2 * HOUR,
    );
    assert_eq!(e.status, LoadStatus::AtMin);
    assert_eq!(e.action, None);

    // At the bottom of the ladder there is nothing to report.
    let e = eval(
        "c1",
        bounds("c1", "c32"),
        steady(0, 2 * HOUR, "c1", &fresh(100, 1)),
        None,
        2 * HOUR,
    );
    assert_eq!(e.status, LoadStatus::Steady);
}

#[mz_ore::test]
fn evaluate_scale_down_steps_around_a_backed_off_target() {
    let samples = steady(0, 2 * HOUR, "c16", &fresh(1000, 2));

    // c2 failed, which rules out everything below it too: go to c4.
    let record = backoff("c2", LoadDirection::Down, 3 * HOUR);
    let e = eval(
        "c16",
        bounds("c1", "c32"),
        samples.clone(),
        Some(&record),
        2 * HOUR,
    );
    assert_eq!(target(&e), Some("c4"));
    assert_eq!(e.recommended_size.as_deref(), Some("c2"));

    let record = backoff("c8", LoadDirection::Down, 3 * HOUR);
    let e = eval(
        "c16",
        bounds("c1", "c32"),
        samples.clone(),
        Some(&record),
        2 * HOUR,
    );
    assert_eq!(e.status, LoadStatus::BackingOff);
    assert_eq!(e.action, None);

    // An expired entry blocks nothing, and an Up entry does not block a
    // scale-down.
    for record in [
        backoff("c8", LoadDirection::Down, HOUR),
        backoff("c2", LoadDirection::Up, 3 * HOUR),
    ] {
        let e = eval(
            "c16",
            bounds("c1", "c32"),
            samples.clone(),
            Some(&record),
            2 * HOUR,
        );
        assert_eq!(target(&e), Some("c2"));
    }
}

#[mz_ore::test]
fn evaluate_ignores_samples_from_another_size_and_from_the_warm_up() {
    let record = LoadRecord {
        last_cutover: Some(ts(HOUR)),
        ..Default::default()
    };
    // Lagging samples at the old size, and at the new size during its
    // warm-up.
    let mut samples = steady(0, HOUR, "c4", &behind(60, 300));
    samples.extend(steady(
        HOUR + MINUTE,
        HOUR + 4 * MINUTE,
        "c8",
        &behind(60, 300),
    ));
    let e = eval(
        "c8",
        bounds("c1", "c32"),
        samples.clone(),
        Some(&record),
        HOUR + 4 * MINUTE,
    );
    assert_eq!(e.status, LoadStatus::WarmingUp);

    samples.extend(steady(
        HOUR + 5 * MINUTE,
        HOUR + 7 * MINUTE,
        "c8",
        &fresh(4000, 4),
    ));
    let e = eval(
        "c8",
        bounds("c1", "c32"),
        samples,
        Some(&record),
        HOUR + 7 * MINUTE,
    );
    assert_eq!(e.status, LoadStatus::Steady);
    assert_eq!(e.action, None);
}

#[mz_ore::test]
fn evaluate_is_steady_under_stable_load() {
    // 2.4 cores and 10 GiB on c4: c2 holds neither at its margin, and the
    // cluster keeps up, for as long as the load holds.
    let samples = steady(0, 6 * HOUR, "c4", &fresh(2400, 10));
    for now in (HOUR..=6 * HOUR).step_by(usize::cast_from(MINUTE)) {
        let window: Vec<_> = samples
            .iter()
            .filter(|s| u64::from(s.at) <= now)
            .cloned()
            .collect();
        let e = eval("c4", bounds("c1", "c32"), window, None, now);
        assert_eq!(e.status, LoadStatus::Steady, "at {now}");
        assert_eq!(e.action, None, "at {now}");
        assert_eq!(e.recommended_size.as_deref(), Some("c4"), "at {now}");
    }
}

fn config() -> ConfigSignals {
    ConfigSignals {
        load_enabled: true,
        load: load_params(),
        default_reconfiguration_timeout: Duration::from_secs(24 * 60 * 60),
        ..Default::default()
    }
}

fn state(size: &str) -> ClusterState {
    ClusterState {
        cluster_id: ClusterId::user(1).expect("valid"),
        size: size.to_string(),
        replication_factor: 1,
        availability_zones: Vec::new(),
        logging: ComputeReplicaLogging::default(),
        arrangement_compression: false,
        schedule: ClusterSchedule::Manual,
        auto_scaling_policy: Some(AutoScalingPolicy {
            on_hydration: None,
            on_load: Some(OnLoadPolicy {
                min_size: "c1".to_string(),
                max_size: "c32".to_string(),
                scale_down_after: None,
            }),
        }),
        reconfiguration: None,
        burst: None,
        load: None,
        replicas: Vec::new(),
    }
}

fn live(samples: Vec<LoadSample>) -> LiveSignals {
    LiveSignals {
        load: Some(signals(samples)),
        ..Default::default()
    }
}

fn update(state: &ClusterState, samples: Vec<LoadSample>, now: u64) -> StateWrite {
    OnLoadStrategy.update_state(state, &live(samples), &config(), ts(now))
}

fn audits(write: &StateWrite) -> Vec<LoadAudit> {
    write
        .load
        .as_ref()
        .map(|w| w.audit.clone())
        .unwrap_or_default()
}

/// The decision behind the reconfiguration `write` starts.
fn decision(write: &StateWrite) -> LoadScaleDecision {
    match write.reconfiguration.as_ref().and_then(|w| w.audit) {
        Some(ReconfigurationAudit::Started {
            load: Some(decision),
        }) => decision,
        audit => panic!("expected a load-started audit: {audit:?}"),
    }
}

fn written_record(write: &StateWrite) -> &ReconfigurationRecord {
    write
        .reconfiguration
        .as_ref()
        .and_then(|w| w.record.as_ref())
        .expect("reconfiguration written")
}

fn busy_samples(size: &str) -> Vec<LoadSample> {
    steady(0, 2 * MINUTE, size, &behind(60, 300))
}

/// A state whose scale-up from `c4` to `c16` is in flight.
fn scaling_up() -> ClusterState {
    let mut s = state("c4");
    let write = update(&s, busy_samples("c4"), 2 * MINUTE);
    apply_write(&mut s, &write);
    s
}

fn settle(state: &mut ClusterState, status: ReconfigurationStatus) {
    let record = state.reconfiguration.as_mut().expect("in flight");
    record.status = status;
    if status == ReconfigurationStatus::Finalized {
        state.size = record.target.size.clone();
    }
}

#[mz_ore::test]
fn strategy_engages_only_with_policy_flag_and_a_running_manual_cluster() {
    let request = |s: &ClusterState, c: &ConfigSignals| OnLoadStrategy.signal_request(s, c).load;
    assert!(request(&state("c4"), &config()));

    let off = ConfigSignals {
        load_enabled: false,
        ..config()
    };
    assert!(!request(&state("c4"), &off));

    let mut s = state("c4");
    s.auto_scaling_policy = None;
    assert!(!request(&s, &config()));

    let mut s = state("c4");
    s.replication_factor = 0;
    assert!(!request(&s, &config()));

    let mut s = state("c4");
    s.schedule = ClusterSchedule::Refresh {
        hydration_time_estimate: Duration::ZERO,
    };
    assert!(!request(&s, &config()));
}

#[mz_ore::test]
fn strategy_clears_its_record_when_disengaged() {
    let mut s = state("c4");
    s.load = Some(LoadRecord::default());
    let off = ConfigSignals {
        load_enabled: false,
        ..config()
    };
    let write = OnLoadStrategy.update_state(&s, &LiveSignals::default(), &off, ts(0));
    assert_eq!(write.load.expect("write").record, None);
    assert!(write.reconfiguration.is_none());

    s.load = None;
    assert!(
        OnLoadStrategy
            .update_state(&s, &LiveSignals::default(), &off, ts(0))
            .is_empty()
    );
}

#[mz_ore::test]
fn strategy_does_nothing_without_the_signal() {
    let write =
        OnLoadStrategy.update_state(&state("c4"), &LiveSignals::default(), &config(), ts(0));
    assert!(write.is_empty());
}

#[mz_ore::test]
fn strategy_starts_a_rollback_reconfiguration_and_contributes_no_replicas() {
    let s = state("c4");
    let write = update(&s, busy_samples("c4"), 2 * MINUTE);

    let record = written_record(&write);
    assert_eq!(record.target.size, "c16");
    assert_eq!(record.target.replication_factor, 1);
    assert_eq!(record.status, ReconfigurationStatus::InProgress);
    assert_eq!(record.on_timeout, OnTimeout::Rollback);
    assert_eq!(record.origin, ReconfigurationOrigin::LoadAutoscaling);
    assert_eq!(record.deadline, ts(2 * MINUTE + 24 * HOUR));
    assert_eq!(record.trial, None, "a scale-up is not a trial");

    let attempt = write
        .load
        .as_ref()
        .and_then(|w| w.record.as_ref())
        .and_then(|r| r.attempt.as_ref())
        .expect("attempt recorded");
    assert_eq!(attempt.from_size, "c4");
    assert_eq!(attempt.target_size, "c16");
    assert_eq!(attempt.deadline, record.deadline);
    assert_eq!(attempt.progress_before, Some(300));
    assert_eq!(audits(&write), Vec::new());
    let decision = decision(&write);
    assert_eq!(decision.reason, LoadScaleReason::ScaleUp);
    let sample = decision.sample.expect("sample");
    assert_eq!(sample.lag, Duration::from_secs(60));
    assert_eq!(sample.progress, 300);

    assert!(
        OnLoadStrategy
            .desired_replicas(&s, &live(busy_samples("c4")), &config(), ts(2 * MINUTE))
            .is_empty()
    );
}

#[mz_ore::test]
fn strategy_starts_a_scale_down_as_a_trial() {
    let s = state("c16");
    let write = update(&s, steady(0, 2 * HOUR, "c16", &fresh(1000, 2)), 2 * HOUR);
    let record = written_record(&write);
    assert_eq!(record.target.size, "c2");
    assert_eq!(
        record.trial,
        Some(ReconfigurationTrial {
            allowed_lag: Duration::from_secs(2),
            hold_for: Duration::from_secs(15 * 60),
            max_heap_fraction: 800,
            ready_since: None,
        })
    );
    assert_eq!(audits(&write), Vec::new());
    let decision = decision(&write);
    assert_eq!(decision.reason, LoadScaleReason::ScaleDown);
    assert!(decision.sample.is_some());
}

#[mz_ore::test]
fn strategy_waits_for_its_reconfiguration() {
    let s = scaling_up();
    assert!(update(&s, busy_samples("c4"), 3 * MINUTE).is_empty());
}

#[mz_ore::test]
fn strategy_never_overwrites_a_users_reconfiguration() {
    let mut s = scaling_up();
    s.load = None;
    s.reconfiguration.as_mut().expect("in flight").origin = ReconfigurationOrigin::User;
    assert!(update(&s, busy_samples("c4"), 3 * MINUTE).is_empty());
}

#[mz_ore::test]
fn strategy_records_the_cutover() {
    let mut s = state("c4");
    let write = update(&s, vec![killed(MINUTE, "c4")], MINUTE);
    apply_write(&mut s, &write);
    settle(&mut s, ReconfigurationStatus::Finalized);
    let write = update(&s, Vec::new(), HOUR);
    let record = write.load.expect("write").record.expect("record");
    assert_eq!(record.last_cutover, Some(ts(HOUR)));
    // Not motivated by lag, so there is nothing to evaluate afterwards.
    assert_eq!(record.attempt, None);
    assert!(write.reconfiguration.is_none());
}

#[mz_ore::test]
fn strategy_backs_off_a_failed_target_for_longer_each_time() {
    for status in [
        ReconfigurationStatus::TimedOut,
        ReconfigurationStatus::ResourceExhausted,
    ] {
        let mut s = scaling_up();
        settle(&mut s, status);
        let write = update(&s, Vec::new(), 25 * HOUR);
        let record = write
            .load
            .as_ref()
            .expect("write")
            .record
            .clone()
            .expect("record");
        assert_eq!(record.attempt, None);
        assert_eq!(
            record.backoff,
            vec![LoadBackoff {
                size: "c16".to_string(),
                direction: LoadDirection::Up,
                until: ts(26 * HOUR),
                failures: 1,
            }]
        );
        assert!(matches!(
            audits(&write).as_slice(),
            [LoadAudit::BackoffStarted { .. }]
        ));
    }

    let mut record = LoadRecord::default();
    let untils: Vec<u64> = (0..7)
        .map(|_| {
            back_off(&mut record, "c2", LoadDirection::Down, ts(0));
            u64::from(record.backoff[0].until)
        })
        .collect();
    assert_eq!(
        untils,
        [1, 2, 4, 8, 16, 24, 24].map(|hours| hours * HOUR).to_vec()
    );
    assert_eq!(record.backoff.len(), 1);
}

#[mz_ore::test]
fn strategy_backs_off_a_failed_trial_in_the_down_direction() {
    let mut s = state("c16");
    let write = update(&s, steady(0, 2 * HOUR, "c16", &fresh(1000, 2)), 2 * HOUR);
    apply_write(&mut s, &write);
    assert_eq!(
        s.reconfiguration.as_ref().expect("written").target.size,
        "c2"
    );
    settle(&mut s, ReconfigurationStatus::TimedOut);
    let write = update(&s, Vec::new(), 26 * HOUR);
    apply_write(&mut s, &write);
    let record = s.load.clone().expect("record");
    assert_eq!(record.backoff[0].direction, LoadDirection::Down);
    assert_eq!(record.backoff[0].size, "c2");

    // The next proposal tries the next size up, and not c2 again.
    let write = update(
        &s,
        steady(25 * HOUR, 26 * HOUR + 30 * MINUTE, "c16", &fresh(1000, 2)),
        26 * HOUR + 30 * MINUTE,
    );
    assert_eq!(written_record(&write).target.size, "c4");
}

#[mz_ore::test]
fn strategy_drops_an_attempt_a_user_wrote_over() {
    for change in [
        |s: &mut ClusterState| s.reconfiguration = None,
        |s: &mut ClusterState| {
            let record = s.reconfiguration.as_mut().expect("in flight");
            record.origin = ReconfigurationOrigin::User;
            record.status = ReconfigurationStatus::TimedOut;
        },
        |s: &mut ClusterState| settle(s, ReconfigurationStatus::Cancelled),
    ] {
        let mut s = scaling_up();
        change(&mut s);
        let write = update(&s, Vec::new(), HOUR);
        let record = write.load.expect("write").record.expect("record");
        assert_eq!(record.attempt, None);
        assert!(record.backoff.is_empty());
    }
}

/// A state that scaled up from `c4` to `c8` because of lag and has cut over.
fn cut_over_after_lag() -> ClusterState {
    let mut s = state("c4");
    let write = update(&s, steady(0, 2 * MINUTE, "c4", &behind(3, 700)), 2 * MINUTE);
    apply_write(&mut s, &write);
    assert_eq!(written_record(&write).target.size, "c8");
    settle(&mut s, ReconfigurationStatus::Finalized);
    let write = update(&s, Vec::new(), HOUR);
    apply_write(&mut s, &write);
    let record = s.load.as_ref().expect("record");
    assert_eq!(record.attempt, None);
    assert_eq!(
        record.last_scale_up,
        Some(LoadScaleUp {
            from_size: "c4".to_string(),
            target_size: "c8".to_string(),
            progress: 700,
        })
    );
    s
}

#[mz_ore::test]
fn strategy_reverts_a_scale_up_that_did_not_help() {
    let s = cut_over_after_lag();

    // Inside the warm-up there is nothing to judge yet.
    let early = steady(HOUR + MINUTE, HOUR + 4 * MINUTE, "c8", &behind(60, 700));
    assert!(update(&s, early, HOUR + 4 * MINUTE).is_empty());

    // Still behind, with progress up by less than 0.1, on twice the cores.
    let after = steady(HOUR + 5 * MINUTE, HOUR + 7 * MINUTE, "c8", &behind(60, 790));
    let write = update(&s, after, HOUR + 7 * MINUTE);
    assert_eq!(written_record(&write).target.size, "c4");
    assert_eq!(written_record(&write).trial, None);
    let record = write
        .load
        .as_ref()
        .expect("write")
        .record
        .clone()
        .expect("record");
    assert_eq!(record.backoff[0].size, "c8");
    assert_eq!(record.backoff[0].direction, LoadDirection::Up);
    assert!(record.attempt.is_some());
    assert_eq!(record.last_scale_up, None);
    assert!(matches!(
        audits(&write).as_slice(),
        [LoadAudit::BackoffStarted { .. }]
    ));
    assert_eq!(decision(&write).reason, LoadScaleReason::IneffectiveScaleUp);
}

#[mz_ore::test]
fn strategy_keeps_a_scale_up_that_helped() {
    // Progress up by 0.1, or no longer behind.
    for after in [behind(60, 800), fresh(4000, 4), behind(2, 700)] {
        let s = cut_over_after_lag();
        let samples = steady(HOUR + 5 * MINUTE, HOUR + 7 * MINUTE, "c8", &after);
        let write = update(&s, samples, HOUR + 7 * MINUTE);
        assert!(write.reconfiguration.is_none(), "{after:?}");
        let record = write.load.expect("write").record.expect("record");
        assert_eq!(record.attempt, None);
        assert_eq!(record.last_scale_up, None);
        assert!(record.backoff.is_empty());
    }
}

#[mz_ore::test]
fn strategy_audits_a_hold_once_on_entry_and_once_on_exit() {
    for (busiest, hold) in [(950, LoadHold::SkewLimited), (500, LoadHold::Stalled)] {
        let mut s = state("c8");
        let idle = LoadMeasurement {
            mean_utilization: 300,
            max_worker_utilization: busiest,
            ..behind(60, 500)
        };
        let held = steady(0, 2 * MINUTE, "c8", &idle);

        let write = update(&s, held.clone(), 2 * MINUTE);
        assert_eq!(
            audits(&write),
            vec![LoadAudit::HoldChanged { hold: Some(hold) }]
        );
        assert!(write.reconfiguration.is_none());
        apply_write(&mut s, &write);

        assert!(update(&s, held, 2 * MINUTE).is_empty());

        let calm = steady(0, 2 * MINUTE, "c8", &fresh(4000, 4));
        let write = update(&s, calm, 2 * MINUTE);
        assert_eq!(audits(&write), vec![LoadAudit::HoldChanged { hold: None }]);
    }
}

#[mz_ore::test]
fn strategy_writes_nothing_when_steady() {
    let s = state("c8");
    let calm = steady(0, 2 * MINUTE, "c8", &fresh(4000, 4));
    assert!(update(&s, calm, 2 * MINUTE).is_empty());
}
