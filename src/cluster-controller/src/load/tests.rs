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
    AutoScalingPolicy, LoadAttemptPhase, LoadAudit, LoadBackoff, LoadDirection,
    LoadHold, LoadRecord, LoadScaleReason, OnLoadPolicy, ReconfigurationOrigin,
};
use mz_compute_types::config::ComputeReplicaLogging;
use mz_controller_types::ClusterId;
use mz_repr::{GlobalId, Timestamp};

use super::*;
use crate::ctx::{ClusterSchedule, ClusterState, ReconfigurationStatus};
use crate::strategy::{ConfigSignals, LiveSignals, Strategy};

const GIB: u64 = 1 << 30;
const MINUTE: u64 = 60_000;
const HOUR: u64 = 60 * MINUTE;

fn params() -> LoadParams {
    LoadParams {
        policy_interval: Duration::from_secs(60),
        target_utilization: 700,
        target_memory: 800,
        activation_intervals: 3,
        default_scale_down_after: Duration::from_secs(60 * 60),
        warm_up: Duration::from_secs(5 * 60),
        catch_up: Duration::from_secs(15 * 60),
        min_progress: 250,
    }
}

/// Sizes `c1` through `c32`: `n` cores and `4n` GiB each.
fn ladder() -> Vec<SizeCapacity> {
    [1u64, 2, 4, 8, 16, 32]
        .into_iter()
        .map(|n| SizeCapacity {
            size: format!("c{n}"),
            cpu_millicores: n * 1000,
            memory_bytes: n * 4 * GIB,
        })
        .collect()
}

fn ts(ms: u64) -> Timestamp {
    Timestamp::from(ms)
}

fn measurement(cpu_millicores: u64, memory_gib: u64) -> LoadMeasurement {
    LoadMeasurement {
        cpu_demand_millicores: cpu_millicores,
        memory_demand_bytes: memory_gib * GIB,
        max_worker_utilization: 500,
        min_progress: 1000,
        slowest_dataflow: None,
        hydration_time: None,
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
    evaluate(size, bounds, &signals(samples), record, &params(), ts(now))
        .expect("size and bounds are on the ladder")
}

fn target(evaluation: &LoadEvaluation) -> Option<&str> {
    evaluation.action.as_ref().map(|a| a.target_size.as_str())
}

fn replica(workers: usize, busy_secs: f64, dataflows: Vec<DataflowLoad>) -> ReplicaLoad {
    ReplicaLoad {
        cpu_millicores: u64::cast_from(workers) * 1000,
        interval: Duration::from_secs(60),
        worker_busy: vec![Duration::from_secs_f64(busy_secs); workers],
        dataflows,
        memory_bytes: GIB,
        hydration_time: None,
    }
}

fn dataflow(id: u64, busy_secs: f64, input: u64, output: u64, lag: u64) -> DataflowLoad {
    DataflowLoad {
        id: Some(GlobalId::User(id)),
        busy: Duration::from_secs_f64(busy_secs),
        progress: Some(DataflowProgress {
            input_advance: Duration::from_secs(input),
            output_advance: Duration::from_secs(output),
            local_lag: Duration::from_secs(lag),
        }),
    }
}

#[mz_ore::test]
fn measure_keeping_up_is_busy_cores() {
    // 4 workers each busy half the interval, all of it in one dataflow that
    // keeps up: the demand is the 2 cores it used.
    let m = measure(
        &[replica(4, 30.0, vec![dataflow(1, 120.0, 60, 60, 0)])],
        &params(),
    )
    .expect("measured");
    assert_eq!(m.cpu_demand_millicores, 2000);
    assert_eq!(m.max_worker_utilization, 500);
    assert_eq!(m.min_progress, 1000);
    assert_eq!(m.slowest_dataflow, None);
}

#[mz_ore::test]
fn measure_inflates_a_lagging_dataflow_by_its_progress_ratio() {
    // Saturated, and the output advanced half as far as the input: the
    // dataflow needs twice the 4 cores it used. No backlog, so no catch-up.
    let m = measure(
        &[replica(4, 60.0, vec![dataflow(7, 240.0, 60, 30, 0)])],
        &params(),
    )
    .expect("measured");
    assert_eq!(m.cpu_demand_millicores, 8000);
    assert_eq!(m.min_progress, 500);
    assert_eq!(m.slowest_dataflow, Some(GlobalId::User(7)));
}

#[mz_ore::test]
fn measure_adds_catch_up_capacity_for_backlog() {
    // As above with a 15 minute backlog and a 15 minute catch-up time: twice
    // the keep-up capacity again.
    let m = measure(
        &[replica(4, 60.0, vec![dataflow(7, 240.0, 60, 30, 15 * 60)])],
        &params(),
    )
    .expect("measured");
    assert_eq!(m.cpu_demand_millicores, 16000);
}

#[mz_ore::test]
fn measure_clamps_a_stalled_dataflow() {
    // The output frontier did not move. The floor of 0.25 bounds the step at
    // four times the busy cores.
    let m = measure(
        &[replica(4, 60.0, vec![dataflow(7, 240.0, 60, 0, 0)])],
        &params(),
    )
    .expect("measured");
    assert_eq!(m.cpu_demand_millicores, 16000);
    assert_eq!(m.min_progress, 250);
}

#[mz_ore::test]
fn measure_treats_jitter_and_stalled_inputs_as_keeping_up() {
    for (input, output) in [(60, 59), (0, 0), (30, 60)] {
        let m = measure(
            &[replica(4, 30.0, vec![dataflow(1, 120.0, input, output, 0)])],
            &params(),
        )
        .expect("measured");
        assert_eq!(m.min_progress, 1000, "input {input}s, output {output}s");
        assert_eq!(m.cpu_demand_millicores, 2000);
    }
}

#[mz_ore::test]
fn measure_scales_only_the_lagging_dataflow_and_shares_overhead() {
    // Two dataflows with 60s busy each, one at half progress, plus 60s of
    // worker time outside operators. Adjusted operator time is 60 + 120, a
    // factor of 1.5 that the overhead inherits: 270 busy seconds a minute.
    let m = measure(
        &[replica(
            4,
            45.0,
            vec![dataflow(1, 60.0, 60, 60, 0), dataflow(2, 60.0, 60, 30, 0)],
        )],
        &params(),
    )
    .expect("measured");
    assert_eq!(m.cpu_demand_millicores, 4500);
}

#[mz_ore::test]
fn measure_counts_a_throttled_worker_as_its_core_share() {
    // One worker on half a core, busy the whole interval, used half a core.
    let mut load = replica(1, 60.0, vec![dataflow(1, 60.0, 60, 60, 0)]);
    load.cpu_millicores = 500;
    let m = measure(&[load], &params()).expect("measured");
    assert_eq!(m.cpu_demand_millicores, 500);
}

#[mz_ore::test]
fn measure_takes_the_maximum_over_replicas() {
    let busy = replica(4, 60.0, vec![dataflow(7, 240.0, 60, 30, 0)]);
    let idle = replica(4, 15.0, vec![dataflow(7, 60.0, 60, 60, 0)]);
    let m = measure(&[idle, busy], &params()).expect("measured");
    assert_eq!(m.cpu_demand_millicores, 8000);
    assert_eq!(m.min_progress, 500);
    assert!(measure(&[], &params()).is_none());
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
        assert!(evaluate(size, bounds, &signals, None, &params(), ts(HOUR)).is_none());
    }
}

#[mz_ore::test]
fn evaluate_warms_up_until_the_activation_window_is_full() {
    let m = measurement(7000, 4);
    let e = eval("c4", bounds("c1", "c32"), steady(0, MINUTE, "c4", &m), None, MINUTE);
    assert_eq!(e.status, LoadStatus::WarmingUp);
    assert_eq!(e.action, None);
    assert_eq!(e.recommended_size, None);
}

#[mz_ore::test]
fn evaluate_scales_up_to_the_cheapest_fitting_size_in_one_step() {
    // 7 cores at a 0.7 target need 10 cores: c16, skipping c8.
    let m = measurement(7000, 4);
    let e = eval(
        "c4",
        bounds("c1", "c32"),
        steady(0, 2 * MINUTE, "c4", &m),
        None,
        2 * MINUTE,
    );
    assert_eq!(e.status, LoadStatus::ScalingUp);
    assert_eq!(target(&e), Some("c16"));
    assert_eq!(e.action.expect("action").reason, LoadScaleReason::ScaleUp);
}

#[mz_ore::test]
fn evaluate_sizes_for_memory_independently_of_cpu() {
    // Idle CPU, 30 GiB at a 0.8 target need 37.5 GiB: c16.
    let m = measurement(100, 30);
    let e = eval(
        "c8",
        bounds("c1", "c32"),
        steady(0, 2 * MINUTE, "c8", &m),
        None,
        2 * MINUTE,
    );
    assert_eq!(target(&e), Some("c16"));
}

#[mz_ore::test]
fn evaluate_ignores_a_single_interval_spike() {
    let mut samples = steady(0, MINUTE, "c4", &measurement(1000, 4));
    samples.push(sample(2 * MINUTE, "c4", measurement(20000, 4)));
    let e = eval("c4", bounds("c4", "c32"), samples, None, 2 * MINUTE);
    assert_eq!(e.status, LoadStatus::Steady);
    assert_eq!(e.action, None);
}

#[mz_ore::test]
fn evaluate_clamps_to_the_maximum_and_reports_at_max() {
    let m = measurement(20000, 4);
    let e = eval(
        "c4",
        bounds("c1", "c8"),
        steady(0, 2 * MINUTE, "c4", &m),
        None,
        2 * MINUTE,
    );
    assert_eq!(target(&e), Some("c8"));

    let e = eval(
        "c8",
        bounds("c1", "c8"),
        steady(0, 2 * MINUTE, "c8", &m),
        None,
        2 * MINUTE,
    );
    assert_eq!(e.status, LoadStatus::AtMax);
    assert_eq!(e.action, None);
}

#[mz_ore::test]
fn evaluate_scales_up_on_oom_without_waiting() {
    // One sample, no measurement. The next size with twice the memory.
    let samples = vec![LoadSample {
        at: ts(MINUTE),
        size: "c4".to_string(),
        measurement: None,
        oom_killed: true,
    }];
    let e = eval("c4", bounds("c1", "c32"), samples.clone(), None, MINUTE);
    assert_eq!(target(&e), Some("c8"));
    assert_eq!(
        e.action.expect("action").reason,
        LoadScaleReason::OutOfMemory
    );

    let e = eval("c4", bounds("c1", "c4"), samples, None, MINUTE);
    assert_eq!(e.status, LoadStatus::AtMax);
}

#[mz_ore::test]
fn evaluate_reports_skew_and_does_not_scale() {
    // Behind, one worker saturated, yet the load fits the current size.
    let m = LoadMeasurement {
        max_worker_utilization: 990,
        min_progress: 600,
        ..measurement(1500, 4)
    };
    let samples = steady(0, 2 * HOUR, "c8", &m);
    let e = eval("c8", bounds("c1", "c32"), samples, None, 2 * HOUR);
    assert_eq!(e.status, LoadStatus::SkewLimited);
    assert_eq!(e.action, None);
}

#[mz_ore::test]
fn evaluate_never_scales_down_a_lagging_cluster() {
    let m = LoadMeasurement {
        min_progress: 600,
        ..measurement(500, 1)
    };
    let e = eval(
        "c8",
        bounds("c1", "c32"),
        steady(0, 2 * HOUR, "c8", &m),
        None,
        2 * HOUR,
    );
    assert_eq!(e.status, LoadStatus::Steady);
    assert_eq!(e.action, None);
}

#[mz_ore::test]
fn evaluate_scales_down_after_a_quiet_window() {
    // 1.2 cores need 1.72 at the target: c2.
    let m = measurement(1200, 2);
    let e = eval(
        "c8",
        bounds("c1", "c32"),
        steady(0, 2 * HOUR, "c8", &m),
        None,
        2 * HOUR,
    );
    assert_eq!(e.status, LoadStatus::ScalingDown);
    assert_eq!(target(&e), Some("c2"));
}

#[mz_ore::test]
fn evaluate_scale_down_takes_the_window_maximum() {
    // One busy interval inside the window holds the size at what it needed.
    let mut samples = steady(0, 2 * HOUR, "c8", &measurement(1200, 2));
    samples[90] = sample(90 * MINUTE, "c8", measurement(2500, 2));
    let e = eval("c8", bounds("c1", "c32"), samples.clone(), None, 2 * HOUR);
    assert_eq!(target(&e), Some("c4"));

    samples[90] = sample(90 * MINUTE, "c8", measurement(5000, 2));
    let e = eval("c8", bounds("c1", "c32"), samples, None, 2 * HOUR);
    assert_eq!(e.status, LoadStatus::Steady);
    assert_eq!(e.action, None);
}

#[mz_ore::test]
fn evaluate_scale_down_needs_the_window_observed() {
    let m = measurement(1200, 2);
    // Samples reach back only half the window, as after a restart.
    let e = eval(
        "c8",
        bounds("c1", "c32"),
        steady(90 * MINUTE, 2 * HOUR, "c8", &m),
        None,
        2 * HOUR,
    );
    assert_eq!(e.status, LoadStatus::Steady);
    assert_eq!(e.action, None);

    // The window's ends are observed but most of its intervals are missing.
    let mut sparse = steady(0, 10 * MINUTE, "c8", &m);
    sparse.extend(steady(115 * MINUTE, 2 * HOUR, "c8", &m));
    let e = eval("c8", bounds("c1", "c32"), sparse, None, 2 * HOUR);
    assert_eq!(e.action, None);
}

#[mz_ore::test]
fn evaluate_scale_down_window_is_at_least_twice_the_hydration_time() {
    let m = LoadMeasurement {
        hydration_time: Some(Duration::from_secs(2 * 60 * 60)),
        ..measurement(1200, 2)
    };
    let e = eval(
        "c8",
        bounds("c1", "c32"),
        steady(0, 2 * HOUR, "c8", &m),
        None,
        2 * HOUR,
    );
    assert_eq!(e.action, None);
    let e = eval(
        "c8",
        bounds("c1", "c32"),
        steady(0, 4 * HOUR, "c8", &m),
        None,
        4 * HOUR,
    );
    assert_eq!(target(&e), Some("c2"));
}

#[mz_ore::test]
fn evaluate_honors_the_policy_scale_down_window() {
    let m = measurement(1200, 2);
    let bounds = LoadBounds {
        scale_down_after: Some(Duration::from_secs(4 * 60 * 60)),
        ..bounds("c1", "c32")
    };
    let e = eval("c8", bounds, steady(0, 2 * HOUR, "c8", &m), None, 2 * HOUR);
    assert_eq!(e.action, None);
}

#[mz_ore::test]
fn evaluate_reports_at_min() {
    let m = measurement(100, 1);
    let e = eval(
        "c4",
        bounds("c4", "c32"),
        steady(0, 2 * HOUR, "c4", &m),
        None,
        2 * HOUR,
    );
    assert_eq!(e.status, LoadStatus::AtMin);
    assert_eq!(e.action, None);
}

#[mz_ore::test]
fn evaluate_is_steady_under_stable_load() {
    // 2.5 cores on c4 is 62% utilization: no smaller size fits, none larger
    // is needed, for as long as the load holds.
    let m = measurement(2500, 8);
    let samples = steady(0, 6 * HOUR, "c4", &m);
    for now in (HOUR..=6 * HOUR).step_by(usize::cast_from(MINUTE)) {
        let window: Vec<_> = samples
            .iter()
            .filter(|s| u64::from(s.at) <= now)
            .cloned()
            .collect();
        let e = eval("c4", bounds("c1", "c32"), window, None, now);
        assert_eq!(e.status, LoadStatus::Steady, "at {now}");
        assert_eq!(e.action, None, "at {now}");
    }
}

#[mz_ore::test]
fn evaluate_ignores_samples_from_another_size_and_from_the_warm_up() {
    let record = LoadRecord {
        last_cutover: Some(ts(HOUR)),
        ..Default::default()
    };
    // Heavy samples at the old size, and at the new size during its warm-up.
    let mut samples = steady(0, HOUR, "c4", &measurement(20000, 4));
    samples.extend(steady(
        HOUR + MINUTE,
        HOUR + 4 * MINUTE,
        "c8",
        &measurement(20000, 4),
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
        &measurement(3000, 4),
    ));
    let e = eval(
        "c8",
        bounds("c1", "c32"),
        samples,
        Some(&record),
        HOUR + 7 * MINUTE,
    );
    assert_eq!(e.status, LoadStatus::Steady);
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

#[mz_ore::test]
fn evaluate_scale_down_steps_around_a_backed_off_target() {
    let m = measurement(1200, 2);
    let samples = steady(0, 2 * HOUR, "c8", &m);

    // c2 failed: go to c4 instead.
    let record = backoff("c2", LoadDirection::Down, 3 * HOUR);
    let e = eval("c8", bounds("c1", "c32"), samples.clone(), Some(&record), 2 * HOUR);
    assert_eq!(target(&e), Some("c4"));

    // c4 failed, which rules out everything below it too.
    let record = backoff("c4", LoadDirection::Down, 3 * HOUR);
    let e = eval("c8", bounds("c1", "c32"), samples.clone(), Some(&record), 2 * HOUR);
    assert_eq!(e.status, LoadStatus::BackingOff);
    assert_eq!(e.action, None);

    // An expired entry blocks nothing.
    let record = backoff("c4", LoadDirection::Down, HOUR);
    let e = eval("c8", bounds("c1", "c32"), samples, Some(&record), 2 * HOUR);
    assert_eq!(target(&e), Some("c2"));
}

#[mz_ore::test]
fn evaluate_scale_up_stops_below_a_backed_off_target() {
    let m = measurement(7000, 4);
    let samples = steady(0, 2 * MINUTE, "c4", &m);

    let record = backoff("c16", LoadDirection::Up, HOUR);
    let e = eval("c4", bounds("c1", "c32"), samples.clone(), Some(&record), 2 * MINUTE);
    assert_eq!(target(&e), Some("c8"));

    let record = backoff("c8", LoadDirection::Up, HOUR);
    let e = eval("c4", bounds("c1", "c32"), samples, Some(&record), 2 * MINUTE);
    assert_eq!(e.status, LoadStatus::BackingOff);
    assert_eq!(e.action, None);
}

/// A closed loop over a synthetic replica: `cores^alpha` units of capacity,
/// so `alpha < 1` is the sub-linear scaling the model's linear estimate has to
/// converge under. Returns the sizes visited after the starting one.
fn converge(start: &str, demand: f64, alpha: f64) -> Vec<String> {
    let ladder = ladder();
    let cores = |size: &str| {
        ladder
            .iter()
            .find(|s| s.size == size)
            .expect("on the ladder")
            .cpu_millicores as f64
            / 1000.0
    };
    let observe = |size: &str| {
        let cores = cores(size);
        let capacity = cores.powf(alpha);
        let progress = (capacity / demand).min(1.0);
        let busy_cores = (demand / capacity).min(1.0) * cores;
        LoadMeasurement {
            cpu_demand_millicores: (busy_cores / progress.max(0.25) * 1000.0).ceil() as u64,
            max_worker_utilization: ((busy_cores / cores) * 1000.0) as u32,
            min_progress: (progress.max(0.25) * 1000.0) as u32,
            ..measurement(0, 1)
        }
    };

    let mut size = start.to_string();
    let mut record = LoadRecord::default();
    let mut samples = Vec::new();
    let mut visited = Vec::new();
    // Twelve hours is several scale-down windows past the last change.
    for at in (0..12 * HOUR).step_by(usize::cast_from(MINUTE)) {
        samples.push(sample(at, &size, observe(&size)));
        let e = eval(&size, bounds("c1", "c32"), samples.clone(), Some(&record), at);
        if let Some(action) = e.action {
            size = action.target_size;
            record.last_cutover = Some(ts(at));
            visited.push(size.clone());
        }
    }
    visited
}

#[mz_ore::test]
fn closed_loop_converges_within_three_steps_and_stays() {
    for alpha in [1.0, 0.9, 0.8] {
        for demand in [0.3, 1.0, 2.5, 5.0, 9.0] {
            // The cheapest size whose capacity covers the demand at the
            // target utilization.
            let expected = ladder()
                .into_iter()
                .find(|s| (s.cpu_millicores as f64 / 1000.0).powf(alpha) * 0.7 >= demand)
                .expect("some size fits")
                .size;
            for start in ladder() {
                let visited = converge(&start.size, demand, alpha);
                let context = format!(
                    "alpha {alpha}, demand {demand}, start {}: visited {visited:?}",
                    start.size
                );
                assert!(visited.len() <= 3, "{context}");
                let settled = visited.last().unwrap_or(&start.size);
                assert_eq!(settled, &expected, "{context}");
                let mut distinct = visited.clone();
                distinct.sort();
                distinct.dedup();
                assert_eq!(distinct.len(), visited.len(), "revisited a size: {context}");
            }
        }
    }
}

fn config() -> ConfigSignals {
    ConfigSignals {
        load_enabled: true,
        load: params(),
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

/// Fold a write into the state the way the catalog would.
fn apply(state: &mut ClusterState, write: &StateWrite) {
    if let Some(reconfiguration) = &write.reconfiguration {
        state.reconfiguration = reconfiguration.record.clone();
    }
    if let Some(load) = &write.load {
        state.load = load.record.clone();
    }
}

fn audits(write: &StateWrite) -> Vec<LoadAudit> {
    write.load.as_ref().map(|w| w.audit.clone()).unwrap_or_default()
}

fn busy_samples(size: &str) -> Vec<LoadSample> {
    steady(0, 2 * MINUTE, size, &measurement(7000, 4))
}

/// A state whose scale-up from `c4` to `c16` is in flight.
fn scaling_up() -> ClusterState {
    let mut s = state("c4");
    let write = update(&s, busy_samples("c4"), 2 * MINUTE);
    apply(&mut s, &write);
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
    let write = OnLoadStrategy.update_state(&state("c4"), &LiveSignals::default(), &config(), ts(0));
    assert!(write.is_empty());
}

#[mz_ore::test]
fn strategy_starts_a_rollback_reconfiguration_and_contributes_no_replicas() {
    let s = state("c4");
    let write = update(&s, busy_samples("c4"), 2 * MINUTE);

    let record = write
        .reconfiguration
        .as_ref()
        .and_then(|w| w.record.as_ref())
        .expect("reconfiguration written");
    assert_eq!(record.target.size, "c16");
    assert_eq!(record.target.replication_factor, 1);
    assert_eq!(record.status, ReconfigurationStatus::InProgress);
    assert_eq!(record.on_timeout, OnTimeout::Rollback);
    assert_eq!(record.origin, ReconfigurationOrigin::LoadAutoscaling);
    assert_eq!(record.deadline, ts(2 * MINUTE + 24 * HOUR));

    let attempt = write
        .load
        .as_ref()
        .and_then(|w| w.record.as_ref())
        .and_then(|r| r.attempt.as_ref())
        .expect("attempt recorded");
    assert_eq!(attempt.from_size, "c4");
    assert_eq!(attempt.target_size, "c16");
    assert_eq!(attempt.deadline, record.deadline);
    assert_eq!(attempt.phase, LoadAttemptPhase::Reconfiguring);
    assert!(matches!(
        audits(&write).as_slice(),
        [LoadAudit::ScaleStarted {
            reason: LoadScaleReason::ScaleUp,
            cpu_demand_millicores: 7000,
            ..
        }]
    ));

    assert!(
        OnLoadStrategy
            .desired_replicas(&s, &live(busy_samples("c4")), &config(), ts(2 * MINUTE))
            .is_empty()
    );
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
    let mut s = scaling_up();
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
        let record = write.load.as_ref().expect("write").record.clone().expect("record");
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
fn strategy_backs_off_a_failed_scale_down_in_the_down_direction() {
    let mut s = state("c8");
    let write = update(&s, steady(0, 2 * HOUR, "c8", &measurement(1200, 2)), 2 * HOUR);
    apply(&mut s, &write);
    assert_eq!(
        s.reconfiguration.as_ref().expect("written").target.size,
        "c2"
    );
    settle(&mut s, ReconfigurationStatus::TimedOut);
    let write = update(&s, Vec::new(), 26 * HOUR);
    let record = write.load.expect("write").record.expect("record");
    assert_eq!(record.backoff[0].direction, LoadDirection::Down);
    assert_eq!(record.backoff[0].size, "c2");
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

fn lagging(progress: u32, cpu_millicores: u64) -> LoadMeasurement {
    LoadMeasurement {
        min_progress: progress,
        max_worker_utilization: 990,
        ..measurement(cpu_millicores, 4)
    }
}

/// A state that scaled up from `c4` to `c8` because of lag and has cut over.
fn evaluating() -> ClusterState {
    let mut s = state("c4");
    let write = update(&s, steady(0, 2 * MINUTE, "c4", &lagging(700, 5000)), 2 * MINUTE);
    apply(&mut s, &write);
    settle(&mut s, ReconfigurationStatus::Finalized);
    let write = update(&s, Vec::new(), HOUR);
    apply(&mut s, &write);
    let attempt = s.load.as_ref().and_then(|r| r.attempt.as_ref()).expect("attempt");
    assert_eq!(attempt.phase, LoadAttemptPhase::Evaluating);
    assert_eq!(attempt.progress_before, Some(700));
    s
}

#[mz_ore::test]
fn strategy_reverts_a_scale_up_that_did_not_help() {
    let s = evaluating();

    // Inside the warm-up there is nothing to judge yet.
    let early = steady(HOUR + MINUTE, HOUR + 4 * MINUTE, "c8", &lagging(700, 2000));
    assert!(update(&s, early, HOUR + 4 * MINUTE).is_empty());

    // Still behind by as much as before, on twice the cores.
    let after = steady(HOUR + 5 * MINUTE, HOUR + 7 * MINUTE, "c8", &lagging(720, 2000));
    let write = update(&s, after, HOUR + 7 * MINUTE);
    let reconfiguration = write
        .reconfiguration
        .as_ref()
        .and_then(|w| w.record.as_ref())
        .expect("revert written");
    assert_eq!(reconfiguration.target.size, "c4");
    let record = write.load.as_ref().expect("write").record.clone().expect("record");
    assert_eq!(record.backoff[0].size, "c8");
    assert_eq!(record.backoff[0].direction, LoadDirection::Up);
    assert_eq!(
        record.attempt.expect("attempt").phase,
        LoadAttemptPhase::Reconfiguring
    );
    assert!(matches!(
        audits(&write).as_slice(),
        [
            LoadAudit::BackoffStarted { .. },
            LoadAudit::ScaleStarted {
                reason: LoadScaleReason::IneffectiveScaleUp,
                ..
            }
        ]
    ));
}

#[mz_ore::test]
fn strategy_keeps_a_scale_up_that_helped() {
    for progress in [1000, 850] {
        let s = evaluating();
        let after = steady(
            HOUR + 5 * MINUTE,
            HOUR + 7 * MINUTE,
            "c8",
            &lagging(progress, 2000),
        );
        let write = update(&s, after, HOUR + 7 * MINUTE);
        assert!(write.reconfiguration.is_none(), "progress {progress}");
        let record = write.load.expect("write").record.expect("record");
        assert_eq!(record.attempt, None);
        assert!(record.backoff.is_empty());
    }
}

#[mz_ore::test]
fn strategy_audits_a_hold_once_on_entry_and_once_on_exit() {
    let mut s = state("c8");
    let skewed = steady(0, 2 * MINUTE, "c8", &lagging(600, 1500));

    let write = update(&s, skewed.clone(), 2 * MINUTE);
    assert_eq!(
        audits(&write),
        vec![LoadAudit::HoldChanged {
            hold: Some(LoadHold::SkewLimited)
        }]
    );
    assert!(write.reconfiguration.is_none());
    apply(&mut s, &write);

    assert!(update(&s, skewed, 2 * MINUTE).is_empty());

    let calm = steady(0, 2 * MINUTE, "c8", &measurement(4000, 4));
    let write = update(&s, calm, 2 * MINUTE);
    assert_eq!(audits(&write), vec![LoadAudit::HoldChanged { hold: None }]);
}

#[mz_ore::test]
fn strategy_writes_nothing_when_steady() {
    let s = state("c8");
    let calm = steady(0, 2 * MINUTE, "c8", &measurement(4000, 4));
    assert!(update(&s, calm, 2 * MINUTE).is_empty());
}
