// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The cases of the design's simulation table, by number.

use std::time::Duration;

use mz_adapter_types::cluster_state::{LoadCutoverReason, LoadVerdict, TrialKind};

use super::*;

const STARTS: [&str; 6] = ["c1", "c2", "c4", "c8", "c16", "c32"];

fn size_index(size: &str) -> usize {
    STARTS.iter().position(|s| *s == size).expect("a size")
}

/// A sawtooth with the given median, between `min` and `max`.
fn sawtooth(min: f64, max: f64, median: f64, period: f64, phase: f64) -> Sawtooth {
    Sawtooth {
        min,
        max,
        period,
        shape: ((median - min) / (max - min)).ln() / 0.5f64.ln(),
        phase,
    }
}

fn idle_with_noise(noise: Sawtooth) -> Workload {
    let mut dataflow = DataflowModel::new(1, constant(0.2));
    dataflow.noise = Some(noise);
    Workload::new(vec![dataflow])
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn case_15_sawtooth_fresh_in_the_median_does_not_scale_up_and_descends() {
    for start in STARTS {
        let workload = idle_with_noise(sawtooth(1.0, 6.0, 1.5, 60.0, 3.0));
        let run = run(Scenario::new(workload, start, 14.0));
        assert_eq!(
            run.trials_of(TrialKind::Up),
            0,
            "start {start}: {:?}",
            run.events
        );
        assert_eq!(run.durable.size, "c1", "start {start}: {:?}", run.events);
        for summary in &run.summaries {
            assert!(
                summary.dataflows.values().all(|d| !d.behind),
                "start {start}: {summary:?}"
            );
        }
    }
}

/// Whether `GlobalId::User(1)` is behind in each summary with lag of a sawtooth of 1s to 11s
/// around a median of 6s, per phase, with the size pinned so that no trial or floor changes
/// what the summaries say.
fn sawtooth_behind(period: f64) -> Vec<(f64, Vec<bool>)> {
    [0.0, 3.0, 7.0, 13.0, 29.0]
        .into_iter()
        .map(|phase| {
            let workload = idle_with_noise(sawtooth(1.0, 11.0, 6.0, period, phase));
            let mut scenario = Scenario::new(workload, "c4", 4.0);
            scenario.min = "c4";
            scenario.max = "c4";
            let run = run(scenario);
            let behind: Vec<_> = run
                .summaries
                .iter()
                .filter(|s| !s.hydrating)
                .map(|s| s.dataflows[&GlobalId::User(1)].behind)
                .collect();
            assert!(behind.len() >= 20, "period {period}, phase {phase}");
            (phase, behind)
        })
        .collect()
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn case_16_sawtooth_behind_in_the_median_is_behind_in_every_summary() {
    // From 4 sample intervals to one evaluation window.
    for period in [40.0, 60.0, 95.0, 150.0, 200.0, 300.0, 450.0, 600.0] {
        for (phase, behind) in sawtooth_behind(period) {
            assert!(
                behind.iter().all(|b| *b),
                "period {period}, phase {phase}: {behind:?}"
            );
        }
    }
}

/// The two periods the design names as known limits of the signal: the dataflow is behind in
/// some summaries and not in others.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn case_16_sawtooth_outside_the_period_range_is_a_known_limit() {
    // Twice the sample interval, where the samples can land on the same two points of every
    // cycle, and longer than the evaluation window, where a window with the reset reads as
    // draining.
    for period in [20.0, 900.0] {
        let behind: Vec<bool> = sawtooth_behind(period)
            .into_iter()
            .flat_map(|(_, behind)| behind)
            .collect();
        assert!(behind.iter().any(|b| *b), "period {period}: {behind:?}");
        assert!(behind.iter().any(|b| !*b), "period {period}: {behind:?}");
    }
}

/// Overloaded at c2 for three hours, then nearly idle.
fn step_down_workload(faults: Faults) -> Workload {
    let demand: Curve = Box::new(|h| if h < 3.0 { 3.0 } else { 0.3 });
    let mut workload = Workload::new(vec![DataflowModel::new(1, demand)]);
    workload.faults = faults;
    workload
}

fn up_decisions(run: &Run) -> Vec<Event> {
    run.events
        .iter()
        .filter(|e| match e {
            Event::TrialStarted { kind, .. } | Event::Verdict { kind, .. } => {
                *kind == TrialKind::Up
            }
            _ => false,
        })
        .cloned()
        .collect()
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn case_17_lost_busy_windows_do_not_change_lag_decisions_and_hold_back_descents() {
    let all = run(Scenario::new(
        step_down_workload(Faults::default()),
        "c2",
        16.0,
    ));
    let half = run(Scenario::new(
        step_down_workload(Faults {
            busy_loss: 0.5,
            busy_loss_while_behind: true,
            ..Faults::default()
        }),
        "c2",
        16.0,
    ));
    assert!(!up_decisions(&all).is_empty(), "{:?}", all.events);
    assert_eq!(up_decisions(&all), up_decisions(&half));
    assert!(all.trials_of(TrialKind::Down) > 0, "{:?}", all.events);

    // Every descent proposal had busy time in every summary of its quiet window.
    for (at, kind, from, _) in half.trials() {
        if kind != TrialKind::Down {
            continue;
        }
        let quiet = half
            .summaries
            .iter()
            .filter(|s| s.size == from && u64::from(s.at) <= at && u64::from(s.at) + HOUR > at);
        for summary in quiet {
            assert!(summary.busy.is_some(), "{summary:?}");
        }
    }

    // Without busy time, no descent is proposed.
    let none = run(Scenario::new(
        step_down_workload(Faults {
            busy_loss: 1.0,
            ..Faults::default()
        }),
        "c2",
        16.0,
    ));
    assert_eq!(up_decisions(&all), up_decisions(&none));
    assert_eq!(none.trials_of(TrialKind::Down), 0, "{:?}", none.events);
}

fn fixed_heap(bytes: f64) -> Workload {
    let mut workload = Workload::new(vec![DataflowModel::new(1, constant(0.05))]);
    workload.heap = constant(bytes);
    workload
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn case_18_heap_decides_a_descent() {
    let ladder = ladder();
    for start in 1..ladder.len() {
        let smaller = f64::cast_lossy(ladder[start - 1].heap_limit);
        let start = STARTS[start];
        let run = run_fixed(start, 0.6 * smaller);
        let changes = run.size_changes();
        assert_eq!(changes.len(), 1, "start {start}: {:?}", run.events);
        assert_eq!(size_index(&run.durable.size) + 1, size_index(start));

        let run = run_fixed(start, 0.75 * smaller);
        assert!(
            run.size_changes().is_empty(),
            "start {start}: {:?}",
            run.events
        );
        assert_eq!(run.trials_of(TrialKind::Down), 0);
    }
}

fn run_fixed(start: &'static str, bytes: f64) -> Run {
    run(Scenario::new(fixed_heap(bytes), start, 8.0))
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn case_19_growing_heap_moves_up_before_a_kill_and_never_down() {
    let mut workload = Workload::new(vec![DataflowModel::new(1, constant(0.1))]);
    // Half of c2's 16 GiB, growing 5% an hour.
    workload.heap = Box::new(|h| 8.0 * GIB * 1.05f64.powf(h));
    let run = run(Scenario::new(workload, "c2", 36.0));
    assert_eq!(run.kills(), 0, "{:?}", run.events);
    let moves: Vec<_> = run
        .events
        .iter()
        .filter_map(|e| match e {
            Event::Reconfigured { at, target, reason } => Some((*at, target.clone(), *reason)),
            _ => None,
        })
        .collect();
    assert!(moves.len() >= 2, "{:?}", run.events);
    for (at, target, reason) in &moves {
        assert_eq!(*reason, LoadCutoverReason::HighHeap);
        let hours = f64::cast_lossy(at - T0) / f64::cast_lossy(HOUR);
        let heap = 8.0 * GIB * 1.05f64.powf(hours);
        let limit = f64::cast_lossy(ladder()[size_index(target)].heap_limit);
        assert!(heap / limit <= 0.7, "moved to {target} at {}", heap / limit);
    }
    assert_eq!(run.trials_of(TrialKind::Down), 0, "{:?}", run.events);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn case_20_heap_past_the_limit_moves_to_twice_the_limit() {
    let mut workload = Workload::new(vec![DataflowModel::new(1, constant(0.1))]);
    // 40% of c2, jumping to 130% of it at three hours.
    workload.heap = Box::new(|h| if h < 3.0 { 6.4 * GIB } else { 20.8 * GIB });
    let run = run(Scenario::new(workload, "c2", 24.0));
    assert!(run.kills() > 0);
    let moves: Vec<_> = run
        .events
        .iter()
        .filter_map(|e| match e {
            Event::Reconfigured { target, reason, .. } => Some((target.clone(), *reason)),
            _ => None,
        })
        .collect();
    assert_eq!(
        moves,
        vec![("c4".to_string(), LoadCutoverReason::OutOfMemory)]
    );
    let record = run.durable.load.clone().expect("a record");
    assert!(
        record
            .evidence
            .iter()
            .any(|e| e.size == "c2" && e.verdict == LoadVerdict::Insufficient)
    );
    assert_eq!(run.durable.size, "c4");
    assert!(
        !run.trials()
            .iter()
            .any(|(_, kind, _, target)| *kind == TrialKind::Down && target == "c2"),
        "{:?}",
        run.events
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn case_21_unbounded_heap_climbs_to_max_and_reports_out_of_memory() {
    let mut workload = Workload::new(vec![DataflowModel::new(1, constant(0.1))]);
    // 30% of c1, doubling every three hours.
    workload.heap = Box::new(|h| 2.4 * GIB * 2f64.powf(h / 3.0));
    let run = run(Scenario::new(workload, "c1", 30.0));
    assert_eq!(run.durable.size, "c32", "{:?}", run.events);
    assert!(
        run.statuses
            .iter()
            .any(|(_, s)| *s == LoadStatus::OutOfMemoryAtMax)
    );
    assert_eq!(run.final_status(), LoadStatus::OutOfMemoryAtMax);
    assert!(run.trials().is_empty(), "{:?}", run.events);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn case_23_cpu_needs_four_sizes_above_memory() {
    for start in ["c1", "c2", "c4", "c8"] {
        // 9.6 cores of demand: c16 keeps up, c8 does not. 4 GiB of heap fits c1 at 50%.
        let mut workload = Workload::new(vec![DataflowModel::new(1, constant(9.6))]);
        workload.heap = constant(4.0 * GIB);
        let run = run(Scenario::new(workload, start, 16.0));
        assert_eq!(run.durable.size, "c16", "start {start}: {:?}", run.events);
        assert_eq!(
            run.trials_of(TrialKind::Down),
            0,
            "start {start}: {:?}",
            run.events
        );
        let last = run.summaries.last().expect("a summary");
        assert!(last.heap.unwrap().fraction.unwrap() < 0.5);
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn case_28_crash_loop_without_memory_kill_changes_nothing() {
    let mut workload = Workload::new(vec![DataflowModel::new(1, constant(3.0))]);
    workload.faults.replica_restart_every = Some(Duration::from_secs(6 * 60));
    let run = run(Scenario::new(workload, "c4", 6.0));
    assert!(run.summaries.iter().all(|s| s.dataflows.is_empty()));
    assert!(run.trials().is_empty(), "{:?}", run.events);
    assert!(run.size_changes().is_empty());
    assert_eq!(run.final_status(), LoadStatus::WarmingUp);
}

/// Settled at c4 with 2 cores of demand, then an object that takes 30 minutes to hydrate and
/// saturates the workers is created at six hours.
fn late_object_workload(batching_gain: f64) -> Workload {
    let mut steady = DataflowModel::new(1, constant(2.0));
    steady.batching_gain = batching_gain;
    let mut late = DataflowModel::new(2, constant(0.1));
    late.created_at = 6.0;
    late.hydration = Some(Duration::from_secs(30 * 60));
    let mut workload = Workload::new(vec![steady, late]);
    workload.hydration_share = 1.0;
    workload
}

fn check_case_31(run: &Run) {
    let created = T0 + 6 * HOUR;
    let after: Vec<_> = run
        .summaries
        .iter()
        .filter(|s| u64::from(s.start) >= created && u64::from(s.at) <= created + 3 * HOUR)
        .collect();
    assert!(after.iter().any(|s| s.hydrating));
    for summary in after {
        assert!(summary.dataflows.values().all(|d| !d.behind), "{summary:?}");
    }
    let late: Vec<_> = run
        .trials()
        .into_iter()
        .filter(|(at, ..)| *at >= created)
        .collect();
    assert!(late.is_empty(), "{late:?}");
    assert!(
        run.size_changes()
            .iter()
            .all(|(at, ..)| *at < created || *at > created + 3 * HOUR),
        "{:?}",
        run.events
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn case_31_hydration_of_a_new_object_is_not_load() {
    let mut scenario = Scenario::new(late_object_workload(1.0), "c4", 10.0);
    scenario.min = "c4";
    check_case_31(&run(scenario));
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn case_31_with_batching_the_backlog_drains_and_is_not_behind() {
    let mut scenario = Scenario::new(late_object_workload(3.0), "c4", 10.0);
    scenario.min = "c4";
    check_case_31(&run(scenario));
}

/// An idle cluster at c8 on which an object is created every `every` hours from the first
/// hour, each hydrating for 10 minutes.
fn hourly_objects(every: Option<f64>) -> Workload {
    let mut dataflows = vec![DataflowModel::new(1, constant(0.1))];
    if let Some(every) = every {
        let count = u64::cast_lossy((12.0 / every).floor());
        for i in 0..count {
            let mut object = DataflowModel::new(100 + i, constant(0.001));
            object.created_at = 1.0 + every * f64::cast_lossy(i) + 0.05;
            object.hydration = Some(Duration::from_secs(10 * 60));
            dataflows.push(object);
        }
    }
    let mut workload = Workload::new(dataflows);
    workload.hydration_share = 0.5;
    workload
}

fn first_descent(run: &Run) -> Option<u64> {
    run.size_changes()
        .iter()
        .find(|(_, from, to)| size_index(to) < size_index(from))
        .map(|(at, ..)| *at)
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn case_32_hydrating_windows_delay_a_descent_and_frequent_ones_prevent_it() {
    let scenario = |every| {
        let mut scenario = Scenario::new(hourly_objects(every), "c8", 12.0);
        scenario.scale_down_after = Some(Duration::from_secs(3600));
        scenario
    };
    let quiet = run(scenario(None));
    let hourly = run(scenario(Some(1.0)));
    let quarter = run(scenario(Some(0.25)));
    let without = first_descent(&quiet).expect("descends without hydrations");
    let with = first_descent(&hourly).expect("descends with hourly hydrations");
    assert!(with > without, "{with} <= {without}");
    assert!(hourly.summaries.iter().any(|s| s.hydrating));
    // Descents with a quarter-hourly hydration are only possible before the first object.
    assert!(
        quarter
            .trials()
            .iter()
            .all(|(at, kind, ..)| *kind != TrialKind::Down || *at < T0 + HOUR),
        "{:?}",
        quarter.events
    );
}
