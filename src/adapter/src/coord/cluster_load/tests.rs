// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bytesize::ByteSize;
use mz_cluster_client::ReplicaId;
use mz_cluster_controller::load::{
    BusySummary, DataflowFrontiers, DataflowSummary, HeapSummary, LoadSummary, ProcessHeap,
    SummaryParams,
};
use mz_controller::clusters::ReplicaAllocation;
use mz_controller_types::ClusterId;
use mz_orchestrator::{DiskLimit, MemoryLimit, ServiceProcessMetrics};
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};

use super::{
    ClusterLoad, ClusterObservation, ReplicaObservation, decode_summaries, fullest_process,
    next_boundary, process_heap_limit, summary_rows, timestamptz,
};

const SIZE: &str = "100cc";
const C1: ClusterId = ClusterId::User(1);
const R1: ReplicaId = ReplicaId::User(1);
const D1: GlobalId = GlobalId::User(1);

/// Windows of 60s sampled every 5s, with a warm-up of 30s.
fn params() -> SummaryParams {
    SummaryParams {
        window: Duration::from_secs(60),
        sample_interval: Duration::from_secs(5),
        warm_up: Duration::from_secs(30),
        lag_tolerance: Duration::from_secs(2),
        floor_factor: 1.5,
    }
}

fn observation(at: u64, size: &str, hydrated: bool, lag_ms: u64) -> ClusterObservation {
    let replica = ReplicaObservation {
        size: size.into(),
        hydrated,
        dataflows: BTreeMap::from([(
            D1,
            DataflowFrontiers {
                input: Timestamp::new(at),
                output: Timestamp::new(at.saturating_sub(lag_ms)),
            },
        )]),
        heap: Some(ProcessHeap {
            bytes: 100,
            limit: Some(1000),
        }),
    };
    ClusterObservation {
        size: SIZE.into(),
        record: None,
        exports: BTreeSet::from([D1]),
        replicas: BTreeMap::from([(R1, replica)]),
    }
}

/// Records samples every 5s in `[from, to)` and returns the summaries they produce.
fn run(
    load: &mut ClusterLoad,
    from: u64,
    to: u64,
    observe: impl Fn(u64) -> ClusterObservation,
) -> Vec<(ClusterId, LoadSummary)> {
    let mut summaries = Vec::new();
    for at in (from..to).step_by(5_000) {
        let observations = BTreeMap::from([(C1, observe(at))]);
        summaries.extend(load.record(Timestamp::new(at), observations, &params()));
    }
    summaries
}

#[mz_ore::test]
fn record_summarizes_each_window_after_warm_up() {
    let mut load = ClusterLoad::default();
    // The replica becomes eligible at 60s, so the window [60s, 120s) is in warm-up.
    let summaries = run(&mut load, 60_000, 245_000, |at| {
        observation(at, SIZE, true, 1_000)
    });
    let ends: Vec<_> = summaries.iter().map(|(_, s)| u64::from(s.at)).collect();
    assert_eq!(ends, vec![180_000, 240_000]);
    let (cluster_id, summary) = &summaries[0];
    assert_eq!(*cluster_id, C1);
    assert_eq!(summary.dataflows[&D1].median_lag, Duration::from_secs(1));
    assert_eq!(summary.heap.unwrap().fraction, Some(0.1));
    assert_eq!(load.summaries(C1).count(), 2);
}

#[mz_ore::test]
fn hydrating_replica_yields_no_summary_for_its_window() {
    let mut load = ClusterLoad::default();
    let summaries = run(&mut load, 0, 245_000, |at| {
        // A new dataflow hydrates during [130s, 140s) on the running replica.
        let hydrated = !(130_000..140_000).contains(&at);
        observation(at, SIZE, hydrated, 1_000)
    });
    let ends: Vec<_> = summaries.iter().map(|(_, s)| u64::from(s.at)).collect();
    // The first window is in warm-up. A rehydration does not restart the warm-up, so the window
    // after the hydration is summarized.
    assert_eq!(ends, vec![120_000, 240_000]);
}

#[mz_ore::test]
fn size_change_and_restart_restart_the_warm_up() {
    let mut load = ClusterLoad::default();
    let summaries = run(&mut load, 0, 125_000, |at| {
        observation(at, "50cc", true, 1_000)
    });
    assert!(
        summaries.is_empty(),
        "a replica at another size never qualifies"
    );

    // At the realized size from 125s, eligible then, so warm-up covers [120s, 180s).
    let summaries = run(&mut load, 125_000, 305_000, |at| {
        observation(at, SIZE, true, 1_000)
    });
    let ends: Vec<_> = summaries.iter().map(|(_, s)| u64::from(s.at)).collect();
    assert_eq!(ends, vec![240_000, 300_000]);

    load.note_restarted(R1);
    let summaries = run(&mut load, 305_000, 365_000, |at| {
        observation(at, SIZE, true, 1_000)
    });
    assert!(summaries.is_empty(), "a restarted replica warms up again");
}

#[mz_ore::test]
fn busy_rows_and_kills_reach_the_summary() {
    let mut load = ClusterLoad::default();
    run(&mut load, 0, 60_000, |at| {
        observation(at, SIZE, true, 1_000)
    });

    let busy_row = |start: u64, end: u64| {
        Row::pack_slice(&[
            Datum::String(&R1.to_string()),
            timestamptz(Timestamp::new(start)),
            timestamptz(Timestamp::new(end)),
            Datum::UInt64(2),
            Datum::UInt64(30_000_000_000),
            Datum::UInt64(20_000_000_000),
        ])
    };
    load.observe_busy_rows(
        R1,
        &[
            (busy_row(60_000, 90_000), Diff::ONE),
            (busy_row(90_000, 120_000), Diff::ONE),
            // A retraction is not a window.
            (busy_row(60_000, 90_000), Diff::MINUS_ONE),
        ],
    );
    load.note_memory_kill(C1, R1, SIZE.into(), Timestamp::new(100_000));
    let summaries = run(&mut load, 60_000, 125_000, |at| {
        observation(at, SIZE, true, 1_000)
    });
    let (_, summary) = summaries.last().unwrap();
    assert_eq!(u64::from(summary.at), 120_000);
    let busy = summary.busy.unwrap();
    assert_eq!(busy.mean_millicores, 1000);
    assert!((busy.busiest_worker_utilization - 2.0 / 3.0).abs() < 1e-9);
    assert!(summary.killed);

    // The kill and the busy windows belong to their window only.
    let summaries = run(&mut load, 125_000, 185_000, |at| {
        observation(at, SIZE, true, 1_000)
    });
    let (_, summary) = summaries.last().unwrap();
    assert!(!summary.killed);
    assert_eq!(summary.busy, None);
}

#[mz_ore::test]
fn kill_of_a_replica_that_never_qualifies_is_summarized() {
    let mut load = ClusterLoad::default();
    load.note_memory_kill(C1, R1, SIZE.into(), Timestamp::new(10_000));
    let summaries = run(&mut load, 0, 65_000, |at| observation(at, SIZE, false, 0));
    assert_eq!(summaries.len(), 1);
    assert!(summaries[0].1.killed);
    assert!(summaries[0].1.dataflows.is_empty());
}

#[mz_ore::test]
fn stale_samples_are_ignored() {
    let mut load = ClusterLoad::default();
    run(&mut load, 0, 125_000, |at| {
        observation(at, SIZE, true, 1_000)
    });
    let before = load.summaries(C1).count();
    // A sample from an already summarized window, delivered late.
    let late = BTreeMap::from([(C1, observation(50_000, SIZE, true, 1_000))]);
    assert!(
        load.record(Timestamp::new(50_000), late, &params())
            .is_empty()
    );
    assert_eq!(load.summaries(C1).count(), before);
}

#[mz_ore::test]
fn hydration_peak_is_the_largest_heap_before_first_hydration() {
    let mut load = ClusterLoad::default();
    run(&mut load, 0, 30_000, |at| {
        let mut observation = observation(at, SIZE, at >= 20_000, 0);
        let heap = &mut observation.replicas.get_mut(&R1).unwrap().heap;
        heap.as_mut().unwrap().bytes = if at == 10_000 { 900 } else { 500 };
        observation
    });
    // Heap after hydration does not count.
    run(&mut load, 30_000, 40_000, |at| {
        let mut observation = observation(at, SIZE, true, 0);
        observation.replicas.get_mut(&R1).unwrap().heap = Some(ProcessHeap {
            bytes: 5_000,
            limit: Some(1000),
        });
        observation
    });
    assert_eq!(load.hydration_peak(R1).unwrap().bytes, 900);
}

#[mz_ore::test]
fn restore_keeps_summaries_in_memory_and_retain_prunes() {
    let mut load = ClusterLoad::default();
    run(&mut load, 0, 185_000, |at| {
        observation(at, SIZE, true, 1_000)
    });
    let in_memory: Vec<_> = load.summaries(C1).cloned().collect();
    assert_eq!(in_memory.len(), 2);

    let mut restored = in_memory[0].clone();
    restored.killed = true;
    let mut older = in_memory[0].clone();
    older.at = Timestamp::new(0);
    load.restore(vec![(C1, restored), (C1, older.clone())]);
    let summaries: Vec<_> = load.summaries(C1).cloned().collect();
    assert_eq!(summaries.len(), 3);
    assert_eq!(summaries[0], older);
    assert_eq!(summaries[1], in_memory[0], "the in-memory summary wins");

    load.retain(
        &BTreeSet::from([C1]),
        &BTreeSet::from([R1]),
        Timestamp::new(60_000),
    );
    assert_eq!(load.summaries(C1).count(), 2);
    load.retain(&BTreeSet::new(), &BTreeSet::new(), Timestamp::new(0));
    assert_eq!(load.summaries(C1).count(), 0);
}

#[mz_ore::test]
fn rows_round_trip() {
    let full = LoadSummary {
        start: Timestamp::new(600_000),
        at: Timestamp::new(1_200_000),
        size: SIZE.into(),
        fingerprint: u64::MAX,
        dataflows: BTreeMap::from([
            (
                GlobalId::User(7),
                DataflowSummary {
                    median_lag: Duration::from_millis(31_500),
                    progress_ratio: Some(0.75),
                    behind: true,
                },
            ),
            (
                GlobalId::System(3),
                DataflowSummary {
                    median_lag: Duration::ZERO,
                    progress_ratio: None,
                    behind: false,
                },
            ),
        ]),
        heap: Some(HeapSummary {
            bytes: 1 << 40,
            fraction: Some(0.5),
        }),
        busy: Some(BusySummary {
            mean_millicores: 1500,
            busiest_worker_utilization: 0.25,
        }),
        killed: false,
    };
    let empty = LoadSummary {
        start: Timestamp::new(1_200_000),
        at: Timestamp::new(1_800_000),
        dataflows: BTreeMap::new(),
        heap: Some(HeapSummary {
            bytes: 10,
            fraction: None,
        }),
        busy: None,
        killed: true,
        ..full.clone()
    };
    let mut cluster_rows = Vec::new();
    let mut dataflow_rows = Vec::new();
    for summary in [&full, &empty] {
        let (cluster_row, rows) = summary_rows(C1, summary);
        cluster_rows.push(cluster_row);
        dataflow_rows.extend(rows);
    }
    // A dataflow row whose cluster row is gone, after truncation, is dropped.
    let (_, orphans) = summary_rows(
        ClusterId::User(2),
        &LoadSummary {
            at: Timestamp::new(0),
            ..full.clone()
        },
    );
    dataflow_rows.extend(orphans);
    let decoded = decode_summaries(&cluster_rows, &dataflow_rows);
    assert_eq!(decoded, vec![(C1, full), (C1, empty)]);
}

#[mz_ore::test]
fn heap_of_the_fullest_process() {
    let process =
        |heap: Option<u64>, memory: Option<u64>, limit: Option<u64>| ServiceProcessMetrics {
            cpu_nano_cores: None,
            memory_bytes: memory,
            disk_bytes: None,
            heap_bytes: heap,
            heap_limit: limit,
            swap_bytes: None,
        };
    // By fraction of the limit, not by bytes.
    let metrics = [
        process(Some(600), None, Some(2000)),
        process(Some(500), None, Some(1000)),
    ];
    assert_eq!(
        fullest_process(&metrics, None),
        Some(ProcessHeap {
            bytes: 500,
            limit: Some(1000),
        })
    );
    // Memory stands in for the heap, and the size's limit for the process's.
    let metrics = [process(None, Some(300), None)];
    assert_eq!(
        fullest_process(&metrics, Some(1000)),
        Some(ProcessHeap {
            bytes: 300,
            limit: Some(1000),
        })
    );
    assert_eq!(fullest_process(&[process(None, None, None)], None), None);
}

#[mz_ore::test]
fn heap_limit_of_a_size() {
    let mut allocation: ReplicaAllocation =
        serde_json::from_str(r#"{"scale": 1, "workers": 2, "credits_per_hour": "1"}"#).unwrap();
    assert_eq!(process_heap_limit(&allocation), None);
    allocation.memory_limit = Some(MemoryLimit(ByteSize::gib(4)));
    assert_eq!(process_heap_limit(&allocation), Some(4 << 30));
    allocation.disk_limit = Some(DiskLimit(ByteSize::gib(8)));
    assert_eq!(process_heap_limit(&allocation), Some(4 << 30));
    allocation.swap_enabled = true;
    assert_eq!(process_heap_limit(&allocation), Some(12 << 30));
    allocation.disk_limit = Some(DiskLimit(ByteSize::gib(0)));
    assert_eq!(process_heap_limit(&allocation), Some(4 << 30));
}

#[mz_ore::test]
fn next_boundary_is_strictly_after() {
    let interval = Duration::from_secs(10);
    assert_eq!(next_boundary(0, interval), 10_000);
    assert_eq!(next_boundary(9_999, interval), 10_000);
    assert_eq!(next_boundary(10_000, interval), 20_000);
}
