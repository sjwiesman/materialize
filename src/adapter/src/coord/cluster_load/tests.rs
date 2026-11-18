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
use itertools::Itertools;
use mz_catalog::builtin::MZ_CLUSTER_LOAD_RECOMMENDATIONS;
use mz_catalog::config::ClusterReplicaSizeMap;
use mz_catalog::memory::objects::ClusterVariantManaged;
use mz_cluster_client::ReplicaId;
use mz_cluster_controller::load::rules::RuleParams;
use mz_cluster_controller::load::{
    BusySummary, DataflowFrontiers, DataflowSummary, HeapSummary, LoadSummary, ProcessHeap,
    SummaryParams,
};
use mz_controller::clusters::ReplicaAllocation;
use mz_controller_types::ClusterId;
use mz_dyncfg::ConfigSet;
use mz_orchestrator::{DiskLimit, MemoryLimit, ServiceProcessMetrics};
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::{Datum, Diff, GlobalId, Row, SqlScalarType, Timestamp};
use mz_sql::plan::{AutoScalingStrategy, OnLoad};

use super::{
    ClusterLoad, ClusterObservation, ReplicaObservation, RestoredSummary, decode_summaries,
    fullest_process, load_rules_inputs, next_boundary, recommendation_row, summary_rows,
    timestamptz,
};
use crate::catalog::cluster_state::process_heap_limit;

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
        draining_progress_ratio: 1.1,
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
    assert_eq!(load.summaries(C1).len(), 2);
}

#[mz_ore::test]
fn hydrating_replica_yields_a_hydrating_window() {
    let mut load = ClusterLoad::default();
    let summaries = run(&mut load, 0, 245_000, |at| {
        // A new dataflow hydrates during [130s, 140s) on the running replica.
        let hydrated = !(130_000..140_000).contains(&at);
        observation(at, SIZE, hydrated, 1_000)
    });
    let ends: Vec<_> = summaries
        .iter()
        .map(|(_, s)| (u64::from(s.at), s.hydrating))
        .collect();
    // The first window is in warm-up, which is not hydration. A rehydration does not restart the
    // warm-up, so the window after the hydration is summarized.
    assert_eq!(
        ends,
        vec![(120_000, false), (180_000, true), (240_000, false)]
    );
    assert!(summaries[1].1.dataflows.is_empty());
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
    let before = load.summaries(C1).len();
    // A sample from an already summarized window, delivered late.
    let late = BTreeMap::from([(C1, observation(50_000, SIZE, true, 1_000))]);
    assert!(
        load.record(Timestamp::new(50_000), late, &params())
            .is_empty()
    );
    assert_eq!(load.summaries(C1).len(), before);
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
    assert_eq!(load.hydration_peak(C1, SIZE), Some(900));
    assert_eq!(load.hydration_peak(C1, "50cc"), None);
}

#[mz_ore::test]
fn restore_keeps_summaries_in_memory_and_retain_prunes() {
    let mut load = ClusterLoad::default();
    run(&mut load, 0, 185_000, |at| {
        observation(at, SIZE, true, 1_000)
    });
    let in_memory: Vec<_> = load.summaries(C1).to_vec();
    assert_eq!(in_memory.len(), 2);

    let mut restored_summary = in_memory[0].clone();
    restored_summary.killed = true;
    let mut older = in_memory[0].clone();
    older.at = Timestamp::new(0);
    let restored = |summary| RestoredSummary {
        cluster_id: C1,
        summary,
        hydration_time: None,
    };
    load.restore(vec![restored(restored_summary), restored(older.clone())]);
    let summaries: Vec<_> = load.summaries(C1).to_vec();
    assert_eq!(summaries.len(), 3);
    assert_eq!(summaries[0], older);
    assert_eq!(summaries[1], in_memory[0], "the in-memory summary wins");

    load.retain(
        &BTreeSet::from([C1]),
        &BTreeSet::from([R1]),
        Timestamp::new(60_000),
    );
    assert_eq!(load.summaries(C1).len(), 2);
    load.retain(&BTreeSet::new(), &BTreeSet::new(), Timestamp::new(0));
    assert_eq!(load.summaries(C1).len(), 0);
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
        hydrating: false,
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
        hydrating: true,
        ..full.clone()
    };
    let mut cluster_rows = Vec::new();
    let mut dataflow_rows = Vec::new();
    let took = Some(Duration::from_millis(90_500));
    for (summary, hydration_time) in [(&full, took), (&empty, None)] {
        let (cluster_row, rows) = summary_rows(C1, summary, hydration_time);
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
        None,
    );
    dataflow_rows.extend(orphans);
    let decoded = decode_summaries(&cluster_rows, &dataflow_rows);
    let restored = |summary, hydration_time| RestoredSummary {
        cluster_id: C1,
        summary,
        hydration_time,
    };
    assert_eq!(decoded, vec![restored(full, took), restored(empty, None)]);
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

#[mz_ore::test]
fn hydration_time_runs_from_the_first_sample_to_hydrated() {
    let mut load = ClusterLoad::default();
    // R1 is running at the first sample, so its hydration started at an unknown time.
    run(&mut load, 0, 20_000, |at| {
        observation(at, SIZE, at >= 10_000, 0)
    });
    assert_eq!(load.last_hydration(C1, SIZE), None);

    // After a restart, R1 hydrates from 20s to 45s.
    load.note_restarted(R1);
    run(&mut load, 20_000, 60_000, |at| {
        observation(at, SIZE, at >= 45_000, 0)
    });
    assert_eq!(load.last_hydration(C1, SIZE), Some(Duration::from_secs(25)));
    assert_eq!(load.last_hydration(C1, "50cc"), None);

    // A hydration time in memory wins over a restored one, and a restored one is the latest at
    // its size.
    let summary = |at: u64, size: &str| LoadSummary {
        start: Timestamp::new(at - 60_000),
        at: Timestamp::new(at),
        size: size.into(),
        fingerprint: 0,
        dataflows: BTreeMap::new(),
        heap: None,
        busy: None,
        killed: false,
        hydrating: false,
    };
    let restored = |at, size, secs: Option<u64>| RestoredSummary {
        cluster_id: C1,
        summary: summary(at, size),
        hydration_time: secs.map(Duration::from_secs),
    };
    load.restore(vec![
        restored(60_000, SIZE, Some(99)),
        restored(120_000, "50cc", Some(7)),
        restored(180_000, "50cc", None),
        restored(60_000, "50cc", Some(3)),
    ]);
    assert_eq!(load.last_hydration(C1, SIZE), Some(Duration::from_secs(25)));
    assert_eq!(
        load.last_hydration(C1, "50cc"),
        Some(Duration::from_secs(7))
    );
}

const MINUTE: u64 = 60_000;
/// A time aligned to 10 minute evaluation windows.
const NOW: u64 = 1000 * 60 * MINUTE;
const GIB: u64 = 1 << 30;

/// `c1` to `c8`, doubling cores with 8 GiB of memory per core, and `x1` in another family.
fn sizes() -> ClusterReplicaSizeMap {
    let size = |cores: u64, family: &str| {
        serde_json::json!({
            "scale": 1,
            "workers": cores,
            "cpu_limit": cores,
            "memory_limit": format!("{}GiB", cores * 8),
            "credits_per_hour": cores.to_string(),
            "family": family,
        })
    };
    let map = serde_json::json!({
        "c1": size(1, "c"),
        "c2": size(2, "c"),
        "c4": size(4, "c"),
        "c8": size(8, "c"),
        "x1": size(1, "x"),
    });
    ClusterReplicaSizeMap::parse_from_str(&map.to_string(), false).unwrap()
}

fn managed(size: &str, on_load: Option<(&str, &str)>) -> ClusterVariantManaged {
    ClusterVariantManaged {
        size: size.into(),
        availability_zones: Vec::new(),
        logging: Default::default(),
        arrangement_compression: false,
        replication_factor: 1,
        optimizer_feature_overrides: Default::default(),
        schedule: Default::default(),
        auto_scaling_strategy: on_load.map(|(min, max)| AutoScalingStrategy {
            on_hydration: None,
            on_load: Some(OnLoad {
                min_size: min.into(),
                max_size: max.into(),
                scale_down_after: None,
            }),
        }),
        reconfiguration: None,
        burst: None,
        load: None,
    }
}

/// A summary of the window ending at `end`: D1 at `lag_secs`, 2 GiB of heap, and half a core
/// busy.
fn window(size: &str, end: u64, lag_secs: u64, behind: bool) -> LoadSummary {
    LoadSummary {
        start: Timestamp::new(end - 10 * MINUTE),
        at: Timestamp::new(end),
        size: size.into(),
        fingerprint: 0,
        dataflows: BTreeMap::from([(
            D1,
            DataflowSummary {
                median_lag: Duration::from_secs(lag_secs),
                progress_ratio: Some(1.0),
                behind,
            },
        )]),
        heap: Some(HeapSummary {
            bytes: 2 * GIB,
            fraction: Some(0.25),
        }),
        busy: Some(BusySummary {
            mean_millicores: 500,
            busiest_worker_utilization: 0.5,
        }),
        killed: false,
        hydrating: false,
    }
}

/// A load signal for C1 at `size` with the given summaries, sampled once at `NOW`.
fn signal(size: &str, summaries: Vec<LoadSummary>) -> ClusterLoad {
    let mut load = ClusterLoad::default();
    load.restore(
        summaries
            .into_iter()
            .map(|summary| RestoredSummary {
                cluster_id: C1,
                summary,
                hydration_time: None,
            })
            .collect(),
    );
    let mut observe = observation(NOW, size, true, 0);
    observe.size = size.into();
    load.record(
        Timestamp::new(NOW),
        BTreeMap::from([(C1, observe)]),
        &params(),
    );
    load
}

fn dyncfgs() -> ConfigSet {
    mz_adapter_types::dyncfgs::all_dyncfgs(ConfigSet::default())
}

#[mz_ore::test]
fn rules_inputs_come_from_the_catalog_and_the_signal() {
    let sizes = sizes();
    let dyncfgs = dyncfgs();
    let load = signal("c2", vec![window("c2", NOW, 1, false)]);

    let inputs = load_rules_inputs(
        C1,
        &managed("c2", Some(("c1", "c4"))),
        &sizes,
        &load,
        &dyncfgs,
    )
    .expect("inputs");
    let names: Vec<_> = inputs.ladder.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names, ["c1", "c2", "c4", "c8"]);
    assert_eq!(
        (
            inputs.policy.min_size.as_str(),
            inputs.policy.max_size.as_str()
        ),
        ("c1", "c4")
    );
    assert_eq!(inputs.size, "c2");
    assert_eq!(
        inputs.fingerprint,
        mz_cluster_controller::load::fingerprint(&BTreeSet::from([D1]))
    );
    assert_eq!(inputs.summaries.len(), 1);
    assert_eq!(inputs.params, RuleParams::from_config(&dyncfgs));

    // Without a policy, the range is the whole ladder of the family.
    let inputs = load_rules_inputs(C1, &managed("c2", None), &sizes, &load, &dyncfgs).unwrap();
    assert_eq!(
        (
            inputs.policy.min_size.as_str(),
            inputs.policy.max_size.as_str()
        ),
        ("c1", "c8")
    );
    let inputs = load_rules_inputs(C1, &managed("x1", None), &sizes, &load, &dyncfgs).unwrap();
    assert_eq!(
        (
            inputs.policy.min_size.as_str(),
            inputs.policy.max_size.as_str()
        ),
        ("x1", "x1")
    );

    // An unknown size, or a cluster not sampled yet, has no inputs.
    assert!(load_rules_inputs(C1, &managed("nope", None), &sizes, &load, &dyncfgs).is_none());
    let unsampled = ClusterLoad::default();
    assert!(load_rules_inputs(C1, &managed("c2", None), &sizes, &unsampled, &dyncfgs).is_none());
}

/// The recommendation row of C1 at `size`, by column name.
fn recommendation(
    size: &str,
    on_load: Option<(&str, &str)>,
    summaries: Vec<LoadSummary>,
) -> BTreeMap<String, String> {
    let sizes = sizes();
    let dyncfgs = dyncfgs();
    let load = signal(size, summaries);
    let inputs = load_rules_inputs(C1, &managed(size, on_load), &sizes, &load, &dyncfgs);
    let decision = inputs
        .as_ref()
        .and_then(|inputs| inputs.evaluate(None, Timestamp::new(NOW)));
    let row = recommendation_row(C1, size, inputs.as_ref().zip(decision.as_ref()));
    let desc = &MZ_CLUSTER_LOAD_RECOMMENDATIONS.desc;
    let datums: Vec<_> = row.iter().collect();
    assert_eq!(datums.len(), desc.arity());
    desc.iter()
        .zip_eq(datums)
        .map(|((name, typ), datum)| {
            assert!(datum.is_instance_of_sql(typ), "{name}: {datum:?}");
            let text = match (&typ.scalar_type, datum) {
                (_, Datum::Null) => "null".to_string(),
                (SqlScalarType::Jsonb, datum) => JsonbRef::from_datum(datum).to_string(),
                (_, Datum::String(text)) => text.to_string(),
                (_, datum) => datum.to_string(),
            };
            (name.to_string(), text)
        })
        .collect()
}

#[mz_ore::test]
fn recommendation_rows_show_the_decision() {
    let policy = Some(("c1", "c8"));

    // Steady: fresh, but not quiet for long enough to descend.
    let row = recommendation("c2", policy, vec![window("c2", NOW, 1, false)]);
    assert_eq!(row["cluster_id"], "u1");
    assert_eq!(row["status"], "steady");
    assert_eq!(row["next_action"], "null");
    assert_eq!(row["descent_blocked_by"], "not-observed");
    assert_eq!(row["behind"], "[]");
    assert_eq!(row["heap_bytes"], (2 * GIB).to_string());
    assert_eq!(row["busy_millicores"], "500");

    // Behind with a steady lag: an up-trial one size up.
    let row = recommendation("c2", policy, vec![window("c2", NOW, 30, true)]);
    assert_eq!(row["status"], "trial-up");
    assert_eq!(row["next_action"], "up-trial");
    assert_eq!(row["target_size"], "c4");
    assert_eq!(row["descent_blocked_by"], "null");
    assert_eq!(
        row["behind"],
        r#"[{"dataflow_id":"u1","floor_ms":null,"lag_ms":30000,"progress_ratio":1}]"#
    );

    // Quiet for two hours: a down-trial one size down.
    let quiet = (0..12)
        .map(|i| window("c4", NOW - i * 10 * MINUTE, 1, false))
        .collect();
    let row = recommendation("c4", policy, quiet);
    assert_eq!(row["status"], "trial-down");
    assert_eq!(row["next_action"], "down-trial");
    assert_eq!(row["target_size"], "c2");

    // Without a policy, the rules consider the whole family.
    let row = recommendation("c8", None, vec![window("c8", NOW, 30, true)]);
    assert_eq!(
        (row["min_size"].as_str(), row["max_size"].as_str()),
        ("c1", "c8")
    );
    assert_eq!(row["status"], "at-max");
    assert_eq!(row["next_action"], "null");

    // A size off its family's ladder cannot be evaluated.
    let row = recommendation("nope", None, Vec::new());
    assert_eq!(row["size"], "nope");
    assert_eq!(row["status"], "null");
    assert_eq!(row["behind"], "[]");
}
