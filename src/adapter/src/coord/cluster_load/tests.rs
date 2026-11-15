// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use chrono::DateTime;
use mz_catalog::builtin::MZ_CLUSTER_LOAD_RECOMMENDATIONS;
use mz_cluster_client::ReplicaId;
use mz_cluster_controller::load::{
    DataflowLoad, DataflowProgress, LoadEvaluation, LoadMeasurement, LoadSample, LoadStatus,
    ReplicaLoad,
};
use mz_compute_client::controller::{LoadFrontiers, ReplicaLoadFrontiers};
use mz_controller_types::ClusterId;
use mz_orchestrator::ServiceProcessMetrics;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_storage_client::controller::IntrospectionType;
use timely::progress::Antichain;

use super::{
    BoundaryFrontiers, ClusterLoad, ExportFrontiers, FRONTIER_HISTORY, MAX_SAMPLES, ReplicaHeap,
    ReplicaObservation, ReplicaWindow, next_boundary, push_sample, recommendation_row,
    replica_heap, replica_load, sample,
};

const WARM_UP: Duration = Duration::from_secs(300);

fn secs(secs: u64) -> Duration {
    Duration::from_secs(secs)
}

/// A window from `start_secs` to `end_secs` with per-worker busy seconds.
fn window(start_secs: u64, end_secs: u64, workers: &[u64]) -> ReplicaWindow {
    ReplicaWindow {
        start: start_secs * 1000,
        end: end_secs * 1000,
        workers: u64::try_from(workers.len()).unwrap(),
        total_busy: secs(workers.iter().sum()),
        max_worker_busy: secs(workers.iter().copied().max().unwrap_or(0)),
    }
}

/// Per-export `(input, output)` frontiers in seconds.
fn frontiers(frontiers: &[(u64, u64, u64)]) -> BoundaryFrontiers {
    let ts = |secs: u64| Timestamp::from(secs * 1000);
    frontiers
        .iter()
        .map(|(id, input, output)| {
            let frontiers = ExportFrontiers {
                input: ts(*input),
                output: ts(*output),
            };
            (GlobalId::User(*id), frontiers)
        })
        .collect()
}

fn observation(window: ReplicaWindow) -> ReplicaObservation {
    ReplicaObservation {
        window: Some(window),
        // Export 2 started within the window.
        frontiers: Some((
            frontiers(&[(1, 1, 1)]),
            frontiers(&[(1, 61, 31), (2, 61, 61)]),
        )),
        workers: 2,
        at_size: true,
        hydrated_since: Some(0),
        oom_killed: false,
        cpu_millicores: 2000,
        heap: Some(ReplicaHeap {
            bytes: 600,
            fraction: 300,
        }),
    }
}

fn current() -> ReplicaWindow {
    window(600, 660, &[30, 50])
}

fn expected() -> ReplicaLoad {
    ReplicaLoad {
        cpu_millicores: 2000,
        interval: secs(60),
        workers: 2,
        total_busy: secs(80),
        max_worker_busy: secs(50),
        slowest: Some(DataflowLoad {
            id: GlobalId::User(1),
            progress: DataflowProgress {
                input_advance: secs(60),
                output_advance: secs(30),
                local_lag: secs(30),
            },
        }),
        heap_fraction: 300,
        heap_bytes: 600,
    }
}

#[mz_ore::test]
fn replica_load_cases() {
    type Case = (&'static str, ReplicaObservation, Option<ReplicaLoad>);
    let with = |f: &dyn Fn(&mut ReplicaObservation)| {
        let mut observation = observation(current());
        f(&mut observation);
        observation
    };
    let cases: Vec<Case> = vec![
        ("new window", observation(current()), Some(expected())),
        ("no new window", with(&|o| o.window = None), None),
        (
            "window boundaries without frontier snapshots",
            with(&|o| o.frontiers = None),
            None,
        ),
        (
            "window reported by some of the workers",
            with(&|o| o.workers = 3),
            None,
        ),
        (
            "replica not at the realized size",
            with(&|o| o.at_size = false),
            None,
        ),
        (
            "hydrating replica",
            with(&|o| o.hydrated_since = None),
            None,
        ),
        (
            "window starts before hydration",
            with(&|o| o.hydrated_since = Some(630_000)),
            None,
        ),
        (
            "window ends within the warm-up",
            with(&|o| o.hydrated_since = Some(400_000)),
            None,
        ),
        (
            "window ends as the warm-up does",
            with(&|o| o.hydrated_since = Some(360_000)),
            Some(expected()),
        ),
        (
            "the export with the largest local lag is the slowest",
            with(&|o| {
                o.frontiers = Some((
                    frontiers(&[(1, 1, 1), (3, 1, 1)]),
                    frontiers(&[(1, 61, 31), (3, 61, 51)]),
                ))
            }),
            Some(expected()),
        ),
        ("no heap metrics", with(&|o| o.heap = None), None),
        ("empty window", observation(window(600, 600, &[0, 0])), None),
        (
            "shortened window measures its true duration",
            observation(window(615, 660, &[45, 20])),
            Some(ReplicaLoad {
                interval: secs(45),
                total_busy: secs(65),
                max_worker_busy: secs(45),
                ..expected()
            }),
        ),
        (
            "stalled input",
            ReplicaObservation {
                frontiers: Some((frontiers(&[(1, 1, 1)]), frontiers(&[(1, 1, 1)]))),
                ..observation(current())
            },
            Some(ReplicaLoad {
                slowest: Some(DataflowLoad {
                    id: GlobalId::User(1),
                    progress: DataflowProgress {
                        input_advance: secs(0),
                        output_advance: secs(0),
                        local_lag: secs(0),
                    },
                }),
                ..expected()
            }),
        ),
    ];
    for (name, observation, expected) in cases {
        let actual = replica_load(&observation, WARM_UP);
        assert_eq!(actual, expected, "{name}");
    }
}

#[mz_ore::test]
fn replica_heap_takes_the_busiest_process() {
    let process =
        |heap: Option<u64>, memory: Option<u64>, limit: Option<u64>| ServiceProcessMetrics {
            heap_bytes: heap,
            memory_bytes: memory,
            heap_limit: limit,
            ..Default::default()
        };
    // Bytes from the busiest process, the fraction from the fullest one.
    assert_eq!(
        replica_heap(
            &[
                process(Some(300), None, Some(2000)),
                process(Some(200), None, Some(400)),
            ],
            2,
            1000,
        ),
        Some(ReplicaHeap {
            bytes: 600,
            fraction: 500,
        })
    );
    // Without a reported heap, the process's memory. Without a reported limit, the size's.
    assert_eq!(
        replica_heap(&[process(None, Some(250), None)], 1, 1000),
        Some(ReplicaHeap {
            bytes: 250,
            fraction: 250,
        })
    );
    assert_eq!(
        replica_heap(&[process(None, None, Some(1000))], 1, 1000),
        None
    );
    assert_eq!(replica_heap(&[], 1, 1000), None);
}

#[mz_ore::test]
fn sample_flags_oom_at_the_realized_size_only() {
    let replica = |at_size, oom_killed| ReplicaObservation {
        at_size,
        oom_killed,
        // Hydrating, so not measurable: an OOM kill needs no measurement.
        hydrated_since: None,
        ..observation(current())
    };
    let cases = [
        ("no kill", vec![replica(true, false)], false),
        ("kill at another size", vec![replica(false, true)], false),
        (
            "kill at the realized size",
            vec![replica(false, false), replica(true, true)],
            true,
        ),
    ];
    for (name, replicas, oom_killed) in cases {
        let observations: BTreeMap<_, _> = replicas
            .into_iter()
            .enumerate()
            .map(|(i, o)| (ReplicaId::User(u64::try_from(i).unwrap()), o))
            .collect();
        let sample = sample("small", Timestamp::from(1), &observations, WARM_UP);
        assert_eq!(sample.oom_killed, oom_killed, "{name}");
        assert_eq!(sample.measurement, None, "{name}");
        assert_eq!(sample.size, "small", "{name}");
    }
}

#[mz_ore::test]
fn sample_measures_representative_replicas() {
    let observations = BTreeMap::from([
        (ReplicaId::User(1), observation(current())),
        // Its window arrived late and was used by an earlier sample, so not measured.
        (
            ReplicaId::User(2),
            ReplicaObservation {
                window: None,
                heap: Some(ReplicaHeap {
                    bytes: 10_000,
                    fraction: 900,
                }),
                ..observation(current())
            },
        ),
    ]);
    let sample = sample("small", Timestamp::from(1), &observations, WARM_UP);
    let measurement = sample.measurement.expect("one replica is measurable");
    assert_eq!(measurement.heap_bytes, 600);
    assert_eq!(measurement.lag, secs(30));
    assert_eq!(measurement.progress(), 500);
    assert_eq!(measurement.slowest_dataflow, Some(GlobalId::User(1)));
    assert!(!sample.oom_killed);
}

fn load_sample(at_secs: u64) -> LoadSample {
    LoadSample {
        at: Timestamp::from(at_secs * 1000),
        size: "small".into(),
        measurement: Some(LoadMeasurement {
            lag: Duration::ZERO,
            input_advance: secs(60),
            output_advance: secs(60),
            slowest_dataflow: None,
            heap_fraction: 0,
            heap_bytes: 0,
            mean_utilization: 0,
            max_worker_utilization: 0,
            busy_millicores: 0,
        }),
        oom_killed: false,
    }
}

#[mz_ore::test]
fn push_sample_keeps_twice_the_scale_down_window() {
    let times = |window: &VecDeque<LoadSample>| -> Vec<u64> {
        window.iter().map(|s| u64::from(s.at) / 1000).collect()
    };

    // Twice a 100s window.
    let mut window = VecDeque::new();
    for at in [0, 50, 100, 150, 200, 250] {
        push_sample(&mut window, load_sample(at), secs(100), None);
    }
    assert_eq!(times(&window), vec![50, 100, 150, 200, 250]);

    // A hydration time of 100s floors the window at 200s, so 400s are kept.
    let mut window = VecDeque::new();
    for at in [0, 100, 200, 300, 400, 500] {
        push_sample(&mut window, load_sample(at), secs(100), Some(secs(100)));
    }
    assert_eq!(times(&window), vec![100, 200, 300, 400, 500]);

    // The entry cap applies whatever the window.
    let mut window = VecDeque::new();
    for at in 0..u64::try_from(MAX_SAMPLES).unwrap() + 10 {
        push_sample(&mut window, load_sample(at), secs(1_000_000), None);
    }
    assert_eq!(window.len(), MAX_SAMPLES);
    assert_eq!(u64::from(window.front().unwrap().at), 10_000);
}

fn load_row(start_secs: u64, end_secs: u64, workers: u64, busy_secs: u64, max_secs: u64) -> Row {
    let nanos = |secs: u64| Datum::UInt64(secs * 1_000_000_000);
    let ts = |secs: u64| {
        let secs = i64::try_from(secs).unwrap();
        let ts = DateTime::from_timestamp(secs, 0).unwrap();
        Datum::TimestampTz(CheckedTimestamp::from_timestamplike(ts).unwrap())
    };
    Row::pack_slice(&[
        Datum::String("u7"),
        ts(start_secs),
        ts(end_secs),
        Datum::UInt64(workers),
        nanos(busy_secs),
        nanos(max_secs),
    ])
}

/// The subscribe's updates for a replica's window replacing `previous`.
fn window_updates(previous: &[Row], current: &[Row]) -> Vec<(Row, Diff)> {
    previous
        .iter()
        .map(|row| (row.clone(), Diff::MINUS_ONE))
        .chain(current.iter().map(|row| (row.clone(), Diff::ONE)))
        .collect()
}

#[mz_ore::test]
fn mirror_yields_each_window_once() {
    let replica = ReplicaId::User(7);
    let mut load = ClusterLoad::default();
    let observe = |load: &mut ClusterLoad, updates: &[(Row, Diff)]| {
        load.observe_introspection(
            IntrospectionType::ReplicaBusyHistory,
            replica,
            false,
            updates,
        )
    };

    assert_eq!(load.take_new_window(replica), None);
    // A new subscribe's first batch retracts a window it never reported.
    let before = [load_row(540, 600, 2, 0, 0)];
    let first = [load_row(600, 660, 2, 80, 50)];
    observe(&mut load, &window_updates(&before, &first));
    assert_eq!(
        load.take_new_window(replica),
        Some(window(600, 660, &[30, 50]))
    );
    // A window already used is not used again.
    assert_eq!(load.take_new_window(replica), None);
    // A batch without insertions keeps the latest window.
    observe(&mut load, &window_updates(&first, &[]));
    assert_eq!(load.window(replica), Some(window(600, 660, &[30, 50])));

    let second = [load_row(660, 720, 2, 10, 10)];
    observe(&mut load, &window_updates(&first, &second));
    assert_eq!(
        load.take_new_window(replica),
        Some(window(660, 720, &[0, 10]))
    );

    // Workers that disagree on the window report it in parts, which is not a window.
    let split = [load_row(720, 780, 1, 30, 30), load_row(750, 780, 1, 20, 20)];
    observe(&mut load, &window_updates(&second, &split));
    assert_eq!(load.window(replica), None);

    // Of windows delivered in one batch, the latest counts.
    let batched = [load_row(780, 840, 2, 0, 0), load_row(840, 900, 2, 2, 1)];
    observe(&mut load, &window_updates(&split, &batched));
    assert_eq!(
        load.take_new_window(replica),
        Some(window(840, 900, &[1, 1]))
    );

    load.forget_replica(replica);
    assert_eq!(load.window(replica), None);
}

#[mz_ore::test]
fn window_frontiers_pair_a_window_with_its_own_boundaries() {
    let replica = ReplicaId::User(7);
    let export = GlobalId::User(1);
    let load_frontiers = |input: u64, output: u64| {
        let replicas = BTreeMap::from([(
            replica,
            ReplicaLoadFrontiers {
                hydrated: true,
                output: BTreeMap::from([(export, Antichain::from_elem(Timestamp::from(output)))]),
            },
        )]);
        BTreeMap::from([(
            ClusterId::User(1),
            LoadFrontiers {
                input: BTreeMap::from([(export, Antichain::from_elem(Timestamp::from(input)))]),
                replicas,
            },
        )])
    };
    let snapshot = |input: u64, output: u64| {
        BTreeMap::from([(
            export,
            ExportFrontiers {
                input: Timestamp::from(input),
                output: Timestamp::from(output),
            },
        )])
    };

    let mut load = ClusterLoad::default();
    load.record_frontiers(600_000, &load_frontiers(600_000, 590_000));
    load.record_frontiers(660_000, &load_frontiers(660_000, 640_000));
    // A window delivered after the sample at its end boundary still pairs with its boundaries.
    load.record_frontiers(720_000, &load_frontiers(720_000, 700_000));
    let late = window(600, 660, &[1, 1]);
    assert_eq!(
        load.window_frontiers(replica, &late),
        Some((snapshot(600_000, 590_000), snapshot(660_000, 640_000)))
    );
    // A window whose start is no boundary the coordinator ticked at has no frontier interval.
    assert_eq!(
        load.window_frontiers(replica, &window(615, 660, &[1, 1])),
        None
    );
    // A window whose end has not been ticked at yet has none either.
    assert_eq!(
        load.window_frontiers(replica, &window(720, 780, &[1, 1])),
        None
    );

    // Only the latest boundaries are kept.
    for boundary in 0..u64::try_from(FRONTIER_HISTORY).unwrap() {
        let at = 780_000 + boundary * 60_000;
        load.record_frontiers(at, &load_frontiers(at, at));
    }
    assert_eq!(load.window_frontiers(replica, &late), None);

    load.forget_replica(replica);
    assert_eq!(
        load.window_frontiers(replica, &window(780, 840, &[1, 1])),
        None
    );
}

#[mz_ore::test]
fn next_boundary_is_the_next_multiple_of_the_length() {
    assert_eq!(next_boundary(0, secs(60)), 60_000);
    assert_eq!(next_boundary(59_999, secs(60)), 60_000);
    assert_eq!(next_boundary(60_000, secs(60)), 120_000);
    assert_eq!(next_boundary(61_234, secs(5)), 65_000);
}

#[mz_ore::test]
fn hydration_time_is_known_once_every_object_hydrated() {
    let replica = ReplicaId::User(7);
    let mut load = ClusterLoad::default();
    let hydration_row = |object: &str, ns: Option<u64>| {
        Row::pack_slice(&[
            Datum::String("u7"),
            Datum::String(object),
            ns.map_or(Datum::Null, Datum::UInt64),
        ])
    };
    load.observe_introspection(
        IntrospectionType::ComputeHydrationTimes,
        replica,
        false,
        &[
            (hydration_row("u1", Some(2_000_000_000)), Diff::ONE),
            (hydration_row("u2", None), Diff::ONE),
        ],
    );
    assert_eq!(load.hydration_time(replica), None);
    load.observe_introspection(
        IntrospectionType::ComputeHydrationTimes,
        replica,
        false,
        &[
            (hydration_row("u2", None), Diff::MINUS_ONE),
            (hydration_row("u2", Some(5_000_000_000)), Diff::ONE),
        ],
    );
    assert_eq!(load.hydration_time(replica), Some(secs(5)));
    load.forget_replica(replica);
    assert_eq!(load.hydration_time(replica), None);
}

#[mz_ore::test]
fn recommendation_row_matches_the_relation() {
    let desc = &MZ_CLUSTER_LOAD_RECOMMENDATIONS.desc;
    let measurement = LoadMeasurement {
        lag: Duration::from_millis(1500),
        input_advance: secs(60),
        output_advance: secs(30),
        slowest_dataflow: Some(GlobalId::User(3)),
        heap_fraction: 250,
        heap_bytes: 1 << 30,
        mean_utilization: 400,
        max_worker_utilization: 900,
        busy_millicores: 1600,
    };
    for latest in [None, Some(measurement)] {
        let evaluation = LoadEvaluation {
            status: LoadStatus::Steady,
            recommended_size: latest.as_ref().map(|_| "small".to_string()),
            action: None,
            latest,
            judged_scale_up: false,
        };
        let row = recommendation_row(ClusterId::User(1), &evaluation, 1_000);
        let datums: Vec<_> = row.iter().collect();
        assert_eq!(datums.len(), desc.arity());
        for (datum, (name, typ)) in datums.iter().zip(desc.iter()) {
            assert!(datum.is_instance_of_sql(typ), "{name}: {datum:?}");
        }
    }
}
