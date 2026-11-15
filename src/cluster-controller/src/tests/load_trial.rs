// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The trial hold of the graceful strategy, and a closed loop of the `ON LOAD`
//! strategy and the trial against a synthetic replica.

use mz_adapter_types::cluster_state::{LoadRecord, OnLoadPolicy, ReconfigurationTrial};

use super::*;
use crate::load::{
    DataflowLoad, DataflowProgress, LoadSample, OnLoadStrategy, ReplicaLoad, measure,
};
use crate::strategy::{BaselineStrategy, ReadinessProbe, SignalRequest};
use crate::testing::{GIB, HOUR, MINUTE, apply_write, ladder, load_params, ts};

fn trial(ready_since: Option<u64>) -> ReconfigurationTrial {
    ReconfigurationTrial {
        allowed_lag: Duration::from_secs(2),
        hold_for: Duration::from_secs(15 * 60),
        max_heap_fraction: 800,
        ready_since: ready_since.map(ts),
    }
}

fn trial_criteria() -> ReadinessCriteria {
    ReadinessCriteria {
        allowed_lag: Some(Duration::from_secs(2)),
        max_heap_fraction: Some(800),
    }
}

/// A cluster at `c8` trying `c2`, with the `c2` replica ready under the trial
/// criteria when `ready`.
fn trying(ready_since: Option<u64>, ready: bool) -> (ClusterState, LiveSignals) {
    let rec = ReconfigurationRecord {
        origin: ReconfigurationOrigin::LoadAutoscaling,
        trial: Some(trial(ready_since)),
        ..record("c2", 1, 24 * HOUR)
    };
    let (state, _) = reconfiguring_state(
        cluster(1),
        "c8",
        1,
        vec![
            observed(replica(1), "r1", "c8"),
            observed(replica(2), "r2", "c2"),
        ],
        rec,
        BTreeSet::new(),
    );
    let probe = ReadinessProbe {
        reference: BTreeSet::from([replica(1)]),
        criteria: trial_criteria(),
    };
    let signals = LiveSignals {
        ready_replicas: BTreeMap::from([(
            probe,
            if ready {
                BTreeSet::from([replica(2)])
            } else {
                BTreeSet::new()
            },
        )]),
        ..Default::default()
    };
    (state, signals)
}

fn written_trial(write: &StateWrite) -> Option<ReconfigurationTrial> {
    write
        .reconfiguration
        .as_ref()
        .and_then(|w| w.record.as_ref())
        .and_then(|r| r.trial.clone())
}

#[mz_ore::test]
fn trial_probes_readiness_with_its_own_criteria() {
    let (state, _) = trying(None, false);
    let request = GracefulReconfigurationStrategy.signal_request(&state, &config());
    assert_eq!(
        request.readiness,
        BTreeSet::from([ReadinessProbe {
            reference: BTreeSet::from([replica(1)]),
            criteria: trial_criteria(),
        }])
    );

    // Readiness under the default criteria, which has no heap check and a
    // looser lag allowance, does not count for a trial.
    let (state, _) = trying(Some(0), true);
    let signals = LiveSignals {
        ready_replicas: BTreeMap::from([(
            BTreeSet::from([replica(1)]).into(),
            BTreeSet::from([replica(2)]),
        )]),
        ..Default::default()
    };
    let write = GracefulReconfigurationStrategy.update_state(&state, &signals, &config(), ts(HOUR));
    assert_eq!(write.new_size, None);
    assert_eq!(written_trial(&write), Some(trial(None)));
}

#[mz_ore::test]
fn trial_stamps_the_first_ready_tick() {
    let (state, signals) = trying(None, true);
    let write =
        GracefulReconfigurationStrategy.update_state(&state, &signals, &config(), ts(MINUTE));
    assert_eq!(write.new_size, None);
    assert_eq!(written_trial(&write), Some(trial(Some(MINUTE))));
    assert_eq!(
        written_reconfiguration_status(&write),
        Some(ReconfigurationStatus::InProgress)
    );
    assert_eq!(written_reconfiguration_audit(&write), None);
}

#[mz_ore::test]
fn trial_holds_and_then_cuts_over() {
    let (state, signals) = trying(Some(MINUTE), true);
    let g = GracefulReconfigurationStrategy;
    let config = config();
    for now in [MINUTE, 10 * MINUTE, 16 * MINUTE - 1] {
        assert!(
            g.update_state(&state, &signals, &config, ts(now))
                .is_empty(),
            "at {now}"
        );
        let desired = g.desired_replicas(&state, &signals, &config, ts(now));
        assert_eq!(desired.len(), 1);
        assert_eq!(desired[0].shape.size, "c2");
    }
    let write = g.update_state(&state, &signals, &config, ts(16 * MINUTE));
    assert_eq!(write.new_size.as_deref(), Some("c2"));
    assert_eq!(
        written_reconfiguration_status(&write),
        Some(ReconfigurationStatus::Finalized)
    );
    assert_eq!(
        written_reconfiguration_audit(&write),
        Some(ReconfigurationAudit::Finalized { forced: false })
    );
}

#[mz_ore::test]
fn trial_resets_the_hold_on_a_failed_check() {
    let (state, signals) = trying(Some(MINUTE), false);
    let write =
        GracefulReconfigurationStrategy.update_state(&state, &signals, &config(), ts(30 * MINUTE));
    assert_eq!(write.new_size, None);
    assert_eq!(written_trial(&write), Some(trial(None)));
    assert_eq!(written_reconfiguration_audit(&write), None);

    // Not ready and never stamped: nothing to write.
    let (state, signals) = trying(None, false);
    assert!(
        GracefulReconfigurationStrategy
            .update_state(&state, &signals, &config(), ts(30 * MINUTE))
            .is_empty()
    );
}

#[mz_ore::test]
fn trial_rolls_back_at_the_deadline_with_the_hold_incomplete() {
    let g = GracefulReconfigurationStrategy;
    let config = config();
    for (ready_since, ready) in [(None, false), (Some(24 * HOUR - MINUTE), true)] {
        let (state, signals) = trying(ready_since, ready);
        let write = g.update_state(&state, &signals, &config, ts(24 * HOUR));
        assert_eq!(write.new_size, None);
        assert_eq!(
            written_reconfiguration_status(&write),
            Some(ReconfigurationStatus::TimedOut)
        );
        assert_eq!(
            written_reconfiguration_audit(&write),
            Some(ReconfigurationAudit::TimedOut)
        );
        assert!(
            g.desired_replicas(&state, &signals, &config, ts(24 * HOUR))
                .is_empty()
        );
    }
    // A hold that completes by the deadline still cuts over.
    let (state, signals) = trying(Some(HOUR), true);
    let write = g.update_state(&state, &signals, &config, ts(24 * HOUR));
    assert_eq!(write.new_size.as_deref(), Some("c2"));
}

#[mz_ore::test]
fn a_user_record_cuts_over_on_the_first_ready_tick() {
    let (state, signals) = reconfiguring_state(
        cluster(1),
        "c8",
        1,
        vec![
            observed(replica(1), "r1", "c8"),
            observed(replica(2), "r2", "c2"),
        ],
        record("c2", 1, 24 * HOUR),
        BTreeSet::from([replica(2)]),
    );
    let request = GracefulReconfigurationStrategy.signal_request(&state, &config());
    assert_eq!(
        request.readiness,
        BTreeSet::from([BTreeSet::from([replica(1)]).into()])
    );
    let write =
        GracefulReconfigurationStrategy.update_state(&state, &signals, &config(), ts(MINUTE));
    assert_eq!(write.new_size.as_deref(), Some("c2"));
}

#[mz_ore::test(tokio::test)]
async fn trial_criteria_reach_the_ctx() {
    let (state, _) = trying(None, false);
    let mut ctx = FakeCtx::new(vec![state]);
    controller().reconcile(&mut ctx).await;
    assert_eq!(
        ctx.readiness_criteria,
        vec![ReadinessCriteria {
            allowed_lag: Some(Duration::from_secs(2)),
            max_heap_fraction: Some(800),
        }]
    );
}

/// A synthetic replica: `cores^alpha` units of capacity, spread evenly over
/// one worker per core, against a workload of `demand` units of which `skew`
/// lands on one worker, and a heap of `heap_gib` at any size.
#[derive(Clone, Copy, Debug)]
struct Workload {
    alpha: f64,
    demand: f64,
    skew: f64,
    heap_gib: f64,
}

/// What a replica of `cores` cores shows under a workload.
struct Behavior {
    /// The share of the arriving input it processes.
    rho: f64,
    /// Spare capacity relative to the arrival rate, for working off a
    /// backlog.
    spare: f64,
    mean_utilization: f64,
    max_worker_utilization: f64,
}

impl Workload {
    fn behavior(&self, cores: u64) -> Behavior {
        let n = cores as f64;
        let speed = n.powf(self.alpha) / n;
        let even = (1.0 - self.skew) * self.demand / n;
        let hot = (self.skew * self.demand + even) / speed;
        let other = even / speed;
        let rho = (1.0 / hot).min(1.0);
        Behavior {
            rho,
            spare: (1.0 / hot - 1.0).max(0.0),
            mean_utilization: ((hot + (n - 1.0) * other) * rho / n).min(1.0),
            max_worker_utilization: (hot * rho).min(1.0),
        }
    }

    fn keeps_up(&self, cores: u64) -> bool {
        self.behavior(cores).rho >= 1.0
    }
}

fn cores(size: &str) -> u64 {
    size.strip_prefix('c')
        .and_then(|n| n.parse().ok())
        .expect("a c<n> size")
}

const HYDRATION: u64 = 10 * MINUTE;

struct SimReplica {
    id: ReplicaId,
    size: String,
    created_at: u64,
    /// Seconds the output trails the input. Grows while hydrating.
    lag: f64,
}

impl SimReplica {
    fn hydrated(&self, now: u64) -> bool {
        now >= self.created_at + HYDRATION
    }
}

/// What a closed loop did.
#[derive(Debug)]
struct Run {
    /// The realized size after each cut-over, starting with the initial one.
    sizes: Vec<String>,
    /// When the realized size last changed.
    last_change: u64,
    /// Reconfigurations that rolled back, by target size and time.
    timed_out: Vec<(String, u64)>,
    /// Trials started, by target size and time.
    trials: Vec<(String, u64)>,
    record: Option<LoadRecord>,
}

/// Runs the controller's baseline, graceful, and `ON LOAD` strategies with the
/// trial against a synthetic replica for `duration`, one tick and one sample
/// per minute.
fn simulate(start: &str, workload: Workload, duration: u64) -> Run {
    let controller = controller_with(vec![
        Box::new(BaselineStrategy),
        Box::new(GracefulReconfigurationStrategy),
        Box::new(OnLoadStrategy),
    ]);
    let config = ConfigSignals {
        load_enabled: true,
        load: load_params(),
        default_reconfiguration_timeout: Duration::from_secs(4 * 60 * 60),
        ..Default::default()
    };
    let ladder = ladder();
    let mut state = state(cluster(1), start, 1, Vec::new());
    state.auto_scaling_policy = Some(AutoScalingPolicy {
        on_hydration: None,
        on_load: Some(OnLoadPolicy {
            min_size: "c1".into(),
            max_size: "c32".into(),
            scale_down_after: None,
        }),
    });
    let mut replicas = vec![SimReplica {
        id: replica(1),
        size: start.into(),
        // Hydrated and warmed up before the loop starts.
        created_at: 0,
        lag: 1.0,
    }];
    let mut next_id = 2;
    let mut samples: Vec<LoadSample> = Vec::new();
    let mut run = Run {
        sizes: vec![start.to_string()],
        last_change: 0,
        timed_out: Vec::new(),
        trials: Vec::new(),
        record: None,
    };
    let sync = |state: &mut ClusterState, replicas: &[SimReplica]| {
        state.replicas = replicas
            .iter()
            .map(|r| observed(r.id, &format!("r{}", r.id), &r.size))
            .collect();
    };
    sync(&mut state, &replicas);

    let first = HYDRATION + 10 * MINUTE;
    for now in (first..first + duration).step_by(usize::cast_from(MINUTE)) {
        // Advance every replica by one interval and sample the realized ones.
        let mut loads = Vec::new();
        let mut oom_killed = false;
        for r in &mut replicas {
            if !r.hydrated(now) {
                continue;
            }
            let c = cores(&r.size);
            let b = workload.behavior(c);
            let before = r.lag;
            r.lag = if b.rho < 1.0 {
                r.lag + 60.0 * (1.0 - b.rho)
            } else {
                (r.lag - 60.0 * b.spare).max(1.0)
            };
            let heap_limit = c * 8 * GIB;
            let heap_bytes = (workload.heap_gib * GIB as f64) as u64;
            if heap_bytes > heap_limit {
                // Killed and restarted: it rehydrates.
                r.created_at = now;
                r.lag = 1.0;
                oom_killed |= r.size == state.size;
                continue;
            }
            let warmed_up = r.created_at + HYDRATION + config.load.warm_up.as_millis() as u64;
            if r.size != state.size || now < warmed_up {
                continue;
            }
            let workers = u32::try_from(c).expect("small");
            loads.push(ReplicaLoad {
                cpu_millicores: c * 1000,
                interval: Duration::from_secs(60),
                workers: usize::cast_from(c),
                total_busy: Duration::from_secs_f64(60.0 * b.mean_utilization) * workers,
                max_worker_busy: Duration::from_secs_f64(60.0 * b.max_worker_utilization),
                slowest: Some(DataflowLoad {
                    id: GlobalId::User(1),
                    progress: DataflowProgress {
                        input_advance: Duration::from_secs(60),
                        output_advance: Duration::from_secs_f64(60.0 + before - r.lag),
                        local_lag: Duration::from_secs_f64(r.lag),
                    },
                }),
                heap_fraction: u32::try_from(heap_bytes * 1000 / heap_limit).expect("permille"),
                heap_bytes,
            });
        }
        samples.push(LoadSample {
            at: ts(now),
            size: state.size.clone(),
            measurement: measure(&loads),
            oom_killed,
        });
        samples.retain(|s| u64::from(s.at) + 4 * HOUR > now);

        // One controller tick: signals, merged state writes, replica diff.
        let request = controller
            .strategies
            .iter()
            .fold(SignalRequest::default(), |acc, s| {
                acc.union(s.signal_request(&state, &config))
            });
        let mut signals = LiveSignals::default();
        if request.load {
            signals.load = Some(crate::load::LoadSignals {
                ladder: ladder.clone(),
                samples: samples.clone(),
                hydration_time: Some(Duration::from_millis(HYDRATION)),
            });
        }
        for probe in request.readiness {
            let reference_lag = replicas
                .iter()
                .filter(|r| probe.reference.contains(&r.id))
                .map(|r| r.lag)
                .fold(f64::INFINITY, f64::min);
            let allowed = probe
                .criteria
                .allowed_lag
                .unwrap_or(Duration::from_secs(60))
                .as_secs_f64();
            let ready = replicas
                .iter()
                .filter(|r| r.hydrated(now))
                .filter(|r| r.lag <= reference_lag + allowed)
                .filter(|r| {
                    probe.criteria.max_heap_fraction.is_none_or(|max| {
                        let fraction = workload.heap_gib * 1000.0 / (cores(&r.size) * 8) as f64;
                        fraction < f64::from(max)
                    })
                })
                .map(|r| r.id)
                .collect();
            signals.ready_replicas.insert(probe, ready);
        }
        let before = state.reconfiguration.clone();
        let write = controller.merge_state_writes(&state, &signals, &config, ts(now));
        apply_write(&mut state, &write);
        if let Some(record) = &state.reconfiguration {
            let started = before
                .as_ref()
                .is_none_or(|b| b.deadline != record.deadline);
            if started && record.trial.is_some() {
                run.trials.push((record.target.size.clone(), now));
            }
            if record.status == ReconfigurationStatus::TimedOut
                && before.is_some_and(|b| b.is_in_progress())
            {
                run.timed_out.push((record.target.size.clone(), now));
            }
        }
        if write.new_size.is_some() {
            run.sizes.push(state.size.clone());
            run.last_change = now;
        }
        for decision in controller.collect_replica_decisions(&state, &signals, &config, ts(now)) {
            match decision {
                Decision::CreateReplica { shape, .. } => {
                    replicas.push(SimReplica {
                        id: replica(next_id),
                        size: shape.size,
                        created_at: now,
                        lag: 1.0,
                    });
                    next_id += 1;
                }
                Decision::DropReplica { replica_id, .. } => {
                    replicas.retain(|r| r.id != replica_id);
                }
                Decision::UpdateClusterState { .. } => unreachable!("phase 2 only diffs"),
            }
        }
        sync(&mut state, &replicas);
    }
    run.record = state.load.clone();
    run
}

/// The smallest size that keeps up with its busy time within the CPU margin
/// and holds the heap at or below the heap target.
fn expected(workload: Workload) -> usize {
    ladder()
        .iter()
        .position(|size| {
            let c = cores(&size.size);
            let b = workload.behavior(c);
            b.rho >= 1.0 && b.mean_utilization <= 0.8 && workload.heap_gib <= 0.5 * (c * 8) as f64
        })
        .expect("some size fits")
}

fn check_run(workload: Workload, start: &str, run: &Run) -> String {
    let context = format!("{workload:?}, start {start}: {run:?}");
    let mut distinct = run.sizes.clone();
    distinct.sort();
    distinct.dedup();
    assert_eq!(
        distinct.len(),
        run.sizes.len(),
        "revisited a size: {context}"
    );
    assert!(run.last_change <= 10 * HOUR, "still moving late: {context}");
    assert!(run.sizes.len() <= 3, "more than two changes: {context}");
    let settled = run.sizes.last().expect("a size");
    assert!(workload.keeps_up(cores(settled)), "{context}");
    assert!(
        workload.heap_gib <= 0.8 * (cores(settled) * 8) as f64,
        "{context}"
    );
    context
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn closed_loop_reaches_the_smallest_fitting_size_and_stays() {
    // Workloads whose smallest fitting size is not within a ladder step of a
    // size that keeps up only above the CPU margin. See the next test.
    for (alpha, demand, heap_gib) in [
        (1.0, 0.3, 1.0),
        (1.0, 1.5, 1.0),
        (1.0, 5.0, 3.0),
        (1.0, 10.0, 1.0),
        (0.9, 0.3, 1.0),
        (0.9, 2.0, 10.0),
        (0.8, 0.3, 1.0),
        (0.8, 2.0, 10.0),
        (0.8, 10.0, 1.0),
    ] {
        let workload = Workload {
            alpha,
            demand,
            skew: 0.0,
            heap_gib,
        };
        let expected = &ladder()[expected(workload)].size;
        for start in ladder() {
            let run = simulate(&start.size, workload, 16 * HOUR);
            let context = check_run(workload, &start.size, &run);
            assert_eq!(run.sizes.last(), Some(expected), "{context}");
        }
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn closed_loop_settles_within_a_step_of_the_smallest_fitting_size() {
    // A size that keeps up with its busy time above the CPU margin, or holds
    // its heap between the heap target and the high-water mark, is kept when
    // the cluster arrives there from below: no rule moves it. That includes a
    // size reached by doubling the heap limit after a memory kill. From above, busy time
    // measured at a larger size overstates what a smaller one needs when
    // capacity grows slower than cores, so the CPU condition can stop the
    // descent a step early.
    for alpha in [1.0, 0.9, 0.8] {
        for (demand, heap_gib) in [
            (0.3, 1.0),
            (1.5, 1.0),
            (2.0, 10.0),
            (3.0, 1.0),
            (5.0, 3.0),
            (10.0, 1.0),
            (1.0, 20.0),
        ] {
            let workload = Workload {
                alpha,
                demand,
                skew: 0.0,
                heap_gib,
            };
            let expected = expected(workload);
            for start in ladder() {
                let run = simulate(&start.size, workload, 16 * HOUR);
                let context = check_run(workload, &start.size, &run);
                let settled = ladder()
                    .iter()
                    .position(|c| Some(&c.size) == run.sizes.last())
                    .expect("on the ladder");
                assert!(settled <= expected + 1, "{context}");
            }
        }
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn closed_loop_does_not_repeat_a_failed_trial_before_its_backoff() {
    // A hot worker carries 40% of the work. Total busy time fits c4 within the
    // margin, but its hot worker cannot keep up there. c8 can.
    let workload = Workload {
        alpha: 1.0,
        demand: 2.0,
        skew: 0.4,
        heap_gib: 1.0,
    };
    assert!(!workload.keeps_up(4));
    assert!(workload.keeps_up(8));
    let run = simulate("c32", workload, 16 * HOUR);
    let context = format!("{run:?}");
    assert_eq!(
        run.sizes.last().map(String::as_str),
        Some("c8"),
        "{context}"
    );
    assert!(
        run.timed_out.iter().any(|(size, _)| size == "c4"),
        "{context}"
    );
    // Each failed size is tried again only after its backoff, which starts at
    // an hour.
    for (size, failed_at) in &run.timed_out {
        assert!(
            !run.trials
                .iter()
                .any(|(s, at)| s == size && *at > *failed_at && *at < failed_at + HOUR),
            "{context}"
        );
    }
    let backoff = run.record.expect("record").backoff;
    assert!(backoff.iter().any(|b| b.size == "c4"), "{backoff:?}");
}
