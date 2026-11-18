// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Projects a managed cluster's durable catalog config into the
//! [`ExpectedClusterState`] compare-and-append witness, and checks a witness
//! against the current config.
//!
//! [`project_expected`] is the one projection from catalog config to the
//! witness. Building the witness the same way wherever a write is conditioned
//! and wherever it is checked keeps the compared fields from drifting apart.
//!
//! [`size_ladder`] is the one ordering of replica sizes the `ON LOAD` strategy
//! moves a cluster along.

use dec::OrderedDecimal;
use mz_adapter_types::cluster_state::{
    AutoScalingPolicy, AvailabilityZones, BurstRecord, ClusterSchedule, ExpectedClusterState,
    LoadAttempt, LoadCutover, LoadCutoverReason, LoadEvidence, LoadFloor, LoadRecord, LoadVerdict,
    OnHydrationPolicy, OnLoadPolicy, OnTimeout, ReconfigurationOrigin, ReconfigurationRecord,
    ReconfigurationStatus, ReconfigurationTarget, ReconfigurationTrial, TrialKind,
};
use mz_catalog::config::ClusterReplicaSizeMap;
use mz_catalog::memory::objects::{
    BurstState, ClusterVariant, ClusterVariantManaged, LoadState, ReconfigurationState,
};
use mz_cluster_controller::load::rules::SizeCapacity;
use mz_controller::clusters::ReplicaAllocation;
use mz_controller_types::ClusterId;
use mz_ore::cast::CastLossy;
use mz_sql::plan::OnTimeoutAction;

use crate::catalog::CatalogState;

/// Project a managed cluster's durable config into the compare-and-append
/// witness: the fields a conditional write is conditioned on.
pub(crate) fn project_expected(managed: &ClusterVariantManaged) -> ExpectedClusterState {
    // Exhaustive destructure (no `..`): a field added to the managed config is a
    // compile error here until we decide whether the witness must cover it.
    let ClusterVariantManaged {
        size,
        availability_zones,
        logging,
        arrangement_compression,
        replication_factor,
        optimizer_feature_overrides: _,
        schedule,
        auto_scaling_strategy,
        reconfiguration,
        burst,
        load,
    } = managed;
    ExpectedClusterState {
        size: size.clone(),
        replication_factor: *replication_factor,
        availability_zones: AvailabilityZones(availability_zones.clone()),
        logging: logging.clone(),
        arrangement_compression: *arrangement_compression,
        schedule: cluster_schedule(schedule),
        auto_scaling_policy: auto_scaling_strategy.as_ref().map(auto_scaling_policy),
        reconfiguration: reconfiguration.as_ref().map(reconfiguration_record),
        burst: burst.as_ref().map(burst_record),
        load: load.as_ref().map(load_record),
    }
}

/// Whether `cluster_id`'s current managed state still equals `expected`. A
/// missing or unmanaged cluster never matches. This is the compare half of the
/// compare-and-append, evaluated inside the catalog transaction so the check and
/// the commit cannot be separated.
pub(crate) fn cluster_matches_expected(
    state: &CatalogState,
    cluster_id: ClusterId,
    expected: &ExpectedClusterState,
) -> bool {
    let Some(cluster) = state.try_get_cluster(cluster_id) else {
        return false;
    };
    let ClusterVariant::Managed(managed) = &cluster.config.variant else {
        return false;
    };
    project_expected(managed) == *expected
}

fn reconfiguration_record(record: &ReconfigurationState) -> ReconfigurationRecord {
    // Exhaustive destructure (no `..`), like `project_expected`: a field added
    // to either catalog type is a compile error here until we decide whether the
    // witness must carry it.
    let ReconfigurationState {
        target,
        deadline,
        on_timeout: on_timeout_action,
        status,
        origin,
        trial,
    } = record;
    let mz_catalog::memory::objects::ReconfigurationTarget {
        size,
        replication_factor,
        availability_zones,
        logging,
        arrangement_compression,
    } = target;
    ReconfigurationRecord {
        target: ReconfigurationTarget {
            size: size.clone(),
            replication_factor: *replication_factor,
            availability_zones: AvailabilityZones(availability_zones.clone()),
            logging: logging.clone(),
            arrangement_compression: *arrangement_compression,
        },
        deadline: *deadline,
        on_timeout: on_timeout(*on_timeout_action),
        status: reconfiguration_status(*status),
        origin: reconfiguration_origin(*origin),
        trial: trial.as_ref().map(reconfiguration_trial),
    }
}

fn reconfiguration_origin(
    origin: mz_catalog::memory::objects::ReconfigurationOrigin,
) -> ReconfigurationOrigin {
    match origin {
        mz_catalog::memory::objects::ReconfigurationOrigin::User => ReconfigurationOrigin::User,
        mz_catalog::memory::objects::ReconfigurationOrigin::LoadAutoscaling => {
            ReconfigurationOrigin::LoadAutoscaling
        }
    }
}

fn reconfiguration_trial(
    trial: &mz_catalog::memory::objects::ReconfigurationTrial,
) -> ReconfigurationTrial {
    let mz_catalog::memory::objects::ReconfigurationTrial {
        kind,
        hold_for,
        deadline,
        hold_started,
    } = trial;
    ReconfigurationTrial {
        kind: match kind {
            mz_catalog::memory::objects::TrialKind::Up => TrialKind::Up,
            mz_catalog::memory::objects::TrialKind::Down => TrialKind::Down,
        },
        hold_for: *hold_for,
        deadline: *deadline,
        hold_started: *hold_started,
    }
}

fn reconfiguration_status(
    status: mz_catalog::memory::objects::ReconfigurationStatus,
) -> ReconfigurationStatus {
    match status {
        mz_catalog::memory::objects::ReconfigurationStatus::InProgress => {
            ReconfigurationStatus::InProgress
        }
        mz_catalog::memory::objects::ReconfigurationStatus::Finalized => {
            ReconfigurationStatus::Finalized
        }
        mz_catalog::memory::objects::ReconfigurationStatus::TimedOut => {
            ReconfigurationStatus::TimedOut
        }
        mz_catalog::memory::objects::ReconfigurationStatus::Cancelled => {
            ReconfigurationStatus::Cancelled
        }
        mz_catalog::memory::objects::ReconfigurationStatus::ResourceExhausted => {
            ReconfigurationStatus::ResourceExhausted
        }
    }
}

fn on_timeout(action: OnTimeoutAction) -> OnTimeout {
    match action {
        OnTimeoutAction::Commit => OnTimeout::Commit,
        OnTimeoutAction::Rollback => OnTimeout::Rollback,
    }
}

fn cluster_schedule(schedule: &mz_sql::plan::ClusterSchedule) -> ClusterSchedule {
    match schedule {
        mz_sql::plan::ClusterSchedule::Manual => ClusterSchedule::Manual,
        mz_sql::plan::ClusterSchedule::Refresh {
            hydration_time_estimate,
        } => ClusterSchedule::Refresh {
            hydration_time_estimate: *hydration_time_estimate,
        },
    }
}

fn burst_record(record: &BurstState) -> BurstRecord {
    // Exhaustive destructure (no `..`), like `project_expected`: a field added
    // to the catalog type is a compile error here until the witness accounts for
    // it.
    let BurstState {
        burst_size,
        linger_duration,
        steady_ready_at,
    } = record;
    BurstRecord {
        burst_size: burst_size.clone(),
        linger_duration: *linger_duration,
        steady_ready_at: *steady_ready_at,
    }
}

fn load_record(record: &LoadState) -> LoadRecord {
    // Exhaustive destructures (no `..`), like `project_expected`: a field added
    // to a catalog type is a compile error here until the witness accounts for
    // it.
    let LoadState {
        fingerprint,
        attempt,
        last_cutover,
        evidence,
        floors,
        hydration_peak_ratio,
    } = record;
    LoadRecord {
        fingerprint: *fingerprint,
        attempt: attempt.as_ref().map(|attempt| {
            let mz_catalog::memory::objects::LoadAttempt {
                from_size,
                target_size,
                behind,
            } = attempt;
            LoadAttempt {
                from_size: from_size.clone(),
                target_size: target_size.clone(),
                behind: behind.clone(),
            }
        }),
        last_cutover: last_cutover.as_ref().map(|cutover| {
            let mz_catalog::memory::objects::LoadCutover {
                at,
                from_size,
                reason,
            } = cutover;
            use mz_catalog::memory::objects::LoadCutoverReason as Reason;
            LoadCutover {
                at: *at,
                from_size: from_size.clone(),
                reason: match reason {
                    Reason::UpTrial => LoadCutoverReason::UpTrial,
                    Reason::DownTrial => LoadCutoverReason::DownTrial,
                    Reason::HighHeap => LoadCutoverReason::HighHeap,
                    Reason::OutOfMemory => LoadCutoverReason::OutOfMemory,
                    Reason::ProbationReturn => LoadCutoverReason::ProbationReturn,
                },
            }
        }),
        evidence: evidence
            .iter()
            .map(|evidence| {
                let mz_catalog::memory::objects::LoadEvidence {
                    size,
                    verdict,
                    at,
                    interval,
                } = evidence;
                LoadEvidence {
                    size: size.clone(),
                    verdict: match verdict {
                        mz_catalog::memory::objects::LoadVerdict::Insufficient => {
                            LoadVerdict::Insufficient
                        }
                        mz_catalog::memory::objects::LoadVerdict::NoGain => LoadVerdict::NoGain,
                    },
                    at: *at,
                    interval: *interval,
                }
            })
            .collect(),
        floors: floors
            .iter()
            .map(|floor| {
                let mz_catalog::memory::objects::LoadFloor { dataflow, lag } = floor;
                LoadFloor {
                    dataflow: *dataflow,
                    lag: *lag,
                }
            })
            .collect(),
        hydration_peak_ratio: *hydration_peak_ratio,
    }
}

fn auto_scaling_policy(strategy: &mz_sql::plan::AutoScalingStrategy) -> AutoScalingPolicy {
    let mz_sql::plan::AutoScalingStrategy {
        on_hydration,
        on_load,
    } = strategy;
    AutoScalingPolicy {
        on_hydration: on_hydration.as_ref().map(|on_hydration| {
            let mz_sql::plan::OnHydration {
                hydration_size,
                linger_duration,
            } = on_hydration;
            OnHydrationPolicy {
                hydration_size: hydration_size.clone(),
                linger_duration: *linger_duration,
            }
        }),
        on_load: on_load.as_ref().map(|on_load| {
            let mz_sql::plan::OnLoad {
                min_size,
                max_size,
                scale_down_after,
            } = on_load;
            OnLoadPolicy {
                min_size: min_size.clone(),
                max_size: max_size.clone(),
                scale_down_after: *scale_down_after,
            }
        }),
    }
}

/// The sizes of the family of `size`, cheapest first. Empty when `size` is
/// unknown.
///
/// The ladder is every enabled size of the same [`ReplicaAllocation::family`]
/// as `size`. A disabled `size` selects its family and is not on the ladder.
/// Sizes are ordered by credits per hour, then cores, then heap limit, then
/// name, so sizes of equal cost have a stable order.
pub(crate) fn size_ladder(sizes: &ClusterReplicaSizeMap, size: &str) -> Vec<SizeCapacity> {
    let Some(allocation) = sizes.0.get(size) else {
        return Vec::new();
    };
    let family = allocation.family();
    let mut ladder: Vec<_> = sizes
        .enabled_allocations()
        .filter(|(_, allocation)| allocation.family() == family)
        .map(|(name, allocation)| {
            let credits = OrderedDecimal(allocation.credits_per_hour);
            (credits, size_capacity(name, allocation))
        })
        .collect();
    ladder.sort_by(|(a_credits, a), (b_credits, b)| {
        a_credits
            .cmp(b_credits)
            .then_with(|| a.cores.total_cmp(&b.cores))
            .then_with(|| a.heap_limit.cmp(&b.heap_limit))
            .then_with(|| a.name.cmp(&b.name))
    });
    ladder.into_iter().map(|(_, capacity)| capacity).collect()
}

/// The capacity of a replica of a size.
///
/// `cores` and `memory_bytes` are per replica, summed over its processes. A
/// size without a CPU limit counts one core per worker, and one without a
/// memory limit counts `u64::MAX` bytes. `heap_limit` is per process, see
/// [`process_heap_limit`], since heap is measured on a replica's fullest
/// process.
pub(crate) fn size_capacity(name: &str, allocation: &ReplicaAllocation) -> SizeCapacity {
    let processes = u64::from(allocation.scale.get());
    let cores_per_process = match allocation.cpu_limit {
        Some(limit) => f64::cast_lossy(limit.as_millicpus()) / 1000.0,
        None => f64::cast_lossy(allocation.workers.get()),
    };
    let memory_bytes = allocation.memory_limit.map_or(u64::MAX, |memory| {
        memory.0.as_u64().saturating_mul(processes)
    });
    SizeCapacity {
        name: name.to_string(),
        cores: cores_per_process * f64::cast_lossy(processes),
        memory_bytes,
        heap_limit: process_heap_limit(allocation).unwrap_or(u64::MAX),
    }
}

/// The heap limit of one process of a size: its memory limit plus its disk
/// limit when the size spills to swap, the sum the controller passes to
/// `clusterd` as `--heap-limit`. `None` when the size has no memory limit.
pub(crate) fn process_heap_limit(allocation: &ReplicaAllocation) -> Option<u64> {
    let memory = allocation.memory_limit?.0.as_u64();
    // A disk limit of zero requests unlimited swap, and then `clusterd` gets no heap limit.
    match allocation.disk_limit {
        Some(disk) if allocation.swap_enabled && disk.0.as_u64() != 0 => {
            Some(memory.saturating_add(disk.0.as_u64()))
        }
        _ => Some(memory),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    fn sizes() -> ClusterReplicaSizeMap {
        let allocation = |scale: u16,
                          workers: u64,
                          cpu: Option<f64>,
                          memory: Option<&str>,
                          credits: &str,
                          family: Option<&str>,
                          disabled: bool| {
            serde_json::json!({
                "scale": scale,
                "workers": workers,
                "cpu_limit": cpu,
                "memory_limit": memory,
                "credits_per_hour": credits,
                "family": family,
                "disabled": disabled,
            })
        };
        let mut swapping = allocation(2, 2, Some(2.0), Some("8GiB"), "2", None, false);
        swapping["disk_limit"] = "8GiB".into();
        swapping["swap_enabled"] = true.into();
        let mut unlimited_swap = allocation(1, 8, Some(8.0), Some("32GiB"), "4", None, false);
        unlimited_swap["disk_limit"] = "0".into();
        unlimited_swap["swap_enabled"] = true.into();
        let map = serde_json::json!({
            "50cc": allocation(1, 1, Some(0.5), Some("4GiB"), "0.5", None, false),
            "100cc": allocation(1, 2, Some(2.0), Some("8GiB"), "1", None, false),
            "200cc": swapping,
            "400cc": unlimited_swap,
            "old": allocation(1, 4, Some(4.0), Some("16GiB"), "1.5", None, true),
            "M.1-small": allocation(1, 2, None, None, "1", Some("M"), false),
            "M.1-large": allocation(1, 4, None, Some("32GiB"), "1", Some("M"), false),
        });
        ClusterReplicaSizeMap::parse_from_str(&map.to_string(), false).expect("valid size map")
    }

    fn capacity(name: &str, cores: f64, memory_bytes: u64, heap_limit: u64) -> SizeCapacity {
        SizeCapacity {
            name: name.into(),
            cores,
            memory_bytes,
            heap_limit,
        }
    }

    #[mz_ore::test]
    fn size_ladder_is_the_enabled_family_by_credits() {
        let sizes = sizes();
        let cc = vec![
            capacity("50cc", 0.5, 4 * GIB, 4 * GIB),
            capacity("100cc", 2.0, 8 * GIB, 8 * GIB),
            // Two processes, each with 8 GiB of memory and 8 GiB of swap.
            capacity("200cc", 4.0, 16 * GIB, 16 * GIB),
            // A disk limit of zero is unlimited swap, which leaves the memory as the limit.
            capacity("400cc", 8.0, 32 * GIB, 32 * GIB),
        ];
        assert_eq!(size_ladder(&sizes, "100cc"), cc);
        assert_eq!(
            size_ladder(&sizes, "old"),
            cc,
            "a disabled size selects its family"
        );
        assert_eq!(
            size_ladder(&sizes, "M.1-large"),
            vec![
                // Equal credits order by cores. No CPU limit is a core per worker, and no
                // memory limit is unbounded.
                capacity("M.1-small", 2.0, u64::MAX, u64::MAX),
                capacity("M.1-large", 4.0, 32 * GIB, 32 * GIB),
            ]
        );
        assert_eq!(size_ladder(&sizes, "unknown"), Vec::new());
    }
}
