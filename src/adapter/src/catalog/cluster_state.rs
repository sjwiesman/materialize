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
//! moves a cluster along. Policy validation and the load signal both read it,
//! so a range accepted at `ALTER` time is a range the strategy can walk.

use std::fmt;

use dec::OrderedDecimal;
use mz_adapter_types::cluster_state::{
    AutoScalingPolicy, AvailabilityZones, BurstRecord, ClusterSchedule, ExpectedClusterState,
    LoadAttempt, LoadAttemptPhase, LoadBackoff, LoadDirection, LoadHold, LoadRecord,
    OnHydrationPolicy, OnLoadPolicy, OnTimeout, ReconfigurationOrigin, ReconfigurationRecord,
    ReconfigurationStatus, ReconfigurationTarget,
};
use mz_catalog::config::ClusterReplicaSizeMap;
use mz_catalog::memory::objects::{
    BurstState, ClusterVariant, ClusterVariantManaged, LoadState, ReconfigurationState,
};
use mz_cluster_controller::load::SizeCapacity;
use mz_controller::clusters::ReplicaAllocation;
use mz_controller_types::ClusterId;
use mz_ore::cast::CastFrom;
use mz_sql::plan::{OnLoad, OnTimeoutAction};

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
    use mz_catalog::memory::objects as memory;
    // Exhaustive destructures (no `..`), like `project_expected`.
    let LoadState {
        attempt,
        last_cutover,
        backoff,
        hold,
    } = record;
    LoadRecord {
        attempt: attempt.as_ref().map(|attempt| {
            let memory::LoadAttempt {
                from_size,
                target_size,
                deadline,
                progress_before,
                phase,
            } = attempt;
            LoadAttempt {
                from_size: from_size.clone(),
                target_size: target_size.clone(),
                deadline: *deadline,
                progress_before: *progress_before,
                phase: match phase {
                    memory::LoadAttemptPhase::Reconfiguring => LoadAttemptPhase::Reconfiguring,
                    memory::LoadAttemptPhase::Evaluating => LoadAttemptPhase::Evaluating,
                },
            }
        }),
        last_cutover: *last_cutover,
        backoff: backoff
            .iter()
            .map(|backoff| {
                let memory::LoadBackoff {
                    size,
                    direction,
                    until,
                    failures,
                } = backoff;
                LoadBackoff {
                    size: size.clone(),
                    direction: match direction {
                        memory::LoadDirection::Up => LoadDirection::Up,
                        memory::LoadDirection::Down => LoadDirection::Down,
                    },
                    until: *until,
                    failures: *failures,
                }
            })
            .collect(),
        hold: hold.map(|hold| match hold {
            memory::LoadHold::SkewLimited => LoadHold::SkewLimited,
            memory::LoadHold::BackingOff => LoadHold::BackingOff,
        }),
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
            let OnLoad {
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

/// The sizes a cluster at `size` may be moved between by the `ON LOAD`
/// strategy, cheapest first. Empty when `size` is unknown.
///
/// The ladder is every enabled size of the same family as `size`. The family
/// is [`ReplicaAllocation::family`]: the explicit `family` of the size
/// definition, else `cc` for modern sizes and `legacy` for the t-shirt sizes.
/// A disabled `size` selects its family but is not on the ladder itself.
///
/// Sizes are ordered by credits per hour, then CPU, then memory, then name.
/// `cpu_millicores` is the CPU limit per process times the number of
/// processes, or one core per worker when the size has no CPU limit.
/// `memory_bytes` is the memory limit per process times the number of
/// processes, or `u64::MAX` when the size has no memory limit.
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
            .then_with(|| a.cpu_millicores.cmp(&b.cpu_millicores))
            .then_with(|| a.memory_bytes.cmp(&b.memory_bytes))
            .then_with(|| a.size.cmp(&b.size))
    });
    ladder.into_iter().map(|(_, capacity)| capacity).collect()
}

fn size_capacity(name: &str, allocation: &ReplicaAllocation) -> SizeCapacity {
    let processes = u64::from(allocation.scale.get());
    let cpu_per_process = match allocation.cpu_limit {
        Some(limit) => u64::cast_from(limit.as_millicpus()),
        None => u64::cast_from(allocation.workers.get()).saturating_mul(1000),
    };
    let memory_per_process = allocation
        .memory_limit
        .map_or(u64::MAX, |limit| limit.0.as_u64());
    SizeCapacity {
        size: name.to_string(),
        cpu_millicores: cpu_per_process.saturating_mul(processes),
        memory_bytes: memory_per_process.saturating_mul(processes),
    }
}

/// Why an `ON LOAD` size range does not fit a cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OnLoadRangeError {
    /// A bound is not on the ladder of the cluster `SIZE`. `option` is
    /// `MIN SIZE` or `MAX SIZE`.
    NotInFamily {
        option: &'static str,
        size: String,
        cluster_size: String,
        family_sizes: Vec<String>,
    },
    MinAboveMax {
        min_size: String,
        max_size: String,
    },
    /// The cluster `SIZE`, or the target of its in-progress resize, is outside
    /// the range or not an enabled size.
    SizeOutOfRange {
        size: String,
        min_size: String,
        max_size: String,
        in_progress_target: bool,
    },
}

impl OnLoadRangeError {
    pub fn hint(&self) -> Option<String> {
        match self {
            OnLoadRangeError::NotInFamily { family_sizes, .. } => Some(format!(
                "The sizes in the same family as the cluster SIZE are, in increasing order: {}.",
                family_sizes
                    .iter()
                    .map(|size| format!("'{size}'"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            OnLoadRangeError::MinAboveMax { .. } => None,
            OnLoadRangeError::SizeOutOfRange {
                in_progress_target, ..
            } => Some(if *in_progress_target {
                "Wait for the resize to settle, cancel it by altering the cluster back to its \
                 current SIZE, or widen the range."
                    .into()
            } else {
                "Set SIZE and AUTO SCALING STRATEGY in the same ALTER CLUSTER statement to move \
                 both."
                    .into()
            }),
        }
    }
}

impl fmt::Display for OnLoadRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OnLoadRangeError::NotInFamily {
                option,
                size,
                cluster_size,
                family_sizes: _,
            } => write!(
                f,
                "ON LOAD {option} '{size}' is not in the same size family as the cluster SIZE \
                 ('{cluster_size}')"
            ),
            OnLoadRangeError::MinAboveMax { min_size, max_size } => write!(
                f,
                "ON LOAD MIN SIZE ('{min_size}') must not be larger than MAX SIZE ('{max_size}')"
            ),
            OnLoadRangeError::SizeOutOfRange {
                size,
                min_size,
                max_size,
                in_progress_target,
            } => {
                let subject = if *in_progress_target {
                    "target SIZE of the in-progress cluster resize"
                } else {
                    "cluster SIZE"
                };
                write!(
                    f,
                    "{subject} ('{size}') must be between ON LOAD MIN SIZE ('{min_size}') and \
                     MAX SIZE ('{max_size}')"
                )
            }
        }
    }
}

/// Check that `cluster_size` lies within the `ON LOAD` range of `policy`, in
/// the order of [`size_ladder`], and that both bounds are on that ladder.
/// `in_progress_target` only selects the wording for an out-of-range size.
pub(crate) fn validate_on_load_range(
    sizes: &ClusterReplicaSizeMap,
    policy: &OnLoad,
    cluster_size: &str,
    in_progress_target: bool,
) -> Result<(), OnLoadRangeError> {
    let ladder = size_ladder(sizes, cluster_size);
    let position = |size: &str| ladder.iter().position(|capacity| capacity.size == size);
    let bound = |option: &'static str, size: &str| {
        position(size).ok_or_else(|| OnLoadRangeError::NotInFamily {
            option,
            size: size.to_string(),
            cluster_size: cluster_size.to_string(),
            family_sizes: ladder.iter().map(|c| c.size.clone()).collect(),
        })
    };
    let min = bound("MIN SIZE", &policy.min_size)?;
    let max = bound("MAX SIZE", &policy.max_size)?;
    if min > max {
        return Err(OnLoadRangeError::MinAboveMax {
            min_size: policy.min_size.clone(),
            max_size: policy.max_size.clone(),
        });
    }
    match position(cluster_size) {
        Some(current) if (min..=max).contains(&current) => Ok(()),
        _ => Err(OnLoadRangeError::SizeOutOfRange {
            size: cluster_size.to_string(),
            min_size: policy.min_size.clone(),
            max_size: policy.max_size.clone(),
            in_progress_target,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sizes() -> ClusterReplicaSizeMap {
        let allocation = |scale: u16,
                          workers: u64,
                          cpu: Option<f64>,
                          mem: Option<&str>,
                          credits: &str,
                          family: Option<&str>,
                          disabled: bool| {
            serde_json::json!({
                "scale": scale,
                "workers": workers,
                "cpu_limit": cpu,
                "memory_limit": mem,
                "credits_per_hour": credits,
                "family": family,
                "disabled": disabled,
            })
        };
        let map = serde_json::json!({
            "50cc": allocation(1, 1, Some(0.5), Some("4GiB"), "0.5", None, false),
            "100cc": allocation(1, 2, Some(2.0), Some("8GiB"), "1", None, false),
            "200cc": allocation(2, 2, Some(2.0), Some("8GiB"), "2", None, false),
            "old": allocation(1, 4, Some(4.0), Some("16GiB"), "1.5", None, true),
            "M.1-small": allocation(1, 2, None, None, "1", Some("M"), false),
            "M.1-large": allocation(1, 4, None, Some("32GiB"), "1", Some("M"), false),
        });
        ClusterReplicaSizeMap::parse_from_str(&map.to_string(), false).expect("valid size map")
    }

    #[mz_ore::test]
    fn test_size_ladder() {
        let sizes = sizes();
        let gib = 1024 * 1024 * 1024;
        let expected = vec![
            SizeCapacity {
                size: "50cc".into(),
                cpu_millicores: 500,
                memory_bytes: 4 * gib,
            },
            SizeCapacity {
                size: "100cc".into(),
                cpu_millicores: 2000,
                memory_bytes: 8 * gib,
            },
            SizeCapacity {
                size: "200cc".into(),
                cpu_millicores: 4000,
                memory_bytes: 16 * gib,
            },
        ];
        assert_eq!(size_ladder(&sizes, "100cc"), expected);
        // A disabled size selects its family and is not on the ladder.
        assert_eq!(size_ladder(&sizes, "old"), expected);
        // Equal credits fall back to CPU. Missing limits fall back to one core
        // per worker and unbounded memory.
        assert_eq!(
            size_ladder(&sizes, "M.1-large"),
            vec![
                SizeCapacity {
                    size: "M.1-small".into(),
                    cpu_millicores: 2000,
                    memory_bytes: u64::MAX,
                },
                SizeCapacity {
                    size: "M.1-large".into(),
                    cpu_millicores: 4000,
                    memory_bytes: 32 * gib,
                },
            ]
        );
        assert_eq!(size_ladder(&sizes, "nope"), Vec::new());
    }

    #[mz_ore::test]
    fn test_validate_on_load_range() {
        let sizes = sizes();
        let policy = |min: &str, max: &str| OnLoad {
            min_size: min.into(),
            max_size: max.into(),
            scale_down_after: None,
        };
        let validate =
            |min, max, size| validate_on_load_range(&sizes, &policy(min, max), size, false);

        assert_eq!(validate("50cc", "200cc", "100cc"), Ok(()));
        assert_eq!(validate("100cc", "100cc", "100cc"), Ok(()));
        assert!(matches!(
            validate("M.1-small", "200cc", "100cc"),
            Err(OnLoadRangeError::NotInFamily {
                option: "MIN SIZE",
                ..
            })
        ));
        // Unknown and disabled bounds are not on the ladder either.
        assert!(matches!(
            validate("50cc", "nope", "100cc"),
            Err(OnLoadRangeError::NotInFamily {
                option: "MAX SIZE",
                ..
            })
        ));
        assert!(matches!(
            validate("50cc", "old", "100cc"),
            Err(OnLoadRangeError::NotInFamily {
                option: "MAX SIZE",
                ..
            })
        ));
        assert!(matches!(
            validate("200cc", "50cc", "100cc"),
            Err(OnLoadRangeError::MinAboveMax { .. })
        ));
        assert!(matches!(
            validate("50cc", "100cc", "200cc"),
            Err(OnLoadRangeError::SizeOutOfRange { .. })
        ));
        assert!(matches!(
            validate("100cc", "200cc", "50cc"),
            Err(OnLoadRangeError::SizeOutOfRange { .. })
        ));
        assert!(matches!(
            validate("50cc", "200cc", "old"),
            Err(OnLoadRangeError::SizeOutOfRange { .. })
        ));
    }
}
