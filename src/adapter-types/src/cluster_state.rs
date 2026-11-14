// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Plain-data mirror of a managed cluster's durable configuration.
//!
//! These types carry the slices of a managed cluster's config that a caller
//! reasons over without touching the catalog or SQL layers. Keeping them free
//! of catalog and SQL dependencies lets one component reason over the config
//! and another project the live config onto the same types, without either
//! depending on the other.
//!
//! [`ExpectedClusterState`] is a compare-and-append witness. A caller captures
//! it from a config snapshot and pairs it with a conditional write. The applier
//! re-projects the live config and applies the write only if it still equals
//! the witness.

use std::collections::BTreeSet;
use std::time::Duration;

use mz_compute_types::config::ComputeReplicaLogging;
use mz_repr::Timestamp;

/// The availability zones a managed cluster's replicas are provisioned across,
/// in configured *provisioning order*.
///
/// The order is significant to provisioning: the orchestrator round-robins
/// replica placement across the list, so the first configured zone is filled
/// first. Equality is therefore structural and order-sensitive, and the order is
/// part of the [`ExpectedClusterState`] compare-and-append witness. To compare
/// two configurations as unordered *pools* of zones, ignoring placement order,
/// convert with [`AvailabilityZones::pool`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AvailabilityZones(pub Vec<String>);

impl AvailabilityZones {
    /// This configuration as an unordered [`AvailabilityZonePool`]: the same
    /// zones compared by membership rather than provisioning order. Two replicas
    /// drawing from the same pool are interchangeable however the lists were
    /// ordered.
    pub fn pool(&self) -> AvailabilityZonePool {
        AvailabilityZonePool(self.0.iter().cloned().collect())
    }
}

/// An unordered set of availability zones: an [`AvailabilityZones`] provisioning
/// list reduced to membership. Produced by [`AvailabilityZones::pool`] for the
/// one comparison that must ignore order, replica interchangeability.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AvailabilityZonePool(pub BTreeSet<String>);

/// The config dimensions that distinguish one replica from another. Two replicas
/// with equal shape are interchangeable. An `AVAILABILITY ZONES` difference is a
/// shape difference, but only as an unordered pool: reordering the same zones
/// does not change a replica's shape (see [`ReplicaShape::matches`]).
#[derive(Clone, Debug)]
pub struct ReplicaShape {
    pub size: String,
    pub availability_zones: AvailabilityZones,
    pub logging: ComputeReplicaLogging,
    pub arrangement_compression: bool,
}

impl ReplicaShape {
    /// Whether two shapes are interchangeable. Availability zones are compared as
    /// unordered [pools](AvailabilityZones::pool): a replica already placed
    /// satisfies a desired shape with the same zones in a different order, so a
    /// mere reorder does not force a reprovision.
    pub fn matches(&self, other: &ReplicaShape) -> bool {
        self.size == other.size
            && self.logging == other.logging
            && self.arrangement_compression == other.arrangement_compression
            && self.availability_zones.pool() == other.availability_zones.pool()
    }
}

/// Compare-and-append witness over a managed cluster's durable config: the
/// fields a conditional write is conditioned on. The applier applies the write
/// only if the cluster's current config still projects to an equal witness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedClusterState {
    pub size: String,
    pub replication_factor: u32,
    pub availability_zones: AvailabilityZones,
    pub logging: ComputeReplicaLogging,
    pub arrangement_compression: bool,
    /// The scheduling policy. Part of the witness because it determines which
    /// policy owns the replica set, so a write conditioned on one schedule must
    /// not apply under another.
    pub schedule: ClusterSchedule,
    /// The autoscaling policy. Part of the witness because it determines whether,
    /// and at what size, a burst is warranted.
    pub auto_scaling_policy: Option<AutoScalingPolicy>,
    pub reconfiguration: Option<ReconfigurationRecord>,
    pub burst: Option<BurstRecord>,
    pub load: Option<LoadRecord>,
}

/// The status of the latest graceful reconfiguration record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconfigurationStatus {
    /// The controller is converging the cluster onto the target shape.
    InProgress,
    /// The realized config reached the target shape.
    Finalized,
    /// The deadline fired under rollback and the realized config stayed put.
    TimedOut,
    /// The user retargeted the reconfiguration back to the realized shape.
    Cancelled,
    /// The controller could not create the target replicas within the budget.
    ResourceExhausted,
}

/// The latest graceful reconfiguration record, mirrored from durable state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconfigurationRecord {
    pub target: ReconfigurationTarget,
    pub deadline: Timestamp,
    /// What to do once the deadline passes with the target not yet hydrated.
    pub on_timeout: OnTimeout,
    pub status: ReconfigurationStatus,
    pub origin: ReconfigurationOrigin,
}

/// Who wrote a reconfiguration record.
///
/// A user-written record always wins: the load strategy writes a record only
/// when none is in progress, and a user `ALTER` overwrites a record of either
/// origin.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconfigurationOrigin {
    /// An `ALTER CLUSTER` statement.
    User,
    /// The `ON LOAD` autoscaling strategy.
    LoadAutoscaling,
}

impl ReconfigurationRecord {
    /// Whether this record should still drive target-replica convergence.
    pub fn is_in_progress(&self) -> bool {
        matches!(self.status, ReconfigurationStatus::InProgress)
    }
}

/// The lifecycle transition a write to the `reconfiguration` record represents,
/// declared by the writer at the decision point and carried alongside the state
/// so the audit event is emitted in the same catalog transaction as the write.
/// A plain-data mirror of the audit-log lifecycle vocabulary, free of a
/// dependency on `mz-audit-log`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconfigurationAudit {
    /// A record was written or re-targeted: converging onto a new target.
    Started,
    /// The user re-targeted back to the still-realized shape.
    Cancelled,
    /// The realized config cut over to the target. `forced` records whether
    /// the cut-over was forced by `ON TIMEOUT COMMIT` at the deadline rather
    /// than reached by hydration, information only the writer has.
    Finalized { forced: bool },
    /// The deadline fired under rollback and the realized config stayed put.
    TimedOut,
    /// The controller could not create the target replicas within the budget.
    ResourceExhausted,
}

/// The lifecycle transition a write to the `burst` record represents, declared
/// by the writer like [`ReconfigurationAudit`]. A burst record is also
/// rewritten in place for bookkeeping (stamping or resetting the linger
/// clock), which is not a lifecycle transition and declares no audit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BurstAudit {
    /// A burst record was written: a burst replica is being provisioned.
    Started,
    /// The burst record was cleared and the burst replica torn down.
    Finished { cause: BurstFinishCause },
}

/// Why a hydration burst finished, known only to the strategy arm that
/// decided the teardown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BurstFinishCause {
    /// A steady replica stayed ready for the linger duration.
    LingerElapsed,
    /// The burst is no longer warranted by current config: the auto-scaling
    /// policy was removed or its hydration size changed, the cluster was
    /// turned off, or burst was disabled environment-wide.
    NoLongerWarranted,
}

/// The action a graceful reconfiguration applies once its `deadline` passes with
/// the target replicas not yet hydrated. Success always takes precedence: a
/// hydrated target cuts over regardless of this. A plain-data mirror of
/// `mz_sql::plan::OnTimeoutAction`, free of a dependency on the SQL layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnTimeout {
    /// Cut over to the (not-yet-hydrated) target anyway.
    Commit,
    /// Drop the target replica set, reverting to the pre-reconfiguration shape.
    Rollback,
}

/// The full config shape a reconfiguration is moving the cluster to. Distinct
/// from a replica shape because it additionally carries `replication_factor`, a
/// cluster-level rather than replica-level dimension.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconfigurationTarget {
    pub size: String,
    pub replication_factor: u32,
    pub availability_zones: AvailabilityZones,
    pub logging: ComputeReplicaLogging,
    pub arrangement_compression: bool,
}

impl ReconfigurationTarget {
    /// The per-replica shape of the target: everything but `replication_factor`.
    pub fn shape(&self) -> ReplicaShape {
        ReplicaShape {
            size: self.size.clone(),
            availability_zones: self.availability_zones.clone(),
            logging: self.logging.clone(),
            arrangement_compression: self.arrangement_compression,
        }
    }
}

/// An active hydration-burst record, mirrored from durable state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BurstRecord {
    pub burst_size: String,
    pub linger_duration: Duration,
    pub steady_ready_at: Option<Timestamp>,
}

/// The single definition of when an in-flight burst record of `record_size` is
/// warranted: the cluster is on and the `ON HYDRATION` policy in force (if any)
/// bursts at the record's size. `hydration_size` is `None` when no policy is in
/// force. The catalog and the cluster controller wrap this over their own
/// config representations, so the two sides of the burst lifecycle cannot
/// drift.
pub fn burst_record_warranted(
    record_size: &str,
    replication_factor: u32,
    hydration_size: Option<&str>,
) -> bool {
    replication_factor != 0 && hydration_size == Some(record_size)
}

/// A managed cluster's scheduling policy, mirrored from durable state. A
/// plain-data mirror of `mz_sql::plan::ClusterSchedule`, free of a dependency on
/// the SQL layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterSchedule {
    /// The cluster is user-managed: `replication_factor` is the capacity knob
    /// that sizes the replica set.
    Manual,
    /// The cluster is scheduled `ON REFRESH`: `replication_factor` is held at `0`
    /// and replicas run only around refresh times. `hydration_time_estimate` is
    /// how far ahead of a refresh the cluster should turn on so it can rehydrate
    /// before the refresh time.
    Refresh { hydration_time_estimate: Duration },
}

/// The user-configured autoscaling policy of a managed cluster, mirrored from
/// durable state. A plain-data mirror of `mz_sql::plan::AutoScalingStrategy`,
/// free of a dependency on the SQL layer.
///
/// Extensible: strategies are optional sub-policies that engage independently.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutoScalingPolicy {
    pub on_hydration: Option<OnHydrationPolicy>,
    pub on_load: Option<OnLoadPolicy>,
}

/// The `ON LOAD` sub-policy: keep the cluster at the cheapest size in
/// `[min_size, max_size]` that keeps up with its observed load.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OnLoadPolicy {
    pub min_size: String,
    pub max_size: String,
    /// How long the load must have fit a smaller size before scaling down.
    /// `None` falls back to the system default.
    pub scale_down_after: Option<Duration>,
}

/// The `ON LOAD` strategy's durable bookkeeping, mirrored from durable state.
///
/// Holds only what cannot be re-derived from load samples after a restart: the
/// reconfiguration the strategy is waiting on, the targets that recently
/// failed, and the hold state already audited.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LoadRecord {
    /// The strategy's own reconfiguration that has not been accounted for yet.
    pub attempt: Option<LoadAttempt>,
    /// When the strategy last cut the cluster over to a new size. Samples up
    /// to a warm-up period after it describe the transition, not the workload.
    pub last_cutover: Option<Timestamp>,
    /// Sizes that recently failed as a target, at most one entry per size and
    /// direction.
    pub backoff: Vec<LoadBackoff>,
    /// The reason the strategy is declining to act, if any. Stored so that
    /// entering and leaving a hold is audited once and not every tick.
    pub hold: Option<LoadHold>,
}

/// A reconfiguration written by the `ON LOAD` strategy whose outcome the
/// strategy has not yet recorded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadAttempt {
    pub from_size: String,
    pub target_size: String,
    /// The deadline of the reconfiguration record this attempt wrote. Together
    /// with the target size it identifies that record, so a record a user
    /// wrote over it is not mistaken for the attempt's outcome.
    pub deadline: Timestamp,
    /// The lowest progress ratio, in permille, among the samples that
    /// motivated a scale-up. `None` for an attempt not motivated by lag.
    pub progress_before: Option<u32>,
    pub phase: LoadAttemptPhase,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoadAttemptPhase {
    /// The reconfiguration record is in progress.
    Reconfiguring,
    /// The cluster cut over to the target. The strategy is waiting for enough
    /// samples at the new size to judge whether a lag-motivated scale-up
    /// helped.
    Evaluating,
}

/// A size the `ON LOAD` strategy must not target until `until`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadBackoff {
    pub size: String,
    pub direction: LoadDirection,
    pub until: Timestamp,
    /// Consecutive failures for this size and direction. Kept after `until`
    /// passes so the next failure backs off for longer.
    pub failures: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoadDirection {
    /// A scale-up to this size or any larger one is blocked.
    Up,
    /// A scale-down to this size or any smaller one is blocked.
    Down,
}

/// Why the `ON LOAD` strategy is declining to act although the cluster is not
/// at its recommended size or is not keeping up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoadHold {
    /// A dataflow is behind and one worker is saturated, but the load does not
    /// call for a larger size. More workers would not help.
    SkewLimited,
    /// The size the load calls for recently failed as a target.
    BackingOff,
}

/// Why the `ON LOAD` strategy started a reconfiguration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoadScaleReason {
    /// The load over the activation window calls for a larger size.
    ScaleUp,
    /// A replica at the realized size was killed for exceeding its memory.
    OutOfMemory,
    /// The load over the scale-down window fits a smaller size.
    ScaleDown,
    /// A lag-motivated scale-up did not improve progress, so it is reverted.
    IneffectiveScaleUp,
}

/// The lifecycle transition a write to the `load` record represents, declared
/// by the writer like [`ReconfigurationAudit`]. Bookkeeping rewrites declare no
/// audit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LoadAudit {
    /// The strategy started a reconfiguration. The demand is what the decision
    /// was sized from.
    ScaleStarted {
        from_size: String,
        target_size: String,
        reason: LoadScaleReason,
        cpu_demand_millicores: u64,
        memory_demand_bytes: u64,
    },
    /// A target failed and is now backed off.
    BackoffStarted {
        size: String,
        direction: LoadDirection,
        until: Timestamp,
    },
    /// The strategy entered (`Some`) or left (`None`) a hold.
    HoldChanged { hold: Option<LoadHold> },
}

/// The `ON HYDRATION` burst sub-policy: while some object on the cluster is not
/// hydrated on a steady replica, run one extra replica at `hydration_size` to
/// accelerate hydration, lingering for `linger_duration` after a steady replica catches up.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OnHydrationPolicy {
    pub hydration_size: String,
    /// `None` falls back to the system default linger when the burst strategy
    /// writes its record.
    pub linger_duration: Option<Duration>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn azs(zones: &[&str]) -> AvailabilityZones {
        AvailabilityZones(zones.iter().map(|z| z.to_string()).collect())
    }

    fn shape(zones: &[&str]) -> ReplicaShape {
        ReplicaShape {
            size: "small".to_string(),
            availability_zones: azs(zones),
            logging: ComputeReplicaLogging::default(),
            arrangement_compression: false,
        }
    }

    #[mz_ore::test]
    fn availability_zones_identity_is_ordered_pool_is_not() {
        // Provisioning order is part of the value's identity, so a reorder is a
        // distinct configuration that the compare-and-append witness can see.
        assert_ne!(azs(&["a", "b"]), azs(&["b", "a"]));
        // The pool drops the order: the same zones in any order are one pool, but
        // a different set of zones is a different pool.
        assert_eq!(azs(&["a", "b"]).pool(), azs(&["b", "a"]).pool());
        assert_ne!(azs(&["a", "b"]).pool(), azs(&["a", "c"]).pool());
    }

    #[mz_ore::test]
    fn replica_shape_matches_ignores_zone_order() {
        // Interchangeability ignores provisioning order: an already-placed
        // replica satisfies a desired shape with the same zones reordered, so a
        // reorder alone never forces a reprovision.
        assert!(shape(&["a", "b"]).matches(&shape(&["b", "a"])));
        // A different zone set is a different shape.
        assert!(!shape(&["a", "b"]).matches(&shape(&["a", "c"])));
    }
}
