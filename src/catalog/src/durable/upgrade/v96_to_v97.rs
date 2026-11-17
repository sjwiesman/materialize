// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::json_compatible::JsonCompatible;
use crate::durable::upgrade::objects_v96 as v96;
use crate::durable::upgrade::objects_v97 as v97;

crate::json_compatible!(v96::ClusterKey with v97::ClusterKey);
crate::json_compatible!(v96::RoleId with v97::RoleId);
crate::json_compatible!(v96::MzAclItem with v97::MzAclItem);
crate::json_compatible!(v96::QueryPolicyId with v97::QueryPolicyId);
crate::json_compatible!(v96::ReplicaLogging with v97::ReplicaLogging);
crate::json_compatible!(v96::OptimizerFeatureOverride with v97::OptimizerFeatureOverride);
crate::json_compatible!(v96::ClusterSchedule with v97::ClusterSchedule);
crate::json_compatible!(v96::OnHydration with v97::OnHydration);
crate::json_compatible!(v96::ReconfigurationTarget with v97::ReconfigurationTarget);
crate::json_compatible!(v96::OnTimeoutAction with v97::OnTimeoutAction);
crate::json_compatible!(v96::ReconfigurationStatus with v97::ReconfigurationStatus);
crate::json_compatible!(v96::BurstState with v97::BurstState);

/// Adds the `ON LOAD` autoscaling state to managed clusters. An existing
/// policy gets no `on_load` sub-policy, an existing reconfiguration record gets
/// origin `User` and no trial, and every managed cluster gets no `load` record.
pub fn upgrade(
    snapshot: Vec<v96::StateUpdateKind>,
) -> Vec<MigrationAction<v96::StateUpdateKind, v97::StateUpdateKind>> {
    // Every managed cluster is rewritten, including those with nothing to
    // backfill, because a later retraction must match the stored JSON, which
    // now carries `"load": null`.
    snapshot
        .into_iter()
        .filter_map(|update| match update {
            v96::StateUpdateKind::Cluster(cluster)
                if matches!(
                    cluster.value.config.variant,
                    v96::ClusterVariant::Managed(_)
                ) =>
            {
                let new_cluster = migrate_cluster(cluster.clone());
                Some(MigrationAction::Update(
                    v96::StateUpdateKind::Cluster(cluster),
                    v97::StateUpdateKind::Cluster(new_cluster),
                ))
            }
            _ => None,
        })
        .collect()
}

fn migrate_cluster(old: v96::Cluster) -> v97::Cluster {
    let v96::Cluster { key, value } = old;
    let v96::ClusterVariant::Managed(m) = value.config.variant else {
        unreachable!("caller guards on the managed variant");
    };
    v97::Cluster {
        key: JsonCompatible::convert(&key),
        value: v97::ClusterValue {
            name: value.name,
            owner_id: JsonCompatible::convert(&value.owner_id),
            privileges: value
                .privileges
                .iter()
                .map(JsonCompatible::convert)
                .collect(),
            config: v97::ClusterConfig {
                workload_class: value.config.workload_class,
                query_policy: value
                    .config
                    .query_policy
                    .as_ref()
                    .map(JsonCompatible::convert),
                variant: v97::ClusterVariant::Managed(v97::ManagedCluster {
                    size: m.size,
                    replication_factor: m.replication_factor,
                    availability_zones: m.availability_zones,
                    logging: JsonCompatible::convert(&m.logging),
                    arrangement_compression: m.arrangement_compression,
                    optimizer_feature_overrides: m
                        .optimizer_feature_overrides
                        .iter()
                        .map(JsonCompatible::convert)
                        .collect(),
                    schedule: JsonCompatible::convert(&m.schedule),
                    auto_scaling_strategy: m.auto_scaling_strategy.map(|strategy| {
                        v97::AutoScalingStrategy {
                            on_hydration: strategy
                                .on_hydration
                                .as_ref()
                                .map(JsonCompatible::convert),
                            on_load: None,
                        }
                    }),
                    reconfiguration: m.reconfiguration.map(|record| v97::ReconfigurationState {
                        target: JsonCompatible::convert(&record.target),
                        deadline: record.deadline,
                        on_timeout: JsonCompatible::convert(&record.on_timeout),
                        status: JsonCompatible::convert(&record.status),
                        // `ALTER CLUSTER` is the only writer of records before v97.
                        origin: v97::ReconfigurationOrigin::User,
                        trial: None,
                    }),
                    burst: m.burst.as_ref().map(JsonCompatible::convert),
                    load: None,
                }),
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn backfills_managed_clusters_and_preserves_the_rest(
            mut cluster in any::<v96::Cluster>(),
            managed in any::<v96::ManagedCluster>(),
        ) {
            cluster.value.config.variant = v96::ClusterVariant::Managed(managed);
            let old = v96::StateUpdateKind::Cluster(cluster);
            let actions = upgrade(vec![old.clone()]);
            prop_assert_eq!(actions.len(), 1);
            let MigrationAction::Update(retracted, inserted) = &actions[0] else {
                panic!("expected a cluster update");
            };
            prop_assert_eq!(retracted, &old);

            let mut expected = serde_json::to_value(&old).expect("serializable");
            let managed_json = expected["value"]["config"]["variant"]["Managed"]
                .as_object_mut()
                .expect("managed object");
            managed_json.insert("load".into(), serde_json::Value::Null);
            if let Some(strategy) = managed_json["auto_scaling_strategy"].as_object_mut() {
                strategy.insert("on_load".into(), serde_json::Value::Null);
            }
            if let Some(record) = managed_json["reconfiguration"].as_object_mut() {
                record.insert("origin".into(), "User".into());
                record.insert("trial".into(), serde_json::Value::Null);
            }
            prop_assert_eq!(serde_json::to_value(inserted).expect("serializable"), expected);
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn leaves_other_records_unchanged(
            mut cluster in any::<v96::Cluster>(),
            replica in any::<v96::ClusterReplica>(),
        ) {
            cluster.value.config.variant = v96::ClusterVariant::Unmanaged;
            let unmanaged = v96::StateUpdateKind::Cluster(cluster);
            let replica = v96::StateUpdateKind::ClusterReplica(replica);
            for old in [&unmanaged, &replica] {
                let encoded = serde_json::to_vec(old).expect("serializable");
                let new: v97::StateUpdateKind =
                    serde_json::from_slice(&encoded).expect("compatible");
                prop_assert_eq!(serde_json::to_vec(&new).expect("serializable"), encoded);
            }
            prop_assert!(upgrade(vec![unmanaged, replica]).is_empty());
        }
    }
}
