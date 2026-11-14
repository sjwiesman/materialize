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
use crate::durable::upgrade::objects_v93 as v93;
use crate::durable::upgrade::objects_v94 as v94;

crate::json_compatible!(v93::ClusterKey with v94::ClusterKey);
crate::json_compatible!(v93::RoleId with v94::RoleId);
crate::json_compatible!(v93::MzAclItem with v94::MzAclItem);
crate::json_compatible!(v93::ReplicaLogging with v94::ReplicaLogging);
crate::json_compatible!(v93::OptimizerFeatureOverride with v94::OptimizerFeatureOverride);
crate::json_compatible!(v93::ClusterSchedule with v94::ClusterSchedule);
crate::json_compatible!(v93::OnHydration with v94::OnHydration);
crate::json_compatible!(v93::BurstState with v94::BurstState);
crate::json_compatible!(v93::ReconfigurationTarget with v94::ReconfigurationTarget);
crate::json_compatible!(v93::OnTimeoutAction with v94::OnTimeoutAction);
crate::json_compatible!(v93::ReconfigurationStatus with v94::ReconfigurationStatus);

/// Adds the durable state of the `ON LOAD` autoscaling strategy to managed
/// clusters: the `load` record (backfilled as `None`), the `on_load` sub-policy
/// of an existing autoscaling policy (backfilled as `None`), and the `origin`
/// of an existing reconfiguration record (backfilled as `User`, the only
/// writer of such records before this version).
///
/// Every managed cluster is rewritten, including those whose fields all
/// backfill to `None`. A later edit retracts the record by its v94 encoding,
/// which carries `"load": null`. A stored record without that field would not
/// match the retraction.
///
/// The remaining v93->v94 changes are additive and confined to the
/// append-only audit log.
pub fn upgrade(
    snapshot: Vec<v93::StateUpdateKind>,
) -> Vec<MigrationAction<v93::StateUpdateKind, v94::StateUpdateKind>> {
    snapshot
        .into_iter()
        .filter_map(|update| match update {
            v93::StateUpdateKind::Cluster(cluster)
                if matches!(
                    cluster.value.config.variant,
                    v93::ClusterVariant::Managed(_)
                ) =>
            {
                let new_cluster = migrate_cluster(cluster.clone());
                Some(MigrationAction::Update(
                    v93::StateUpdateKind::Cluster(cluster),
                    v94::StateUpdateKind::Cluster(new_cluster),
                ))
            }
            _ => None,
        })
        .collect()
}

fn migrate_cluster(old: v93::Cluster) -> v94::Cluster {
    let v93::Cluster { key, value } = old;
    let v93::ClusterVariant::Managed(m) = value.config.variant else {
        unreachable!("caller guards on the managed variant");
    };
    v94::Cluster {
        key: JsonCompatible::convert(&key),
        value: v94::ClusterValue {
            name: value.name,
            owner_id: JsonCompatible::convert(&value.owner_id),
            privileges: value
                .privileges
                .iter()
                .map(JsonCompatible::convert)
                .collect(),
            config: v94::ClusterConfig {
                workload_class: value.config.workload_class,
                variant: v94::ClusterVariant::Managed(v94::ManagedCluster {
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
                        v94::AutoScalingStrategy {
                            on_hydration: strategy
                                .on_hydration
                                .as_ref()
                                .map(JsonCompatible::convert),
                            on_load: None,
                        }
                    }),
                    reconfiguration: m.reconfiguration.map(|record| v94::ReconfigurationState {
                        target: JsonCompatible::convert(&record.target),
                        deadline: record.deadline,
                        on_timeout: JsonCompatible::convert(&record.on_timeout),
                        status: JsonCompatible::convert(&record.status),
                        origin: v94::ReconfigurationOrigin::User,
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
        fn backfills_managed_clusters_and_preserves_the_rest(
            mut cluster in any::<v93::Cluster>(),
            managed in any::<v93::ManagedCluster>(),
        ) {
            cluster.value.config.variant = v93::ClusterVariant::Managed(managed);
            let old = v93::StateUpdateKind::Cluster(cluster);
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
            }
            prop_assert_eq!(serde_json::to_value(inserted).expect("serializable"), expected);
        }

        #[mz_ore::test]
        fn leaves_other_records_unchanged(
            mut cluster in any::<v93::Cluster>(),
            replica in any::<v93::ClusterReplica>(),
        ) {
            cluster.value.config.variant = v93::ClusterVariant::Unmanaged;
            let unmanaged = v93::StateUpdateKind::Cluster(cluster);
            let replica = v93::StateUpdateKind::ClusterReplica(replica);
            for old in [&unmanaged, &replica] {
                let encoded = serde_json::to_vec(old).expect("serializable");
                let new: v94::StateUpdateKind =
                    serde_json::from_slice(&encoded).expect("compatible");
                prop_assert_eq!(serde_json::to_vec(&new).expect("serializable"), encoded);
            }
            prop_assert!(upgrade(vec![unmanaged, replica]).is_empty());
        }
    }
}
