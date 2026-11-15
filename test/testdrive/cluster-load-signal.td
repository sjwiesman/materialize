# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The load signal behind the ON LOAD autoscaling strategy: the recommendations
# computed from the replicas' busy time. None of it depends on
# `enable_load_autoscaling`, which stays off here. The busy time itself is
# covered by cluster-replica-busy-history.td.

$ set-sql-timeout duration=120s

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET compute_worker_busy_window = '1s'
ALTER SYSTEM SET load_autoscaling_warm_up = '0s'

> CREATE CLUSTER load_c (SIZE 'scale=1,workers=2')
> CREATE TABLE load_t (a int)
> INSERT INTO load_t SELECT generate_series(1, 1000)
> CREATE INDEX load_i IN CLUSTER load_c ON load_t (a)
> CREATE MATERIALIZED VIEW load_mv IN CLUSTER load_c AS SELECT count(*) FROM load_t

# An idle cluster gets a measured, steady recommendation once enough samples
# are in, without an ON LOAD policy. It keeps up, so its lag is within the
# tolerance.
> SELECT rec.lag <= INTERVAL '2s', rec.progress_ratio IS NOT NULL, o.name IN ('load_i', 'load_mv'), rec.heap_fraction BETWEEN 0 AND 1, rec.mean_worker_utilization BETWEEN 0 AND 1, rec.max_worker_utilization BETWEEN 0 AND 1, rec.recommended_size IS NOT NULL, rec.status FROM mz_internal.mz_cluster_load_recommendations rec JOIN mz_clusters c ON c.id = rec.cluster_id JOIN mz_objects o ON o.id = rec.slowest_object_id WHERE c.name = 'load_c'
true true true true true true true steady

# A dropped cluster loses its row.
> DROP CLUSTER load_c CASCADE

> SELECT count(*) FROM mz_internal.mz_cluster_load_recommendations rec LEFT JOIN mz_clusters c ON c.id = rec.cluster_id WHERE c.id IS NULL
0

> DROP TABLE load_t

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM RESET compute_worker_busy_window
ALTER SYSTEM RESET load_autoscaling_warm_up
