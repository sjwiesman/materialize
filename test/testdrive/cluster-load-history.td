# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Heap metrics arrive once a minute, so the first summary with a heap can take
# that long.
$ set-sql-timeout duration=180s

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET load_autoscaling_lag_sample_interval = '1s'
ALTER SYSTEM SET load_autoscaling_evaluation_window = '10s'
ALTER SYSTEM SET load_autoscaling_warm_up = '0s'
ALTER SYSTEM SET compute_worker_busy_window = '1s'

> SELECT c.name, c.type FROM mz_columns c JOIN mz_objects o ON c.id = o.id JOIN mz_schemas s ON o.schema_id = s.id WHERE s.name = 'mz_internal' AND o.name = 'mz_cluster_load_history' ORDER BY c.position
cluster_id                 text
window_start               "timestamp with time zone"
window_end                 "timestamp with time zone"
size                       text
fingerprint                uint8
heap_bytes                 uint8
heap_fraction              "double precision"
busy_millicores            uint8
busiest_worker_utilization "double precision"
killed                     boolean

> SELECT c.name, c.type FROM mz_columns c JOIN mz_objects o ON c.id = o.id JOIN mz_schemas s ON o.schema_id = s.id WHERE s.name = 'mz_internal' AND o.name = 'mz_cluster_load_dataflow_history' ORDER BY c.position
cluster_id     text
window_end     "timestamp with time zone"
dataflow_id    text
median_lag     interval
progress_ratio "double precision"
behind         boolean

> CREATE CLUSTER load_c (SIZE 'scale=1,workers=1')
> CREATE TABLE load_t (a int)
> INSERT INTO load_t VALUES (1)
> CREATE INDEX load_i IN CLUSTER load_c ON load_t (a)
> CREATE MATERIALIZED VIEW load_mv IN CLUSTER load_c AS SELECT count(*) FROM load_t

# A managed cluster without an autoscaling policy gets a summary per window,
# with its size and without a memory kill.
> SELECT count(*) >= 2, bool_and(h.size = 'scale=1,workers=1'), bool_and(h.window_end - h.window_start = INTERVAL '10s'), bool_and(extract(epoch FROM h.window_end)::int8 % 10 = 0), bool_and(NOT h.killed) FROM mz_internal.mz_cluster_load_history h JOIN mz_clusters c ON h.cluster_id = c.id WHERE c.name = 'load_c'
true true true true true

# Busy time and heap reach the summaries.
> SELECT count(h.busy_millicores) > 0, count(h.heap_bytes) > 0 FROM mz_internal.mz_cluster_load_history h JOIN mz_clusters c ON h.cluster_id = c.id WHERE c.name = 'load_c'
true true

# Summaries have a row per dataflow. The idle dataflows keep up.
> SELECT o.name, count(*) > 0, bool_and(d.median_lag < INTERVAL '10s') FROM mz_internal.mz_cluster_load_dataflow_history d JOIN mz_clusters c ON d.cluster_id = c.id JOIN mz_internal.mz_object_global_ids g ON g.global_id = d.dataflow_id JOIN mz_objects o ON o.id = g.id WHERE c.name = 'load_c' GROUP BY o.name ORDER BY o.name
load_i true true
load_mv true true

# Dataflow rows belong to a summary.
> SELECT count(*) FROM mz_internal.mz_cluster_load_dataflow_history d JOIN mz_clusters c ON d.cluster_id = c.id LEFT JOIN mz_internal.mz_cluster_load_history h ON d.cluster_id = h.cluster_id AND d.window_end = h.window_end WHERE c.name = 'load_c' AND h.cluster_id IS NULL
0

# Dropping the cluster keeps its history.
$ set-from-sql var=cluster-id
SELECT id FROM mz_clusters WHERE name = 'load_c'

> DROP CLUSTER load_c CASCADE

> SELECT count(*) > 0 FROM mz_internal.mz_cluster_load_history WHERE cluster_id = '${cluster-id}'
true

> DROP TABLE load_t

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM RESET load_autoscaling_lag_sample_interval
ALTER SYSTEM RESET load_autoscaling_evaluation_window
ALTER SYSTEM RESET load_autoscaling_warm_up
ALTER SYSTEM RESET compute_worker_busy_window
