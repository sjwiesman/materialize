# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=120s

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET compute_worker_busy_window = '1s'

> CREATE CLUSTER busy_c (SIZE 'scale=1,workers=2')
> CREATE TABLE busy_t (a int)
> INSERT INTO busy_t SELECT generate_series(1, 1000)
> CREATE INDEX busy_i IN CLUSTER busy_c ON busy_t (a)

# The replica-local log has, for the last closed window, one row per worker.
# Windows end at multiples of the window length, and all workers report the
# same window.
> SET cluster = busy_c
> SET cluster_replica = r1

> SELECT count(*), count(DISTINCT window_end), bool_and(window_end::text::uint8 % 1000 = 0), bool_and(window_end::text::uint8 > window_start::text::uint8), bool_and(busy_ns <= (window_end::text::uint8 - window_start::text::uint8) * 1000000) FROM mz_introspection.mz_scheduling_busy_per_worker
2 1 true true true

> RESET cluster_replica
> RESET cluster

# The history accumulates a row per replica and closed window. Workers that
# start on different sides of a window boundary report their first windows in
# separate rows.
> SELECT count(*) FILTER (WHERE h.workers = 2) >= 3, bool_and(h.workers <= 2), bool_and(h.max_worker_busy_ns <= h.busy_ns), bool_and(h.busy_ns <= h.workers * extract(epoch FROM h.window_end - h.window_start) * 1000000000) FROM mz_internal.mz_cluster_replica_busy_history h JOIN mz_cluster_replicas r ON h.replica_id = r.id JOIN mz_clusters c ON r.cluster_id = c.id WHERE c.name = 'busy_c'
true true true true true

# Dropping the replica keeps its history.
$ set-from-sql var=replica-id
SELECT r.id FROM mz_cluster_replicas r JOIN mz_clusters c ON r.cluster_id = c.id WHERE c.name = 'busy_c'

> DROP CLUSTER busy_c CASCADE

> SELECT count(*) >= 3 FROM mz_internal.mz_cluster_replica_busy_history WHERE replica_id = '${replica-id}'
true

> DROP TABLE busy_t

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM RESET compute_worker_busy_window
