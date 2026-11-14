# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""All SQL the autoscaler issues.

Unsigned and `mz_timestamp` columns are cast to `text` and parsed here, so the
driver never has to know Materialize's custom type OIDs.
"""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg import sql

from materialize.cluster_autoscaler.config import Connection
from materialize.cluster_autoscaler.history import (
    MetricsRow,
    ReconfigurationEvent,
    summarize_metrics,
    summarize_transitions,
)
from materialize.cluster_autoscaler.model import (
    ClusterObject,
    ClusterPolicy,
    ClusterSnapshot,
    Reconfiguration,
    Replica,
    Size,
    WindowHistory,
)
from materialize.cluster_autoscaler.sampling import (
    TIMEOUT,
    ReplicaRead,
    classify_read_failure,
)

CATALOG_STATEMENT_TIMEOUT = "60s"
# How far back "recent" lag reaches. `mz_wallclock_global_lag_recent_history`
# has one row per object and minute.
RECENT_LAG_SECONDS = 180.0
# Reconfiguration events read per cluster. Only the latest settled transition
# and the latest failure are used, so this bounds how far back a failure can
# be found, not correctness.
AUDIT_EVENT_LIMIT = 200


class ReadTimeout(Exception):
    """A targeted read hit `statement_timeout` on a live connection."""


class ReadFailure(Exception):
    """A targeted read failed for any reason other than a statement timeout."""


class Database:
    def __init__(self, connection: Connection, connect_timeout: int = 10):
        self.connection = connection
        self.connect_timeout = connect_timeout

    def connect(self) -> psycopg.Connection[Any]:
        c = self.connection
        return psycopg.connect(
            host=c.host,
            port=c.port,
            user=c.user,
            password=c.password,
            dbname=c.database,
            sslmode=c.sslmode,
            connect_timeout=self.connect_timeout,
            autocommit=True,
            # Client-side binding, so that no statement depends on
            # server-side parameter type inference.
            cursor_factory=psycopg.ClientCursor,
        )

    def catalog_session(self) -> psycopg.Connection[Any]:
        """A session whose reads run on `mz_catalog_server` and touch no user
        cluster."""
        conn = self.connect()
        conn.execute("SET cluster = mz_catalog_server")
        conn.execute(
            sql.SQL("SET statement_timeout = {}").format(
                sql.Literal(CATALOG_STATEMENT_TIMEOUT)
            )
        )
        return conn


def _int(value: Any) -> int | None:
    return None if value is None else int(value)


def read_sizes(conn: psycopg.Connection[Any]) -> dict[str, Size]:
    """Reads the enabled sizes. Disabled sizes are not in the relation."""
    rows = conn.execute("""
        SELECT size, processes::text, workers::text, cpu_nano_cores::text,
               memory_bytes::text, credits_per_hour::text
        FROM mz_catalog.mz_cluster_replica_sizes
        """).fetchall()
    return {
        name: Size(
            name=name,
            processes=int(processes),
            workers=int(workers),
            cpu_nano_cores=int(cpu),
            memory_bytes=int(memory),
            credits_per_hour=float(credits),
        )
        for name, processes, workers, cpu, memory, credits in rows
    }


def read_role(conn: psycopg.Connection[Any]) -> str:
    row = conn.execute("SELECT current_user").fetchone()
    assert row is not None
    return row[0]


_OBJECTS = """
    SELECT id, 'compute' AS kind FROM mz_catalog.mz_indexes
    WHERE cluster_id = %(cluster)s
    UNION ALL
    SELECT id, 'compute' FROM mz_catalog.mz_materialized_views
    WHERE cluster_id = %(cluster)s
    UNION ALL
    SELECT id, 'storage' FROM mz_catalog.mz_sources
    WHERE cluster_id = %(cluster)s AND type != 'webhook'
    UNION ALL
    SELECT id, 'storage' FROM mz_catalog.mz_sinks
    WHERE cluster_id = %(cluster)s
"""


def read_snapshot(
    conn: psycopg.Connection[Any], policy: ClusterPolicy, role: str
) -> ClusterSnapshot | None:
    """Performs the catalog read for one cluster. `None` if it does not exist.

    `conn` must come from `Database.catalog_session`.
    """
    row = conn.execute(
        """
        SELECT c.id, c.managed, c.size, c.introspection_interval IS NOT NULL,
               coalesce(s.type, 'manual'),
               extract(epoch FROM now())::float8
        FROM mz_catalog.mz_clusters c
        LEFT JOIN mz_internal.mz_cluster_schedules s ON s.cluster_id = c.id
        WHERE c.name = %s
        """,
        (policy.name,),
    ).fetchone()
    if row is None:
        return None
    cluster_id, managed, size, introspection, schedule, now = row
    params = {"cluster": cluster_id, "since": now - policy.cooldown_seconds}

    replicas = tuple(
        Replica(id=id, name=name, size=size)
        for id, name, size in conn.execute(
            """
            SELECT id, name, size FROM mz_catalog.mz_cluster_replicas
            WHERE cluster_id = %(cluster)s
            ORDER BY name
            """,
            params,
        ).fetchall()
    )

    row = conn.execute(
        """
        SELECT status, target->>'size'
        FROM mz_internal.mz_cluster_reconfigurations
        WHERE cluster_id = %(cluster)s
        """,
        params,
    ).fetchone()
    reconfiguration = Reconfiguration(*row) if row is not None else None

    # Reconfiguration lifecycle events carry `target_size`. The plain alter
    # event written next to each of them and the hydration burst events do
    # not.
    events = [
        ReconfigurationEvent(
            id=int(id),
            transition=transition,
            target_size=target_size,
            user=user,
            occurred_at=occurred_at,
        )
        for id, transition, target_size, user, occurred_at in conn.execute(
            """
            SELECT id::text, details->>'transition', details->>'target_size',
                   "user", extract(epoch FROM occurred_at)::float8
            FROM mz_catalog.mz_audit_events
            WHERE event_type = 'alter'
              AND object_type = 'cluster'
              AND details->>'cluster_id' = %(cluster)s
              AND details->>'transition' IS NOT NULL
              AND details->>'target_size' IS NOT NULL
            ORDER BY id DESC
            LIMIT %(limit)s
            """,
            {**params, "limit": AUDIT_EVENT_LIMIT},
        ).fetchall()
    ]
    last_transition, last_failure = summarize_transitions(events, role)

    objects = tuple(
        ClusterObject(id=id, kind=kind)
        for id, kind in conn.execute(_OBJECTS, params).fetchall()
    )

    hydrated = frozenset(
        (object_id, replica_id)
        for object_id, replica_id in conn.execute(
            f"""
            SELECT h.object_id, h.replica_id
            FROM mz_internal.mz_hydration_statuses h
            WHERE h.hydrated
              AND h.replica_id IS NOT NULL
              AND h.object_id IN (SELECT id FROM ({_OBJECTS}) AS objects)
            """,
            params,
        ).fetchall()
    )

    oom_kills = tuple(
        (replica_id, at)
        for replica_id, at in conn.execute(
            """
            SELECT h.replica_id, extract(epoch FROM h.occurred_at)::float8
            FROM mz_internal.mz_cluster_replica_status_history h
            JOIN mz_catalog.mz_cluster_replicas r ON r.id = h.replica_id
            WHERE r.cluster_id = %(cluster)s
              AND h.reason = 'oom-killed'
              AND h.occurred_at > to_timestamp(%(since)s::float8)
            """,
            params,
        ).fetchall()
    )

    metrics = [
        MetricsRow(
            replica_id=replica_id,
            process_id=process_id,
            cpu_nano_cores=_int(cpu),
            memory_bytes=_int(memory),
            heap_bytes=_int(heap),
            swap_bytes=_int(swap),
            occurred_at=occurred_at,
        )
        for replica_id, process_id, cpu, memory, heap, swap, occurred_at in conn.execute(
            """
            SELECT m.replica_id, m.process_id::text, m.cpu_nano_cores::text,
                   m.memory_bytes::text, m.heap_bytes::text, m.swap_bytes::text,
                   extract(epoch FROM m.occurred_at)::float8
            FROM mz_internal.mz_cluster_replica_metrics_history m
            JOIN mz_catalog.mz_cluster_replicas r ON r.id = m.replica_id
            WHERE r.cluster_id = %(cluster)s
              AND r.size = %(size)s
              AND m.occurred_at > to_timestamp(%(since)s::float8)
            """,
            {**params, "size": size},
        ).fetchall()
    ]
    peak_cpu, peak_memory, oldest_metrics_at = summarize_metrics(metrics)

    def max_lag(since: float) -> float | None:
        row = conn.execute(
            f"""
            SELECT max(extract(epoch FROM l.lag))::float8
            FROM mz_internal.mz_wallclock_global_lag_recent_history l
            WHERE l.occurred_at > to_timestamp(%(since)s::float8)
              AND l.object_id IN (SELECT id FROM ({_OBJECTS}) AS objects)
            """,
            {**params, "since": since},
        ).fetchone()
        return row[0] if row is not None else None

    return ClusterSnapshot(
        id=cluster_id,
        name=policy.name,
        now=now,
        managed=managed,
        schedule=schedule,
        size=size,
        introspection=introspection,
        replicas=replicas,
        reconfiguration=reconfiguration,
        last_transition=last_transition,
        last_failure=last_failure,
        objects=objects,
        hydrated=hydrated,
        oom_kills=oom_kills,
        history=WindowHistory(
            peak_cpu_nano_cores=peak_cpu,
            peak_memory_bytes=peak_memory,
            oldest_metrics_at=oldest_metrics_at,
            peak_lag_seconds=max_lag(now - policy.cooldown_seconds),
            recent_lag_seconds=max_lag(now - RECENT_LAG_SECONDS),
        ),
    )


def read_cluster_state(
    conn: psycopg.Connection[Any], cluster_id: str
) -> tuple[str | None, Reconfiguration | None]:
    """Reads a cluster's realized size and its reconfiguration record."""
    row = conn.execute(
        "SELECT size FROM mz_catalog.mz_clusters WHERE id = %s", (cluster_id,)
    ).fetchone()
    size = row[0] if row is not None else None
    row = conn.execute(
        """
        SELECT status, target->>'size'
        FROM mz_internal.mz_cluster_reconfigurations
        WHERE cluster_id = %s
        """,
        (cluster_id,),
    ).fetchone()
    return size, Reconfiguration(*row) if row is not None else None


def alter_cluster_size(conn: psycopg.Connection[Any], cluster: str, size: str) -> None:
    conn.execute(
        sql.SQL("ALTER CLUSTER {} SET (SIZE = {})").format(
            sql.Identifier(cluster), sql.Literal(size)
        )
    )


# One query, so that busy time and frontiers are read at one timestamp.
# `mz_now()` reports it, which makes the interval between two reads exact
# however long each read took to return.
_REPLICA_READ = """
    SELECT 'clock' AS kind, NULL::text AS k1, NULL::text AS k2, NULL::text AS k3,
           mz_now()::text AS value
    UNION ALL
    SELECT 'elapsed', e.id::text, e.worker_id::text, a.address[1]::text,
           e.elapsed_ns::text
    FROM mz_introspection.mz_scheduling_elapsed_per_worker e
    JOIN mz_introspection.mz_dataflow_addresses a ON a.id = e.id
    WHERE mz_catalog.list_length(a.address) = 1
    UNION ALL
    SELECT 'export', x.export_id, NULL, x.dataflow_id::text, f.time::text
    FROM mz_introspection.mz_compute_exports x
    JOIN mz_introspection.mz_compute_frontiers f ON f.export_id = x.export_id
    UNION ALL
    SELECT 'import', i.export_id, i.import_id, NULL, i.time::text
    FROM mz_introspection.mz_compute_import_frontiers i
"""


def parse_replica_read(rows: list[tuple[str, Any, Any, Any, str]]) -> ReplicaRead:
    read = ReplicaRead(at_ms=0.0)
    for kind, k1, k2, k3, value in rows:
        if kind == "clock":
            read.at_ms = float(value)
        elif kind == "elapsed":
            read.elapsed_ns[(k1, k2)] = int(value)
            read.operator_dataflow[k1] = k3
        elif kind == "export":
            read.exports[k1] = (k3, int(value))
        elif kind == "import":
            read.imports[(k1, k2)] = int(value)
    return read


class ReplicaSession:
    """A session targeted at one replica, for the busy-time reads.

    Only ever opened for a replica the gate reported as hydrated.
    """

    def __init__(
        self, db: Database, cluster: str, replica: str, timeout_seconds: float
    ):
        self.db = db
        self.cluster = cluster
        self.replica = replica
        self.timeout_seconds = timeout_seconds
        self.conn: psycopg.Connection[Any] | None = None

    def _connect(self) -> psycopg.Connection[Any]:
        conn = self.db.connect()
        # A timeout of 0 would disable the limit.
        timeout = f"{max(1, int(self.timeout_seconds * 1000))}ms"
        try:
            for statement, value in (
                (sql.SQL("SET cluster = {}"), self.cluster),
                (sql.SQL("SET cluster_replica = {}"), self.replica),
                # Introspection reads need no linearizability.
                (sql.SQL("SET transaction_isolation = {}"), "serializable"),
                (sql.SQL("SET statement_timeout = {}"), timeout),
            ):
                conn.execute(statement.format(sql.Literal(value)))
        except BaseException:
            conn.close()
            raise
        return conn

    def read(self) -> ReplicaRead:
        """Takes one read. Raises `ReadTimeout` or `ReadFailure`."""
        try:
            if self.conn is None:
                self.conn = self._connect()
            rows = self.conn.execute(_REPLICA_READ).fetchall()
        except psycopg.Error as e:
            alive = self.conn is not None and not self.conn.closed
            if classify_read_failure(e.sqlstate, str(e), alive) == TIMEOUT:
                raise ReadTimeout(str(e)) from e
            raise ReadFailure(str(e).strip() or type(e).__name__) from e
        return parse_replica_read(rows)

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None
