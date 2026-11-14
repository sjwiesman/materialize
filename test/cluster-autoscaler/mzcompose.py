# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Tests the stateless external cluster autoscaler against a live environment.

The autoscaler runs in this process, against the `materialized` service, so
that the tests can observe which replicas it targets and can stop it at a
chosen step.
"""

import json
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import yaml

from materialize.cluster_autoscaler import catalog, runner
from materialize.cluster_autoscaler.catalog import Database, ReplicaSession
from materialize.cluster_autoscaler.config import Config, Connection
from materialize.cluster_autoscaler.model import (
    UNRESPONSIVE,
    ClusterPolicy,
    ModelParams,
)
from materialize.cluster_autoscaler.runner import RunOptions, run_once
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized

SERVICES = [Materialized()]

# The sizes mzcompose defines do not form a family by name, so every policy
# lists its ladder.
LADDER = ("scale=1,workers=1", "scale=1,workers=2", "scale=1,workers=4")


def workflow_default(c: Composition) -> None:
    for name in c.workflows:
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)


def setup(c: Composition) -> None:
    c.down(destroy_volumes=True)
    c.up("materialized")


def connection(c: Composition) -> Connection:
    return Connection(
        host="127.0.0.1",
        port=c.default_port("materialized"),
        user="materialize",
        password=None,
        sslmode="disable",
        database="materialize",
    )


def policy(cluster: str, cooldown: float = 3600.0) -> ClusterPolicy:
    return ClusterPolicy(cluster, LADDER[0], LADDER[-1], cooldown, LADDER)


def autoscale(
    c: Composition,
    *policies: ClusterPolicy,
    dry_run: bool = False,
    sample_interval: float = 5.0,
    read_timeout: float = 10.0,
    conn: Connection | None = None,
) -> tuple[bool, list[dict[str, Any]]]:
    """Performs one run and returns whether it succeeded, and its records."""
    config = Config(
        connection=conn or connection(c),
        clusters=policies,
        sample_interval_seconds=sample_interval,
        replica_read_timeout_seconds=read_timeout,
        dry_run=dry_run,
    )
    db = Database(config.connection, connect_timeout=5)
    records: list[dict[str, Any]] = []
    ok = run_once(db, config, RunOptions(ModelParams(), dry_run), records.append)
    for record in records:
        print(json.dumps(record))
    return ok, records


def system(c: Composition, statement: str) -> None:
    c.sql(statement, port=6877, user="mz_system")


def create_cluster(c: Composition, name: str, size: str = LADDER[0]) -> None:
    """Creates a cluster with one indexed table and waits for it to hydrate."""
    c.sql(f"""
        CREATE CLUSTER {name} (SIZE = '{size}');
        CREATE TABLE {name}_t (k int, v int);
        INSERT INTO {name}_t SELECT g % 4, g FROM generate_series(1, 1000) g;
        CREATE INDEX {name}_idx IN CLUSTER {name} ON {name}_t (k);
        """)
    wait_for(
        lambda: hydrated_replicas(c, name) == ["r1"],
        f"cluster {name} did not hydrate",
    )


def wait_for(condition: Callable[[], bool], message: str, timeout: float = 180) -> None:
    deadline = time.time() + timeout
    while not condition():
        assert time.time() < deadline, message
        time.sleep(1)


def cluster_size(c: Composition, name: str) -> str:
    return c.sql_query(f"SELECT size FROM mz_clusters WHERE name = '{name}'")[0][0]


def hydrated_replicas(c: Composition, cluster: str) -> list[str]:
    """Replicas on which every index of the cluster reports hydrated."""
    rows = c.sql_query(f"""
        SELECT r.name
        FROM mz_cluster_replicas r
        JOIN mz_clusters c ON c.id = r.cluster_id
        WHERE c.name = '{cluster}'
          AND NOT EXISTS (
            SELECT 1 FROM mz_indexes i
            WHERE i.cluster_id = c.id
              AND NOT EXISTS (
                SELECT 1 FROM mz_internal.mz_hydration_statuses h
                WHERE h.object_id = i.id AND h.replica_id = r.id AND h.hydrated
              )
          )
        ORDER BY r.name
        """)
    return [name for (name,) in rows]


def reconfiguration_status(c: Composition, cluster: str) -> str | None:
    rows = c.sql_query(f"""
        SELECT r.status
        FROM mz_internal.mz_cluster_reconfigurations r
        JOIN mz_clusters c ON c.id = r.cluster_id
        WHERE c.name = '{cluster}'
        """)
    return rows[0][0] if rows else None


def wait_settled(c: Composition, cluster: str) -> None:
    wait_for(
        lambda: reconfiguration_status(c, cluster) != "in-progress",
        f"reconfiguration of {cluster} did not settle",
        timeout=600,
    )


@contextmanager
def record_targeted_replicas() -> Iterator[list[tuple[str, str]]]:
    """Records every replica a targeted session is opened for."""
    targeted: list[tuple[str, str]] = []
    original = ReplicaSession.__init__

    def init(
        self: ReplicaSession, db: Any, cluster: str, replica: str, *a: Any
    ) -> None:
        targeted.append((cluster, replica))
        original(self, db, cluster, replica, *a)

    ReplicaSession.__init__ = init  # type: ignore[method-assign]
    try:
        yield targeted
    finally:
        ReplicaSession.__init__ = original  # type: ignore[method-assign]


def workflow_dry_run(c: Composition) -> None:
    """A dry run samples the cluster, reports, and changes nothing."""
    setup(c)
    create_cluster(c, "dry", LADDER[1])

    ok, [record] = autoscale(c, policy("dry"), dry_run=True)
    assert ok, record
    assert record["state"] == "eligible", record
    assert record["mode"] == "sampled", record
    assert record["dry_run"], record
    assert record["action"] in ("none", "dry-run"), record
    [replica] = record["replicas"]
    assert replica["sampled"] and replica["timeouts"] == 0, record
    assert 0 <= replica["busy_cores"] < 2, record
    assert 4 < replica["interval_seconds"] < 30, record
    assert cluster_size(c, "dry") == LADDER[1]
    assert reconfiguration_status(c, "dry") is None

    # The same through the command line, which adds configuration parsing and
    # the exit code.
    with tempfile.TemporaryDirectory() as tmp:
        document = {
            "connection": {
                "host": "127.0.0.1",
                "port": c.default_port("materialized"),
                "user": "materialize",
                "sslmode": "disable",
            },
            "defaults": {"sample_interval": "2s"},
            "clusters": [
                {
                    "name": "dry",
                    "min_size": LADDER[0],
                    "max_size": LADDER[-1],
                    "sizes": list(LADDER),
                }
            ],
            "dry_run": True,
        }
        path = Path(tmp) / "autoscaler.yml"
        path.write_text(yaml.safe_dump(document))
        command = [sys.executable, "-m", "materialize.cli.cluster_autoscaler"]
        result = subprocess.run(
            [*command, str(path)], capture_output=True, text=True, check=False
        )
        assert result.returncode == 0, result.stderr
        [line] = result.stdout.splitlines()
        assert json.loads(line)["cluster"] == "dry", line

        document["clusters"][0]["max_size"] = "no-such-size"
        path.write_text(yaml.safe_dump(document))
        result = subprocess.run(
            [*command, str(path)], capture_output=True, text=True, check=False
        )
        assert result.returncode == 2, result.stderr
        assert "no-such-size" in result.stderr, result.stderr
    assert cluster_size(c, "dry") == LADDER[1]


def workflow_hydrating_replica_is_not_targeted(c: Composition) -> None:
    """No session ever targets a replica that is hydrating."""
    setup(c)
    create_cluster(c, "mixed")
    try:
        # With no hydration slots, replicas created from here on stay
        # hydrating for as long as the test needs.
        system(c, "ALTER SYSTEM SET compute_hydration_concurrency = 0")
        c.sql("ALTER CLUSTER mixed SET (REPLICATION FACTOR 2)")
        c.sql(f"""
            CREATE CLUSTER cold (SIZE = '{LADDER[0]}');
            CREATE INDEX cold_idx IN CLUSTER cold ON mixed_t (v);
            """)
        wait_for(
            lambda: c.sql_query("""
                SELECT count(*) FROM mz_cluster_replicas r
                JOIN mz_clusters c ON c.id = r.cluster_id WHERE c.name = 'mixed'
                """)[0][0] == 2,
            "second replica was not created",
        )
        # Give a hydration that was wrongly allowed the time to finish.
        time.sleep(15)
        assert hydrated_replicas(c, "mixed") == ["r1"]
        assert hydrated_replicas(c, "cold") == []

        with record_targeted_replicas() as targeted:
            ok, [mixed, cold] = autoscale(c, policy("mixed"), policy("cold"))
        assert ok, (mixed, cold)
        assert targeted == [("mixed", "r1")], targeted

        assert mixed["state"] == "eligible", mixed
        by_name = {r["name"]: r for r in mixed["replicas"]}
        assert by_name["r1"]["sampled"], mixed
        assert by_name["r2"] == {
            "name": "r2",
            "size": LADDER[0],
            "sampled": False,
            "hydrated": False,
        }, mixed

        assert (cold["state"], cold["decision"]) == ("hydrating", "hydrating"), cold
        assert cold["action"] == "none", cold
    finally:
        system(c, "ALTER SYSTEM RESET compute_hydration_concurrency")


def workflow_timeout_is_a_signal(c: Composition) -> None:
    """Targeted reads that time out buy one step. A dead connection buys none.

    A statement timeout of one millisecond stands in for a saturated replica.
    It exercises the same path, a `statement_timeout` error from Materialize on
    a live connection, without depending on how hard the test can load a
    replica.
    """
    setup(c)
    create_cluster(c, "busy")

    ok, [record] = autoscale(c, policy("busy"), read_timeout=0.001)
    assert ok, record
    [replica] = record["replicas"]
    assert (replica["status"], replica["timeouts"]) == (UNRESPONSIVE, 2), record
    assert (record["decision"], record["target_size"]) == (
        "scale-up",
        LADDER[1],
    ), record
    assert record["action"] == "altered", record
    wait_settled(c, "busy")
    assert cluster_size(c, "busy") == LADDER[1]

    # The next run sees the change and waits out the cooldown.
    ok, [record] = autoscale(c, policy("busy"), read_timeout=0.001)
    assert ok, record
    assert record["state"] == "cooling-down", record
    assert cluster_size(c, "busy") == LADDER[1]

    # Nothing listens on this port.
    unreachable = Connection(
        "127.0.0.1", 1, "materialize", None, "disable", "materialize"
    )
    ok, [record] = autoscale(c, policy("busy"), conn=unreachable)
    assert not ok, record
    assert (record["decision"], record["action"]) == ("error", "none"), record
    assert cluster_size(c, "busy") == LADDER[1]


class Killed(BaseException):
    """Stands in for the process being killed.

    Not an `Exception`, so nothing in the autoscaler catches it and the run
    ends where it is raised.
    """


def workflow_killed_mid_run(c: Composition) -> None:
    """A run killed at any step leaves a cluster that later runs converge.

    Every run here would step the cluster up once, because its reads time
    out. Whatever step the first run dies at, the cluster must end exactly one
    step up.
    """
    setup(c)

    def before(function: Callable[..., Any]) -> Callable[..., Any]:
        def killed(*args: Any, **kwargs: Any) -> Any:
            raise Killed()

        return killed

    def after(function: Callable[..., Any]) -> Callable[..., Any]:
        def killed(*args: Any, **kwargs: Any) -> Any:
            function(*args, **kwargs)
            raise Killed()

        return killed

    # Where the run dies, and whether the `ALTER` was issued before it did.
    steps: list[tuple[str, Any, str, Callable[..., Any], bool]] = [
        ("after_catalog_read", catalog, "read_snapshot", after, False),
        ("while_sampling", runner, "sample_replica", before, False),
        ("before_alter", catalog, "alter_cluster_size", before, False),
        ("after_alter", catalog, "alter_cluster_size", after, True),
    ]
    for name, module, attribute, wrap, altered in steps:
        cluster = f"kill_{name}"
        create_cluster(c, cluster)
        original = getattr(module, attribute)
        setattr(module, attribute, wrap(original))
        try:
            autoscale(c, policy(cluster), read_timeout=0.001)
            raise AssertionError(f"run was not killed {name}")
        except Killed:
            pass
        finally:
            setattr(module, attribute, original)

        if not altered:
            assert cluster_size(c, cluster) == LADDER[0], name
            assert reconfiguration_status(c, cluster) is None, name

        for _ in range(3):
            ok, [record] = autoscale(c, policy(cluster), read_timeout=0.001)
            assert ok, (name, record)
            wait_settled(c, cluster)
        assert cluster_size(c, cluster) == LADDER[1], name
        assert record["state"] == "cooling-down", (name, record)


def workflow_step_load(c: Composition) -> None:
    """Load steps the cluster up, and its absence steps it back down.

    The load is a join whose every input row changes on each write, driven as
    fast as the writer can go. How far up the ladder that pushes the cluster
    depends on the machine, so only the direction of each step is asserted.
    """
    setup(c)
    cooldown = 120.0
    c.sql(f"""
        CREATE CLUSTER load (SIZE = '{LADDER[0]}');
        CREATE TABLE load_t (k int, v int);
        INSERT INTO load_t SELECT g % 4, g FROM generate_series(1, 4000) g;
        CREATE VIEW load_v AS
            SELECT a.k, count(*) AS pairs, sum(a.v::int8 * b.v) AS total
            FROM load_t a JOIN load_t b ON a.k = b.k
            GROUP BY a.k;
        CREATE INDEX load_idx IN CLUSTER load ON load_v (k);
        """)
    wait_for(lambda: hydrated_replicas(c, "load") == ["r1"], "load did not hydrate")

    stop = threading.Event()

    def write() -> None:
        conn = c.sql_connection(reuse_connection=False)
        with conn.cursor() as cursor:
            while not stop.is_set():
                cursor.execute("UPDATE load_t SET v = v + 1")

    writer = threading.Thread(target=write, daemon=True)
    writer.start()

    sizes = [cluster_size(c, "load")]

    def step() -> dict[str, Any]:
        ok, [record] = autoscale(
            c, policy("load", cooldown), sample_interval=10.0, read_timeout=30.0
        )
        assert ok, record
        wait_settled(c, "load")
        size = cluster_size(c, "load")
        if size != sizes[-1]:
            sizes.append(size)
        return record

    try:
        deadline = time.time() + 600
        while len(sizes) == 1:
            assert time.time() < deadline, "load did not scale the cluster up"
            step()
            time.sleep(5)
        # Let it take whatever further steps the load asks for.
        deadline = time.time() + 2 * cooldown
        while time.time() < deadline:
            step()
            time.sleep(5)
    finally:
        stop.set()
        writer.join()
    peak = len(sizes)
    assert [LADDER.index(s) for s in sizes] == sorted(
        LADDER.index(s) for s in sizes
    ), sizes

    deadline = time.time() + 900
    while cluster_size(c, "load") != LADDER[0]:
        assert time.time() < deadline, f"cluster did not scale back down: {sizes}"
        step()
        time.sleep(5)
    down = [LADDER.index(s) for s in sizes[peak - 1 :]]
    assert down == sorted(down, reverse=True), sizes
