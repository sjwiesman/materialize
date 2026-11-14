# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import json
from typing import Any

import psycopg
import pytest

from materialize.cluster_autoscaler import catalog, runner
from materialize.cluster_autoscaler.catalog import (
    ReadFailure,
    ReadTimeout,
    parse_replica_read,
)
from materialize.cluster_autoscaler.config import Config, Connection
from materialize.cluster_autoscaler.decision import decide
from materialize.cluster_autoscaler.model import (
    ERROR,
    SAMPLED,
    UNRESPONSIVE,
    ClusterObject,
    ClusterPolicy,
    ClusterSnapshot,
    ModelParams,
    Reconfiguration,
    Replica,
    ReplicaSample,
    Size,
    WindowHistory,
)
from materialize.cluster_autoscaler.runner import (
    ALTERED,
    DRY_RUN,
    FAILED,
    NONE,
    SUPERSEDED,
    RunOptions,
    build_report,
    evaluate_cluster,
    is_failure,
    run_once,
    take_sample,
)
from materialize.cluster_autoscaler.sampling import ReplicaRead

GIB = 2**30
NOW = 1_000_000.0
PARAMS = ModelParams()
POLICY = ClusterPolicy("c", "100cc", "400cc", 3600.0)
SIZES = {
    name: Size(name, 1, cores, cores * 10**9, cores * 8 * GIB, cores / 2)
    for name, cores in (("100cc", 2), ("200cc", 4), ("400cc", 8))
}
CONFIG = Config(
    connection=Connection(
        "localhost", 6875, "autoscaler", None, "disable", "materialize"
    ),
    clusters=(POLICY,),
    sample_interval_seconds=30.0,
    replica_read_timeout_seconds=10.0,
    dry_run=False,
)


def reads(busy_cores: float) -> list[ReplicaRead]:
    return [
        ReplicaRead(
            at_ms=0, elapsed_ns={("o1", "0"): 0}, operator_dataflow={"o1": "1"}
        ),
        ReplicaRead(
            at_ms=30_000,
            elapsed_ns={("o1", "0"): int(busy_cores * 30e9)},
            operator_dataflow={"o1": "1"},
        ),
    ]


class Script:
    """A scripted sequence of read outcomes, and a record of the waits."""

    def __init__(self, outcomes: list[Any]):
        self.outcomes = list(outcomes)
        self.events: list[str] = []

    def read(self) -> ReplicaRead:
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            self.events.append(type(outcome).__name__)
            raise outcome
        self.events.append("read")
        return outcome

    def wait(self) -> None:
        self.events.append("wait")


@pytest.mark.parametrize(
    "name,outcomes,status,timeouts,events",
    [
        ("two reads", reads(1.0), SAMPLED, 0, ["read", "wait", "read"]),
        (
            "first read recovers on retry",
            [ReadTimeout(), *reads(1.0)],
            SAMPLED,
            1,
            ["ReadTimeout", "read", "wait", "read"],
        ),
        (
            "second read recovers on retry",
            [reads(1.0)[0], ReadTimeout(), reads(1.0)[1]],
            SAMPLED,
            1,
            ["read", "wait", "ReadTimeout", "read"],
        ),
        (
            "first read times out twice",
            [ReadTimeout(), ReadTimeout()],
            UNRESPONSIVE,
            2,
            ["ReadTimeout", "ReadTimeout"],
        ),
        (
            "second read times out twice",
            [reads(1.0)[0], ReadTimeout(), ReadTimeout()],
            UNRESPONSIVE,
            2,
            ["read", "wait", "ReadTimeout", "ReadTimeout"],
        ),
        (
            "connection failure is not retried",
            [ReadFailure("connection lost")],
            ERROR,
            0,
            ["ReadFailure"],
        ),
        (
            "connection failure after a timeout is still an error",
            [ReadTimeout(), ReadFailure("connection lost")],
            ERROR,
            1,
            ["ReadTimeout", "ReadFailure"],
        ),
        (
            "connection failure on the second read",
            [reads(1.0)[0], ReadFailure("connection lost")],
            ERROR,
            0,
            ["read", "wait", "ReadFailure"],
        ),
    ],
)
def test_take_sample(
    name: str, outcomes: list[Any], status: str, timeouts: int, events: list[str]
) -> None:
    script = Script(outcomes)
    sample = take_sample("r1", script.read, script.wait, PARAMS)
    assert (sample.status, sample.timeouts) == (status, timeouts), name
    assert script.events == events, name
    assert (sample.measurement is not None) == (status == SAMPLED), name
    assert (sample.error is not None) == (status == ERROR), name


def test_take_sample_measures_the_interval_between_the_reads() -> None:
    script = Script(reads(1.5))
    sample = take_sample("r1", script.read, script.wait, PARAMS)
    assert sample.measurement is not None
    assert sample.measurement.busy_cores == pytest.approx(1.5)


def test_parse_replica_read() -> None:
    read = parse_replica_read(
        [
            ("clock", None, None, None, "1700000000000"),
            ("elapsed", "17", "0", "3", "5000"),
            ("elapsed", "17", "1", "3", "7000"),
            ("export", "u5", None, "3", "1699999999000"),
            ("import", "u5", "u4", None, "1700000000000"),
        ]
    )
    assert read == ReplicaRead(
        at_ms=1700000000000.0,
        elapsed_ns={("17", "0"): 5000, ("17", "1"): 7000},
        operator_dataflow={"17": "3"},
        exports={"u5": ("3", 1699999999000)},
        imports={("u5", "u4"): 1700000000000},
    )


def snapshot(**overrides: Any) -> ClusterSnapshot:
    fields: dict[str, Any] = dict(
        id="u7",
        name="c",
        now=NOW,
        managed=True,
        schedule="manual",
        size="200cc",
        introspection=True,
        replicas=(Replica("r1-id", "r1", "200cc"), Replica("r2-id", "r2", "200cc")),
        objects=(ClusterObject("u1", "compute"),),
        hydrated=frozenset({("u1", "r1-id")}),
        history=WindowHistory(
            peak_cpu_nano_cores=3.6e9,
            peak_memory_bytes=12.0 * GIB,
            oldest_metrics_at=NOW - 3590,
            peak_lag_seconds=3.0,
        ),
    )
    fields.update(overrides)
    return ClusterSnapshot(**fields)


def test_report_explains_a_decision() -> None:
    snap = snapshot()
    samples = [take_sample("r1", Script(reads(3.5)).read, lambda: None, PARAMS)]
    decision = decide(snap, POLICY, SIZES, samples, PARAMS)
    record = build_report(POLICY, snap, decision, samples, SIZES, False, ALTERED)
    assert json.loads(json.dumps(record)) == {
        "cluster": "c",
        "state": "eligible",
        "current_size": "200cc",
        "recommended_size": "400cc",
        "decision": "scale-up",
        "reason": "cpu demand 3.50 cores exceeds 4 cores at 0.7 target",
        "target_size": "400cc",
        "action": "altered",
        "dry_run": False,
        "mode": "sampled",
        "cpu_demand_cores": 3.5,
        "replicas": [
            {
                "name": "r1",
                "size": "200cc",
                "sampled": True,
                "status": "sampled",
                "timeouts": 0,
                "busy_cores": 3.5,
                "cpu_demand_cores": 3.5,
                "max_worker_utilization": 3.5,
                "min_progress_ratio": 1.0,
                "slowest_dataflow": None,
                "interval_seconds": 30.0,
            },
            {"name": "r2", "size": "200cc", "sampled": False, "hydrated": False},
        ],
        "window": {
            "seconds": 3600.0,
            "peak_memory_bytes": 12.0 * GIB,
            "peak_lag_seconds": 3.0,
            "peak_cpu_cores": 3.6,
            "peak_cpu_percent": 90.0,
        },
    }
    assert not is_failure(record)


class FakeDatabase:
    """Stands in for `Database`. Sessions are opaque tokens."""

    def __init__(self) -> None:
        self.sessions = 0

    def catalog_session(self) -> Any:
        self.sessions += 1
        return FakeSession()


class FakeSession:
    closed = False

    def __enter__(self) -> FakeSession:
        return self

    def __exit__(self, *args: Any) -> None:
        pass


class Environment:
    """Patches every SQL entry point of `runner` with scripted answers."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, snap: ClusterSnapshot | None):
        self.snapshot = snap
        self.state: tuple[str | None, Reconfiguration | None] = (
            snap.size if snap else None,
            None,
        )
        self.alters: list[tuple[str, str]] = []
        self.alter_error: Exception | None = None
        self.targeted: list[str] = []
        self.sample: dict[str, ReplicaSample] = {}
        monkeypatch.setattr(catalog, "read_sizes", lambda conn: SIZES)
        monkeypatch.setattr(catalog, "read_role", lambda conn: "autoscaler")
        monkeypatch.setattr(
            catalog, "read_snapshot", lambda conn, policy, role: self.snapshot
        )
        monkeypatch.setattr(
            catalog, "read_cluster_state", lambda conn, cluster_id: self.state
        )
        monkeypatch.setattr(catalog, "alter_cluster_size", self._alter)
        monkeypatch.setattr(runner, "sample_replica", self._sample)

    def _alter(self, conn: Any, cluster: str, size: str) -> None:
        if self.alter_error is not None:
            raise self.alter_error
        self.alters.append((cluster, size))

    def _sample(self, db: Any, cluster: str, replica: str, *args: Any) -> ReplicaSample:
        self.targeted.append(replica)
        return self.sample[replica]

    def evaluate(self, dry_run: bool = False) -> dict[str, Any]:
        return evaluate_cluster(
            FakeDatabase(),  # type: ignore[arg-type]
            CONFIG,
            POLICY,
            SIZES,
            "autoscaler",
            RunOptions(PARAMS, dry_run),
        )


def busy(replica: str, cores: float) -> ReplicaSample:
    return take_sample(replica, Script(reads(cores)).read, lambda: None, PARAMS)


def test_run_alters_when_a_rule_fires(monkeypatch: pytest.MonkeyPatch) -> None:
    env = Environment(monkeypatch, snapshot())
    env.sample["r1"] = busy("r1", 3.5)
    record = env.evaluate()
    assert env.alters == [("c", "400cc")]
    assert (record["decision"], record["action"]) == ("scale-up", ALTERED)


def test_hydrating_replica_is_never_targeted(monkeypatch: pytest.MonkeyPatch) -> None:
    env = Environment(monkeypatch, snapshot())
    env.sample["r1"] = busy("r1", 2.0)
    env.evaluate()
    assert env.targeted == ["r1"]


@pytest.mark.parametrize(
    "overrides",
    [
        dict(hydrated=frozenset()),
        dict(reconfiguration=Reconfiguration("in-progress", "400cc")),
        dict(oom_kills=(("r1-id", NOW - 5),)),
        dict(managed=False),
        dict(introspection=False),
    ],
)
def test_no_replica_is_targeted_unless_eligible_with_introspection(
    monkeypatch: pytest.MonkeyPatch, overrides: dict[str, Any]
) -> None:
    env = Environment(monkeypatch, snapshot(**overrides))
    record = env.evaluate(dry_run=True)
    assert env.targeted == []
    assert not is_failure(record)


def test_dry_run_issues_no_alter(monkeypatch: pytest.MonkeyPatch) -> None:
    env = Environment(monkeypatch, snapshot())
    env.sample["r1"] = busy("r1", 3.5)
    record = env.evaluate(dry_run=True)
    assert env.alters == []
    assert (record["target_size"], record["action"], record["dry_run"]) == (
        "400cc",
        DRY_RUN,
        True,
    )


def test_no_alter_without_a_decision(monkeypatch: pytest.MonkeyPatch) -> None:
    env = Environment(monkeypatch, snapshot())
    env.sample["r1"] = busy("r1", 2.0)
    record = env.evaluate()
    assert env.alters == []
    assert (record["decision"], record["action"]) == ("steady", NONE)


@pytest.mark.parametrize(
    "state",
    [
        ("400cc", None),
        ("200cc", Reconfiguration("in-progress", "100cc")),
    ],
)
def test_no_alter_when_the_cluster_changed_during_the_run(
    monkeypatch: pytest.MonkeyPatch, state: tuple[str, Reconfiguration | None]
) -> None:
    env = Environment(monkeypatch, snapshot())
    env.sample["r1"] = busy("r1", 3.5)
    env.state = state
    record = env.evaluate()
    assert env.alters == []
    assert record["action"] == SUPERSEDED
    assert not is_failure(record)


def test_settled_reconfiguration_record_does_not_supersede(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = Environment(monkeypatch, snapshot())
    env.sample["r1"] = busy("r1", 3.5)
    env.state = ("200cc", Reconfiguration("timed-out", "100cc"))
    assert env.evaluate()["action"] == ALTERED


def test_rejected_alter_is_a_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    env = Environment(monkeypatch, snapshot())
    env.sample["r1"] = busy("r1", 3.5)
    env.alter_error = psycopg.errors.InsufficientPrivilege("must be owner of CLUSTER c")
    record = env.evaluate()
    assert record["action"] == FAILED
    assert "must be owner" in record["error"]
    assert is_failure(record)


def test_failed_read_reports_an_error_and_issues_no_alter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = Environment(monkeypatch, snapshot())
    env.sample["r1"] = ReplicaSample("r1", ERROR, error="connection lost")
    record = env.evaluate()
    assert env.alters == []
    assert record["decision"] == ERROR
    assert is_failure(record)


def test_unresponsive_replica_scales_up_one_step(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = Environment(monkeypatch, snapshot())
    env.sample["r1"] = ReplicaSample("r1", UNRESPONSIVE, timeouts=2)
    record = env.evaluate()
    assert env.alters == [("c", "400cc")]
    assert record["replicas"][0]["status"] == UNRESPONSIVE
    assert not is_failure(record)


def test_missing_cluster_is_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    env = Environment(monkeypatch, None)
    record = env.evaluate()
    assert record["decision"] == ERROR
    assert "does not exist" in record["reason"]


def test_one_failing_cluster_does_not_stop_the_others(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = Environment(monkeypatch, snapshot())
    env.sample["r1"] = busy("r1", 2.0)

    def read_snapshot(conn: Any, policy: ClusterPolicy, role: str) -> ClusterSnapshot:
        if policy.name == "broken":
            raise psycopg.OperationalError("connection lost")
        return snapshot(name=policy.name)

    monkeypatch.setattr(catalog, "read_snapshot", read_snapshot)
    config = Config(
        connection=CONFIG.connection,
        clusters=(
            ClusterPolicy("broken", "100cc", "400cc", 3600.0),
            ClusterPolicy("fine", "100cc", "400cc", 3600.0),
        ),
        sample_interval_seconds=30.0,
        replica_read_timeout_seconds=10.0,
        dry_run=False,
    )
    records: list[dict[str, Any]] = []
    ok = run_once(
        FakeDatabase(),  # type: ignore[arg-type]
        config,
        RunOptions(PARAMS, dry_run=False),
        records.append,
    )
    assert not ok
    assert [(r["cluster"], r["decision"]) for r in records] == [
        ("broken", ERROR),
        ("fine", "steady"),
    ]


def test_run_succeeds_when_every_cluster_was_evaluated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = Environment(monkeypatch, snapshot(hydrated=frozenset()))
    records: list[dict[str, Any]] = []
    assert run_once(
        FakeDatabase(),  # type: ignore[arg-type]
        CONFIG,
        RunOptions(PARAMS, dry_run=False),
        records.append,
    )
    assert [r["decision"] for r in records] == ["hydrating"]
    assert env.alters == []
