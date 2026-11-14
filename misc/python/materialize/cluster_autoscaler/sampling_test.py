# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import dataclasses

import pytest

from materialize.cluster_autoscaler.model import ModelParams
from materialize.cluster_autoscaler.sampling import (
    FAILURE,
    TIMEOUT,
    ReplicaRead,
    classify_read_failure,
    measure,
)

PARAMS = ModelParams()
SECOND_NS = 1_000_000_000


def read(
    at_s: float,
    elapsed: dict[tuple[str, str], float],
    exports: dict[str, tuple[str, float]] | None = None,
    imports: dict[tuple[str, str], float] | None = None,
    dataflows: dict[str, str] | None = None,
) -> ReplicaRead:
    """A read with times in seconds. Operator `o<n>` roots dataflow `<n>`."""
    return ReplicaRead(
        at_ms=at_s * 1000,
        elapsed_ns={k: int(v * SECOND_NS) for k, v in elapsed.items()},
        operator_dataflow=dataflows
        or {operator: operator.removeprefix("o") for operator, _ in elapsed},
        exports={k: (d, int(t * 1000)) for k, (d, t) in (exports or {}).items()},
        imports={k: int(t * 1000) for k, t in (imports or {}).items()},
    )


def test_busy_cores_per_dataflow_and_worker() -> None:
    first = read(0, {("o1", "0"): 100, ("o1", "1"): 100, ("o2", "0"): 50})
    second = read(30, {("o1", "0"): 115, ("o1", "1"): 103, ("o2", "0"): 62})
    m = measure(first, second, PARAMS)
    assert m.interval_seconds == 30
    assert m.busy_cores == pytest.approx(1.0)
    assert m.cpu_demand == pytest.approx(1.0)
    assert m.max_worker_utilization == pytest.approx(0.9)
    assert {d.dataflow_id: d.busy_cores for d in m.dataflows} == {
        "1": pytest.approx(0.6),
        "2": pytest.approx(0.4),
    }
    assert m.min_progress_ratio == 1.0
    assert m.slowest_dataflow is None


def test_dataflow_dropped_between_reads_is_not_counted() -> None:
    first = read(0, {("o1", "0"): 100, ("o2", "0"): 500})
    second = read(30, {("o1", "0"): 115})
    m = measure(first, second, PARAMS)
    assert m.busy_cores == pytest.approx(0.5)
    assert [d.dataflow_id for d in m.dataflows] == ["1"]


def test_dataflow_created_between_reads_is_not_counted() -> None:
    first = read(0, {("o1", "0"): 100})
    second = read(30, {("o1", "0"): 115, ("o2", "0"): 25})
    assert measure(first, second, PARAMS).busy_cores == pytest.approx(0.5)


def test_operator_id_reused_by_another_dataflow_is_not_counted() -> None:
    first = read(0, {("o1", "0"): 100}, dataflows={"o1": "1"})
    second = read(30, {("o1", "0"): 105}, dataflows={"o1": "9"})
    assert measure(first, second, PARAMS).busy_cores == 0


def test_elapsed_time_that_went_backwards_counts_as_zero() -> None:
    first = read(0, {("o1", "0"): 100})
    second = read(30, {("o1", "0"): 90})
    assert measure(first, second, PARAMS).busy_cores == 0


@pytest.mark.parametrize(
    "name,out_advance,in_advance,lag,ratio,demand",
    [
        ("keeping up", 30, 30, 0, 1.0, 0.5),
        ("a tick short still counts as keeping up", 28, 30, 0, 1.0, 0.5),
        ("half the progress doubles the demand", 15, 30, 0, 0.5, 1.0),
        ("no progress is clamped", 0, 30, 0, 0.25, 2.0),
        ("output frontier going backwards is clamped", -5, 30, 0, 0.25, 2.0),
        ("stalled inputs are not this cluster's problem", 0, 0, 0, 1.0, 0.5),
        ("catching up is capped at one", 60, 30, 0, 1.0, 0.5),
        ("lag adds catch-up capacity", 15, 30, 450, 0.5, 1.5),
    ],
)
def test_progress_ratio(
    name: str,
    out_advance: float,
    in_advance: float,
    lag: float,
    ratio: float,
    demand: float,
) -> None:
    """`lag` is the distance from output to input frontier at the second read."""
    in_before = 10_000.0
    out_after = in_before + in_advance - lag
    first = read(
        0,
        {("o1", "0"): 0},
        exports={"u5": ("1", out_after - out_advance)},
        imports={("u5", "u4"): in_before},
    )
    second = read(
        30,
        {("o1", "0"): 15},
        exports={"u5": ("1", out_after)},
        imports={("u5", "u4"): in_before + in_advance},
    )
    m = measure(first, second, PARAMS)
    assert m.min_progress_ratio == pytest.approx(ratio), name
    assert m.cpu_demand == pytest.approx(demand), name
    assert m.slowest_dataflow == ("u5" if ratio < 1 else None), name


def test_input_frontier_is_the_minimum_over_imports() -> None:
    first = read(
        0,
        {("o1", "0"): 0},
        exports={"u5": ("1", 100)},
        imports={("u5", "u3"): 100, ("u5", "u4"): 100},
    )
    second = read(
        30,
        {("o1", "0"): 15},
        exports={"u5": ("1", 115)},
        imports={("u5", "u3"): 160, ("u5", "u4"): 130},
    )
    assert measure(first, second, PARAMS).min_progress_ratio == pytest.approx(0.5)


def test_export_created_between_reads_has_no_ratio() -> None:
    first = read(0, {("o1", "0"): 0})
    second = read(
        30,
        {("o1", "0"): 15},
        exports={"u5": ("1", 100)},
        imports={("u5", "u4"): 400},
    )
    assert measure(first, second, PARAMS).min_progress_ratio == 1.0


def test_clamp_is_a_parameter() -> None:
    first = read(
        0, {("o1", "0"): 0}, exports={"u5": ("1", 0)}, imports={("u5", "u4"): 0}
    )
    second = read(
        30, {("o1", "0"): 15}, exports={"u5": ("1", 0)}, imports={("u5", "u4"): 30}
    )
    params = dataclasses.replace(PARAMS, min_progress_ratio=0.5, catch_up_seconds=1e12)
    assert measure(first, second, params).cpu_demand == pytest.approx(1.0)


def test_reads_out_of_order_are_rejected() -> None:
    with pytest.raises(ValueError):
        measure(read(30, {}), read(30, {}), PARAMS)


@pytest.mark.parametrize(
    "sqlstate,message,alive,expected",
    [
        ("57014", "canceling statement due to statement timeout", True, TIMEOUT),
        ("57014", "canceling statement due to statement timeout", False, FAILURE),
        ("57014", "canceling statement due to user request", True, FAILURE),
        ("42501", "permission denied for CLUSTER", True, FAILURE),
        ("XX000", "cluster replica 'c.r1' does not exist", True, FAILURE),
        (None, "connection refused", False, FAILURE),
        (None, "server closed the connection unexpectedly", True, FAILURE),
    ],
)
def test_classify_read_failure(
    sqlstate: str | None, message: str, alive: bool, expected: str
) -> None:
    assert classify_read_failure(sqlstate, message, alive) == expected
