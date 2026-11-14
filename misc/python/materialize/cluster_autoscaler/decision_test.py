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
from typing import Any

import pytest

from materialize.cluster_autoscaler.decision import (
    AT_MAX,
    AT_MIN,
    BACKING_OFF,
    COOLING_DOWN,
    ELIGIBLE,
    FALLBACK,
    HYDRATING,
    LIVE,
    OOM,
    RECONFIGURING,
    SCALE_DOWN,
    SCALE_UP,
    SKEW_LIMITED,
    SKIPPED,
    STEADY,
    PolicyError,
    build_ladder,
    decide,
    gate,
)
from materialize.cluster_autoscaler.model import (
    ERROR,
    SAMPLED,
    UNRESPONSIVE,
    ClusterObject,
    ClusterPolicy,
    ClusterSnapshot,
    Measurement,
    ModelParams,
    Reconfiguration,
    Replica,
    ReplicaSample,
    Size,
    Transition,
    WindowHistory,
)

GIB = 2**30
NOW = 1_000_000.0
COOLDOWN = 3600.0
PARAMS = ModelParams()


def cc(credits: int) -> Size:
    """A size with one core and 8GiB per 50 credits, like the `cc` ladder."""
    cores = credits / 50
    return Size(
        name=f"{credits}cc",
        processes=1,
        workers=max(1, int(cores)),
        cpu_nano_cores=int(cores * 1e9),
        memory_bytes=int(cores * 8 * GIB),
        credits_per_hour=credits / 100,
    )


SIZES = {s.name: s for s in [cc(c) for c in (25, 50, 100, 200, 400, 800, 1600)]}
SIZES["medium"] = Size("medium", 1, 4, 4_000_000_000, 32 * GIB, 2.0)
# Same capacity as 200cc under another name.
SIZES["200cc-alias"] = dataclasses.replace(SIZES["200cc"], name="200cc-alias")

POLICY = ClusterPolicy("c", "100cc", "800cc", COOLDOWN)

R1 = Replica("r1-id", "r1", "200cc")
R2 = Replica("r2-id", "r2", "200cc")
INDEX = ClusterObject("u1", "compute")
SOURCE = ClusterObject("u2", "storage")

# A window that looks quiet: a little CPU and memory, a full hour of history.
QUIET = WindowHistory(
    peak_cpu_nano_cores=0.3e9,
    peak_memory_bytes=2 * GIB,
    oldest_metrics_at=NOW - COOLDOWN + 30,
    peak_lag_seconds=2.0,
    recent_lag_seconds=1.0,
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
        replicas=(R1,),
        objects=(INDEX,),
        hydrated=frozenset({("u1", "r1-id")}),
        history=QUIET,
    )
    fields.update(overrides)
    if "replicas" not in overrides:
        fields["replicas"] = (Replica("r1-id", "r1", fields["size"]),)
    return ClusterSnapshot(**fields)


def sample(
    cpu: float,
    replica: str = "r1",
    ratio: float = 1.0,
    worker: float | None = None,
    timeouts: int = 0,
) -> ReplicaSample:
    return ReplicaSample(
        replica=replica,
        status=SAMPLED,
        timeouts=timeouts,
        measurement=Measurement(
            interval_seconds=30.0,
            busy_cores=cpu * ratio,
            cpu_demand=cpu,
            max_worker_utilization=worker if worker is not None else min(1.0, cpu / 4),
            min_progress_ratio=ratio,
            slowest_dataflow="u1" if ratio < 1 else None,
        ),
    )


def transition(
    kind: str, target: str, age: float, by_autoscaler: bool = True
) -> Transition:
    return Transition(kind, target, NOW - age, by_autoscaler)


@pytest.mark.parametrize(
    "name,overrides,state,sampleable",
    [
        ("unmanaged", dict(managed=False), SKIPPED, []),
        ("on-refresh", dict(schedule="on-refresh"), SKIPPED, []),
        (
            "in-progress reconfiguration",
            dict(reconfiguration=Reconfiguration("in-progress", "400cc")),
            RECONFIGURING,
            [],
        ),
        (
            "reconfiguring wins over an out-of-memory kill",
            dict(
                reconfiguration=Reconfiguration("in-progress", "400cc"),
                oom_kills=(("r1-id", NOW - 10),),
            ),
            RECONFIGURING,
            [],
        ),
        (
            "settled reconfiguration does not gate",
            dict(reconfiguration=Reconfiguration("finalized", "200cc")),
            ELIGIBLE,
            ["r1"],
        ),
        ("out-of-memory kill", dict(oom_kills=(("r1-id", NOW - 10),)), OOM, []),
        (
            "out-of-memory kill skips the cooldown",
            dict(
                oom_kills=(("r1-id", NOW - 10),),
                last_transition=transition("finalized", "200cc", 60),
            ),
            OOM,
            [],
        ),
        (
            "kill before the last transition is not acted on again",
            dict(
                oom_kills=(("r1-id", NOW - 120),),
                last_transition=transition("timed-out", "400cc", 60),
            ),
            COOLING_DOWN,
            [],
        ),
        (
            "kill on a replica of another size is ignored",
            dict(
                replicas=(R1, Replica("r9-id", "r9", "400cc")),
                oom_kills=(("r9-id", NOW - 10),),
            ),
            ELIGIBLE,
            ["r1"],
        ),
        (
            "recent size change",
            dict(last_transition=transition("finalized", "200cc", COOLDOWN - 1)),
            COOLING_DOWN,
            [],
        ),
        (
            "recent failed attempt, by anyone",
            dict(last_transition=transition("timed-out", "100cc", 60, False)),
            COOLING_DOWN,
            [],
        ),
        (
            "size change older than the cooldown",
            dict(last_transition=transition("finalized", "200cc", COOLDOWN + 1)),
            ELIGIBLE,
            ["r1"],
        ),
        ("no replica hydrated", dict(hydrated=frozenset()), HYDRATING, []),
        ("no replica at all", dict(replicas=()), HYDRATING, []),
        (
            "no replica at the realized size",
            dict(replicas=(Replica("r9-id", "r9", "400cc"),)),
            HYDRATING,
            [],
        ),
        (
            "one object of two not hydrated",
            dict(objects=(INDEX, ClusterObject("u3", "compute"))),
            HYDRATING,
            [],
        ),
        (
            "only the hydrated replica may be sampled",
            dict(replicas=(R1, R2)),
            ELIGIBLE,
            ["r1"],
        ),
        (
            "a source hydrated on another replica does not hold a replica back",
            dict(
                replicas=(R1, R2),
                objects=(INDEX, SOURCE),
                hydrated=frozenset({("u1", "r1-id"), ("u1", "r2-id"), ("u2", "r2-id")}),
            ),
            ELIGIBLE,
            ["r1", "r2"],
        ),
        (
            "a source hydrated nowhere holds every replica back",
            dict(objects=(INDEX, SOURCE)),
            HYDRATING,
            [],
        ),
        ("a cluster without objects is hydrated", dict(objects=()), ELIGIBLE, ["r1"]),
    ],
)
def test_gate(
    name: str, overrides: dict[str, Any], state: str, sampleable: list[str]
) -> None:
    got_state, got_replicas, _ = gate(snapshot(**overrides), POLICY)
    assert got_state == state, name
    assert [r.name for r in got_replicas] == sampleable, name


@pytest.mark.parametrize(
    "name,overrides,samples,decision,target",
    [
        ("steady", {}, [sample(2.0)], STEADY, None),
        ("cpu demand above target", {}, [sample(3.0)], SCALE_UP, "400cc"),
        ("cpu demand sizes the step", {}, [sample(9.0)], SCALE_UP, "800cc"),
        ("demand beyond max_size clamps", {}, [sample(40.0)], SCALE_UP, "800cc"),
        (
            "the busiest replica sizes the cluster",
            dict(
                replicas=(R1, R2),
                hydrated=frozenset({("u1", "r1-id"), ("u1", "r2-id")}),
            ),
            [sample(1.0), sample(3.0, "r2")],
            SCALE_UP,
            "400cc",
        ),
        (
            "memory demand above target",
            dict(history=dataclasses.replace(QUIET, peak_memory_bytes=30 * GIB)),
            [sample(0.2)],
            SCALE_UP,
            "400cc",
        ),
        ("at max", dict(size="800cc"), [sample(40.0)], AT_MAX, None),
        (
            "at min",
            dict(size="100cc"),
            [sample(0.1)],
            AT_MIN,
            None,
        ),
        (
            "at min but nothing smaller would fit",
            dict(size="100cc"),
            [sample(1.0)],
            STEADY,
            None,
        ),
        (
            "below the range moves to min_size",
            dict(size="50cc"),
            [sample(0.1)],
            SCALE_UP,
            "100cc",
        ),
        (
            "above the range moves to what the history allows",
            dict(size="1600cc"),
            [sample(0.2)],
            SCALE_DOWN,
            "100cc",
        ),
        (
            "above the range moves to max_size despite a veto",
            dict(
                size="1600cc",
                history=dataclasses.replace(QUIET, peak_lag_seconds=600.0),
            ),
            [sample(0.2)],
            SCALE_DOWN,
            "800cc",
        ),
        (
            "above the range with demand beyond max_size moves to max_size",
            dict(size="1600cc"),
            [sample(40.0)],
            SCALE_DOWN,
            "800cc",
        ),
        (
            "a size of equal capacity under another name is steady",
            dict(size="200cc-alias"),
            [sample(2.0)],
            STEADY,
            None,
        ),
        ("scale down", {}, [sample(0.2)], SCALE_DOWN, "100cc"),
        (
            "scale down vetoed by a dataflow that is behind",
            {},
            [sample(0.4, ratio=0.5)],
            STEADY,
            None,
        ),
        (
            "skew limited instead of scaling down",
            {},
            [sample(0.4, ratio=0.5, worker=0.97)],
            SKEW_LIMITED,
            None,
        ),
        (
            "skew limited at the recommended size",
            {},
            [sample(2.0, ratio=0.5, worker=0.97)],
            SKEW_LIMITED,
            None,
        ),
        (
            "behind and saturated but a larger size is recommended",
            {},
            [sample(4.0, ratio=0.5, worker=0.97)],
            SCALE_UP,
            "400cc",
        ),
        (
            "scale down vetoed by a timeout a retry recovered from",
            {},
            [sample(0.2, timeouts=1)],
            STEADY,
            None,
        ),
        (
            "scale down vetoed by window cpu",
            dict(history=dataclasses.replace(QUIET, peak_cpu_nano_cores=2.0e9)),
            [sample(0.2)],
            STEADY,
            None,
        ),
        (
            "window cpu allows an intermediate size",
            dict(
                size="800cc",
                history=dataclasses.replace(QUIET, peak_cpu_nano_cores=5.0e9),
            ),
            [sample(0.2)],
            SCALE_DOWN,
            "400cc",
        ),
        (
            "scale down vetoed by window memory",
            dict(history=dataclasses.replace(QUIET, peak_memory_bytes=14 * GIB)),
            [sample(0.2)],
            STEADY,
            None,
        ),
        (
            "scale down vetoed by window lag",
            dict(history=dataclasses.replace(QUIET, peak_lag_seconds=61.0)),
            [sample(0.2)],
            STEADY,
            None,
        ),
        (
            "scale down vetoed without metrics history",
            dict(history=WindowHistory()),
            [sample(0.2)],
            STEADY,
            None,
        ),
        (
            "scale down vetoed until the history covers the window",
            dict(history=dataclasses.replace(QUIET, oldest_metrics_at=NOW - 600)),
            [sample(0.2)],
            STEADY,
            None,
        ),
        (
            "scale down without lag history",
            dict(history=dataclasses.replace(QUIET, peak_lag_seconds=None)),
            [sample(0.2)],
            SCALE_DOWN,
            "100cc",
        ),
        (
            "backing off a target that failed",
            dict(last_failure=transition("timed-out", "100cc", 3 * COOLDOWN)),
            [sample(0.2)],
            BACKING_OFF,
            None,
        ),
        (
            "backing off a target that ran out of resources",
            dict(last_failure=transition("resource-exhausted", "100cc", 2 * COOLDOWN)),
            [sample(0.2)],
            BACKING_OFF,
            None,
        ),
        (
            "backoff expires after four cooldowns",
            dict(last_failure=transition("timed-out", "100cc", 4 * COOLDOWN + 1)),
            [sample(0.2)],
            SCALE_DOWN,
            "100cc",
        ),
        (
            "a failure of someone else's attempt does not back off",
            dict(last_failure=transition("timed-out", "100cc", 2 * COOLDOWN, False)),
            [sample(0.2)],
            SCALE_DOWN,
            "100cc",
        ),
        (
            "a failure of another target does not back off",
            dict(last_failure=transition("timed-out", "400cc", 2 * COOLDOWN)),
            [sample(0.2)],
            SCALE_DOWN,
            "100cc",
        ),
        (
            "a failed target does not block a scale up",
            dict(last_failure=transition("timed-out", "400cc", 2 * COOLDOWN)),
            [sample(3.0)],
            SCALE_UP,
            "400cc",
        ),
        (
            "confirmed unresponsive replica buys one step",
            {},
            [ReplicaSample("r1", UNRESPONSIVE, timeouts=2)],
            SCALE_UP,
            "400cc",
        ),
        (
            "unresponsive replica at max_size",
            dict(size="800cc"),
            [ReplicaSample("r1", UNRESPONSIVE, timeouts=2)],
            AT_MAX,
            None,
        ),
        (
            "a responsive replica that asks for more than one step wins",
            dict(
                replicas=(R1, R2),
                hydrated=frozenset({("u1", "r1-id"), ("u1", "r2-id")}),
            ),
            [ReplicaSample("r1", UNRESPONSIVE, timeouts=2), sample(9.0, "r2")],
            SCALE_UP,
            "800cc",
        ),
        (
            "an idle second replica does not undo an unresponsive one",
            dict(
                replicas=(R1, R2),
                hydrated=frozenset({("u1", "r1-id"), ("u1", "r2-id")}),
            ),
            [ReplicaSample("r1", UNRESPONSIVE, timeouts=2), sample(0.1, "r2")],
            SCALE_UP,
            "400cc",
        ),
        (
            "a failed read is an error, not a signal",
            {},
            [ReplicaSample("r1", ERROR, error="connection lost")],
            ERROR,
            None,
        ),
        (
            "one failed read fails the cluster",
            dict(
                replicas=(R1, R2),
                hydrated=frozenset({("u1", "r1-id"), ("u1", "r2-id")}),
            ),
            [sample(9.0), ReplicaSample("r2", ERROR, error="connection lost")],
            ERROR,
            None,
        ),
        ("eligible without any sample", {}, [], ERROR, None),
        (
            "current size that is not an enabled size",
            dict(size="retired"),
            [sample(1.0)],
            ERROR,
            None,
        ),
        (
            "out-of-memory kill doubles the memory",
            dict(oom_kills=(("r1-id", NOW - 10),)),
            [],
            SCALE_UP,
            "400cc",
        ),
        (
            "out-of-memory kill below the range jumps to twice the memory",
            dict(
                size="50cc",
                oom_kills=(("r1-id", NOW - 10),),
            ),
            [],
            SCALE_UP,
            "100cc",
        ),
        (
            "out-of-memory kill at max_size",
            dict(
                size="800cc",
                oom_kills=(("r1-id", NOW - 10),),
            ),
            [],
            AT_MAX,
            None,
        ),
        (
            "gated clusters make no decision",
            dict(last_transition=transition("finalized", "200cc", 60)),
            [sample(9.0)],
            COOLING_DOWN,
            None,
        ),
        (
            "reconfiguring clusters make no decision",
            dict(reconfiguration=Reconfiguration("in-progress", "400cc")),
            [sample(9.0)],
            RECONFIGURING,
            None,
        ),
        (
            "hydrating clusters make no decision",
            dict(hydrated=frozenset()),
            [],
            HYDRATING,
            None,
        ),
        ("skipped clusters make no decision", dict(managed=False), [], SKIPPED, None),
    ],
)
def test_decide(
    name: str,
    overrides: dict[str, Any],
    samples: list[ReplicaSample],
    decision: str,
    target: str | None,
) -> None:
    got = decide(snapshot(**overrides), POLICY, SIZES, samples, PARAMS)
    assert (got.decision, got.target_size) == (decision, target), (name, got.reason)
    if target is not None:
        assert got.recommended_size == target, name


def test_oom_without_a_size_of_twice_the_memory_takes_max_size() -> None:
    policy = ClusterPolicy("c", "100cc", "400cc", COOLDOWN)
    snap = snapshot(
        size="400cc",
        oom_kills=(("r1-id", NOW - 10),),
    )
    assert decide(snap, policy, SIZES, [], PARAMS).decision == AT_MAX
    policy = ClusterPolicy("c", "100cc", "400cc", COOLDOWN, ("100cc", "200cc", "400cc"))
    snap = snapshot(oom_kills=(("r1-id", NOW - 10),))
    sizes = dict(SIZES)
    sizes["400cc"] = dataclasses.replace(SIZES["400cc"], memory_bytes=40 * GIB)
    got = decide(snap, policy, sizes, [], PARAMS)
    assert (got.decision, got.target_size) == (SCALE_UP, "400cc")


FALLBACK_CASES = [
    ("introspection disabled", dict(introspection=False)),
    (
        "no compute objects",
        dict(objects=(SOURCE,), hydrated=frozenset({("u2", "r1-id")})),
    ),
]


@pytest.mark.parametrize("name,base", FALLBACK_CASES)
@pytest.mark.parametrize(
    "case,history,decision,target",
    [
        ("quiet window scales down on process cpu", QUIET, SCALE_DOWN, "100cc"),
        (
            "busy window is steady",
            dataclasses.replace(QUIET, peak_cpu_nano_cores=2.0e9),
            STEADY,
            None,
        ),
        (
            "memory scales up",
            dataclasses.replace(QUIET, peak_memory_bytes=30 * GIB),
            SCALE_UP,
            "400cc",
        ),
        (
            "lag with saturated process cpu steps up",
            dataclasses.replace(
                QUIET,
                peak_cpu_nano_cores=3.5e9,
                peak_lag_seconds=300.0,
                recent_lag_seconds=300.0,
            ),
            SCALE_UP,
            "400cc",
        ),
        (
            "lag without cpu pressure is not this cluster's to fix",
            dataclasses.replace(
                QUIET, peak_lag_seconds=300.0, recent_lag_seconds=300.0
            ),
            STEADY,
            None,
        ),
        ("no history at all is steady", WindowHistory(), STEADY, None),
    ],
)
def test_fallback(
    name: str,
    base: dict[str, Any],
    case: str,
    history: WindowHistory,
    decision: str,
    target: str | None,
) -> None:
    got = decide(snapshot(history=history, **base), POLICY, SIZES, [], PARAMS)
    assert got.mode == FALLBACK, (name, case)
    assert (got.decision, got.target_size) == (decision, target), (
        name,
        case,
        got.reason,
    )


def test_sampled_mode_is_reported() -> None:
    assert decide(snapshot(), POLICY, SIZES, [sample(2.0)], PARAMS).mode == LIVE


def test_model_parameters_move_the_thresholds() -> None:
    relaxed = dataclasses.replace(PARAMS, u_target=1.0)
    assert decide(snapshot(), POLICY, SIZES, [sample(3.0)], relaxed).decision == STEADY
    strict = dataclasses.replace(PARAMS, max_lag_seconds=1.0)
    assert decide(snapshot(), POLICY, SIZES, [sample(0.2)], strict).decision == STEADY
    patient = dataclasses.replace(PARAMS, backoff_cooldowns=8.0)
    snap = snapshot(last_failure=transition("timed-out", "100cc", 6 * COOLDOWN))
    assert decide(snap, POLICY, SIZES, [sample(0.2)], patient).decision == BACKING_OFF


def test_two_copies_reach_the_same_decision() -> None:
    args = (snapshot(), POLICY, SIZES, [sample(3.0)], PARAMS)
    assert decide(*args) == decide(*args)


def test_ladder_from_family() -> None:
    ladder = build_ladder(SIZES, POLICY)
    assert [s.name for s in ladder.in_range] == ["100cc", "200cc", "400cc", "800cc"]
    assert "medium" not in [s.name for s in ladder.family]
    assert "25cc" in [s.name for s in ladder.family]


def test_ladder_from_explicit_sizes() -> None:
    policy = ClusterPolicy("c", "100cc", "medium", COOLDOWN, ("100cc", "medium"))
    ladder = build_ladder(SIZES, policy)
    assert [s.name for s in ladder.in_range] == ["100cc", "medium"]


@pytest.mark.parametrize(
    "policy,message",
    [
        (ClusterPolicy("c", "100cc", "900cc", COOLDOWN), "does not exist"),
        (ClusterPolicy("c", "10cc", "800cc", COOLDOWN), "does not exist"),
        (ClusterPolicy("c", "800cc", "100cc", COOLDOWN), "larger than"),
        (ClusterPolicy("c", "100cc", "medium", COOLDOWN), "same size family"),
        (
            ClusterPolicy("c", "100cc", "800cc", COOLDOWN, ("100cc", "400cc")),
            "does not include",
        ),
        (
            ClusterPolicy("c", "100cc", "800cc", COOLDOWN, ("100cc", "x", "800cc")),
            "does not exist",
        ),
    ],
)
def test_ladder_rejects(policy: ClusterPolicy, message: str) -> None:
    with pytest.raises(PolicyError, match=message):
        build_ladder(SIZES, policy)


def test_capacity_is_bounded_by_workers_and_by_the_cpu_limit() -> None:
    unlimited = Size("s", 2, 4, 2**63, GIB, 1.0)
    assert unlimited.cores == 8
    limited = Size("s", 2, 4, 1_500_000_000, GIB, 1.0)
    assert limited.cores == 3
    assert limited.memory == 2 * GIB
