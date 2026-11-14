# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The decision function: catalog snapshot, samples and policy in, decision out.

Nothing here performs I/O or reads a clock. `now` is `ClusterSnapshot.now`.
"""

from __future__ import annotations

from dataclasses import dataclass

from materialize.cluster_autoscaler.model import (
    ERROR,
    SAMPLED,
    UNRESPONSIVE,
    ClusterPolicy,
    ClusterSnapshot,
    Decision,
    ModelParams,
    Replica,
    ReplicaSample,
    Size,
)

SKIPPED = "skipped"
RECONFIGURING = "reconfiguring"
OOM = "oom"
COOLING_DOWN = "cooling-down"
HYDRATING = "hydrating"
ELIGIBLE = "eligible"

SCALE_UP = "scale-up"
SCALE_DOWN = "scale-down"
STEADY = "steady"
AT_MIN = "at-min"
AT_MAX = "at-max"
SKEW_LIMITED = "skew-limited"
BACKING_OFF = "backing-off"

FALLBACK = "fallback"
LIVE = "sampled"


class PolicyError(Exception):
    """A cluster policy that does not fit the environment's sizes."""


Rank = tuple[float, float, int]


def rank(size: Size) -> Rank:
    """Orders sizes by capacity. Sizes that differ only in name tie."""
    return (size.credits_per_hour, size.cores, size.memory)


@dataclass(frozen=True)
class Ladder:
    """The sizes a cluster may take, cheapest first."""

    # Every size of the family, including ones outside the range. Sizing
    # against it tells a demand that fits `min_size` from one that would fit
    # something smaller.
    family: tuple[Size, ...]
    min_size: Size
    max_size: Size

    @property
    def in_range(self) -> tuple[Size, ...]:
        return tuple(
            s
            for s in self.family
            if rank(self.min_size) <= rank(s) <= rank(self.max_size)
        )


def build_ladder(sizes: dict[str, Size], policy: ClusterPolicy) -> Ladder:
    """Validates a policy against the enabled sizes and builds its ladder."""
    for name in (policy.min_size, policy.max_size, *(policy.sizes or ())):
        if name not in sizes:
            raise PolicyError(
                f"cluster {policy.name}: size {name!r} does not exist or is disabled"
            )
    min_size = sizes[policy.min_size]
    max_size = sizes[policy.max_size]
    if rank(min_size) > rank(max_size):
        raise PolicyError(
            f"cluster {policy.name}: min_size {min_size.name!r} is larger than "
            f"max_size {max_size.name!r}"
        )
    if policy.sizes is not None:
        for bound in (policy.min_size, policy.max_size):
            if bound not in policy.sizes:
                raise PolicyError(
                    f"cluster {policy.name}: sizes does not include {bound!r}"
                )
        family = [sizes[name] for name in policy.sizes]
    elif min_size.family != max_size.family:
        raise PolicyError(
            f"cluster {policy.name}: min_size {min_size.name!r} and max_size "
            f"{max_size.name!r} are not in the same size family, list the "
            "ladder explicitly with `sizes`"
        )
    else:
        family = [s for s in sizes.values() if s.family == min_size.family]
    return Ladder(
        family=tuple(sorted(family, key=lambda s: s.order)),
        min_size=min_size,
        max_size=max_size,
    )


def is_fallback(snapshot: ClusterSnapshot) -> bool:
    """Whether the cluster has no busy-time signal to sample.

    True when introspection is disabled, and when the cluster runs no compute
    objects, since busy time comes from compute introspection.
    """
    return not snapshot.introspection or not any(
        o.kind == "compute" for o in snapshot.objects
    )


def _realized(snapshot: ClusterSnapshot) -> list[Replica]:
    return [r for r in snapshot.replicas if r.size == snapshot.size]


def _hydrated(snapshot: ClusterSnapshot, replica: Replica) -> bool:
    for obj in snapshot.objects:
        if (obj.id, replica.id) in snapshot.hydrated:
            continue
        # Sources and sinks run on a single replica of a multi-replica cluster,
        # so they can only ever report hydrated for that one.
        if obj.kind == "storage" and any(
            (obj.id, other.id) in snapshot.hydrated for other in snapshot.replicas
        ):
            continue
        return False
    return True


def gate(
    snapshot: ClusterSnapshot, policy: ClusterPolicy
) -> tuple[str, tuple[Replica, ...], str]:
    """Sorts a cluster into a gating state from the catalog read alone.

    Returns the state, the replicas that may be targeted, and a reason. The
    replica list is empty in every state but `ELIGIBLE`, and never contains a
    replica that is not fully hydrated.
    """
    if not snapshot.managed:
        return SKIPPED, (), "cluster is unmanaged"
    if snapshot.schedule != "manual":
        return SKIPPED, (), f"cluster has SCHEDULE = {snapshot.schedule}"

    reconfiguration = snapshot.reconfiguration
    if reconfiguration is not None and reconfiguration.status == "in-progress":
        return (
            RECONFIGURING,
            (),
            f"reconfiguration to {reconfiguration.target_size} in progress",
        )

    realized = _realized(snapshot)
    realized_ids = {r.id for r in realized}
    last = snapshot.last_transition
    # A kill that predates the last transition was either answered by it or
    # could not be answered by it. Acting on it again would bypass the
    # cooldown on every run.
    kills = [
        at
        for replica_id, at in snapshot.oom_kills
        if replica_id in realized_ids and (last is None or at > last.at)
    ]
    if kills:
        return OOM, (), f"{len(kills)} out-of-memory kills since the last size change"

    if last is not None and snapshot.now - last.at < policy.cooldown_seconds:
        return (
            COOLING_DOWN,
            (),
            f"{last.kind} reconfiguration to {last.target_size} "
            f"{snapshot.now - last.at:.0f}s ago, cooldown is "
            f"{policy.cooldown_seconds:.0f}s",
        )

    hydrated = tuple(r for r in realized if _hydrated(snapshot, r))
    if not hydrated:
        return HYDRATING, (), "no replica at the realized size is fully hydrated"
    return ELIGIBLE, hydrated, ""


def _fit(
    candidates: tuple[Size, ...], cpu: float, memory: float, params: ModelParams
) -> Size | None:
    for size in candidates:
        if (
            size.cores * params.u_target >= cpu
            and size.memory * params.m_target >= memory
        ):
            return size
    return None


def _step_up(ladder: Ladder, current: Size) -> Size | None:
    for size in ladder.in_range:
        if rank(size) > rank(current):
            return size
    return None


def _backing_off(
    snapshot: ClusterSnapshot,
    policy: ClusterPolicy,
    target: Size,
    params: ModelParams,
) -> bool:
    failure = snapshot.last_failure
    return (
        failure is not None
        and failure.by_autoscaler
        and failure.target_size == target.name
        and snapshot.now - failure.at
        < params.backoff_cooldowns * policy.cooldown_seconds
    )


def _gib(value: float) -> str:
    return f"{value / 2**30:.1f}GiB"


def _history_veto(
    snapshot: ClusterSnapshot,
    policy: ClusterPolicy,
    ladder: Ladder,
    current: Size,
    floor: Size,
    params: ModelParams,
) -> tuple[Size | None, str]:
    """Picks the cheapest size the window's history allows, at or above `floor`.

    Returns `None` and the reason when the history does not allow any size
    below the current one.
    """
    history = snapshot.history
    if history.peak_cpu_nano_cores is None or history.oldest_metrics_at is None:
        return None, "no metrics history in the window"
    # Metrics are polled about once a minute, so a full window starts with a
    # row at most a poll or two after the window does.
    slack = max(120.0, 0.1 * policy.cooldown_seconds)
    if history.oldest_metrics_at > snapshot.now - policy.cooldown_seconds + slack:
        return None, "metrics history does not cover the window yet"
    if (
        history.peak_lag_seconds is not None
        and history.peak_lag_seconds > params.max_lag_seconds
    ):
        return (
            None,
            f"lag reached {history.peak_lag_seconds:.0f}s in the window, "
            f"bound is {params.max_lag_seconds:.0f}s",
        )
    peak_cores = history.peak_cpu_nano_cores / 1e9
    peak_memory = history.peak_memory_bytes or 0.0
    candidates = tuple(
        s for s in ladder.in_range if rank(floor) <= rank(s) < rank(current)
    )
    target = _fit(candidates, peak_cores, peak_memory, params)
    if target is None:
        return (
            None,
            f"window peaks of {peak_cores:.2f} cores and {_gib(peak_memory)} "
            "do not fit a smaller size",
        )
    return target, ""


def decide(
    snapshot: ClusterSnapshot,
    policy: ClusterPolicy,
    sizes: dict[str, Size],
    samples: list[ReplicaSample],
    params: ModelParams,
) -> Decision:
    """Applies the gates and the decision rules to one cluster.

    `samples` holds one entry per replica `gate` allowed to be sampled, and is
    ignored unless the cluster is eligible.
    """
    state, sampleable, gate_reason = gate(snapshot, policy)
    names = tuple(r.name for r in sampleable)
    if state in (SKIPPED, RECONFIGURING, COOLING_DOWN, HYDRATING):
        return Decision(state=state, decision=state, reason=gate_reason)

    current = sizes.get(snapshot.size or "")
    if current is None:
        return Decision(
            state=state,
            decision=ERROR,
            reason=f"current size {snapshot.size!r} is not an enabled size",
        )
    ladder = build_ladder(sizes, policy)
    in_range = ladder.in_range
    largest = in_range[-1]

    if state == OOM:
        target = next(
            (
                s
                for s in in_range
                if rank(s) > rank(current) and s.memory >= 2 * current.memory
            ),
            largest if rank(largest) > rank(current) else None,
        )
        if target is None:
            return Decision(
                state=state,
                decision=AT_MAX,
                reason=f"{gate_reason}, already at max_size",
                recommended_size=current.name,
            )
        return Decision(
            state=state,
            decision=SCALE_UP,
            reason=gate_reason,
            recommended_size=target.name,
            target_size=target.name,
        )

    failed = [s for s in samples if s.status == ERROR]
    if failed:
        return Decision(
            state=state,
            decision=ERROR,
            reason="; ".join(f"replica {s.replica}: {s.error}" for s in failed),
            sampleable_replicas=names,
        )

    fallback = is_fallback(snapshot)
    measured = [s.measurement for s in samples if s.measurement is not None]
    confirmed_unresponsive = [s for s in samples if s.status == UNRESPONSIVE]
    timed_out = [s for s in samples if s.timeouts > 0]
    if not fallback and not any(s.status in (SAMPLED, UNRESPONSIVE) for s in samples):
        return Decision(
            state=state,
            decision=ERROR,
            reason="no sample from any hydrated replica",
            sampleable_replicas=names,
        )

    history = snapshot.history
    memory_demand = history.peak_memory_bytes or 0.0
    # Every replica does all the work, so the busiest one sizes the cluster.
    cpu_demand = max((m.cpu_demand for m in measured), default=None)
    fit = _fit(in_range, cpu_demand or 0.0, memory_demand, params)
    recommended = fit or largest
    step = _step_up(ladder, current)

    def result(decision: str, reason: str, target: Size | None = None) -> Decision:
        return Decision(
            state=state,
            decision=decision,
            reason=reason,
            mode=FALLBACK if fallback else LIVE,
            recommended_size=(target or recommended).name,
            target_size=target.name if target is not None else None,
            cpu_demand=cpu_demand,
            memory_demand=memory_demand,
            sampleable_replicas=names,
        )

    if confirmed_unresponsive and step is not None:
        # A timeout bounds demand from below and does not size it, so it buys
        # one step. A responsive replica that asks for more still wins.
        target = max(step, recommended, key=rank)
        replicas = ", ".join(s.replica for s in confirmed_unresponsive)
        return result(
            SCALE_UP,
            f"replica {replicas} did not answer an introspection read within "
            "the timeout, twice",
            target,
        )

    above_range = rank(current) > rank(largest)
    if confirmed_unresponsive and not above_range:
        return result(
            AT_MAX,
            "replica "
            + ", ".join(s.replica for s in confirmed_unresponsive)
            + " is unresponsive and the cluster is at max_size",
        )

    if rank(recommended) > rank(current):
        if rank(current) < rank(ladder.min_size) and (
            _fit((current,), cpu_demand or 0.0, memory_demand, params) is not None
        ):
            reason = f"current size is below min_size {ladder.min_size.name}"
        elif cpu_demand is not None and cpu_demand > current.cores * params.u_target:
            reason = (
                f"cpu demand {cpu_demand:.2f} cores exceeds {current.cores:g} "
                f"cores at {params.u_target} target"
            )
        else:
            reason = (
                f"memory demand {_gib(memory_demand)} exceeds "
                f"{_gib(current.memory)} at {params.m_target} target"
            )
        return result(SCALE_UP, reason, recommended)

    behind = min((m.min_progress_ratio for m in measured), default=1.0) < 1.0
    saturated = (
        max((m.max_worker_utilization for m in measured), default=0.0)
        >= params.worker_saturation
    )
    slowest = next((m.slowest_dataflow for m in measured if m.slowest_dataflow), None)

    # Without busy time, lag says the cluster is overloaded only together with
    # process CPU at the target. Lag alone is as likely an upstream stall.
    if (
        fallback
        and not above_range
        and history.recent_lag_seconds is not None
        and history.recent_lag_seconds > params.max_lag_seconds
        and history.peak_cpu_nano_cores is not None
        and history.peak_cpu_nano_cores / 1e9 >= current.cores * params.u_target
    ):
        reason = (
            f"lag is {history.recent_lag_seconds:.0f}s and process cpu peaked "
            f"at {history.peak_cpu_nano_cores / 1e9:.2f} cores"
        )
        if step is None:
            return result(AT_MAX, reason)
        return result(SCALE_UP, reason, step)
    if rank(recommended) < rank(current):
        veto = None
        if timed_out:
            veto = (
                STEADY,
                "replica "
                + ", ".join(s.replica for s in timed_out)
                + " timed out on an introspection read",
            )
        elif behind and saturated:
            veto = (
                SKEW_LIMITED,
                f"dataflow {slowest} is behind with one worker saturated, a "
                "larger size is not recommended",
            )
        elif behind:
            veto = (STEADY, f"dataflow {slowest} is behind")
        target = None
        if veto is None:
            target, reason = _history_veto(
                snapshot, policy, ladder, current, recommended, params
            )
            if target is None:
                veto = (STEADY, reason)
        if target is None and above_range:
            # The range is the cost bound. A size above it is pulled back to
            # `max_size` even when nothing says a smaller size would do.
            target = largest
            veto = None
        if veto is not None or target is None:
            assert veto is not None
            return result(*veto)
        if _backing_off(snapshot, policy, target, params):
            return result(
                BACKING_OFF,
                f"a reconfiguration to {target.name} failed less than "
                f"{params.backoff_cooldowns:g} cooldowns ago",
            )
        if above_range:
            reason = f"current size is above max_size {largest.name}"
        elif cpu_demand is not None:
            reason = (
                f"cpu demand {cpu_demand:.2f} cores and window peaks fit "
                f"{target.name}"
            )
        else:
            reason = f"window peaks fit {target.name}"
        return result(SCALE_DOWN, reason, target)

    if fit is None:
        return result(AT_MAX, "demand exceeds max_size")
    if behind and saturated:
        return result(
            SKEW_LIMITED,
            f"dataflow {slowest} is behind with one worker saturated, a larger "
            "size is not recommended",
        )
    unbounded = _fit(ladder.family, cpu_demand or 0.0, memory_demand, params)
    if unbounded is not None and rank(unbounded) < rank(ladder.min_size):
        return result(AT_MIN, f"demand would fit {unbounded.name}")
    return result(STEADY, "current size is the recommended size")
