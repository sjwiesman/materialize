# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Reduces raw history rows to the facts the decision function needs."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from materialize.cluster_autoscaler.model import Transition

FINALIZED = "finalized"
FAILED = ("timed-out", "resource-exhausted")


@dataclass(frozen=True)
class ReconfigurationEvent:
    """A reconfiguration lifecycle event from `mz_catalog.mz_audit_events`."""

    id: int
    transition: str
    target_size: str
    user: str | None
    occurred_at: float


def summarize_transitions(
    events: list[ReconfigurationEvent], autoscaler_role: str
) -> tuple[Transition | None, Transition | None]:
    """Returns the latest settled transition and the latest failed one.

    The controller writes the settling event, so it carries no user. The role
    behind a transition is the user of the latest `started` event before it,
    which is the `ALTER` that set the target the transition settled.
    """
    last_transition = None
    last_failure = None
    started_by: str | None = None
    for event in sorted(events, key=lambda e: e.id):
        if event.transition == "started":
            started_by = event.user
        elif event.transition == FINALIZED or event.transition in FAILED:
            transition = Transition(
                kind=event.transition,
                target_size=event.target_size,
                at=event.occurred_at,
                by_autoscaler=started_by == autoscaler_role,
            )
            last_transition = transition
            if event.transition in FAILED:
                last_failure = transition
    return last_transition, last_failure


@dataclass(frozen=True)
class MetricsRow:
    """A row of `mz_internal.mz_cluster_replica_metrics_history`."""

    replica_id: str
    process_id: str
    cpu_nano_cores: int | None
    memory_bytes: int | None
    heap_bytes: int | None
    swap_bytes: int | None
    occurred_at: float


def summarize_metrics(
    rows: list[MetricsRow],
) -> tuple[float | None, float | None, float | None]:
    """Returns whole-replica peak CPU, peak memory, and the oldest row's time.

    Each process's peak is taken over the window and the peaks are summed per
    replica. Rows of one poll need not share a timestamp, so this is an upper
    bound on the true simultaneous peak, which is the safe direction for a
    veto. The result is the maximum over replicas.
    """
    if not rows:
        return None, None, None
    cpu: dict[str, dict[str, int]] = defaultdict(dict)
    memory: dict[str, dict[str, int]] = defaultdict(dict)
    for row in rows:
        # `heap_bytes` already counts swap. It is nullable, and resident
        # memory plus swap stands in when it is missing.
        used = max(row.heap_bytes or 0, (row.memory_bytes or 0) + (row.swap_bytes or 0))
        per_cpu = cpu[row.replica_id]
        per_memory = memory[row.replica_id]
        per_cpu[row.process_id] = max(
            per_cpu.get(row.process_id, 0), row.cpu_nano_cores or 0
        )
        per_memory[row.process_id] = max(per_memory.get(row.process_id, 0), used)
    return (
        float(max(sum(per.values()) for per in cpu.values())),
        float(max(sum(per.values()) for per in memory.values())),
        min(row.occurred_at for row in rows),
    )
