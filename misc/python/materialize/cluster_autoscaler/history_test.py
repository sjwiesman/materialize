# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from materialize.cluster_autoscaler.history import (
    MetricsRow,
    ReconfigurationEvent,
    summarize_metrics,
    summarize_transitions,
)

GIB = 2**30


def event(
    id: int, transition: str, target: str, user: str | None
) -> ReconfigurationEvent:
    return ReconfigurationEvent(id, transition, target, user, 1000.0 + id)


def test_no_events() -> None:
    assert summarize_transitions([], "autoscaler") == (None, None)


def test_in_flight_reconfiguration_is_not_a_transition() -> None:
    events = [event(1, "started", "400cc", "autoscaler")]
    assert summarize_transitions(events, "autoscaler") == (None, None)


def test_transition_is_attributed_to_the_alter_that_set_its_target() -> None:
    events = [
        event(1, "started", "400cc", "autoscaler"),
        event(2, "finalized", "400cc", None),
        event(3, "started", "100cc", "autoscaler"),
        event(4, "started", "200cc", "human"),
        event(5, "timed-out", "200cc", None),
    ]
    # The catalog read returns newest first. Order must not matter.
    last, failure = summarize_transitions(list(reversed(events)), "autoscaler")
    assert last is not None and failure is not None
    assert (last.kind, last.target_size, last.at) == ("timed-out", "200cc", 1005.0)
    assert not last.by_autoscaler
    assert failure == last


def test_failure_is_remembered_past_a_later_success() -> None:
    events = [
        event(1, "started", "100cc", "autoscaler"),
        event(2, "resource-exhausted", "100cc", None),
        event(3, "started", "400cc", "human"),
        event(4, "finalized", "400cc", None),
    ]
    last, failure = summarize_transitions(events, "autoscaler")
    assert last is not None and failure is not None
    assert (last.kind, last.by_autoscaler) == ("finalized", False)
    assert (failure.kind, failure.target_size) == ("resource-exhausted", "100cc")
    assert failure.by_autoscaler


def test_cancelled_reconfiguration_is_not_a_transition() -> None:
    events = [
        event(1, "started", "400cc", "autoscaler"),
        event(2, "cancelled", "200cc", "human"),
    ]
    assert summarize_transitions(events, "autoscaler") == (None, None)


def row(
    replica: str,
    process: str,
    at: float,
    cpu: int | None = None,
    memory: int | None = None,
    heap: int | None = None,
    swap: int | None = None,
) -> MetricsRow:
    return MetricsRow(replica, process, cpu, memory, heap, swap, at)


def test_no_metrics() -> None:
    assert summarize_metrics([]) == (None, None, None)


def test_peaks_sum_over_processes_and_take_the_busiest_replica() -> None:
    rows = [
        row("r1", "0", 10, cpu=1_000, heap=4 * GIB),
        row("r1", "0", 70, cpu=3_000, heap=2 * GIB),
        row("r1", "1", 11, cpu=2_000, heap=1 * GIB),
        row("r2", "0", 12, cpu=4_500, heap=1 * GIB),
        row("r2", "1", 5, cpu=None, heap=None),
    ]
    assert summarize_metrics(rows) == (5_000.0, 5.0 * GIB, 5)


def test_memory_falls_back_to_resident_plus_swap() -> None:
    rows = [row("r1", "0", 10, memory=3 * GIB, swap=2 * GIB)]
    assert summarize_metrics(rows)[1] == 5.0 * GIB
    rows = [row("r1", "0", 10, memory=3 * GIB, swap=2 * GIB, heap=6 * GIB)]
    assert summarize_metrics(rows)[1] == 6.0 * GIB
