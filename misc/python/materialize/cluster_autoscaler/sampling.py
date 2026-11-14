# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Turns two introspection reads of one replica into a load measurement."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from materialize.cluster_autoscaler.model import (
    DataflowLoad,
    Measurement,
    ModelParams,
)


@dataclass
class ReplicaRead:
    """One read of a replica's introspection relations."""

    # Replica time of the read in milliseconds.
    at_ms: float
    # Cumulative scheduled nanoseconds per `(dataflow operator, worker)`. Only
    # the root operator of each dataflow is read. Its elapsed time includes
    # every operator nested in it, so summing nested operators as well would
    # count their time more than once.
    elapsed_ns: dict[tuple[str, str], int] = field(default_factory=dict)
    # Root operator to dataflow.
    operator_dataflow: dict[str, str] = field(default_factory=dict)
    # Export to `(dataflow, output frontier in milliseconds)`.
    exports: dict[str, tuple[str, int]] = field(default_factory=dict)
    # `(export, import)` to input frontier in milliseconds.
    imports: dict[tuple[str, str], int] = field(default_factory=dict)


def measure(
    first: ReplicaRead, second: ReplicaRead, params: ModelParams
) -> Measurement:
    """Computes the demand model's inputs for the interval between two reads.

    Only operators, exports and imports present in both reads count, so a
    dataflow dropped or created between the reads contributes nothing.
    """
    interval_ns = (second.at_ms - first.at_ms) * 1e6
    if interval_ns <= 0:
        raise ValueError(
            f"second read at {second.at_ms}ms is not after first at {first.at_ms}ms"
        )

    busy_by_dataflow: dict[str, float] = defaultdict(float)
    busy_by_worker: dict[str, float] = defaultdict(float)
    for key, before in first.elapsed_ns.items():
        after = second.elapsed_ns.get(key)
        if after is None:
            continue
        operator, worker = key
        dataflow = second.operator_dataflow.get(operator)
        if dataflow is None or first.operator_dataflow.get(operator) != dataflow:
            continue
        cores = max(0, after - before) / interval_ns
        busy_by_dataflow[dataflow] += cores
        busy_by_worker[worker] += cores

    out_before: dict[str, int] = {}
    out_after: dict[str, int] = {}
    exports_by_dataflow: dict[str, list[str]] = defaultdict(list)
    for export, (dataflow, frontier) in second.exports.items():
        if export not in first.exports or first.exports[export][0] != dataflow:
            continue
        exports_by_dataflow[dataflow].append(export)
        out_before[dataflow] = min(
            out_before.get(dataflow, first.exports[export][1]),
            first.exports[export][1],
        )
        out_after[dataflow] = min(out_after.get(dataflow, frontier), frontier)

    in_before: dict[str, int] = {}
    in_after: dict[str, int] = {}
    for key, frontier in second.imports.items():
        export, _ = key
        if key not in first.imports or export not in second.exports:
            continue
        dataflow = second.exports[export][0]
        in_before[dataflow] = min(
            in_before.get(dataflow, first.imports[key]), first.imports[key]
        )
        in_after[dataflow] = min(in_after.get(dataflow, frontier), frontier)

    dataflows = []
    cpu_demand = 0.0
    for dataflow in sorted(set(busy_by_dataflow) | set(exports_by_dataflow)):
        ratio = 1.0
        lag_seconds = 0.0
        if dataflow in out_after and dataflow in in_after:
            input_advance = in_after[dataflow] - in_before[dataflow]
            output_advance = out_after[dataflow] - out_before[dataflow]
            # Inputs that did not advance are stalled upstream. That is not
            # this cluster's problem to scale for.
            if input_advance > 0:
                ratio = max(0.0, output_advance / input_advance)
            if ratio >= params.keeping_up_ratio:
                ratio = 1.0
            ratio = max(params.min_progress_ratio, ratio)
            lag_seconds = max(0, in_after[dataflow] - out_after[dataflow]) / 1000
        busy = busy_by_dataflow.get(dataflow, 0.0)
        catch_up = 1.0
        if params.catch_up_seconds > 0:
            catch_up += lag_seconds / params.catch_up_seconds
        cpu_demand += busy / ratio * catch_up
        dataflows.append(
            DataflowLoad(
                dataflow_id=dataflow,
                exports=tuple(sorted(exports_by_dataflow.get(dataflow, []))),
                busy_cores=busy,
                progress_ratio=ratio,
                lag_seconds=lag_seconds,
            )
        )

    slowest = min(dataflows, key=lambda d: d.progress_ratio, default=None)
    return Measurement(
        interval_seconds=interval_ns / 1e9,
        busy_cores=sum(busy_by_dataflow.values()),
        cpu_demand=cpu_demand,
        max_worker_utilization=max(busy_by_worker.values(), default=0.0),
        min_progress_ratio=slowest.progress_ratio if slowest else 1.0,
        slowest_dataflow=(
            (slowest.exports[0] if slowest.exports else slowest.dataflow_id)
            if slowest and slowest.progress_ratio < 1.0
            else None
        ),
        dataflows=tuple(dataflows),
    )


TIMEOUT = "timeout"
FAILURE = "failure"

# `SqlState::QUERY_CANCELED`, which `AdapterError::StatementTimeout` maps to in
# src/adapter/src/error.rs. A user-requested cancel shares the code, so the
# message is checked as well.
QUERY_CANCELED = "57014"


def classify_read_failure(
    sqlstate: str | None, message: str, connection_alive: bool
) -> str:
    """Tells a statement timeout on a live connection from any other failure.

    Only the former says anything about the replica. A dead connection, or an
    error with another code, is a failure to measure.
    """
    if (
        connection_alive
        and sqlstate == QUERY_CANCELED
        and "statement timeout" in message
    ):
        return TIMEOUT
    return FAILURE
