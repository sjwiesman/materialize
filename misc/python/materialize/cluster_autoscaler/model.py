# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Plain data types shared by the SQL layer and the decision function.

All timestamps are seconds since the Unix epoch, as read from Materialize, so
that the decision never depends on the clock of the machine running it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Size:
    """A row of `mz_catalog.mz_cluster_replica_sizes`."""

    name: str
    processes: int
    workers: int
    cpu_nano_cores: int
    memory_bytes: int
    credits_per_hour: float

    @property
    def cores(self) -> float:
        """CPU capacity in worker cores.

        Busy time is measured in worker time, and a worker uses at most one
        core, so capacity is bounded by the worker count as well as by the CPU
        limit. A size without a CPU limit reports a very large
        `cpu_nano_cores`, which the worker count then bounds.
        """
        return self.processes * min(float(self.workers), self.cpu_nano_cores / 1e9)

    @property
    def memory(self) -> int:
        return self.processes * self.memory_bytes

    @property
    def order(self) -> tuple[float, float, int, str]:
        return (self.credits_per_hour, self.cores, self.memory, self.name)

    @property
    def family(self) -> str:
        """The size name with its numbers masked, e.g. `#cc`.

        The catalog does not expose a size family, so sizes that differ only
        in their numbers are taken to form one ladder.
        """
        return re.sub(r"\d+", "#", self.name)


@dataclass(frozen=True)
class ModelParams:
    """Parameters of the demand model. Not per cluster."""

    u_target: float = 0.7
    m_target: float = 0.8
    min_progress_ratio: float = 0.25
    # A measured ratio at or above this counts as keeping up. Frontiers tick
    # about once a second, so two reads `sample_interval` apart can disagree
    # by a tick on each end without the dataflow being behind.
    keeping_up_ratio: float = 0.9
    catch_up_seconds: float = 900.0
    max_lag_seconds: float = 60.0
    worker_saturation: float = 0.9
    backoff_cooldowns: float = 4.0


@dataclass(frozen=True)
class ClusterPolicy:
    """One entry of the `clusters` list in the configuration."""

    name: str
    min_size: str
    max_size: str
    cooldown_seconds: float
    # Explicit ladder, for size names that do not form a family by pattern.
    sizes: tuple[str, ...] | None = None


@dataclass(frozen=True)
class Replica:
    id: str
    name: str
    size: str | None


@dataclass(frozen=True)
class Reconfiguration:
    """The cluster's row in `mz_internal.mz_cluster_reconfigurations`."""

    status: str
    target_size: str | None


@dataclass(frozen=True)
class Transition:
    """A settled reconfiguration, from `mz_catalog.mz_audit_events`."""

    kind: str
    target_size: str
    at: float
    by_autoscaler: bool


@dataclass(frozen=True)
class ClusterObject:
    id: str
    # "compute" for indexes and materialized views, "storage" for sources and
    # sinks.
    kind: str


@dataclass(frozen=True)
class WindowHistory:
    """What the retained history says about the cooldown window."""

    # Whole-replica peaks, `None` when the window holds no metrics at all.
    peak_cpu_nano_cores: float | None = None
    peak_memory_bytes: float | None = None
    # Time of the oldest metrics row inside the window.
    oldest_metrics_at: float | None = None
    peak_lag_seconds: float | None = None
    recent_lag_seconds: float | None = None


@dataclass(frozen=True)
class ClusterSnapshot:
    """Everything the catalog read returns for one cluster."""

    id: str
    name: str
    now: float
    managed: bool
    schedule: str
    size: str | None
    introspection: bool
    replicas: tuple[Replica, ...] = ()
    reconfiguration: Reconfiguration | None = None
    last_transition: Transition | None = None
    last_failure: Transition | None = None
    objects: tuple[ClusterObject, ...] = ()
    # `(object_id, replica_id)` pairs reported as hydrated.
    hydrated: frozenset[tuple[str, str]] = frozenset()
    # `(replica_id, occurred_at)` of out-of-memory kills inside the window.
    oom_kills: tuple[tuple[str, float], ...] = ()
    history: WindowHistory = field(default_factory=WindowHistory)


@dataclass(frozen=True)
class DataflowLoad:
    dataflow_id: str
    exports: tuple[str, ...]
    busy_cores: float
    progress_ratio: float
    lag_seconds: float


@dataclass(frozen=True)
class Measurement:
    """The load of one replica over one sample interval."""

    interval_seconds: float
    busy_cores: float
    cpu_demand: float
    max_worker_utilization: float
    min_progress_ratio: float
    slowest_dataflow: str | None
    dataflows: tuple[DataflowLoad, ...] = ()


SAMPLED = "sampled"
UNRESPONSIVE = "unresponsive"
ERROR = "error"


@dataclass(frozen=True)
class ReplicaSample:
    """The outcome of the targeted reads against one hydrated replica."""

    replica: str
    # `SAMPLED`, `UNRESPONSIVE` (a statement timeout confirmed by a retry) or
    # `ERROR` (anything that is not a statement timeout on a live connection).
    status: str
    # Statement timeouts seen, including ones a retry recovered from.
    timeouts: int = 0
    measurement: Measurement | None = None
    error: str | None = None


@dataclass(frozen=True)
class Decision:
    state: str
    decision: str
    reason: str
    # "sampled" when a live sample sized the decision, "fallback" when only
    # memory, lag and out-of-memory kills were available, `None` when the
    # cluster never reached the decision rules.
    mode: str | None = None
    recommended_size: str | None = None
    # Set when a rule fired. The size to `ALTER` the cluster to.
    target_size: str | None = None
    cpu_demand: float | None = None
    memory_demand: float | None = None
    sampleable_replicas: tuple[str, ...] = ()
