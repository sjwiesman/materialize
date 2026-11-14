# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""One run of the autoscaler: read, gate, sample, decide, act, report.

Nothing is carried from one run to the next.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

import psycopg

from materialize.cluster_autoscaler import catalog
from materialize.cluster_autoscaler.catalog import (
    Database,
    ReadFailure,
    ReadTimeout,
    ReplicaSession,
)
from materialize.cluster_autoscaler.config import Config
from materialize.cluster_autoscaler.decision import (
    ELIGIBLE,
    PolicyError,
    build_ladder,
    decide,
    gate,
    is_fallback,
)
from materialize.cluster_autoscaler.model import (
    ERROR,
    SAMPLED,
    UNRESPONSIVE,
    ClusterPolicy,
    ClusterSnapshot,
    Decision,
    ModelParams,
    ReplicaSample,
    Size,
)
from materialize.cluster_autoscaler.sampling import (
    TIMEOUT,
    ReplicaRead,
    classify_read_failure,
    measure,
)

ALTERED = "altered"
DRY_RUN = "dry-run"
SUPERSEDED = "superseded"
FAILED = "failed"
NONE = "none"


@dataclass(frozen=True)
class RunOptions:
    params: ModelParams
    dry_run: bool
    max_concurrent_clusters: int = 4


def take_sample(
    replica: str,
    read: Callable[[], ReplicaRead],
    wait: Callable[[], None],
    params: ModelParams,
) -> ReplicaSample:
    """Takes two reads `wait()` apart and turns them into a sample.

    A read that times out is retried once. Two timeouts in a row on the same
    read make the sample unresponsive. Any other failure makes it an error,
    which says nothing about the replica's load.
    """
    timeouts = 0
    reads: list[ReplicaRead] = []
    for index in range(2):
        if index == 1:
            wait()
        for _ in range(2):
            try:
                reads.append(read())
                break
            except ReadTimeout:
                timeouts += 1
            except ReadFailure as e:
                return ReplicaSample(replica, ERROR, timeouts, error=str(e))
        else:
            return ReplicaSample(replica, UNRESPONSIVE, timeouts)
    try:
        measurement = measure(reads[0], reads[1], params)
    except ValueError as e:
        return ReplicaSample(replica, ERROR, timeouts, error=str(e))
    return ReplicaSample(replica, SAMPLED, timeouts, measurement)


def sample_replica(
    db: Database,
    cluster: str,
    replica: str,
    config: Config,
    params: ModelParams,
    sleep: Callable[[float], None],
) -> ReplicaSample:
    session = ReplicaSession(db, cluster, replica, config.replica_read_timeout_seconds)
    try:
        return take_sample(
            replica,
            session.read,
            lambda: sleep(config.sample_interval_seconds),
            params,
        )
    finally:
        session.close()


def build_report(
    policy: ClusterPolicy,
    snapshot: ClusterSnapshot | None,
    decision: Decision,
    samples: list[ReplicaSample],
    sizes: dict[str, Size],
    dry_run: bool,
    action: str,
    error: str | None = None,
) -> dict[str, Any]:
    """Builds the record emitted for one cluster."""
    record: dict[str, Any] = {
        "cluster": policy.name,
        "state": decision.state,
        "current_size": snapshot.size if snapshot else None,
        "recommended_size": decision.recommended_size,
        "decision": decision.decision,
        "reason": decision.reason,
        "target_size": decision.target_size,
        "action": action,
        "dry_run": dry_run,
    }
    if error is not None:
        record["error"] = error
    if decision.mode is not None:
        record["mode"] = decision.mode
    if decision.cpu_demand is not None:
        record["cpu_demand_cores"] = round(decision.cpu_demand, 3)
    if snapshot is None:
        return record

    by_replica = {s.replica: s for s in samples}
    replicas = []
    for replica in snapshot.replicas:
        entry: dict[str, Any] = {"name": replica.name, "size": replica.size}
        sample = by_replica.get(replica.name)
        entry["sampled"] = sample is not None and sample.status == SAMPLED
        if sample is None:
            if replica.name not in decision.sampleable_replicas:
                entry["hydrated"] = False
        else:
            entry["status"] = sample.status
            entry["timeouts"] = sample.timeouts
            if sample.error is not None:
                entry["error"] = sample.error
            m = sample.measurement
            if m is not None:
                entry["busy_cores"] = round(m.busy_cores, 3)
                entry["cpu_demand_cores"] = round(m.cpu_demand, 3)
                entry["max_worker_utilization"] = round(m.max_worker_utilization, 3)
                entry["min_progress_ratio"] = round(m.min_progress_ratio, 3)
                entry["slowest_dataflow"] = m.slowest_dataflow
                entry["interval_seconds"] = round(m.interval_seconds, 3)
        replicas.append(entry)
    record["replicas"] = replicas

    history = snapshot.history
    window: dict[str, Any] = {
        "seconds": policy.cooldown_seconds,
        "peak_memory_bytes": history.peak_memory_bytes,
        "peak_lag_seconds": history.peak_lag_seconds,
    }
    current = sizes.get(snapshot.size or "")
    if history.peak_cpu_nano_cores is not None:
        window["peak_cpu_cores"] = round(history.peak_cpu_nano_cores / 1e9, 3)
        if current is not None and current.cores > 0:
            window["peak_cpu_percent"] = round(
                100 * history.peak_cpu_nano_cores / 1e9 / current.cores, 1
            )
    record["window"] = window
    return record


def _act(
    conn: psycopg.Connection[Any], snapshot: ClusterSnapshot, target: str
) -> tuple[str, str | None]:
    """Issues the `ALTER`, unless the cluster changed since the catalog read."""
    size, reconfiguration = catalog.read_cluster_state(conn, snapshot.id)
    in_progress = (
        reconfiguration is not None and reconfiguration.status == "in-progress"
    )
    if size != snapshot.size or in_progress:
        return SUPERSEDED, "the cluster was reconfigured during this run"
    try:
        catalog.alter_cluster_size(conn, snapshot.name, target)
    except psycopg.Error as e:
        # With background reconfiguration disabled the statement waits for the
        # cut-over, and the record it wrote outlives a cancelled wait. A
        # statement timeout is then not a failure, so look at the record.
        alive = not conn.closed
        if classify_read_failure(e.sqlstate, str(e), alive) == TIMEOUT:
            _, reconfiguration = catalog.read_cluster_state(conn, snapshot.id)
            if (
                reconfiguration is not None
                and reconfiguration.status == "in-progress"
                and reconfiguration.target_size == target
            ):
                return ALTERED, None
        return FAILED, str(e).strip()
    return ALTERED, None


def evaluate_cluster(
    db: Database,
    config: Config,
    policy: ClusterPolicy,
    sizes: dict[str, Size],
    role: str,
    options: RunOptions,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Runs every step for one cluster and returns its report record."""
    snapshot = None
    samples: list[ReplicaSample] = []
    try:
        build_ladder(sizes, policy)
        with db.catalog_session() as conn:
            snapshot = catalog.read_snapshot(conn, policy, role)
            if snapshot is None:
                raise PolicyError(f"cluster {policy.name} does not exist")

            state, sampleable, _ = gate(snapshot, policy)
            if state == ELIGIBLE and not is_fallback(snapshot):
                with ThreadPoolExecutor(max_workers=len(sampleable)) as pool:
                    samples = list(
                        pool.map(
                            lambda replica: sample_replica(
                                db,
                                policy.name,
                                replica.name,
                                config,
                                options.params,
                                sleep,
                            ),
                            sampleable,
                        )
                    )

            decision = decide(snapshot, policy, sizes, samples, options.params)
            action = NONE
            error = None
            if decision.target_size is not None:
                if options.dry_run:
                    action = DRY_RUN
                else:
                    action, error = _act(conn, snapshot, decision.target_size)
            return build_report(
                policy,
                snapshot,
                decision,
                samples,
                sizes,
                options.dry_run,
                action,
                error,
            )
    # Clusters are independent. Whatever goes wrong with this one is reported
    # in its record and must not take the other clusters' records down.
    except Exception as e:
        decision = Decision(state=ERROR, decision=ERROR, reason=str(e).strip())
        return build_report(
            policy, snapshot, decision, samples, sizes, options.dry_run, NONE
        )


def is_failure(record: dict[str, Any]) -> bool:
    """Whether a record means the cluster could not be evaluated or altered."""
    return record["decision"] == ERROR or record["action"] == FAILED


def run_once(
    db: Database,
    config: Config,
    options: RunOptions,
    emit: Callable[[dict[str, Any]], None],
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    """Performs one run over every configured cluster.

    Emits one record per cluster, in configuration order. Returns whether
    every cluster could be evaluated.
    """
    try:
        with db.catalog_session() as conn:
            sizes = catalog.read_sizes(conn)
            role = catalog.read_role(conn)
    except psycopg.Error as e:
        for policy in config.clusters:
            decision = Decision(state=ERROR, decision=ERROR, reason=str(e).strip())
            emit(build_report(policy, None, decision, [], {}, options.dry_run, NONE))
        return False

    ok = True
    with ThreadPoolExecutor(max_workers=options.max_concurrent_clusters) as pool:
        futures = [
            pool.submit(
                evaluate_cluster, db, config, policy, sizes, role, options, sleep
            )
            for policy in config.clusters
        ]
        for future in futures:
            record = future.result()
            ok = ok and not is_failure(record)
            emit(record)
    return ok


def validate(db: Database, config: Config) -> None:
    """Checks every cluster policy against the environment's sizes.

    Raises `PolicyError`, or `psycopg.Error` if the environment is
    unreachable.
    """
    with db.catalog_session() as conn:
        sizes = catalog.read_sizes(conn)
    for policy in config.clusters:
        build_ladder(sizes, policy)
