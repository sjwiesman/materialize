# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Stateless external autoscaler for managed clusters.

See misc/python/materialize/cluster_autoscaler/README.md.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

import psycopg

from materialize.cluster_autoscaler.catalog import Database
from materialize.cluster_autoscaler.config import (
    ConfigError,
    load_config,
    parse_duration,
)
from materialize.cluster_autoscaler.decision import SKIPPED, PolicyError
from materialize.cluster_autoscaler.model import ModelParams
from materialize.cluster_autoscaler.runner import RunOptions, run_once, validate

logger = logging.getLogger("cluster-autoscaler")

EXIT_OK = 0
EXIT_CLUSTER_FAILED = 1
EXIT_INVALID_CONFIG = 2


def _fraction(text: str) -> float:
    value = float(text)
    if not 0 < value <= 1:
        raise argparse.ArgumentTypeError(f"{text} is not in (0, 1]")
    return value


def _duration(text: str) -> float:
    try:
        return parse_duration(text)
    except ConfigError as e:
        raise argparse.ArgumentTypeError(str(e)) from e


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    defaults = ModelParams()
    parser = argparse.ArgumentParser(
        prog="cluster-autoscaler",
        description="Runs the stateless external cluster autoscaler.",
    )
    parser.add_argument("config", type=Path, help="path to the YAML configuration")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report every decision and issue no ALTER (also `dry_run` in the config)",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="repeat runs in this process, carrying nothing from one to the next",
    )
    parser.add_argument(
        "--interval",
        type=_duration,
        default=60.0,
        metavar="DURATION",
        help="pause between runs with --loop (default: 60s)",
    )
    parser.add_argument(
        "--max-concurrent-clusters",
        type=int,
        default=4,
        metavar="N",
        help="clusters evaluated at once (default: 4)",
    )
    model = parser.add_argument_group("demand model")
    model.add_argument(
        "--u-target",
        type=_fraction,
        default=defaults.u_target,
        help="CPU utilization a size is filled to (default: %(default)s)",
    )
    model.add_argument(
        "--m-target",
        type=_fraction,
        default=defaults.m_target,
        help="memory utilization a size is filled to (default: %(default)s)",
    )
    model.add_argument(
        "--min-progress-ratio",
        type=_fraction,
        default=defaults.min_progress_ratio,
        help="lower clamp of the progress ratio (default: %(default)s)",
    )
    model.add_argument(
        "--keeping-up-ratio",
        type=_fraction,
        default=defaults.keeping_up_ratio,
        help="measured progress ratio that counts as keeping up (default: %(default)s)",
    )
    model.add_argument(
        "--catch-up",
        type=_duration,
        default=defaults.catch_up_seconds,
        metavar="DURATION",
        help="time a lagging dataflow is given to catch up (default: 15min)",
    )
    model.add_argument(
        "--max-lag",
        type=_duration,
        default=defaults.max_lag_seconds,
        metavar="DURATION",
        help="lag over the window above which a scale-down is vetoed (default: 60s)",
    )
    model.add_argument(
        "--worker-saturation",
        type=_fraction,
        default=defaults.worker_saturation,
        help="busiest-worker utilization that counts as saturated (default: %(default)s)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    if args.max_concurrent_clusters < 1:
        parser.error("--max-concurrent-clusters must be at least 1")
    return args


def emit(record: dict[str, Any]) -> None:
    if record["state"] == SKIPPED:
        logger.warning("skipping cluster %s: %s", record["cluster"], record["reason"])
    print(json.dumps(record), flush=True)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    try:
        config = load_config(args.config)
        db = Database(config.connection)
        validate(db, config)
    except (ConfigError, PolicyError) as e:
        logger.error("invalid configuration: %s", e)
        return EXIT_INVALID_CONFIG
    except psycopg.Error as e:
        logger.error("cannot validate configuration: %s", str(e).strip())
        return EXIT_CLUSTER_FAILED

    options = RunOptions(
        params=dataclasses.replace(
            ModelParams(),
            u_target=args.u_target,
            m_target=args.m_target,
            min_progress_ratio=args.min_progress_ratio,
            keeping_up_ratio=args.keeping_up_ratio,
            catch_up_seconds=args.catch_up,
            max_lag_seconds=args.max_lag,
            worker_saturation=args.worker_saturation,
        ),
        dry_run=args.dry_run or config.dry_run,
        max_concurrent_clusters=args.max_concurrent_clusters,
    )

    ok = run_once(db, config, options, emit)
    try:
        while args.loop:
            time.sleep(args.interval)
            ok = run_once(db, config, options, emit)
    except KeyboardInterrupt:
        pass
    return EXIT_OK if ok else EXIT_CLUSTER_FAILED


if __name__ == "__main__":
    sys.exit(main())
