# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Parsing of the autoscaler's YAML configuration."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from materialize.cluster_autoscaler.model import ClusterPolicy


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class Connection:
    host: str
    port: int
    user: str
    password: str | None
    sslmode: str
    database: str


@dataclass(frozen=True)
class Config:
    connection: Connection
    clusters: tuple[ClusterPolicy, ...]
    sample_interval_seconds: float
    replica_read_timeout_seconds: float
    dry_run: bool


_UNITS = {
    "ms": 0.001,
    "s": 1.0,
    "m": 60.0,
    "min": 60.0,
    "h": 3600.0,
    "d": 86400.0,
}
_DURATION_PART = re.compile(r"(\d+(?:\.\d+)?)\s*(ms|min|s|m|h|d)")


def parse_duration(value: Any) -> float:
    """Parses a duration such as `30s`, `15min` or `1h30m` into seconds."""
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ConfigError(f"invalid duration {value!r}")
    if isinstance(value, (int, float)):
        seconds = float(value)
    else:
        text = value.strip().lower()
        position = 0
        seconds = 0.0
        while position < len(text):
            match = _DURATION_PART.match(text, position)
            if match is None:
                raise ConfigError(f"invalid duration {value!r}")
            seconds += float(match.group(1)) * _UNITS[match.group(2)]
            position = match.end()
            while position < len(text) and text[position].isspace():
                position += 1
        if not text:
            raise ConfigError(f"invalid duration {value!r}")
    if seconds <= 0:
        raise ConfigError(f"duration {value!r} must be positive")
    return seconds


def _mapping(value: Any, where: str, allowed: set[str]) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigError(f"{where} must be a mapping")
    unknown = set(value) - allowed
    if unknown:
        raise ConfigError(f"{where}: unknown keys {sorted(map(str, unknown))}")
    return value


def _string(mapping: dict[str, Any], key: str, where: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{where}: {key} is required and must be a string")
    return value


def parse_config(document: Any, environ: dict[str, str] | None = None) -> Config:
    """Validates a parsed YAML document. Sizes are checked later, against the
    environment."""
    environ = dict(os.environ) if environ is None else environ
    root = _mapping(
        document, "configuration", {"connection", "defaults", "clusters", "dry_run"}
    )

    connection = _mapping(
        root.get("connection"),
        "connection",
        {"host", "port", "user", "password_file", "sslmode", "database"},
    )
    password_file = connection.get("password_file")
    if password_file is not None:
        try:
            password: str | None = Path(str(password_file)).read_text().strip()
        except OSError as e:
            raise ConfigError(f"connection: cannot read password_file: {e}") from e
    else:
        password = environ.get("MZ_PASSWORD")
    port = connection.get("port", 6875)
    if isinstance(port, bool) or not isinstance(port, int):
        raise ConfigError("connection: port must be an integer")

    defaults = _mapping(
        root.get("defaults"),
        "defaults",
        {"cooldown", "sample_interval", "replica_read_timeout"},
    )
    default_cooldown = parse_duration(defaults.get("cooldown", "1h"))

    entries = root.get("clusters")
    if not isinstance(entries, list) or not entries:
        raise ConfigError("clusters must be a non-empty list")
    clusters = []
    for index, raw in enumerate(entries):
        where = f"clusters[{index}]"
        entry = _mapping(
            raw, where, {"name", "min_size", "max_size", "cooldown", "sizes"}
        )
        sizes = entry.get("sizes")
        if sizes is not None and (
            not isinstance(sizes, list)
            or not sizes
            or not all(isinstance(s, str) for s in sizes)
        ):
            raise ConfigError(f"{where}: sizes must be a non-empty list of strings")
        clusters.append(
            ClusterPolicy(
                name=_string(entry, "name", where),
                min_size=_string(entry, "min_size", where),
                max_size=_string(entry, "max_size", where),
                cooldown_seconds=(
                    parse_duration(entry["cooldown"])
                    if "cooldown" in entry
                    else default_cooldown
                ),
                sizes=tuple(sizes) if sizes is not None else None,
            )
        )
    names = [c.name for c in clusters]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ConfigError(f"clusters listed more than once: {duplicates}")

    dry_run = root.get("dry_run", False)
    if not isinstance(dry_run, bool):
        raise ConfigError("dry_run must be a boolean")

    return Config(
        connection=Connection(
            host=_string(connection, "host", "connection"),
            port=port,
            user=_string(connection, "user", "connection"),
            password=password,
            sslmode=str(connection.get("sslmode", "require")),
            database=str(connection.get("database", "materialize")),
        ),
        clusters=tuple(clusters),
        sample_interval_seconds=parse_duration(defaults.get("sample_interval", "30s")),
        replica_read_timeout_seconds=parse_duration(
            defaults.get("replica_read_timeout", "10s")
        ),
        dry_run=dry_run,
    )


def load_config(path: Path) -> Config:
    try:
        document = yaml.safe_load(path.read_text())
    except (OSError, yaml.YAMLError) as e:
        raise ConfigError(f"cannot read {path}: {e}") from e
    return parse_config(document)
