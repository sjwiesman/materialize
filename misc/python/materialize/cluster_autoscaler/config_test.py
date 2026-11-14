# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from materialize.cluster_autoscaler.config import (
    ConfigError,
    load_config,
    parse_config,
    parse_duration,
)


def document(**overrides: Any) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "connection": {"host": "mz.example.com", "user": "autoscaler"},
        "clusters": [{"name": "analytics", "min_size": "100cc", "max_size": "800cc"}],
    }
    doc.update(overrides)
    return doc


def test_defaults() -> None:
    config = parse_config(document(), environ={})
    assert config.connection.port == 6875
    assert config.connection.sslmode == "require"
    assert config.connection.database == "materialize"
    assert config.connection.password is None
    assert config.sample_interval_seconds == 30
    assert config.replica_read_timeout_seconds == 10
    assert config.clusters[0].cooldown_seconds == 3600
    assert config.clusters[0].sizes is None
    assert not config.dry_run


def test_design_example(tmp_path: Path) -> None:
    password = tmp_path / "password"
    password.write_text("hunter2\n")
    path = tmp_path / "autoscaler.yml"
    path.write_text(f"""
connection:
  host: example.materialize.cloud
  port: 6875
  user: autoscaler
  password_file: {password}
  sslmode: require
  database: materialize

defaults:
  cooldown: 1h
  sample_interval: 30s
  replica_read_timeout: 10s

clusters:
  - name: analytics
    min_size: 100cc
    max_size: 800cc
    cooldown: 2h
  - name: serving
    min_size: 200cc
    max_size: 400cc

dry_run: true
""")
    config = load_config(path)
    assert config.connection.password == "hunter2"
    assert [(c.name, c.cooldown_seconds) for c in config.clusters] == [
        ("analytics", 7200),
        ("serving", 3600),
    ]
    assert config.dry_run


def test_password_from_environment() -> None:
    config = parse_config(document(), environ={"MZ_PASSWORD": "s3cret"})
    assert config.connection.password == "s3cret"


def test_password_file_wins_over_environment(tmp_path: Path) -> None:
    password = tmp_path / "password"
    password.write_text("from-file")
    doc = document()
    doc["connection"]["password_file"] = str(password)
    config = parse_config(doc, environ={"MZ_PASSWORD": "from-env"})
    assert config.connection.password == "from-file"


@pytest.mark.parametrize(
    "name,mutate,message",
    [
        (
            "password in the file",
            lambda d: d["connection"].update(password="x"),
            "unknown keys",
        ),
        (
            "unreadable password file",
            lambda d: d["connection"].update(password_file="/nonexistent/pw"),
            "password_file",
        ),
        ("missing host", lambda d: d["connection"].pop("host"), "host is required"),
        ("bad port", lambda d: d["connection"].update(port="x"), "port"),
        ("unknown top-level key", lambda d: d.update(cluster=[]), "unknown keys"),
        ("no clusters", lambda d: d.update(clusters=[]), "non-empty list"),
        (
            "missing max_size",
            lambda d: d["clusters"][0].pop("max_size"),
            "max_size is required",
        ),
        (
            "model parameter per cluster",
            lambda d: d["clusters"][0].update(u_target=0.5),
            "unknown keys",
        ),
        (
            "duplicate cluster",
            lambda d: d["clusters"].append(dict(d["clusters"][0])),
            "more than once",
        ),
        (
            "bad cooldown",
            lambda d: d["clusters"][0].update(cooldown="soon"),
            "invalid duration",
        ),
        (
            "bad default",
            lambda d: d.update(defaults={"sample_interval": "0s"}),
            "must be positive",
        ),
        ("bad sizes", lambda d: d["clusters"][0].update(sizes="100cc"), "sizes"),
        ("bad dry_run", lambda d: d.update(dry_run="yes"), "dry_run"),
    ],
)
def test_rejects(name: str, mutate: Any, message: str) -> None:
    doc = document()
    mutate(doc)
    with pytest.raises(ConfigError, match=message):
        parse_config(doc, environ={})


@pytest.mark.parametrize(
    "text,seconds",
    [
        ("30s", 30),
        ("15min", 900),
        ("1h", 3600),
        ("1h30m", 5400),
        ("1h 30m", 5400),
        ("2d", 172800),
        ("500ms", 0.5),
        ("1.5h", 5400),
        (45, 45),
    ],
)
def test_parse_duration(text: Any, seconds: float) -> None:
    assert parse_duration(text) == seconds


@pytest.mark.parametrize("text", ["", "h", "10", "10x", "-5s", "0s", True, None])
def test_parse_duration_rejects(text: Any) -> None:
    with pytest.raises(ConfigError):
        parse_duration(text)
