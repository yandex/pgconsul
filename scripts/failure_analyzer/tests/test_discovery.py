"""Tests for ``failure_analyzer.discovery``."""

from __future__ import annotations

import os
from pathlib import Path

from failure_analyzer.config import Config
from failure_analyzer.discovery import (
    find_container_logs,
    find_feature_and_line_from_logs,
    find_test_execution_logs,
)


def test_find_test_execution_logs_direct_and_nested(tmp_path: Path) -> None:
    # <root>/debug/test_execution.log
    (tmp_path / "debug").mkdir()
    (tmp_path / "debug" / "test_execution.log").write_text("x")
    # <root>/<subdir>/debug/test_execution.log
    sub = tmp_path / "logs-failover"
    (sub / "debug").mkdir(parents=True)
    (sub / "debug" / "test_execution.log").write_text("x")

    found = find_test_execution_logs([tmp_path])
    assert len(found) == 2
    assert all(p.name == "test_execution.log" for p in found)


def test_find_feature_and_line_from_logs_picks_latest(tmp_path: Path) -> None:
    features = tmp_path / "tests" / "features"
    feat = features / "failover.feature"
    line1 = feat / "10"
    line2 = feat / "20"
    line1.mkdir(parents=True)
    line2.mkdir(parents=True)
    # Make line2 newer than line1.
    os.utime(line1, (1, 1))
    os.utime(line2, (2, 2))

    feature, line = find_feature_and_line_from_logs([tmp_path])
    assert feature == "failover.feature"
    assert line == 20


def test_find_container_logs_walks_feature_line_dirs(tmp_path: Path) -> None:
    container_dir = tmp_path / "tests" / "features" / "failover.feature" / "10" / "postgresql1"
    container_dir.mkdir(parents=True)
    (container_dir / "pgconsul.log").write_text("INFO x")
    (container_dir / "postgresql.log").write_text("LOG x")
    # zookeeper log name normalization
    (container_dir / "zookeeper--server-zk1.log").write_text("x")

    logs = find_container_logs([tmp_path], "failover.feature", 10)
    types = sorted(cl.log_type for cl in logs)
    assert types == ["pgconsul", "postgresql", "zookeeper"]
    assert all(cl.container == "postgresql1" for cl in logs)


def test_find_container_logs_empty_when_no_match(tmp_path: Path) -> None:
    assert find_container_logs([tmp_path], "nope", 1) == []
