"""Tests for ``failure_analyzer.parsing``."""

from __future__ import annotations

from pathlib import Path

from failure_analyzer.parsing import parse_failed_steps, parse_running_step


_FINISHED = (
    "2024-01-02 03:04:05 [INFO] Starting step: Given the cluster is up\n"
    "2024-01-02 03:04:06 [INFO] Finished step: Given the cluster is up "
    "(status=Status.passed, duration=1.0s)\n"
    "2024-01-02 03:04:10 [INFO] Starting step: When I kill the primary\n"
    "2024-01-02 03:04:40 [INFO] Finished step: When I kill the primary "
    "(status=Status.failed, duration=30.5s)\n"
)

_RUNNING = (
    "2024-01-02 03:04:05 [INFO] Starting step: Given the cluster is up\n"
    "2024-01-02 03:04:06 [INFO] Finished step: Given the cluster is up "
    "(status=Status.passed, duration=1.0s)\n"
    "2024-01-02 03:04:10 [INFO] Starting step: When I kill the primary\n"
)


def _write(tmp_path: Path, text: str) -> Path:
    p = tmp_path / "test_execution.log"
    p.write_text(text, encoding="utf-8")
    return p


def test_parse_failed_steps_returns_only_failed(tmp_path: Path) -> None:
    log = _write(tmp_path, _FINISHED)
    steps = parse_failed_steps(log)
    assert len(steps) == 1
    s = steps[0]
    assert s.step_text == "When I kill the primary"
    assert s.timestamp == "2024-01-02 03:04:40"
    assert s.duration == 30.5


def test_parse_failed_steps_empty_when_all_pass(tmp_path: Path) -> None:
    log = _write(tmp_path, _FINISHED.replace("Status.failed", "Status.passed"))
    assert parse_failed_steps(log) == []


def test_parse_running_step_detects_unfinished(tmp_path: Path) -> None:
    log = _write(tmp_path, _RUNNING)
    rs = parse_running_step(log)
    assert rs is not None
    assert rs.step_text == "When I kill the primary"
    assert rs.timestamp == "2024-01-02 03:04:10"


def test_parse_running_step_none_when_all_finished(tmp_path: Path) -> None:
    log = _write(tmp_path, _FINISHED)
    assert parse_running_step(log) is None


def test_parse_failed_steps_missing_file(tmp_path: Path) -> None:
    # Non-existent path returns [] (logged, not raised).
    assert parse_failed_steps(tmp_path / "nope.log") == []
