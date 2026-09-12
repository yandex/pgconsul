"""Tests for ``failure_analyzer.scanner``."""

from __future__ import annotations

from typing import Iterator

from failure_analyzer.models import DockerFinding, Finding
from failure_analyzer.patterns import PGCONSUL_PATTERNS, Pattern, patterns_for
from failure_analyzer.scanner import Scanner
from failure_analyzer.sources.base import LogSource


class FakeSource(LogSource):
    """A LogSource backed by an in-memory list of lines."""

    def __init__(self, container: str, log_type: str, lines: list[str]) -> None:
        self._container = container
        self._log_type = log_type
        self._lines = lines

    @property
    def container(self) -> str:
        return self._container

    @property
    def log_type(self) -> str:
        return self._log_type

    def iter_lines(self, keep_debug: bool = False) -> Iterator[tuple[int, str]]:
        for i, line in enumerate(self._lines, 1):
            yield i, line

    def describe(self) -> str:
        return f"{self._container}/{self._log_type}"


def _find_pattern(name_substr: str) -> Pattern:
    for p in PGCONSUL_PATTERNS:
        if name_substr in p.name:
            return p
    raise AssertionError(f"no pattern matching {name_substr!r}")


def test_scan_returns_first_match_per_line() -> None:
    # A line that matches the high-priority WAL divergence pattern.
    line = "2024-01-02 03:04:05 requested starting point 0/1 is ahead of the WAL flush position"
    src = FakeSource("postgresql1", "pgconsul", [line])
    scanner = Scanner()
    findings = scanner.scan(src, PGCONSUL_PATTERNS)
    assert len(findings) == 1
    f = findings[0]
    assert f.container == "postgresql1"
    assert f.log_type == "pgconsul"
    assert "WAL divergence" in f.pattern_name
    assert f.line_no == 1
    assert f.timestamp == "2024-01-02 03:04:05"


def test_scan_no_matches_returns_empty() -> None:
    src = FakeSource("postgresql1", "pgconsul", ["nothing interesting here"])
    assert Scanner().scan(src, PGCONSUL_PATTERNS) == []


def test_scan_truncates_long_lines() -> None:
    long_line = "requested starting point 0/1 is ahead of the WAL flush position " + "x" * 500
    src = FakeSource("c", "pgconsul", [long_line])
    f = Scanner(line_truncate=50).scan(src, PGCONSUL_PATTERNS)[0]
    assert len(f.line) <= 50


def test_scan_many_skips_unknown_log_types() -> None:
    src = FakeSource("c", "unknown_type", ["whatever"])
    findings = Scanner().scan_many([src], patterns_for)
    assert findings == []


def test_scan_many_dispatches_by_log_type() -> None:
    pg_line = "requested starting point 0/1 is ahead of the WAL flush position"
    src = FakeSource("c", "pgconsul", [pg_line])
    findings = Scanner().scan_many([src], patterns_for)
    assert len(findings) == 1
    assert isinstance(findings[0], Finding)


def test_scan_docker_returns_docker_findings() -> None:
    line = "requested starting point 0/1 is ahead of the WAL flush position"
    src = FakeSource("pgconsul_postgresql1_1", "pgconsul", [line])
    findings = Scanner().scan_docker(src, PGCONSUL_PATTERNS)
    assert len(findings) == 1
    assert isinstance(findings[0], DockerFinding)
    assert findings[0].container == "pgconsul_postgresql1_1"
