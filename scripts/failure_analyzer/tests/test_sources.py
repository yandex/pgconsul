"""Tests for ``failure_analyzer.sources`` (file and docker sources)."""

from __future__ import annotations

from pathlib import Path

from failure_analyzer.sources.docker_source import DockerLogSource
from failure_analyzer.sources.file_source import FileLogSource
from failure_analyzer.sources.readers import PurePythonReader


def test_file_log_source_reads_all_lines(tmp_path: Path) -> None:
    p = tmp_path / "pgconsul.log"
    p.write_text("line one\nline two\nline three\n", encoding="utf-8")
    src = FileLogSource("postgresql1", "pgconsul", p, PurePythonReader(max_full_read_size=10))
    # keep_debug=True so lines without INFO/WARNING/ERROR are still yielded.
    lines = list(src.iter_lines(keep_debug=True))
    assert lines == [(1, "line one"), (2, "line two"), (3, "line three")]
    assert src.container == "postgresql1"
    assert src.log_type == "pgconsul"


def test_file_log_source_filters_debug_for_pgconsul(tmp_path: Path) -> None:
    p = tmp_path / "pgconsul.log"
    p.write_text("DEBUG stuff\nINFO useful\nERROR bad\n", encoding="utf-8")
    src = FileLogSource("c", "pgconsul", p, PurePythonReader(max_full_read_size=10))
    lines = [line for _, line in src.iter_lines(keep_debug=False)]
    assert lines == ["INFO useful", "ERROR bad"]


def test_file_log_source_keep_debug_returns_all(tmp_path: Path) -> None:
    p = tmp_path / "pgconsul.log"
    p.write_text("DEBUG stuff\nINFO useful\n", encoding="utf-8")
    src = FileLogSource("c", "pgconsul", p, PurePythonReader(max_full_read_size=10))
    lines = [line for _, line in src.iter_lines(keep_debug=True)]
    assert lines == ["DEBUG stuff", "INFO useful"]


def test_file_log_source_missing_file(tmp_path: Path) -> None:
    src = FileLogSource("c", "pgconsul", tmp_path / "nope.log", PurePythonReader(max_full_read_size=10))
    assert list(src.iter_lines()) == []


class _FakeRunner:
    def __init__(self, lines: list[str]) -> None:
        self._lines = lines

    def read_file(self, container: str, path: str, tail: int) -> list[str]:
        return self._lines


def test_docker_log_source_yields_lines() -> None:
    runner = _FakeRunner(["INFO a", "DEBUG b", "ERROR c"])
    src = DockerLogSource("pgconsul_postgresql1_1", "pgconsul", "/var/log/pgconsul/pgconsul.log", runner, tail_lines=5000)
    lines = list(src.iter_lines(keep_debug=False))
    # DEBUG line is skipped; line numbers are kept from the original stream.
    assert lines == [(1, "INFO a"), (3, "ERROR c")]
    assert src.container == "pgconsul_postgresql1_1"
    assert src.log_path == "/var/log/pgconsul/pgconsul.log"


def test_docker_log_source_keep_debug() -> None:
    runner = _FakeRunner(["INFO a", "DEBUG b"])
    src = DockerLogSource("c", "pgconsul", "p", runner, tail_lines=10)
    lines = [line for _, line in src.iter_lines(keep_debug=True)]
    assert lines == ["INFO a", "DEBUG b"]


def test_docker_log_source_empty_runner() -> None:
    runner = _FakeRunner([])
    src = DockerLogSource("c", "pgconsul", "p", runner, tail_lines=10)
    assert list(src.iter_lines()) == []
