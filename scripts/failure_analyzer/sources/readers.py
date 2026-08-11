"""Line readers for large log files.

Reading a 180 MB ``postgresql.log`` line-by-line in Python is slow. The
original script shelled out to ``grep -n`` to pre-filter DEBUG lines. That
logic is encapsulated here behind a single :class:`LineReader` interface with
two implementations:

* :class:`GrepReader` — fast, uses the external ``grep`` binary (unix).
* :class:`PurePythonReader` — portable fallback, pure Python.

The choice is made by :class:`FileLogSource` based on :class:`~..config.Config`.
"""

from __future__ import annotations

import logging
import re
import subprocess
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator

log = logging.getLogger(__name__)

# Log-level filters used when DEBUG is dropped.
PG_KEEP_LEVELS_RE = re.compile(
    r"\b(LOG:|FATAL|ERROR|WARNING|NOTICE|PANIC|HINT|STATEMENT)\b",
    re.IGNORECASE,
)
PGCONSUL_NON_DEBUG_RE = re.compile(r"\b(INFO|WARNING|ERROR)\b", re.IGNORECASE)


def _filter_pattern_for(log_type: str) -> str | None:
    """Return the grep filter pattern for *log_type* when DEBUG is dropped."""
    if log_type == "postgresql":
        return r"(LOG:|FATAL|ERROR|WARNING|NOTICE|PANIC|HINT|STATEMENT)"
    if log_type == "pgconsul":
        return r"\b(INFO|WARNING|ERROR)\b"
    return None


class LineReader(ABC):
    """Yield ``(line_no, line)`` pairs from a log file."""

    @abstractmethod
    def iter_lines(
        self,
        path: Path,
        log_type: str,
        keep_debug: bool = False,
    ) -> Iterator[tuple[int, str]]:
        ...


class GrepReader(LineReader):
    """Read large files via ``grep -n`` pre-filtering.

    For files above the configured threshold, ``grep -n -E <pattern>`` extracts
    only the relevant lines (with line numbers) in one pass. For small files,
    the file is read fully in Python — grep is only worth the fork for big logs.
    """

    def __init__(self, max_full_read_size: int) -> None:
        self._max_full_read_size = max_full_read_size

    def iter_lines(
        self,
        path: Path,
        log_type: str,
        keep_debug: bool = False,
    ) -> Iterator[tuple[int, str]]:
        try:
            file_size = path.stat().st_size
        except OSError as exc:
            log.warning("cannot stat %s: %s", path, exc)
            return

        if file_size <= self._max_full_read_size:
            yield from _read_full(path)
            return

        if keep_debug:
            # No filtering requested, but the file is large. grep with a pattern
            # that matches every non-empty line keeps it fast.
            grep_pattern = r"."
        else:
            grep_pattern = _filter_pattern_for(log_type) or r"."

        yield from _grep_lines(path, grep_pattern)


class PurePythonReader(LineReader):
    """Portable reader: read the file in Python, optionally filtering DEBUG."""

    def __init__(self, max_full_read_size: int) -> None:
        # Threshold kept for interface symmetry; pure-Python always streams.
        self._max_full_read_size = max_full_read_size

    def iter_lines(
        self,
        path: Path,
        log_type: str,
        keep_debug: bool = False,
    ) -> Iterator[tuple[int, str]]:
        try:
            file_size = path.stat().st_size
        except OSError as exc:
            log.warning("cannot stat %s: %s", path, exc)
            return

        filter_re = None if keep_debug else _python_filter_re(log_type)

        try:
            with path.open("r", encoding="utf-8", errors="replace") as fh:
                for i, line in enumerate(fh, 1):
                    if filter_re is not None and not filter_re.search(line):
                        continue
                    yield i, line.rstrip("\n")
        except OSError as exc:
            log.warning("cannot read %s: %s", path, exc)


def _python_filter_re(log_type: str) -> re.Pattern[str] | None:
    if log_type == "postgresql":
        return PG_KEEP_LEVELS_RE
    if log_type == "pgconsul":
        return PGCONSUL_NON_DEBUG_RE
    return None


def _read_full(path: Path) -> Iterator[tuple[int, str]]:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        log.warning("cannot read %s: %s", path, exc)
        return
    for i, line in enumerate(text.splitlines(), 1):
        yield i, line


def _grep_lines(path: Path, pattern: str) -> Iterator[tuple[int, str]]:
    """Yield ``(line_no, content)`` from ``grep -n -E <pattern> <path>``."""
    try:
        proc = subprocess.Popen(
            ["grep", "-n", "-E", pattern, str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        # grep not available (e.g. non-unix) — fall back to a full read.
        log.warning("grep not found; falling back to full read of %s", path)
        yield from _read_full(path)
        return
    except OSError as exc:
        log.warning("cannot spawn grep for %s: %s", path, exc)
        return

    assert proc.stdout is not None
    for raw_line in proc.stdout:
        line = raw_line.decode("utf-8", errors="replace").rstrip("\n")
        # grep -n format: "123:content" — split on first colon.
        colon_idx = line.find(":")
        if colon_idx > 0:
            try:
                line_no = int(line[:colon_idx])
                content = line[colon_idx + 1:]
                yield line_no, content
            except ValueError:
                yield 0, line
        else:
            yield 0, line
    proc.wait()
