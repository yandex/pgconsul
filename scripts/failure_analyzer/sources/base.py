"""The :class:`LogSource` abstraction.

A log source yields a stream of ``(line_no, line)`` pairs and knows its
container name and log type. The scanner depends only on this interface, so
files-on-disk and live-docker-containers are interchangeable.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterator


class LogSource(ABC):
    """A source of log lines to scan."""

    @property
    @abstractmethod
    def container(self) -> str:
        """The container this log belongs to (for reporting)."""

    @property
    @abstractmethod
    def log_type(self) -> str:
        """The log type: ``pgconsul``, ``postgresql``, ``zookeeper``, ..."""

    @abstractmethod
    def iter_lines(self, keep_debug: bool = False) -> Iterator[tuple[int, str]]:
        """Yield ``(line_no, line)`` pairs.

        When *keep_debug* is False, DEBUG lines may be filtered out (for large
        files this is done via a grep pre-filter for speed). When True, all
        lines are yielded — used for stuck-pattern detection on pgconsul logs.
        """

    def describe(self) -> str:
        """A short human-readable description for diagnostics/logging."""
        return f"{self.container}/{self.log_type}"
