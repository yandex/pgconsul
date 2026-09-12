"""File-on-disk log source."""

from __future__ import annotations

from pathlib import Path
from typing import Iterator

from .base import LogSource
from .readers import LineReader


class FileLogSource(LogSource):
    """A log file on disk, read through a :class:`LineReader`."""

    def __init__(
        self,
        container: str,
        log_type: str,
        path: Path,
        reader: LineReader,
    ) -> None:
        self._container = container
        self._log_type = log_type
        self._path = path
        self._reader = reader

    @property
    def container(self) -> str:
        return self._container

    @property
    def log_type(self) -> str:
        return self._log_type

    @property
    def path(self) -> Path:
        return self._path

    def iter_lines(self, keep_debug: bool = False) -> Iterator[tuple[int, str]]:
        yield from self._reader.iter_lines(self._path, self._log_type, keep_debug)

    def describe(self) -> str:
        return f"{self._container}/{self._log_type}:{self._path}"
