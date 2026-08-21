"""Log sources: abstractions over where log lines come from.

The original script had two near-identical scanning loops — one over files on
disk and one over live docker containers. This package replaces that with a
single :class:`~.base.LogSource` interface; the scanner depends only on the
abstraction, so file/docker are just two interchangeable implementations.
"""

from __future__ import annotations

from .base import LogSource
from .docker_source import DockerLogSource
from .file_source import FileLogSource
from .readers import GrepReader, LineReader, PurePythonReader

__all__ = [
    "LogSource",
    "FileLogSource",
    "DockerLogSource",
    "LineReader",
    "GrepReader",
    "PurePythonReader",
]
