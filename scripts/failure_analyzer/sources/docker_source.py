"""Live docker container log source.

Reads the last N lines of a log file inside a running container via
``docker exec ... tail``. The actual subprocess interaction is isolated in a
:class:`DockerRunner` so it can be faked in tests without docker.
"""

from __future__ import annotations

import logging
import re
import subprocess
from typing import Iterator, Protocol

from .base import LogSource

log = logging.getLogger(__name__)

_DEBUG_RE = re.compile(r"\bDEBUG\b", re.IGNORECASE)


class DockerRunner(Protocol):
    """Abstraction over the docker subprocess calls used by this source."""

    def read_file(self, container: str, path: str, tail: int) -> list[str]:
        """Return the last *tail* lines of *path* from *container*."""
        ...


class SubprocessDockerRunner:
    """Real docker runner using ``docker exec ... tail``."""

    def __init__(self, timeout: int = 30) -> None:
        self._timeout = timeout

    def read_file(self, container: str, path: str, tail: int) -> list[str]:
        try:
            r = subprocess.run(
                ["docker", "exec", container, "tail", f"-{tail}", path],
                capture_output=True,
                text=True,
                timeout=self._timeout,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            log.warning("docker exec tail failed for %s:%s: %s", container, path, exc)
            return []
        if r.returncode != 0:
            log.warning(
                "docker exec tail %s:%s exited %d", container, path, r.returncode
            )
            return []
        return r.stdout.splitlines()


class DockerLogSource(LogSource):
    """A log file inside a live docker container."""

    def __init__(
        self,
        container: str,
        log_type: str,
        log_path: str,
        runner: DockerRunner,
        tail_lines: int,
    ) -> None:
        self._container = container
        self._log_type = log_type
        self._log_path = log_path
        self._runner = runner
        self._tail_lines = tail_lines

    @property
    def container(self) -> str:
        return self._container

    @property
    def log_type(self) -> str:
        return self._log_type

    @property
    def log_path(self) -> str:
        return self._log_path

    def iter_lines(self, keep_debug: bool = False) -> Iterator[tuple[int, str]]:
        lines = self._runner.read_file(self._container, self._log_path, self._tail_lines)
        for i, line in enumerate(lines, 1):
            if not keep_debug and _DEBUG_RE.search(line):
                continue
            yield i, line

    def describe(self) -> str:
        return f"{self._container}:{self._log_path}"
