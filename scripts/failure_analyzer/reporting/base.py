"""Reporter interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TextIO

from ..models import AnalysisResult, DockerFinding


class Reporter(ABC):
    """Render an :class:`AnalysisResult` to a stream."""

    @abstractmethod
    def render(self, result: AnalysisResult, stream: TextIO) -> None:
        """Write the report for *result* to *stream*."""

    @abstractmethod
    def render_docker(
        self,
        findings: list[DockerFinding],
        containers: list[str],
        stream: TextIO,
        pg_containers: list[str],
    ) -> None:
        """Write the docker container log scan section to *stream*.

        *findings* are the raw matches from live docker containers.
        *containers* is the list of scanned container names.
        *pg_containers* is the subset of postgresql containers (used by
        text reporters to emit "dig deeper" commands).
        """
