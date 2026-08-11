"""Reporter interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TextIO

from ..models import AnalysisResult


class Reporter(ABC):
    """Render an :class:`AnalysisResult` to a stream."""

    @abstractmethod
    def render(self, result: AnalysisResult, stream: TextIO) -> None:
        """Write the report for *result* to *stream*."""
