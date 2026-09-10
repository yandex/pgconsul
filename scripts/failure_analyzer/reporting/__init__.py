"""Reporters — render an :class:`~..models.AnalysisResult` to output.

Reporters are the presentation layer. The analyzer builds a pure
``AnalysisResult``; a reporter turns it into text or JSON. This separation
makes the output format swappable (e.g. for CI) and lets reporters be tested
in isolation.
"""

from __future__ import annotations

from .base import Reporter
from .json_reporter import JsonReporter
from .text_reporter import TextReporter

__all__ = ["Reporter", "TextReporter", "JsonReporter"]
