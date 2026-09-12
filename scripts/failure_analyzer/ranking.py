"""Ranking and deduplication of findings.

The original script built a ``weight_map`` keyed by pattern *name* (a string)
in three different places. Now that :class:`~.patterns.Pattern` carries its
own weight, ranking is a simple sort with no string lookups.
"""

from __future__ import annotations

from typing import Iterable, Sequence, TypeVar

from .models import DockerFinding, Finding
from .utils import deduplicate_by

_T = TypeVar("_T")


def rank_findings(findings: Iterable[Finding]) -> list[Finding]:
    """Sort findings by pattern weight (desc), then by timestamp."""
    return sorted(findings, key=lambda f: (-f.weight, f.timestamp))


def deduplicate_findings(findings: Iterable[Finding]) -> list[Finding]:
    """Keep only the first occurrence of each (container, pattern) pair."""
    return deduplicate_by(findings, key=lambda f: (f.container, f.pattern_name))


def rank_docker_findings(findings: Iterable[DockerFinding]) -> list[DockerFinding]:
    """Sort docker findings by pattern weight (desc)."""
    return sorted(findings, key=lambda f: -f.weight)


def deduplicate_docker_findings(
    findings: Iterable[DockerFinding],
) -> list[DockerFinding]:
    """Keep only the first occurrence of each (container, pattern) pair."""
    return deduplicate_by(findings, key=lambda f: (f.container, f.pattern_name))


def limit(items: Sequence[_T], limit_count: int | None) -> list[_T]:
    """Return at most *limit_count* items (or all if *limit_count* is None)."""
    if limit_count is None:
        return list(items)
    return list(items[:limit_count])
