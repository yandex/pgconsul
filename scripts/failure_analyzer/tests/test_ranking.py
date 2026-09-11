"""Tests for ``failure_analyzer.ranking`` and ``failure_analyzer.utils``."""

from __future__ import annotations

from failure_analyzer.models import DockerFinding, Finding
from failure_analyzer.patterns import Pattern
from failure_analyzer.ranking import (
    deduplicate_docker_findings,
    deduplicate_findings,
    limit,
    rank_docker_findings,
    rank_findings,
)
from failure_analyzer.utils import (
    deduplicate_by,
    extract_timestamp,
    human_size,
    normalize_container_name,
)


def _pattern(name: str, weight: int) -> Pattern:
    import re

    return Pattern(regex="x", name=name, weight=weight, compiled=re.compile("x"))


def _finding(container: str, pattern: Pattern, ts: str = "") -> Finding:
    return Finding(
        container=container, log_type="pgconsul", pattern=pattern, line="l", line_no=1, timestamp=ts
    )


def test_rank_findings_sorts_by_weight_desc_then_timestamp() -> None:
    low = _pattern("low", 10)
    high = _pattern("high", 100)
    findings = [
        _finding("c", low, "2024-01-02 03:04:05"),
        _finding("c", high, "2024-01-02 03:04:06"),
        _finding("c", high, "2024-01-02 03:04:04"),
    ]
    ranked = rank_findings(findings)
    assert [f.pattern_name for f in ranked] == ["high", "high", "low"]
    # Among equal weights, earlier timestamp first.
    assert ranked[0].timestamp == "2024-01-02 03:04:04"


def test_deduplicate_findings_keeps_first_per_container_pattern() -> None:
    p = _pattern("p", 50)
    findings = [_finding("c1", p), _finding("c1", p), _finding("c2", p)]
    deduped = deduplicate_findings(findings)
    assert len(deduped) == 2
    assert {f.container for f in deduped} == {"c1", "c2"}


def test_rank_and_dedup_docker_findings() -> None:
    low = _pattern("low", 10)
    high = _pattern("high", 100)
    a = DockerFinding(container="c", log_path="p", pattern=high, line="l")
    b = DockerFinding(container="c", log_path="p", pattern=high, line="l2")
    c = DockerFinding(container="c", log_path="p", pattern=low, line="l")
    ranked = rank_docker_findings([low_f := c, a, b])
    assert ranked[0].pattern_name == "high"
    deduped = deduplicate_docker_findings([a, b, c])
    assert len(deduped) == 2


def test_limit_returns_all_when_none() -> None:
    assert limit([1, 2, 3], None) == [1, 2, 3]
    assert limit([1, 2, 3], 2) == [1, 2]


def test_extract_timestamp() -> None:
    assert extract_timestamp("2024-01-02 03:04:05 something") == "2024-01-02 03:04:05"
    assert extract_timestamp("2024-01-02T03:04:05 x") == "2024-01-02T03:04:05"
    assert extract_timestamp("no ts here") == ""


def test_normalize_container_name() -> None:
    assert normalize_container_name("pgconsul_postgresql1_1", "pgconsul") == "postgresql1"
    assert normalize_container_name("postgresql1", "pgconsul") == "postgresql1"


def test_deduplicate_by_preserves_order() -> None:
    assert deduplicate_by([1, 2, 1, 3, 2], key=lambda x: x) == [1, 2, 3]


def test_human_size() -> None:
    assert human_size(512) == "512B"
    assert human_size(2048) == "2KB"
    assert human_size(5 * 1024 * 1024) == "5.0MB"
