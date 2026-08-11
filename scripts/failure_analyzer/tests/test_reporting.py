"""Tests for ``failure_analyzer.reporting`` (text and JSON reporters)."""

from __future__ import annotations

import io
import json
import re

from failure_analyzer.models import AnalysisResult, ContainerLog, FailedStep, Finding
from failure_analyzer.patterns import PGCONSUL_PATTERNS
from failure_analyzer.reporting import JsonReporter, TextReporter


def _pattern(name_substr: str):
    for p in PGCONSUL_PATTERNS:
        if name_substr in p.name:
            return p
    raise AssertionError(name_substr)


def _result_with_findings() -> AnalysisResult:
    p = _pattern("WAL divergence")
    f = Finding(
        container="postgresql1",
        log_type="pgconsul",
        pattern=p,
        line="requested starting point ahead",
        line_no=42,
        timestamp="2024-01-02 03:04:05",
    )
    return AnalysisResult(
        failed_step=FailedStep(
            timestamp="2024-01-02 03:04:40",
            step_text="When I kill the primary",
            duration=30.5,
            feature_file="failover.feature",
            line_number=10,
        ),
        findings=[f],
    )


def test_text_reporter_contains_key_sections() -> None:
    result = _result_with_findings()
    buf = io.StringIO()
    TextReporter().render(result, buf)
    out = buf.getvalue()
    assert "BEHAVE TEST FAILURE ANALYSIS" in out
    assert "FAILED STEP" in out
    assert "When I kill the primary" in out
    assert "TOP FINDINGS" in out
    assert "LIKELY ROOT CAUSE" in out
    assert "WAL divergence" in out


def test_text_reporter_no_failed_step_message() -> None:
    result = AnalysisResult()
    buf = io.StringIO()
    TextReporter().render(result, buf)
    assert "No failed step found" in buf.getvalue()


def test_json_reporter_emits_valid_json() -> None:
    result = _result_with_findings()
    buf = io.StringIO()
    JsonReporter().render(result, buf)
    payload = json.loads(buf.getvalue())
    assert payload["failed_step"]["step"] == "When I kill the primary"
    assert payload["findings"][0]["pattern"] == result.findings[0].pattern_name
    assert payload["findings"][0]["weight"] == result.findings[0].weight
    assert payload["likely_root_cause"]["pattern"] == result.findings[0].pattern_name


def test_json_reporter_root_cause_stuck_when_no_findings() -> None:
    result = AnalysisResult(stuck_indicators=["  [c/pgconsul] loop: 6 occurrences"])
    buf = io.StringIO()
    JsonReporter().render(result, buf)
    payload = json.loads(buf.getvalue())
    assert payload["likely_root_cause"]["pattern"] == "stuck-in-loop"
