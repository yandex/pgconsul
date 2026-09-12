"""Data models for the failure analyzer.

All dataclasses are plain data holders — no I/O, no side effects. This keeps
them trivially testable and lets the rest of the package depend on stable
shapes rather than on parsing/scanning internals.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .patterns import Pattern


@dataclass
class FailedStep:
    """A single failed step extracted from ``test_execution.log``."""

    timestamp: str
    step_text: str
    duration: float
    feature_file: str = ""
    line_number: int = 0


@dataclass
class RunningStep:
    """The most recent in-progress step (test still running)."""

    timestamp: str
    step_text: str
    feature_file: str = ""
    line_number: int = 0
    elapsed_seconds: float = 0.0


@dataclass
class ContainerLog:
    """A container log file found under the failed-step directory."""

    container: str
    log_type: str  # pgconsul, postgresql, pgbouncer, rsync, zookeeper
    path: Path


@dataclass
class Finding:
    """A notable pattern found in a container log.

    ``pattern`` holds a reference to the matched :class:`~.patterns.Pattern`
    so callers can read its weight/name without a fragile string lookup.
    """

    container: str
    log_type: str
    pattern: Pattern
    line: str
    line_no: int
    timestamp: str = ""

    @property
    def pattern_name(self) -> str:
        return self.pattern.name

    @property
    def weight(self) -> int:
        return self.pattern.weight


@dataclass
class DockerFinding:
    """A notable line found in a live docker container log."""

    container: str
    log_path: str
    pattern: Pattern
    line: str

    @property
    def pattern_name(self) -> str:
        return self.pattern.name

    @property
    def weight(self) -> int:
        return self.pattern.weight


@dataclass
class SwitchoverPhaseEvent:
    """A single switchover phase transition logged by pgconsul.

    Lines like ``SWITCHOVER PHASE → sync_set`` mark each phase transition.
    Collecting them per container gives a timeline that immediately reveals
    where the switchover process stalled.
    """

    container: str
    timestamp: str
    phase: str
    duration_seconds: float = 0.0  # time spent in this phase (filled by reporter)


@dataclass
class AnalysisResult:
    """Aggregated output of the analysis pipeline."""

    failed_step: Optional[FailedStep] = None
    running_step: Optional[RunningStep] = None
    log_file: Optional[Path] = None
    container_logs: list[ContainerLog] = field(default_factory=list)
    findings: list[Finding] = field(default_factory=list)
    docker_findings: list[DockerFinding] = field(default_factory=list)
    stuck_indicators: list[str] = field(default_factory=list)
    switchover_phases: list[SwitchoverPhaseEvent] = field(default_factory=list)
