"""The :class:`Analyzer` orchestrates the analysis pipeline.

It wires together discovery, parsing, scanning, stuck detection and ranking
to build a pure :class:`~.models.AnalysisResult`. All collaborators are
injected, so the analyzer has no hidden dependencies and is unit-testable.

The analyzer deliberately does **not** do any presentation (no ``print``) and
does **not** touch docker directly — docker scanning is opt-in and driven by
the CLI, which calls :meth:`Analyzer.scan_docker` separately.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from .config import Config
from .discovery import (
    find_container_logs,
    find_feature_and_line_from_logs,
    find_last_failed_step_log,
    find_log_roots,
    find_running_step_log,
)
from .models import AnalysisResult, ContainerLog, FailedStep, RunningStep
from .parsing import parse_failed_steps, parse_running_step
from .patterns import patterns_for
from .ranking import deduplicate_findings, rank_findings
from .scanner import Scanner
from .sources.base import LogSource
from .sources.file_source import FileLogSource
from .sources.readers import GrepReader, LineReader, PurePythonReader
from .stuck import detect_stuck_patterns, detect_switchover_phases

log = logging.getLogger(__name__)


class Analyzer:
    """Coordinates the analysis pipeline and produces an :class:`AnalysisResult`."""

    def __init__(
        self,
        config: Config,
        scanner: Optional[Scanner] = None,
        reader: Optional[LineReader] = None,
    ) -> None:
        self._config = config
        self._scanner = scanner or Scanner()
        self._reader = reader or self._default_reader(config)

    @staticmethod
    def _default_reader(config: Config) -> LineReader:
        if config.use_grep:
            return GrepReader(config.max_full_read_size)
        return PurePythonReader(config.max_full_read_size)

    def analyze(self, log_dirs: list[str]) -> AnalysisResult:
        """Run the full pipeline over *log_dirs* and return the result."""
        roots = find_log_roots(log_dirs, self._config)

        failed_step, log_file = find_last_failed_step_log(roots, parse_failed_steps)
        running_step: Optional[RunningStep] = None
        if not failed_step:
            rs, log_file = find_running_step_log(roots, parse_running_step)
            if rs is not None:
                running_step = rs

        feature_name, line_no = find_feature_and_line_from_logs(roots)
        if failed_step:
            failed_step.feature_file = feature_name
            failed_step.line_number = line_no
        if running_step:
            running_step.feature_file = feature_name
            running_step.line_number = line_no

        container_logs: list[ContainerLog] = []
        if feature_name and line_no:
            container_logs = find_container_logs(roots, feature_name, line_no)

        sources = [self._to_source(cl) for cl in container_logs]
        findings = self._scanner.scan_many(sources, patterns_for, keep_debug=False)

        stuck = detect_stuck_patterns(container_logs, self._config)
        switchover_phases = detect_switchover_phases(container_logs, self._config)

        ranked = rank_findings(findings)
        deduped = deduplicate_findings(ranked)

        return AnalysisResult(
            failed_step=failed_step,
            running_step=running_step,
            log_file=log_file,
            container_logs=container_logs,
            findings=deduped,
            stuck_indicators=stuck,
            switchover_phases=switchover_phases,
        )

    def _to_source(self, cl: ContainerLog) -> FileLogSource:
        return FileLogSource(cl.container, cl.log_type, cl.path, self._reader)
