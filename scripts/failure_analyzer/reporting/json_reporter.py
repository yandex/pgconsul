"""JSON reporter — machine-readable output for CI.

Emits a stable JSON document describing the analysis result. Field names are
intentionally explicit so downstream tooling can rely on them.
"""

from __future__ import annotations

import json
from typing import TextIO

from ..models import AnalysisResult
from .base import Reporter


class JsonReporter(Reporter):
    """Render the analysis as JSON."""

    def render(self, result: AnalysisResult, stream: TextIO) -> None:
        payload = self._build(result)
        json.dump(payload, stream, indent=2, default=str)
        stream.write("\n")

    def _build(self, result: AnalysisResult) -> dict:
        step = result.failed_step
        running = result.running_step
        return {
            "failed_step": {
                "step": step.step_text,
                "timestamp": step.timestamp,
                "duration": step.duration,
                "feature_file": step.feature_file,
                "line_number": step.line_number,
            }
            if step
            else None,
            "running_step": {
                "step": running.step_text,
                "timestamp": running.timestamp,
                "elapsed_seconds": running.elapsed_seconds,
                "feature_file": running.feature_file,
                "line_number": running.line_number,
            }
            if running
            else None,
            "log_file": str(result.log_file) if result.log_file else None,
            "container_logs": [
                {
                    "container": cl.container,
                    "log_type": cl.log_type,
                    "path": str(cl.path),
                }
                for cl in result.container_logs
            ],
            "findings": [
                {
                    "container": f.container,
                    "log_type": f.log_type,
                    "pattern": f.pattern_name,
                    "weight": f.weight,
                    "line_no": f.line_no,
                    "timestamp": f.timestamp,
                    "line": f.line,
                }
                for f in result.findings
            ],
            "docker_findings": [
                {
                    "container": f.container,
                    "log_path": f.log_path,
                    "pattern": f.pattern_name,
                    "weight": f.weight,
                    "line": f.line,
                }
                for f in result.docker_findings
            ],
            "stuck_indicators": list(result.stuck_indicators),
            "switchover_phases": [
                {
                    "container": ev.container,
                    "timestamp": ev.timestamp,
                    "phase": ev.phase,
                }
                for ev in result.switchover_phases
            ],
            "likely_root_cause": self._root_cause(result),
        }

    @staticmethod
    def _root_cause(result: AnalysisResult) -> dict | None:
        if result.findings:
            top = result.findings[0]
            return {
                "pattern": top.pattern_name,
                "container": top.container,
                "evidence": top.line,
            }
        if result.stuck_indicators:
            return {"pattern": "stuck-in-loop", "evidence": result.stuck_indicators[0]}
        return None
