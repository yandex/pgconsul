"""Human-readable text reporter.

Reproduces the output of the original ``print_report`` and the docker block
from ``main``. Keeping the format byte-for-byte compatible with the old script
is a regression guard (see the plan's "golden file" criterion).
"""

from __future__ import annotations

from typing import TextIO

from ..models import AnalysisResult, DockerFinding
from ..ranking import deduplicate_docker_findings, limit, rank_docker_findings
from ..utils import human_size
from .base import Reporter


class TextReporter(Reporter):
    """Render the analysis as the original human-readable report."""

    def __init__(self, verbose: bool = False, findings_limit: int = 15) -> None:
        self._verbose = verbose
        self._findings_limit = findings_limit

    def render(self, result: AnalysisResult, stream: TextIO) -> None:
        w = stream.write
        w("=" * 80 + "\n")
        w("BEHAVE TEST FAILURE ANALYSIS\n")
        w("=" * 80 + "\n")

        if not result.failed_step:
            if result.running_step:
                w("\n🔄 TEST IS STILL RUNNING:\n")
                rs = result.running_step
                w(f"   Step:    {rs.step_text}\n")
                w(f"   Started: {rs.timestamp}\n")
                w(f"   Elapsed: {rs.elapsed_seconds:.0f}s\n")
                if rs.elapsed_seconds > 120:
                    w("   ⚠️  Elapsed > 120s — test may be stuck on this step\n")
                if rs.feature_file:
                    w(f"   Feature: {rs.feature_file}:{rs.line_number}\n")
            else:
                w("\n❌ No failed step found in test_execution.log.\n")
                w("   Possible causes:\n")
                w("   - Test is stuck (timeout, not yet marked as failed)\n")
                w("   - Logs are in a non-standard location\n")
                w("   - test_execution.log was not generated\n")
                if result.log_file:
                    w(f"\n   Searched: {result.log_file}\n")
            # Still show stuck patterns and container logs if available
            if not result.container_logs and not result.running_step:
                return

        step = result.failed_step
        if step:
            w("\n📋 FAILED STEP:\n")
            w(f"   Step:      {step.step_text}\n")
            w(f"   Timestamp: {step.timestamp}\n")
            w(f"   Duration:  {step.duration:.1f}s\n")
            if step.duration > 300:
                w("   ⚠️  Duration > 300s — likely a TIMEOUT (stuck waiting)\n")
            if step.feature_file:
                w(f"   Feature:   {step.feature_file}:{step.line_number}\n")

        w(f"\n📁 CONTAINER LOGS FOUND: {len(result.container_logs)}\n")
        for cl in result.container_logs:
            size = cl.path.stat().st_size if cl.path.exists() else 0
            w(f"   {cl.container:40s} {cl.log_type:15s} {human_size(size):>10s}\n")

        if result.stuck_indicators:
            w("\n🔄 STUCK/LOOPING PATTERNS DETECTED:\n")
            for ind in result.stuck_indicators:
                w(ind + "\n")

        if result.switchover_phases:
            w("\n🔀 SWITCHOVER PHASE TIMELINE:\n")
            # Group phases by container, preserving chronological order.
            by_container: dict[str, list] = {}
            for ev in result.switchover_phases:
                by_container.setdefault(ev.container, []).append(ev)
            for container, evs in by_container.items():
                # Build phase string: "sync_set(6.5s) → initiated(23.0s) → ..."
                parts: list[str] = []
                for ev in evs:
                    if ev.duration_seconds > 0:
                        parts.append(f"{ev.phase}({ev.duration_seconds:.1f}s)")
                    else:
                        parts.append(ev.phase)
                w(f"  {container}: {' → '.join(parts)}\n")

        if result.findings:
            w("\n🔍 TOP FINDINGS (ranked by likelihood of being root cause):\n")
            shown = limit(result.findings, None if self._verbose else self._findings_limit)
            for i, f in enumerate(shown, 1):
                w(f"\n   {i}. [{f.container}/{f.log_type}] {f.pattern_name}\n")
                if f.timestamp:
                    w(f"      Time: {f.timestamp}\n")
                w(f"      Line {f.line_no}: {f.line}\n")
        else:
            w("\n   No known failure patterns found in container logs.\n")
            w("   Consider checking logs manually or adding new patterns.\n")

        # Heuristic summary
        w("\n💡 LIKELY ROOT CAUSE:\n")
        if result.findings:
            top = result.findings[0]
            w(f"   {top.pattern_name}\n")
            w(f"   Container: {top.container}\n")
            w(f"   Evidence:  {top.line}\n")
        elif result.stuck_indicators:
            w("   Test appears stuck in a loop — see stuck patterns above.\n")
        else:
            w("   Could not determine automatically. Manual inspection needed.\n")

        w("\n" + "=" * 80 + "\n")

    def render_docker(
        self,
        findings: list[DockerFinding],
        containers: list[str],
        stream: TextIO,
        pg_containers: list[str],
    ) -> None:
        """Render the docker container log scan section."""
        w = stream.write
        w("\n" + "=" * 80 + "\n")
        w("DOCKER CONTAINER LOG SCAN (live containers)\n")
        w("=" * 80 + "\n")

        w(f"  Found {len(containers)} container(s): {', '.join(containers)}\n")
        w("  Scanning logs (last 5000 lines per log file)...\n")
        w(f"\n🔍 DOCKER FINDINGS ({len(findings)} raw matches):\n")
        self._print_docker_findings(findings, stream)

        if findings and pg_containers:
            w("\n💡 TO DIG DEEPER — run these commands:\n")
            for c in pg_containers:
                w(f"   docker exec {c} tail -100 /var/log/pgconsul/pgconsul.log\n")
                w(
                    f'   docker exec {c} grep -E '
                    f'"REWIND|rewind|ACTION-FAILED|pg_rewind" '
                    f"/var/log/pgconsul/pgconsul.log\n"
                )
                w(
                    f'   docker exec {c} grep -E '
                    f'"FATAL|ERROR|WAL" '
                    f"/var/log/postgresql/postgresql.log | tail -30\n"
                )

        w("=" * 80 + "\n")

    def _print_docker_findings(
        self, findings: list[DockerFinding], stream: TextIO
    ) -> None:
        w = stream.write
        if not findings:
            w("   No known failure patterns found in live docker container logs.\n")
            return

        deduped = deduplicate_docker_findings(findings)
        deduped = rank_docker_findings(deduped)
        shown = limit(deduped, None if self._verbose else self._findings_limit)

        for i, f in enumerate(shown, 1):
            w(f"\n   {i}. [{f.container}] {f.pattern_name}\n")
            w(f"      Log:  {f.log_path}\n")
            w(f"      Line: {f.line}\n")
