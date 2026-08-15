"""The pattern scanner.

A single :class:`Scanner` runs over any :class:`~.sources.base.LogSource` and
matches a set of :class:`~.patterns.Pattern` objects against each line. This
replaces the two duplicated loops in the original script
(``scan_log_file`` for files and ``scan_docker_containers`` for containers).
"""

from __future__ import annotations

from typing import Iterable, Sequence

from .models import DockerFinding, Finding
from .patterns import Pattern
from .sources.base import LogSource
from .utils import extract_timestamp


class Scanner:
    """Match patterns against lines from a :class:`LogSource`."""

    def __init__(self, line_truncate: int = 300) -> None:
        self._line_truncate = line_truncate

    def scan(
        self,
        source: LogSource,
        patterns: Sequence[Pattern],
        keep_debug: bool = False,
    ) -> list[Finding]:
        """Return one :class:`Finding` per matching line (first pattern wins)."""
        findings: list[Finding] = []
        for line_no, line in source.iter_lines(keep_debug=keep_debug):
            for pattern in patterns:
                if pattern.search(line):
                    findings.append(
                        Finding(
                            container=source.container,
                            log_type=source.log_type,
                            pattern=pattern,
                            line=line.strip()[: self._line_truncate],
                            line_no=line_no,
                            timestamp=extract_timestamp(line),
                        )
                    )
                    break  # one pattern per line is enough
        return findings

    def scan_many(
        self,
        sources: Iterable[LogSource],
        patterns_for,
        keep_debug: bool = False,
    ) -> list[Finding]:
        """Scan many sources, selecting patterns per source via *patterns_for*.

        ``patterns_for`` is a callable ``(log_type) -> Sequence[Pattern]``.
        Sources whose log type has no patterns are skipped.

        When *keep_debug* is False, pgconsul logs still keep DEBUG lines —
        DEBUG filtering only applies to postgresql logs (which can be 180+ MB).
        """
        findings: list[Finding] = []
        for source in sources:
            patterns = patterns_for(source.log_type)
            if not patterns:
                continue
            src_keep_debug = keep_debug or source.log_type == "pgconsul"
            findings.extend(self.scan(source, patterns, keep_debug=src_keep_debug))
        return findings

    def scan_docker(
        self,
        source: LogSource,
        patterns: Sequence[Pattern],
    ) -> list[DockerFinding]:
        """Like :meth:`scan` but returns :class:`DockerFinding` (no line_no)."""
        findings: list[DockerFinding] = []
        for _line_no, line in source.iter_lines(keep_debug=False):
            for pattern in patterns:
                if pattern.search(line):
                    findings.append(
                        DockerFinding(
                            container=source.container,
                            log_path=getattr(source, "log_path", ""),
                            pattern=pattern,
                            line=line.strip()[: self._line_truncate],
                        )
                    )
                    break
        return findings
