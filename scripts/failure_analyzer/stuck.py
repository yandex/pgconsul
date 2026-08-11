"""Stuck/looping pattern detection and switchover phase extraction.

A "stuck" pattern is not a single error but repeated behavior that leads to a
timeout. We count occurrences per pattern per log; if a pattern fires at least
``Config.stuck_min_occurrences`` times, it's reported with its time span.

For large postgresql logs we keep DEBUG lines out of the Python loop by
grepping for the combined stuck pattern in one pass (the original behavior).

:func:`detect_switchover_phases` extracts ``SWITCHOVER PHASE → <name>`` lines
from pgconsul logs to build a per-container timeline — immediately revealing
where the switchover process stalled (e.g. a candidate stuck in
``candidate_found`` while the old primary already reached ``primary_shut``).
"""

from __future__ import annotations

import logging
import re
import subprocess
from pathlib import Path

from .config import Config
from .models import ContainerLog, SwitchoverPhaseEvent
from .patterns import STUCK_PATTERNS, StuckPattern, combined_stuck_regex
from .utils import extract_timestamp, parse_timestamp_to_float

log = logging.getLogger(__name__)

# Matches lines like: "2026-08-11 07:13:42,354 WARNING : SWITCHOVER PHASE → sync_set"
_PHASE_RE = re.compile(r'SWITCHOVER PHASE\s*→\s*(\S+)')


def detect_stuck_patterns(
    logs: list[ContainerLog],
    config: Config,
) -> list[str]:
    """Detect repeated/stuck patterns across the given container logs."""
    indicators: list[str] = []
    compiled: list[StuckPattern] = list(STUCK_PATTERNS)

    for cl in logs:
        if cl.log_type not in ("pgconsul", "postgresql"):
            continue

        # Stuck patterns live in DEBUG lines for pgconsul, so keep them.
        # For postgresql, reading 180MB with DEBUG is too slow — use grep.
        keep_debug = cl.log_type == "pgconsul"

        try:
            file_size = cl.path.stat().st_size
        except OSError as exc:
            log.warning("cannot stat %s: %s", cl.path, exc)
            continue

        if file_size > config.max_full_read_size and not keep_debug:
            matches = _grep_stuck_matches(cl.path, compiled)
        else:
            matches = _python_stuck_matches(cl.path, cl.log_type, keep_debug, compiled)

        for desc, lines in matches.items():
            if len(lines) < config.stuck_min_occurrences:
                continue
            first_ts = ""
            last_ts = ""
            for line in lines:
                ts = extract_timestamp(line)
                if ts:
                    if not first_ts:
                        first_ts = ts
                    last_ts = ts
            span = f" ({first_ts} → {last_ts})" if first_ts else ""
            indicators.append(
                f"  [{cl.container}/{cl.log_type}] {desc}: "
                f"{len(lines)} occurrences{span}"
            )
    return indicators


def _python_stuck_matches(
    path: Path,
    log_type: str,
    keep_debug: bool,
    compiled: list[StuckPattern],
) -> dict[str, list[str]]:
    from .sources.readers import _python_filter_re  # local to avoid cycle

    matches: dict[str, list[str]] = {p.name: [] for p in compiled}
    filter_re = None if keep_debug else _python_filter_re(log_type)
    try:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if filter_re is not None and not filter_re.search(line):
                    continue
                for p in compiled:
                    if p.search(line):
                        matches[p.name].append(line.rstrip("\n"))
    except OSError as exc:
        log.warning("cannot read %s: %s", path, exc)
    return matches


def _grep_stuck_matches(
    path: Path,
    compiled: list[StuckPattern],
) -> dict[str, list[str]]:
    matches: dict[str, list[str]] = {p.name: [] for p in compiled}
    combined = combined_stuck_regex()
    try:
        proc = subprocess.Popen(
            ["grep", "-n", "-E", combined, str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        log.warning("grep not found; falling back to full read of %s", path)
        return _python_stuck_matches(path, "postgresql", False, compiled)
    except OSError as exc:
        log.warning("cannot spawn grep for %s: %s", path, exc)
        return matches

    assert proc.stdout is not None
    for raw_line in proc.stdout:
        line = raw_line.decode("utf-8", errors="replace").rstrip("\n")
        colon_idx = line.find(":")
        content = line[colon_idx + 1:] if colon_idx > 0 else line
        for p in compiled:
            if p.search(content):
                matches[p.name].append(content)
    proc.wait()
    return matches


def detect_switchover_phases(
    logs: list[ContainerLog],
    config: Config,
) -> list[SwitchoverPhaseEvent]:
    """Extract ``SWITCHOVER PHASE → <name>`` lines from pgconsul logs.

    Returns a flat list of :class:`SwitchoverPhaseEvent` sorted by timestamp.
    The reporter groups them by container to show a per-node timeline that
    immediately reveals where the switchover stalled.

    Each event's ``duration_seconds`` is filled with the elapsed time between
    consecutive phases on the same container — useful to spot which phase
    consumed the most time (e.g. a slow Checkpoint or a blocking pg_stop).
    """
    events: list[SwitchoverPhaseEvent] = []

    for cl in logs:
        if cl.log_type != "pgconsul":
            continue

        try:
            file_size = cl.path.stat().st_size
        except OSError as exc:
            log.warning("cannot stat %s: %s", cl.path, exc)
            continue

        # Phase lines are WARNING level, so they survive DEBUG filtering.
        # Use grep for large files to avoid reading 180MB in Python.
        if file_size > config.max_full_read_size:
            phase_lines = _grep_phase_lines(cl.path)
        else:
            phase_lines = _python_phase_lines(cl.path)

        for line in phase_lines:
            m = _PHASE_RE.search(line)
            if m is None:
                continue
            events.append(
                SwitchoverPhaseEvent(
                    container=cl.container,
                    timestamp=extract_timestamp(line) or "",
                    phase=m.group(1),
                )
            )

    events.sort(key=lambda e: (e.timestamp, e.container))

    # Fill duration_seconds: time between consecutive phases per container.
    _fill_phase_durations(events)

    return events


def _fill_phase_durations(events: list[SwitchoverPhaseEvent]) -> None:
    """Mutate events in-place: fill duration_seconds for each phase.

    Duration of phase N = timestamp(phase N+1) - timestamp(phase N).
    The last phase on each container gets duration 0.
    """
    # Group indices by container, preserving sort order.
    by_container: dict[str, list[int]] = {}
    for idx, ev in enumerate(events):
        by_container.setdefault(ev.container, []).append(idx)

    for indices in by_container.values():
        for i in range(len(indices) - 1):
            cur = events[indices[i]]
            nxt = events[indices[i + 1]]
            cur_ts = parse_timestamp_to_float(cur.timestamp)
            nxt_ts = parse_timestamp_to_float(nxt.timestamp)
            if cur_ts is not None and nxt_ts is not None and nxt_ts >= cur_ts:
                # Mutate the frozen-by-value field via object.__setattr__ since
                # SwitchoverPhaseEvent is a plain dataclass (not frozen).
                cur.duration_seconds = round(nxt_ts - cur_ts, 1)


def _python_phase_lines(path: Path) -> list[str]:
    """Read phase lines via pure Python (small files)."""
    lines: list[str] = []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if _PHASE_RE.search(line):
                    lines.append(line.rstrip("\n"))
    except OSError as exc:
        log.warning("cannot read %s: %s", path, exc)
    return lines


def _grep_phase_lines(path: Path) -> list[str]:
    """Read phase lines via grep (large files)."""
    try:
        proc = subprocess.Popen(
            ["grep", "-n", "-E", r'SWITCHOVER PHASE', str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        log.warning("grep not found; falling back to full read of %s", path)
        return _python_phase_lines(path)
    except OSError as exc:
        log.warning("cannot spawn grep for %s: %s", path, exc)
        return []

    lines: list[str] = []
    assert proc.stdout is not None
    for raw_line in proc.stdout:
        line = raw_line.decode("utf-8", errors="replace").rstrip("\n")
        colon_idx = line.find(":")
        content = line[colon_idx + 1:] if colon_idx > 0 else line
        lines.append(content)
    proc.wait()
    return lines
