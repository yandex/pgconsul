#!/usr/bin/env python3
"""
Analyze failed behave test logs and pinpoint the root cause.

This script automates the diagnostic workflow described in AGENTS.md:
  1. Find the failed step in test_execution.log
  2. Identify the feature file, line number, and container logs
  3. Scan container logs for known failure patterns
  4. Produce a concise summary with the most likely root cause

Usage:
    # Auto-discover the latest test logs (logs/ or logs.local/)
    python scripts/analyze_failed_scenario.py

    # Point at a specific log directory
    python scripts/analyze_failed_scenario.py logs.local/logs-failover_with_network_inconsistency

    # Also scan a second log tree (e.g. logs.local/2/logs)
    python scripts/analyze_failed_scenario.py logs.local/logs-failover_with_network_inconsistency logs.local/2/logs
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class FailedStep:
    """A single failed step extracted from test_execution.log."""
    timestamp: str
    step_text: str
    duration: float
    feature_file: str = ''
    line_number: int = 0


@dataclass
class ContainerLog:
    """A container log file found under the failed-step directory."""
    container: str
    log_type: str  # pgconsul, postgresql, pgbouncer, rsync, zookeeper
    path: Path


@dataclass
class Finding:
    """A notable pattern found in a container log."""
    container: str
    log_type: str
    pattern_name: str
    line: str
    line_no: int
    timestamp: str = ''


@dataclass
class AnalysisResult:
    failed_step: Optional[FailedStep] = None
    container_logs: list[ContainerLog] = field(default_factory=list)
    findings: list[Finding] = field(default_factory=list)
    stuck_indicators: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Known failure patterns — ordered by diagnostic priority
# ---------------------------------------------------------------------------

# Patterns are (regex, human-readable name, severity weight)
# Higher weight = more likely to be the root cause.
PGCONSUL_PATTERNS: list[tuple[str, str, int]] = [
    (r'requested starting point .* is ahead of the WAL flush position',
     'WAL divergence: replica LSN behind new primary (needs pg_rewind)', 100),
    (r'could not receive data from WAL stream.*requested starting point',
     'WAL stream rejected: starting point ahead of flush position', 95),
    # Switchover-specific failures (high priority — often the real root cause)
    (r'SWITCHOVER PHASE.*failed|TransitionTo.*FAILED.*switchover',
     'Switchover transitioned to FAILED state', 98),
    (r'Switchover sync_set: candidate is None, aborting',
     'Switchover aborted: candidate not written to ZK before SYNC_SET (anywhere-switchover bug)', 97),
    (r'Switchover (initiated|candidate_found|pooler_stopped|pg_stopped|primary_shut): candidate is None',
     'Switchover aborted: candidate is None in mid-phase', 96),
    (r'ACTION-FAILED\. Could not simple switch to primary',
     'Simple primary switch failed (WAL likely diverged)', 90),
    (r'Could not do a simple primary switch.*Simple primary switch tried: True',
     'Simple primary switch exhausted, should proceed to pg_rewind', 85),
    (r'Error while using pg_rewind',
     'pg_rewind failed', 80),
    (r'rewind_fail\.flag|Could not rewind.*times, setting rewind-failed flag',
     'Rewind failed flag set (max_rewind_retries exceeded)', 75),
    (r'FAILOVER: Primary has died',
     'Failover triggered (primary unavailable)', 50),
    (r'Participate in election|Successfully voted',
     'Election participation', 30),
    (r'Sleep for test purposes for an election loser',
     'Election loser sleep (test debug delay)', 25),
    (r'Seems that primary has been switched to.*We should switch primary',
     'Primary switch detected by replica', 40),
    (r'Retrying timeout expired\.',
     'Retry timeout expired (operation did not complete in time)', 20),
    (r'PostgreSQL is dead',
     'PostgreSQL reported dead', 60),
    (r'Could not connect to',
     'PostgreSQL connection failure', 55),
    (r'connection to server at.*failed',
     'Network connection failure to PostgreSQL host', 45),
    (r'could not restore file.*from archive',
     'Archive restore failure (WAL segment or history file missing)', 35),
    (r'record with incorrect prev-link',
     'WAL corruption: incorrect prev-link', 70),
    (r'unexpected pageaddr.*in log segment',
     'WAL page address mismatch', 65),
    (r'HA replica shouldn\'t exist inside a single node cluster',
     'HA replica in single-node cluster', 15),
    (r'ZK.*session.*expired|Zookeeper.*session.*expired',
     'ZooKeeper session expired', 50),
]

POSTGRES_PATTERNS: list[tuple[str, str, int]] = [
    (r'requested starting point .* is ahead of the WAL flush position',
     'WAL divergence: requested start point ahead of flush position', 100),
    (r'FATAL:.*could not receive data from WAL stream',
     'WAL stream FATAL error', 90),
    (r'FATAL:.*terminating walreceiver process',
     'Walreceiver terminated', 60),
    (r'record with incorrect prev-link',
     'WAL record prev-link mismatch (timeline divergence)', 85),
    (r'unexpected pageaddr.*in log segment',
     'Unexpected WAL page address', 80),
    (r'could not restore file.*from archive',
     'Archive file not found (exit code 23)', 50),
    (r'new target timeline is \d+',
     'Timeline switch detected', 30),
    (r'started streaming WAL from primary',
     'Streaming started (may have failed immediately after)', 20),
    (r'FATAL:.*requested.*has already been removed',
     'Replication slot removed', 55),
    (r'ERROR:.*replication slot.*does not exist',
     'Replication slot missing', 50),
]

ZOOKEEPER_PATTERNS: list[tuple[str, str, int]] = [
    (r'SessionExpired|session expired',
     'ZooKeeper session expired', 60),
    (r'ConnectionLoss|connection loss',
     'ZooKeeper connection loss', 50),
]

# Patterns that indicate a stuck/looping state (not a single error, but
# repeated behavior that leads to timeout).
STUCK_PATTERNS: list[tuple[str, str]] = [
    (r'Waiting \d+\.\d+ for PostgreSQL started streaming from',
     'Repeatedly waiting for streaming (stuck in streaming wait loop)'),
    (r'Waiting \d+\.\d+ for PostgreSQL started archive recovery',
     'Repeatedly waiting for archive recovery (stuck in recovery loop)'),
    (r'Waiting \d+\.\d+ for PostgreSQL has completed recovery',
     'Repeatedly waiting for recovery completion'),
    (r'could not restore file.*from archive',
     'Repeatedly failing to restore from archive'),
    (r'Retrying timeout expired',
     'Repeated retry timeouts'),
    (r'primary_switch checks is \d+',
     'Primary switch attempt counter (check if it grows slowly)'),
    # Switchover-specific stuck patterns
    (r'Switchover in progress, waiting for candidate to be chosen.*state: failed',
     'Switchover stuck in failed state (replicas waiting, no cleanup)'),
    (r'No lock instance for switchover/lock\. Creating one\.',
     'Switchover lock repeatedly not found (primary stuck in post-switchover loop)'),
]

# Maximum file size to read fully (50 MB). Larger files (e.g. postgresql.log
# can be 180+ MB) are streamed line-by-line with DEBUG filtering.
MAX_FULL_READ_SIZE = 50 * 1024 * 1024

# For PostgreSQL logs: keep only lines with these log levels, skip DEBUG.
PG_KEEP_LEVELS_RE = re.compile(
    r'\b(LOG:|FATAL|ERROR|WARNING|NOTICE|PANIC|HINT|STATEMENT)\b',
    re.IGNORECASE,
)

# For pgconsul logs: keep INFO/WARNING/ERROR always; keep DEBUG only for
# stuck-pattern detection (not for error scanning).
PGCONSUL_NON_DEBUG_RE = re.compile(
    r'\b(INFO|WARNING|ERROR)\b',
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Docker log search
# ---------------------------------------------------------------------------

# Project name used by docker-compose in pgconsul tests.
DOCKER_PROJECT = 'pgconsul'

# Container name patterns for each role (matched with startswith).
DOCKER_POSTGRESQL_CONTAINERS = ('postgresql1', 'postgresql2', 'postgresql3')
DOCKER_ZOOKEEPER_CONTAINERS  = ('zookeeper1', 'zookeeper2', 'zookeeper3')

# Log files to read from postgresql containers.
DOCKER_PGCONSUL_PATHS = [
    '/var/log/pgconsul/pgconsul.log',
]
DOCKER_POSTGRES_PATHS = [
    '/var/log/postgresql/postgresql.log',
]


def _docker_is_available() -> bool:
    """Return True if docker is in PATH and the daemon is reachable."""
    try:
        r = subprocess.run(
            ['docker', 'info'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
        return r.returncode == 0
    except Exception:
        return False


def _docker_list_containers() -> list[str]:
    """Return names of running containers for the pgconsul project."""
    try:
        r = subprocess.run(
            ['docker', 'ps', '--format', '{{.Names}}', '--filter', f'name={DOCKER_PROJECT}'],
            capture_output=True, text=True, timeout=10,
        )
        return [n.strip() for n in r.stdout.splitlines() if n.strip()]
    except Exception:
        return []


def _docker_read_file(container: str, path: str, tail: int = 2000) -> list[str]:
    """Read last *tail* lines of *path* from *container* via docker exec.

    Returns a list of line strings (empty list on any error).
    """
    try:
        r = subprocess.run(
            ['docker', 'exec', container, 'tail', f'-{tail}', path],
            capture_output=True, text=True, timeout=30,
        )
        if r.returncode != 0:
            return []
        return r.stdout.splitlines()
    except Exception:
        return []


def _docker_grep_file(container: str, path: str, pattern: str, tail_lines: int = 0) -> list[str]:
    """Run grep inside a container for *pattern* in *path*.

    If *tail_lines* > 0, pipe through tail first to limit scope.
    Returns matching lines (empty list on error).
    """
    try:
        if tail_lines > 0:
            cmd = [
                'docker', 'exec', container,
                'sh', '-c',
                f'tail -{tail_lines} {path} | grep -E {pattern!r}',
            ]
        else:
            cmd = [
                'docker', 'exec', container,
                'grep', '-E', pattern, path,
            ]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        # grep returns 1 when no match — that's OK
        return r.stdout.splitlines()
    except Exception:
        return []


@dataclass
class DockerFinding:
    """A notable line found in a live docker container log."""
    container: str
    log_path: str
    pattern_name: str
    line: str


def scan_docker_containers(containers: list[str]) -> list[DockerFinding]:
    """Scan running pgconsul docker containers for known failure patterns.

    For each postgresql container, greps pgconsul.log and postgresql.log
    for the last 5000 lines. Returns a list of DockerFinding objects.
    """
    findings: list[DockerFinding] = []

    # Combine all patterns from all pattern lists into (regex, name) pairs.
    all_pgconsul_patterns = [(rx, name) for rx, name, _ in PGCONSUL_PATTERNS]
    all_postgres_patterns = [(rx, name) for rx, name, _ in POSTGRES_PATTERNS]
    all_zk_patterns       = [(rx, name) for rx, name, _ in ZOOKEEPER_PATTERNS]

    for container in containers:
        short = container.replace(f'{DOCKER_PROJECT}_', '').replace('_1', '')

        if any(short.startswith(p) for p in DOCKER_POSTGRESQL_CONTAINERS):
            # Scan pgconsul.log
            for log_path in DOCKER_PGCONSUL_PATHS:
                lines = _docker_read_file(container, log_path, tail=5000)
                for line in lines:
                    for rx, name in all_pgconsul_patterns:
                        if re.search(rx, line, re.IGNORECASE):
                            findings.append(DockerFinding(
                                container=container,
                                log_path=log_path,
                                pattern_name=name,
                                line=line.strip()[:300],
                            ))
                            break  # one pattern per line is enough

            # Scan postgresql.log (last 5000 lines)
            for log_path in DOCKER_POSTGRES_PATHS:
                lines = _docker_read_file(container, log_path, tail=5000)
                for line in lines:
                    # Skip DEBUG lines to reduce noise
                    if re.search(r'\bDEBUG\b', line):
                        continue
                    for rx, name in all_postgres_patterns:
                        if re.search(rx, line, re.IGNORECASE):
                            findings.append(DockerFinding(
                                container=container,
                                log_path=log_path,
                                pattern_name=name,
                                line=line.strip()[:300],
                            ))
                            break

        elif any(short.startswith(p) for p in DOCKER_ZOOKEEPER_CONTAINERS):
            zk_log = f'/var/log/zookeeper/zookeeper--server-{container}.log'
            lines = _docker_read_file(container, zk_log, tail=2000)
            for line in lines:
                for rx, name in all_zk_patterns:
                    if re.search(rx, line, re.IGNORECASE):
                        findings.append(DockerFinding(
                            container=container,
                            log_path=zk_log,
                            pattern_name=name,
                            line=line.strip()[:300],
                        ))
                        break

    return findings


def print_docker_findings(findings: list[DockerFinding], verbose: bool = False) -> None:
    """Print docker-sourced findings to stdout."""
    if not findings:
        print('   No known failure patterns found in live docker container logs.')
        return

    # Deduplicate by (container, pattern_name)
    seen: set[tuple[str, str]] = set()
    deduped: list[DockerFinding] = []
    for f in findings:
        key = (f.container, f.pattern_name)
        if key not in seen:
            seen.add(key)
            deduped.append(f)

    # Sort by pattern weight (highest first)
    weight_map = {name: w for _, name, w in PGCONSUL_PATTERNS + POSTGRES_PATTERNS + ZOOKEEPER_PATTERNS}
    deduped.sort(key=lambda f: -weight_map.get(f.pattern_name, 0))

    limit = None if verbose else 15
    for i, f in enumerate(deduped[:limit], 1):
        print(f'\n   {i}. [{f.container}] {f.pattern_name}')
        print(f'      Log:  {f.log_path}')
        print(f'      Line: {f.line}')


# ---------------------------------------------------------------------------
# Log discovery
# ---------------------------------------------------------------------------

def find_log_roots(explicit: list[str]) -> list[Path]:
    """Return log root directories to search."""
    roots = []
    for arg in explicit:
        p = Path(arg)
        if p.is_dir():
            roots.append(p)
        else:
            print(f'  WARN: {p} does not exist, skipping', file=sys.stderr)
    if not roots:
        # Auto-discover
        candidates = [
            Path('logs'),
            Path('logs.local'),
        ]
        for c in candidates:
            if c.is_dir():
                roots.append(c)
    if not roots:
        print('ERROR: No log directories found. Pass a path explicitly.', file=sys.stderr)
        sys.exit(1)
    return roots


def find_test_execution_logs(roots: list[Path]) -> list[Path]:
    """Find all test_execution.log files under the given roots."""
    results = []
    for root in roots:
        # Direct: <root>/debug/test_execution.log
        direct = root / 'debug' / 'test_execution.log'
        if direct.is_file():
            results.append(direct)
        # Nested: <root>/<subdir>/debug/test_execution.log
        for child in sorted(root.iterdir()) if root.is_dir() else []:
            if child.is_dir():
                nested = child / 'debug' / 'test_execution.log'
                if nested.is_file():
                    results.append(nested)
        # Recursive search as fallback
        for path in root.rglob('test_execution*.log'):
            if path not in results:
                results.append(path)
    return results


# ---------------------------------------------------------------------------
# Step 1: Find failed steps
# ---------------------------------------------------------------------------

STEP_RE = re.compile(
    r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*Finished step:\s+(.*?)\s+'
    r'\(status=(\w+\.\w+),\s+duration=([\d.]+)s\)'
)

# Pattern for the "Starting step" line (test still running).
STEP_START_RE = re.compile(
    r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*Starting step:\s+(.*)'
)


@dataclass
class RunningStep:
    """The most recent in-progress step (test still running)."""
    timestamp: str
    step_text: str
    feature_file: str = ''
    line_number: int = 0
    elapsed_seconds: float = 0.0


def parse_failed_steps(log_path: Path) -> list[FailedStep]:
    """Parse test_execution.log and return all failed steps."""
    steps = []
    text = log_path.read_text(errors='replace')
    for m in STEP_RE.finditer(text):
        ts, step_text, status, dur = m.groups()
        if status == 'Status.failed':
            steps.append(FailedStep(
                timestamp=ts,
                step_text=step_text.strip(),
                duration=float(dur),
            ))
    return steps


def parse_running_step(log_path: Path) -> Optional[RunningStep]:
    """Return the last 'Starting step' that has no matching 'Finished step' yet.

    If the test is still running, the last 'Starting step' line will not have
    a corresponding 'Finished step' entry. We detect this and compute elapsed
    time from the step's start timestamp to now.
    """
    import datetime
    text = log_path.read_text(errors='replace')

    # Collect all "Starting step" lines
    starts = [(m.group(1), m.group(2).strip()) for m in STEP_START_RE.finditer(text)]
    if not starts:
        return None

    # Collect all "Finished step" texts
    finished_texts = {m.group(2).strip() for m in STEP_RE.finditer(text)}

    # Find last started step that hasn't finished yet
    for ts, step_text in reversed(starts):
        if step_text not in finished_texts:
            try:
                start_dt = datetime.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                elapsed = (datetime.datetime.now() - start_dt).total_seconds()
            except ValueError:
                elapsed = 0.0
            return RunningStep(timestamp=ts, step_text=step_text, elapsed_seconds=elapsed)

    return None


def find_last_failed_step(roots: list[Path]) -> tuple[Optional[FailedStep], Optional[Path]]:
    """Find the last failed step across all test_execution.log files."""
    log_files = find_test_execution_logs(roots)
    if not log_files:
        return None, None

    last_step = None
    last_log = None
    for lf in log_files:
        steps = parse_failed_steps(lf)
        if steps:
            # Pick the step with the latest timestamp
            latest = max(steps, key=lambda s: s.timestamp)
            if last_step is None or latest.timestamp > last_step.timestamp:
                last_step = latest
                last_log = lf
    return last_step, last_log


def find_running_step(roots: list[Path]) -> tuple[Optional[RunningStep], Optional[Path]]:
    """Find the currently running step (test still in progress)."""
    log_files = find_test_execution_logs(roots)
    best: Optional[RunningStep] = None
    best_log: Optional[Path] = None
    for lf in log_files:
        rs = parse_running_step(lf)
        if rs is not None:
            if best is None or rs.timestamp > best.timestamp:
                best = rs
                best_log = lf
    return best, best_log


# ---------------------------------------------------------------------------
# Step 2: Find container logs for the failed step
# ---------------------------------------------------------------------------

def find_container_logs(roots: list[Path], feature_name: str, line_number: int) -> list[ContainerLog]:
    """Find container log directories matching the feature/line."""
    logs = []
    for root in roots:
        # Pattern: <root>/tests/features/<feature>/<line>/<container>/<logfile>
        for path in root.rglob(f'{feature_name}/{line_number}/*'):
            if not path.is_dir():
                continue
            container = path.name
            for log_file in sorted(path.iterdir()):
                if not log_file.is_file():
                    continue
                log_type = log_file.stem
                # Normalize zookeeper log names
                if log_type.startswith('zookeeper--server'):
                    log_type = 'zookeeper'
                logs.append(ContainerLog(
                    container=container,
                    log_type=log_type,
                    path=log_file,
                ))
    return logs


# ---------------------------------------------------------------------------
# Step 3: Scan container logs for known patterns
# ---------------------------------------------------------------------------

def _iter_log_lines(path: Path, log_type: str, keep_debug: bool = False):
    """Yield (line_no, line) from a log file, filtering DEBUG for large files.

    For files under MAX_FULL_READ_SIZE, all lines are yielded.
    For larger files (e.g. 180 MB postgresql.log), grep is used to pre-filter
    DEBUG lines — this is orders of magnitude faster than Python line-by-line.
    """
    try:
        file_size = path.stat().st_size
    except OSError:
        return

    if file_size <= MAX_FULL_READ_SIZE:
        # Small file: read fully
        try:
            text = path.read_text(errors='replace')
        except Exception:
            return
        for i, line in enumerate(text.splitlines(), 1):
            yield i, line
        return

    # Large file: use grep to pre-filter, then read grep output.
    # grep -n adds line numbers as prefix: "123:line content"
    if log_type == 'postgresql' and not keep_debug:
        # Keep only LOG/FATAL/ERROR/WARNING/NOTICE/PANIC/HINT/STATEMENT lines
        grep_pattern = r'(LOG:|FATAL|ERROR|WARNING|NOTICE|PANIC|HINT|STATEMENT)'
    elif log_type == 'pgconsul' and not keep_debug:
        grep_pattern = r'\b(INFO|WARNING|ERROR)\b'
    else:
        # No filtering — but for large files this is slow; fall back to grep
        # with a pattern that matches all non-empty lines
        grep_pattern = r'.'

    import subprocess
    try:
        proc = subprocess.Popen(
            ['grep', '-n', '-E', grep_pattern, str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        for raw_line in proc.stdout:
            line = raw_line.decode('utf-8', errors='replace').rstrip('\n')
            # grep -n format: "123:content" — split on first colon
            colon_idx = line.find(':')
            if colon_idx > 0:
                try:
                    line_no = int(line[:colon_idx])
                    content = line[colon_idx + 1:]
                    yield line_no, content
                except ValueError:
                    yield 0, line
            else:
                yield 0, line
        proc.wait()
    except Exception:
        return


def scan_log_file(log: ContainerLog, patterns: list[tuple[str, str, int]]) -> list[Finding]:
    """Scan a single log file for known failure patterns.

    DEBUG lines are filtered out for large files to keep scanning fast.
    """
    findings = []
    for i, line in _iter_log_lines(log.path, log.log_type, keep_debug=False):
        for regex, name, weight in patterns:
            if re.search(regex, line, re.IGNORECASE):
                # Extract timestamp if present
                ts_match = re.match(r'(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})', line)
                ts = ts_match.group(1) if ts_match else ''
                findings.append(Finding(
                    container=log.container,
                    log_type=log.log_type,
                    pattern_name=name,
                    line=line.strip()[:300],
                    line_no=i,
                    timestamp=ts,
                ))
    return findings


def detect_stuck_patterns(logs: list[ContainerLog]) -> list[str]:
    """Detect repeated/stuck patterns that indicate a looping state.

    For stuck detection we need DEBUG lines (e.g. 'Waiting N.NN for ...'),
    so keep_debug=True is used for pgconsul logs. Each log file is read
    only once and all patterns are checked in a single pass.
    """
    indicators = []
    # Pre-compile stuck pattern regexes
    compiled_stuck = [(re.compile(rx, re.IGNORECASE), desc) for rx, desc in STUCK_PATTERNS]

    for log in logs:
        if log.log_type not in ('pgconsul', 'postgresql'):
            continue
        # Stuck patterns are in DEBUG lines for pgconsul, so keep them.
        # For postgresql, stuck patterns (e.g. 'could not restore file') are
        # in DEBUG lines too — but reading 180MB with DEBUG is too slow.
        # Use grep to extract only lines matching any stuck pattern.
        keep_debug = log.log_type == 'pgconsul'

        # For large postgresql logs, use grep with a combined pattern
        # to extract only relevant lines in one pass.
        try:
            file_size = log.path.stat().st_size
        except OSError:
            continue

        if file_size > MAX_FULL_READ_SIZE and not keep_debug:
            # Large postgresql log: grep for all stuck patterns at once
            import subprocess
            combined = '|'.join(f'({rx})' for rx, _ in STUCK_PATTERNS)
            try:
                proc = subprocess.Popen(
                    ['grep', '-n', '-E', combined, str(log.path)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                )
                # Collect matches per pattern
                pattern_matches: dict[str, list[str]] = {desc: [] for _, desc in compiled_stuck}
                for raw_line in proc.stdout:
                    line = raw_line.decode('utf-8', errors='replace').rstrip('\n')
                    # Strip grep -n prefix
                    colon_idx = line.find(':')
                    content = line[colon_idx + 1:] if colon_idx > 0 else line
                    for cre, desc in compiled_stuck:
                        if cre.search(content):
                            pattern_matches[desc].append(content)
                proc.wait()
                for desc, matches in pattern_matches.items():
                    if len(matches) >= 5:
                        first_ts = ''
                        last_ts = ''
                        for m in matches:
                            ts_match = re.match(r'(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})', m)
                            if ts_match:
                                if not first_ts:
                                    first_ts = ts_match.group(1)
                                last_ts = ts_match.group(1)
                        span = f' ({first_ts} → {last_ts})' if first_ts else ''
                        indicators.append(
                            f'  [{log.container}/{log.log_type}] {desc}: '
                            f'{len(matches)} occurrences{span}'
                        )
                continue
            except Exception:
                continue

        # Small file or pgconsul with keep_debug: single-pass scan
        pattern_matches = {desc: [] for _, desc in compiled_stuck}
        for i, line in _iter_log_lines(log.path, log.log_type, keep_debug=keep_debug):
            for cre, desc in compiled_stuck:
                if cre.search(line):
                    pattern_matches[desc].append(line)

        for desc, matches in pattern_matches.items():
            if len(matches) >= 5:
                first_ts = ''
                last_ts = ''
                for m in matches:
                    ts_match = re.match(r'(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})', m)
                    if ts_match:
                        if not first_ts:
                            first_ts = ts_match.group(1)
                        last_ts = ts_match.group(1)
                span = f' ({first_ts} → {last_ts})' if first_ts else ''
                indicators.append(
                    f'  [{log.container}/{log.log_type}] {desc}: '
                    f'{len(matches)} occurrences{span}'
                )
    return indicators


# ---------------------------------------------------------------------------
# Step 4: Summarize and rank findings
# ---------------------------------------------------------------------------

def rank_findings(findings: list[Finding]) -> list[Finding]:
    """Rank findings by pattern weight (descending), then by timestamp."""
    # Build weight lookup from all pattern lists
    weight_map = {}
    for patterns in (PGCONSUL_PATTERNS, POSTGRES_PATTERNS, ZOOKEEPER_PATTERNS):
        for regex, name, weight in patterns:
            weight_map[name] = weight

    def sort_key(f: Finding) -> tuple:
        return (-weight_map.get(f.pattern_name, 0), f.timestamp)

    return sorted(findings, key=sort_key)


def deduplicate_findings(findings: list[Finding]) -> list[Finding]:
    """Keep only the first occurrence of each pattern per container."""
    seen = set()
    result = []
    for f in findings:
        key = (f.container, f.pattern_name)
        if key not in seen:
            seen.add(key)
            result.append(f)
    return result


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def print_report(
    result: AnalysisResult,
    log_file: Optional[Path],
    running_step: 'Optional[RunningStep]' = None,
) -> None:
    """Print a human-readable diagnostic report."""
    print('=' * 80)
    print('BEHAVE TEST FAILURE ANALYSIS')
    print('=' * 80)

    if not result.failed_step:
        if running_step:
            print(f'\n🔄 TEST IS STILL RUNNING:')
            print(f'   Step:    {running_step.step_text}')
            print(f'   Started: {running_step.timestamp}')
            print(f'   Elapsed: {running_step.elapsed_seconds:.0f}s')
            if running_step.elapsed_seconds > 120:
                print(f'   ⚠️  Elapsed > 120s — test may be stuck on this step')
            if running_step.feature_file:
                print(f'   Feature: {running_step.feature_file}:{running_step.line_number}')
        else:
            print('\n❌ No failed step found in test_execution.log.')
            print('   Possible causes:')
            print('   - Test is stuck (timeout, not yet marked as failed)')
            print('   - Logs are in a non-standard location')
            print('   - test_execution.log was not generated')
            if log_file:
                print(f'\n   Searched: {log_file}')
        # Still show stuck patterns and container logs if available
        if not result.container_logs and not running_step:
            return

    step = result.failed_step
    if step:
        print(f'\n📋 FAILED STEP:')
        print(f'   Step:      {step.step_text}')
        print(f'   Timestamp: {step.timestamp}')
        print(f'   Duration:  {step.duration:.1f}s')
        if step.duration > 300:
            print(f'   ⚠️  Duration > 300s — likely a TIMEOUT (stuck waiting)')

        if step.feature_file:
            print(f'   Feature:   {step.feature_file}:{step.line_number}')

    print(f'\n📁 CONTAINER LOGS FOUND: {len(result.container_logs)}')
    for cl in result.container_logs:
        size = cl.path.stat().st_size if cl.path.exists() else 0
        size_str = f'{size / 1024:.0f}KB' if size < 1024 * 1024 else f'{size / 1024 / 1024:.1f}MB'
        print(f'   {cl.container:40s} {cl.log_type:15s} {size_str:>10s}')

    if result.stuck_indicators:
        print(f'\n🔄 STUCK/LOOPING PATTERNS DETECTED:')
        for ind in result.stuck_indicators:
            print(ind)

    if result.findings:
        print(f'\n🔍 TOP FINDINGS (ranked by likelihood of being root cause):')
        for i, f in enumerate(result.findings[:15], 1):
            print(f'\n   {i}. [{f.container}/{f.log_type}] {f.pattern_name}')
            if f.timestamp:
                print(f'      Time: {f.timestamp}')
            print(f'      Line {f.line_no}: {f.line}')
    else:
        print('\n   No known failure patterns found in container logs.')
        print('   Consider checking logs manually or adding new patterns.')

    # Heuristic summary
    print(f'\n💡 LIKELY ROOT CAUSE:')
    if result.findings:
        top = result.findings[0]
        print(f'   {top.pattern_name}')
        print(f'   Container: {top.container}')
        print(f'   Evidence:  {top.line}')
    elif result.stuck_indicators:
        print('   Test appears stuck in a loop — see stuck patterns above.')
    else:
        print('   Could not determine automatically. Manual inspection needed.')

    print('\n' + '=' * 80)


# ---------------------------------------------------------------------------
# Feature/line extraction from log directory structure
# ---------------------------------------------------------------------------

def find_feature_and_line_from_logs(roots: list[Path]) -> tuple[str, int]:
    """Extract feature file name and line number from the log directory structure.

    Behave logs are saved as: <root>/tests/features/<feature>.feature/<line>/<container>/
    We find the most recently modified line directory across all roots.
    """
    best_feature = ''
    best_line = 0
    best_mtime = 0.0

    for root in roots:
        tests_dir = root / 'tests' / 'features'
        if not tests_dir.is_dir():
            continue
        for feat_dir in tests_dir.iterdir():
            if not feat_dir.is_dir():
                continue
            for line_dir in feat_dir.iterdir():
                if not line_dir.is_dir():
                    continue
                try:
                    mtime = line_dir.stat().st_mtime
                except OSError:
                    continue
                if mtime > best_mtime:
                    best_mtime = mtime
                    best_feature = feat_dir.name
                    try:
                        best_line = int(line_dir.name)
                    except ValueError:
                        best_line = 0
    return best_feature, best_line


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Analyze failed behave test logs and pinpoint root cause.'
    )
    parser.add_argument(
        'log_dirs',
        nargs='*',
        help='Log directories to search (default: auto-discover logs/ and logs.local/)',
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Show all findings, not just top 15',
    )
    parser.add_argument(
        '--docker',
        action='store_true',
        default=False,
        help=(
            'Also scan live docker containers (requires running test environment). '
            'Enabled automatically when no saved container logs are found.'
        ),
    )
    parser.add_argument(
        '--no-docker',
        dest='docker',
        action='store_false',
        help='Disable docker container scanning (overrides auto-detect).',
    )
    args = parser.parse_args()

    roots = find_log_roots(args.log_dirs)
    print(f'Searching log roots: {[str(r) for r in roots]}')

    # Step 1: Find the failed step (or detect running step)
    failed_step, log_file = find_last_failed_step(roots)
    running_step: Optional[RunningStep] = None
    if not failed_step:
        running_step, log_file = find_running_step(roots)

    # Step 2: Find container logs — feature/line from directory structure
    feature_name, line_no = find_feature_and_line_from_logs(roots)
    if failed_step:
        failed_step.feature_file = feature_name
        failed_step.line_number = line_no
    if running_step:
        running_step.feature_file = feature_name
        running_step.line_number = line_no

    container_logs = []
    if feature_name and line_no:
        container_logs = find_container_logs(roots, feature_name, line_no)

    # Step 3: Scan saved logs for patterns
    all_findings = []
    for log in container_logs:
        if log.log_type == 'pgconsul':
            findings = scan_log_file(log, PGCONSUL_PATTERNS)
        elif log.log_type == 'postgresql':
            findings = scan_log_file(log, POSTGRES_PATTERNS)
        elif log.log_type == 'zookeeper':
            findings = scan_log_file(log, ZOOKEEPER_PATTERNS)
        else:
            continue
        all_findings.extend(findings)

    # Detect stuck patterns
    stuck = detect_stuck_patterns(container_logs)

    # Rank and deduplicate
    ranked = rank_findings(all_findings)
    deduped = deduplicate_findings(ranked)

    if args.verbose:
        findings_to_show = deduped
    else:
        findings_to_show = deduped[:15]

    result = AnalysisResult(
        failed_step=failed_step,
        container_logs=container_logs,
        findings=findings_to_show,
        stuck_indicators=stuck,
    )

    print_report(result, log_file, running_step=running_step)

    # Step 4: Docker container scan.
    # Auto-enable if no saved container logs were found (e.g. stuck test,
    # logs not yet extracted) and --no-docker was not explicitly passed.
    use_docker = args.docker
    if not use_docker and not container_logs and '--no-docker' not in sys.argv:
        if _docker_is_available():
            use_docker = True
            print('\n⚠️  No saved container logs found. Auto-enabling docker scan...')

    if use_docker:
        print('\n' + '=' * 80)
        print('DOCKER CONTAINER LOG SCAN (live containers)')
        print('=' * 80)

        if not _docker_is_available():
            print('  ❌ Docker is not available or daemon is not running.')
        else:
            containers = _docker_list_containers()
            if not containers:
                print(f'  ❌ No running containers found (filter: name={DOCKER_PROJECT}).')
                print('     Make sure the test environment is still running.')
            else:
                print(f'  Found {len(containers)} container(s): {", ".join(containers)}')
                print('  Scanning logs (last 5000 lines per log file)...')
                docker_findings = scan_docker_containers(containers)
                print(f'\n🔍 DOCKER FINDINGS ({len(docker_findings)} raw matches):')
                print_docker_findings(docker_findings, verbose=args.verbose)

                if docker_findings:
                    # Suggest manual deeper-dive commands
                    pg_containers = [
                        c for c in containers
                        if any(
                            c.replace(f'{DOCKER_PROJECT}_', '').replace('_1', '').startswith(p)
                            for p in DOCKER_POSTGRESQL_CONTAINERS
                        )
                    ]
                    if pg_containers:
                        print('\n💡 TO DIG DEEPER — run these commands:')
                        for c in pg_containers:
                            print(f'   docker exec {c} tail -100 /var/log/pgconsul/pgconsul.log')
                            print(
                                f'   docker exec {c} grep -E '
                                f'"REWIND|rewind|ACTION-FAILED|pg_rewind" '
                                f'/var/log/pgconsul/pgconsul.log'
                            )
                            print(
                                f'   docker exec {c} grep -E '
                                f'"FATAL|ERROR|WAL" '
                                f'/var/log/postgresql/postgresql.log | tail -30'
                            )

        print('=' * 80)


if __name__ == '__main__':
    main()
