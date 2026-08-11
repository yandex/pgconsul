"""Discovery of log roots, test_execution.log files, and feature/line info.

These functions walk the filesystem to find what to analyze. They are kept
separate from parsing/scanning so they can be tested with temp directories.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

from .config import Config
from .models import ContainerLog

log = logging.getLogger(__name__)


def find_log_roots(explicit: list[str], config: Config) -> list[Path]:
    """Return log root directories to search.

    Explicit args are used if they exist; otherwise auto-discovery kicks in.
    Exits the process (via the CLI) if nothing is found — kept here for
    parity with the original script's behavior.
    """
    roots: list[Path] = []
    for arg in explicit:
        p = Path(arg)
        if p.is_dir():
            roots.append(p)
        else:
            log.warning("%s does not exist, skipping", p)
            print(f"  WARN: {p} does not exist, skipping", file=sys.stderr)

    if not roots:
        for c in config.auto_discover_paths():
            if c.is_dir():
                roots.append(c)

    if not roots:
        print(
            "ERROR: No log directories found. Pass a path explicitly.",
            file=sys.stderr,
        )
        sys.exit(1)
    return roots


def find_test_execution_logs(roots: list[Path]) -> list[Path]:
    """Find all test_execution.log files under the given roots."""
    results: list[Path] = []
    for root in roots:
        # Direct: <root>/debug/test_execution.log
        direct = root / "debug" / "test_execution.log"
        if direct.is_file():
            results.append(direct)
        # Nested: <root>/<subdir>/debug/test_execution.log
        if root.is_dir():
            for child in sorted(root.iterdir()):
                if child.is_dir():
                    nested = child / "debug" / "test_execution.log"
                    if nested.is_file() and nested not in results:
                        results.append(nested)
        # Recursive search as fallback
        for path in root.rglob("test_execution*.log"):
            if path not in results:
                results.append(path)
    return results


def find_last_failed_step_log(
    roots: list[Path],
    parse_failed_steps,
) -> tuple[Optional[object], Optional[Path]]:
    """Find the log file containing the latest failed step.

    ``parse_failed_steps`` is injected (a callable ``(Path) -> list[FailedStep]``)
    so this function doesn't depend on the parsing module directly.
    """
    log_files = find_test_execution_logs(roots)
    if not log_files:
        return None, None

    last_step = None
    last_log: Optional[Path] = None
    for lf in log_files:
        steps = parse_failed_steps(lf)
        if steps:
            latest = max(steps, key=lambda s: s.timestamp)
            if last_step is None or latest.timestamp > last_step.timestamp:
                last_step = latest
                last_log = lf
    return last_step, last_log


def find_running_step_log(
    roots: list[Path],
    parse_running_step,
) -> tuple[Optional[object], Optional[Path]]:
    """Find the log file containing the currently running step."""
    log_files = find_test_execution_logs(roots)
    best = None
    best_log: Optional[Path] = None
    for lf in log_files:
        rs = parse_running_step(lf)
        if rs is not None:
            if best is None or rs.timestamp > best.timestamp:
                best = rs
                best_log = lf
    return best, best_log


def find_feature_and_line_from_logs(roots: list[Path]) -> tuple[str, int]:
    """Extract feature file name and line number from the log directory structure.

    Behave logs are saved as: ``<root>/tests/features/<feature>.feature/<line>/<container>/``.
    We find the most recently modified line directory across all roots.
    """
    best_feature = ""
    best_line = 0
    best_mtime = 0.0

    for root in roots:
        tests_dir = root / "tests" / "features"
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


def find_container_logs(
    roots: list[Path], feature_name: str, line_number: int
) -> list[ContainerLog]:
    """Find container log directories matching the feature/line."""
    logs: list[ContainerLog] = []
    for root in roots:
        # Pattern: <root>/tests/features/<feature>/<line>/<container>/<logfile>
        for path in root.rglob(f"{feature_name}/{line_number}/*"):
            if not path.is_dir():
                continue
            container = path.name
            for log_file in sorted(path.iterdir()):
                if not log_file.is_file():
                    continue
                log_type = log_file.stem
                # Normalize zookeeper log names
                if log_type.startswith("zookeeper--server"):
                    log_type = "zookeeper"
                logs.append(
                    ContainerLog(
                        container=container, log_type=log_type, path=log_file
                    )
                )
    return logs
