"""Parsing of ``test_execution.log`` for failed and in-progress steps.

Pure functions over a log path or text — no I/O side effects beyond reading
the file. This makes them straightforward to unit-test.
"""

from __future__ import annotations

import datetime
import logging
import re
from pathlib import Path
from typing import Optional

from .models import FailedStep, RunningStep

log = logging.getLogger(__name__)

# "Finished step: <text> (status=Status.failed, duration=1.23s)"
STEP_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*Finished step:\s+(.*?)\s+"
    r"\(status=(\w+\.\w+),\s+duration=([\d.]+)s\)"
)

# "Starting step: <text>" (test still running).
STEP_START_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*Starting step:\s+(.*)"
)


def parse_failed_steps(log_path: Path) -> list[FailedStep]:
    """Parse test_execution.log and return all failed steps."""
    text = _read_text(log_path)
    if text is None:
        return []
    steps: list[FailedStep] = []
    for m in STEP_RE.finditer(text):
        ts, step_text, status, dur = m.groups()
        if status == "Status.failed":
            steps.append(
                FailedStep(
                    timestamp=ts,
                    step_text=step_text.strip(),
                    duration=float(dur),
                )
            )
    return steps


def parse_running_step(log_path: Path) -> Optional[RunningStep]:
    """Return the last 'Starting step' that has no matching 'Finished step' yet.

    If the test is still running, the last 'Starting step' line will not have a
    corresponding 'Finished step' entry. We detect this and compute elapsed
    time from the step's start timestamp to now.
    """
    text = _read_text(log_path)
    if text is None:
        return None

    starts = [(m.group(1), m.group(2).strip()) for m in STEP_START_RE.finditer(text)]
    if not starts:
        return None

    finished_texts = {m.group(2).strip() for m in STEP_RE.finditer(text)}

    for ts, step_text in reversed(starts):
        if step_text not in finished_texts:
            elapsed = _elapsed_since(ts)
            return RunningStep(
                timestamp=ts, step_text=step_text, elapsed_seconds=elapsed
            )
    return None


def _elapsed_since(ts: str) -> float:
    try:
        start_dt = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        return (datetime.datetime.now() - start_dt).total_seconds()
    except ValueError:
        return 0.0


def _read_text(log_path: Path) -> Optional[str]:
    try:
        return log_path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        log.warning("cannot read %s: %s", log_path, exc)
        return None
