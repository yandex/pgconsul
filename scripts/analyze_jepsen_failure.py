#!/usr/bin/env python3
"""Summarize a failed pgconsul Jepsen run."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TextIO


_TIMESTAMP_RE = re.compile(r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+)\]")
_PLAIN_TIMESTAMP_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+)")
_OPERATION_RE = re.compile(r"\t:(invoke|ok|fail|info)\t:(add|read)\t")
_CHECKER_RE = re.compile(r'\{:valid\?\s+(true|false)(?:,\s+:error\s+"([^"]+)")?')
_KILL_RE = re.compile(
    r':value \[:killed :(pgconsul|postgres) :on "pgconsul_(postgresql\d+)_1[^" ]*"\]'
)
_FAILED_KILL_RE = re.compile(
    r"Command exited with non-zero status .* node pgconsul_(postgresql\d+)_1.*pkill -9 pgconsul"
)
_ZK_STARTUP_ERROR = "Could not initialize ZooKeeper connection"
_SIGNALS = {
    _ZK_STARTUP_ERROR: "ZooKeeper connection failed during pgconsul startup",
    "Could not promote me as a new primary": "PostgreSQL promotion failed",
    "ACTION-FAILED": "Cluster recovery action failed",
    "rewind_fail.flag": "pg_rewind retry limit was reached",
    "AlreadyLocked": "pgconsul PID lock prevented startup",
    "ModuleNotFoundError": "pgconsul package import failed",
}


def _parse_time(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S,%f")


@dataclass
class Event:
    timestamp: str
    line_no: int


@dataclass
class RestartFailure:
    node: str
    killed_at: str
    first_failure_at: str
    last_failure_at: str
    failures: int


@dataclass
class SignalSummary:
    node: str
    signal: str
    occurrences: int
    first_at: str
    last_at: str


@dataclass
class JepsenAnalysis:
    valid: bool | None = None
    checker_error: str | None = None
    operations: dict[str, dict[str, int]] = field(default_factory=dict)
    pgconsul_kills: int = 0
    failed_pgconsul_kills: int = 0
    restart_failures: list[RestartFailure] = field(default_factory=list)
    signals: list[SignalSummary] = field(default_factory=list)
    likely_root_cause: str | None = None


def _scan_jepsen_log(path: Path) -> tuple[bool | None, str | None, Counter, list[tuple[str, Event]], int]:
    valid: bool | None = None
    checker_error: str | None = None
    operations: Counter = Counter()
    kills: list[tuple[str, Event]] = []
    failed_kills = 0

    with path.open(encoding="utf-8", errors="replace") as stream:
        for line_no, line in enumerate(stream, 1):
            operation = _OPERATION_RE.search(line)
            if operation:
                operations[(operation.group(2), operation.group(1))] += 1

            checker = _CHECKER_RE.search(line)
            if checker:
                valid = checker.group(1) == "true"
                checker_error = checker.group(2)

            kill = _KILL_RE.search(line)
            timestamp = _TIMESTAMP_RE.search(line)
            if kill and timestamp and kill.group(1) == "pgconsul":
                kills.append((kill.group(2), Event(timestamp.group(1), line_no)))

            if _FAILED_KILL_RE.search(line):
                failed_kills += 1

    return valid, checker_error, operations, kills, failed_kills


def _scan_pgconsul_logs(root: Path) -> tuple[dict[str, list[Event]], list[SignalSummary]]:
    zk_failures: dict[str, list[Event]] = {}
    summaries: list[SignalSummary] = []

    for path in sorted(root.glob("postgresql*/pgconsul.log")):
        node = path.parent.name
        matched: dict[str, list[Event]] = {needle: [] for needle in _SIGNALS}
        with path.open(encoding="utf-8", errors="replace") as stream:
            for line_no, line in enumerate(stream, 1):
                timestamp = _PLAIN_TIMESTAMP_RE.search(line)
                if not timestamp:
                    continue
                for needle in _SIGNALS:
                    if needle in line:
                        matched[needle].append(Event(timestamp.group(1), line_no))

        zk_failures[node] = matched[_ZK_STARTUP_ERROR]
        for needle, events in matched.items():
            if events:
                summaries.append(
                    SignalSummary(
                        node=node,
                        signal=_SIGNALS[needle],
                        occurrences=len(events),
                        first_at=events[0].timestamp,
                        last_at=events[-1].timestamp,
                    )
                )

    return zk_failures, summaries


def _correlate_restarts(
    kills: list[tuple[str, Event]],
    failures_by_node: dict[str, list[Event]],
    window_seconds: int,
) -> list[RestartFailure]:
    result: list[RestartFailure] = []
    for node, kill in kills:
        killed_at = _parse_time(kill.timestamp)
        related = [
            event
            for event in failures_by_node.get(node, [])
            if 0 <= (_parse_time(event.timestamp) - killed_at).total_seconds() <= window_seconds
        ]
        if related:
            result.append(
                RestartFailure(
                    node=node,
                    killed_at=kill.timestamp,
                    first_failure_at=related[0].timestamp,
                    last_failure_at=related[-1].timestamp,
                    failures=len(related),
                )
            )
    return result


def analyze(root: Path, window_seconds: int = 30) -> JepsenAnalysis:
    jepsen_log = root / "jepsen.log"
    if not jepsen_log.is_file():
        raise FileNotFoundError(f"Jepsen log not found: {jepsen_log}")

    valid, checker_error, raw_operations, kills, failed_kills = _scan_jepsen_log(jepsen_log)
    failures_by_node, signals = _scan_pgconsul_logs(root)
    restart_failures = _correlate_restarts(kills, failures_by_node, window_seconds)
    operations = {
        operation: {status: raw_operations[(operation, status)] for status in ("invoke", "ok", "fail", "info")}
        for operation in ("add", "read")
    }

    root_cause = None
    repeated_failures = [failure for failure in restart_failures if failure.failures >= 4]
    if repeated_failures:
        nodes = ", ".join(sorted({failure.node for failure in repeated_failures}))
        root_cause = (
            "pgconsul repeatedly failed to restart while ZooKeeper was unavailable after a nemesis kill "
            f"({nodes})"
        )
    elif checker_error:
        root_cause = f"Jepsen checker failed: {checker_error}"

    return JepsenAnalysis(
        valid=valid,
        checker_error=checker_error,
        operations=operations,
        pgconsul_kills=len(kills),
        failed_pgconsul_kills=failed_kills,
        restart_failures=restart_failures,
        signals=signals,
        likely_root_cause=root_cause,
    )


def render_text(result: JepsenAnalysis, stream: TextIO) -> None:
    state = "UNKNOWN" if result.valid is None else ("VALID" if result.valid else "INVALID")
    print(f"JEPSEN RESULT: {state}", file=stream)
    if result.checker_error:
        print(f"Checker: {result.checker_error}", file=stream)

    print("\nOperations:", file=stream)
    for operation, counts in result.operations.items():
        values = ", ".join(f"{status}={count}" for status, count in counts.items())
        print(f"  {operation}: {values}", file=stream)

    print(
        f"\nNemesis: pgconsul kills={result.pgconsul_kills}, "
        f"failed later kill attempts={result.failed_pgconsul_kills}",
        file=stream,
    )
    if result.restart_failures:
        print("\nCorrelated restart failures:", file=stream)
        for failure in result.restart_failures:
            print(
                f"  {failure.node}: killed {failure.killed_at}; "
                f"ZK startup failures={failure.failures} "
                f"({failure.first_failure_at} .. {failure.last_failure_at})",
                file=stream,
            )

    if result.signals:
        print("\nHigh-signal log events:", file=stream)
        for signal in result.signals:
            print(
                f"  {signal.node}: {signal.signal}; occurrences={signal.occurrences} "
                f"({signal.first_at} .. {signal.last_at})",
                file=stream,
            )

    if result.likely_root_cause:
        print(f"\nLikely root cause: {result.likely_root_cause}", file=stream)
    else:
        print("\nLikely root cause: not determined", file=stream)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze pgconsul Jepsen failure logs.")
    parser.add_argument("log_dir", nargs="?", default="logs.local/jepsen")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--window", type=int, default=30, help="Kill/startup correlation window in seconds.")
    return parser


def main(argv: list[str] | None = None, stdout: TextIO | None = None, stderr: TextIO | None = None) -> int:
    args = build_parser().parse_args(argv)
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    try:
        result = analyze(Path(args.log_dir), args.window)
    except (FileNotFoundError, OSError) as exc:
        print(f"ERROR: {exc}", file=err)
        return 2

    if args.format == "json":
        json.dump(asdict(result), out, indent=2)
        out.write("\n")
    else:
        render_text(result, out)
    return 0 if result.valid is not False else 1


if __name__ == "__main__":
    raise SystemExit(main())
