"""Small shared helpers: timestamp extraction, container-name normalization,
deduplication, and logging setup.

These helpers used to be inlined (and duplicated) across the original script.
Centralizing them removes the duplication and makes the behavior testable.
"""

from __future__ import annotations

import logging
import re
import sys
from datetime import datetime
from typing import Iterable, TypeVar

_T = TypeVar("_T")

# A leading timestamp like "2024-01-02 03:04:05" or "2024-01-02T03:04:05".
_TIMESTAMP_RE = re.compile(r"(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})")
# Extended timestamp with comma-separated milliseconds: "2024-01-02 03:04:05,123"
_TIMESTAMP_MS_RE = re.compile(r"(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})[,.](\d+)")


def extract_timestamp(line: str) -> str:
    """Return the leading timestamp of *line*, or '' if none."""
    m = _TIMESTAMP_RE.match(line)
    return m.group(1) if m else ""


def parse_timestamp_to_float(ts: str) -> float | None:
    """Parse a timestamp string like '2024-01-02 03:04:05' to a float (Unix time).

    Returns None if parsing fails.
    """
    if not ts:
        return None
    # Try with milliseconds first (pgconsul format: "2024-01-02 03:04:05,123")
    m = _TIMESTAMP_MS_RE.match(ts)
    if m:
        ts = m.group(1)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(ts, fmt).timestamp()
        except ValueError:
            continue
    return None


def normalize_container_name(container: str, project: str) -> str:
    """Strip the docker-compose ``<project>_`` prefix and ``_1`` suffix.

    ``pgconsul_postgresql1_1`` -> ``postgresql1``.
    """
    return container.replace(f"{project}_", "").replace("_1", "")


def deduplicate_by(items: Iterable[_T], key) -> list[_T]:
    """Keep the first occurrence of each key, preserving order."""
    seen: set = set()
    result: list[_T] = []
    for item in items:
        k = key(item)
        if k not in seen:
            seen.add(k)
            result.append(item)
    return result


def configure_logging(verbose: bool = False) -> None:
    """Configure root logging for the CLI.

    Warnings go to stderr so they don't pollute the report on stdout (which may
    be redirected to a file or parsed by CI).
    """
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        stream=sys.stderr,
        format="%(levelname)s: %(message)s",
    )


def human_size(size: int) -> str:
    """Format a byte count as a short human-readable string."""
    if size < 1024:
        return f"{size}B"
    if size < 1024 * 1024:
        return f"{size / 1024:.0f}KB"
    return f"{size / 1024 / 1024:.1f}MB"
