"""Log-level filter patterns shared by readers and stuck detection.

Both :mod:`sources.readers` (the line readers) and :mod:`stuck` (stuck-pattern
detection) need to drop DEBUG lines for large postgresql/pgconsul logs. The
filter logic lived in ``readers.py`` and was imported locally in ``stuck.py``
to avoid a circular dependency. Moving it here removes that workaround: this
module has no dependencies on other package modules, so both importers can
import it at module level safely.
"""

from __future__ import annotations

import re

# Log-level filters used when DEBUG is dropped.
PG_KEEP_LEVELS_RE = re.compile(
    r"\b(LOG:|FATAL|ERROR|WARNING|NOTICE|PANIC|HINT|STATEMENT)\b",
    re.IGNORECASE,
)
PGCONSUL_NON_DEBUG_RE = re.compile(r"\b(INFO|WARNING|ERROR)\b", re.IGNORECASE)


def filter_pattern_for(log_type: str) -> str | None:
    """Return the grep filter pattern for *log_type* when DEBUG is dropped.

    Returns ``None`` for unknown log types, meaning "no filtering" (keep all
    lines). Callers that need a non-None grep pattern should fall back to
    ``r"."`` (match every non-empty line).
    """
    if log_type == "postgresql":
        return r"(LOG:|FATAL|ERROR|WARNING|NOTICE|PANIC|HINT|STATEMENT)"
    if log_type == "pgconsul":
        return r"\b(INFO|WARNING|ERROR)\b"
    return None


def python_filter_re(log_type: str) -> re.Pattern[str] | None:
    """Return the compiled Python filter regex for *log_type*.

    Returns ``None`` for unknown log types, meaning "no filtering".
    """
    if log_type == "postgresql":
        return PG_KEEP_LEVELS_RE
    if log_type == "pgconsul":
        return PGCONSUL_NON_DEBUG_RE
    return None
