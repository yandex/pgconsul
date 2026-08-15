"""Shared domain types and helpers (MDB-41951).

Hosts ``ReplicaInfos`` plus utilities shared by failover/switchover modules:
``StrEnum`` and ``check_last_failover_time`` (de-duplicated from
``failover/types.py`` and ``switchover/types.py``).
"""

import logging
import time
from enum import Enum

ReplicaInfo = dict[str, int | str]
ReplicaInfos = list[ReplicaInfo]


def check_last_failover_time(last: float | None, min_timeout: float) -> bool:
    """True if last failover was long enough ago (or never happened)."""
    if not last:
        return True
    return (time.time() - last) > min_timeout


def is_timed_out(started_ts: float | None, timeout: float, what: str) -> bool:
    """True if ``started_ts`` is set and the elapsed time exceeded ``timeout``.

    Pure timeout predicate shared by failover/switchover timeout gates
    (de-duplicated from ``_is_promote_timed_out`` / ``_is_primary_shut_timed_out``).
    Logs an error with ``what`` describing the timed-out operation.
    """
    if started_ts is None:
        return False
    elapsed = time.time() - started_ts
    if elapsed > timeout:
        logging.error('%s timed out after %.1fs (timeout=%.1fs)', what, elapsed, timeout)
        return True
    return False


class StrEnum(str, Enum):
    """StrEnum for any Python version: str() returns value, not "Class.NAME"."""

    def __str__(self) -> str:
        return self.value
