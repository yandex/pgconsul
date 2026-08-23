# encoding: utf-8
"""
Debug failure injection (MDB-41951, step 14e).

Encapsulates the ``failure_name`` / ``failure_count`` config knobs used to
simulate faults at specific code points. Extracted from ``pgconsul._debug_failure``
so it can be shared by the switchover state machine and other components
without depending on the orchestrator class.
"""

import logging
from dataclasses import dataclass
from configparser import RawConfigParser


@dataclass(frozen=True)
class DebugFailureConfig:
    """Config values for DebugFailure (ADR-0004)."""

    failure_name: str | None = None
    failure_count: int = 0


class DebugFailure:
    """
    Callable that returns True the first N times for the configured ``failure_name``.

    Counters are NOT reset between iterations — fault injection fires N times
    over the whole process lifecycle, not per-iteration.

    Usage::

        debug = DebugFailure(DebugFailureConfig('my_fault', 2))
        if debug('my_fault'):   # True (1st call)
            ...
        if debug('my_fault'):   # True (2nd call)
            ...
        if debug('my_fault'):   # False (count exhausted)
            ...
    """

    def __init__(self, config: DebugFailureConfig) -> None:
        self._config = config
        self._counters: dict[str, int] = {}

    def __call__(self, name: str) -> bool:
        if self._config.failure_name != name:
            return False
        cnt = self._counters.get(name, 0)
        self._counters[name] = cnt + 1
        if cnt < self._config.failure_count:
            logging.error('Debug failure %s', name)
            return True
        return False

    def reset(self) -> None:
        """Reset all failure counters (useful in tests)."""
        self._counters.clear()


def build_debug_failure_config(config: RawConfigParser) -> DebugFailureConfig:
    """Build DebugFailureConfig from RawConfigParser (ADR-0004)."""
    return DebugFailureConfig(
        failure_name=config.get('debug', 'failure_name', fallback=None),
        failure_count=int(config.get('debug', 'failure_count', fallback='100000000')),
    )


def create_debug_failure(config: RawConfigParser) -> DebugFailure:
    """Create DebugFailure from RawConfigParser (ADR-0004)."""
    return DebugFailure(build_debug_failure_config(config))
