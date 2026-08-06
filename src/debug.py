# encoding: utf-8
"""
Debug failure injection (MDB-41951, step 14e).

Encapsulates the ``failure_name`` / ``failure_count`` config knobs used to
simulate faults at specific code points. Extracted from ``pgconsul._debug_failure``
so it can be shared by the switchover state machine and other components
without depending on the orchestrator class.
"""

import logging
from dataclasses import dataclass, field


@dataclass
class DebugFailureConfig:
    """Config values for DebugFailure (ADR-0004)."""

    failure_name: str | None = None
    failure_count: int = 0


class DebugFailure:
    """
    Callable that returns True (simulating a failure) the first N times it is
    invoked with the configured ``failure_name``.

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
