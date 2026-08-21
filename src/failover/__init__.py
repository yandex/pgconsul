# encoding: utf-8
"""Failover package — domain types and state machines (MDB-41951, ADR-0007).

Re-exports public API so ``from .failover import X`` works after the split
into ``types``, ``coordinator`` and ``participant`` submodules.

``StrEnum`` and ``check_last_failover_time`` are re-exported from the shared
``src/types.py`` module for backward compatibility.
"""

from ..types import StrEnum, check_last_failover_time
from .types import (
    FailoverMachineConfig,
    FailoverObservation,
    FailoverPhase,
    FailoverRecord,
)
from .participant import FailoverParticipantMachine
from .coordinator import FailoverCoordinatorMachine

__all__ = [
    'StrEnum',
    'FailoverMachineConfig',
    'FailoverObservation',
    'FailoverPhase',
    'FailoverRecord',
    'FailoverParticipantMachine',
    'FailoverCoordinatorMachine',
    'check_last_failover_time',
]
