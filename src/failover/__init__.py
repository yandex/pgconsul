# encoding: utf-8
"""Failover package — domain types and state machines (MDB-41951, ADR-0007).

Re-exports public API so ``from .failover import X`` works after the split
into ``types``, ``coordinator`` and ``participant`` submodules.
"""

from .types import (
    FailoverMachineConfig,
    FailoverObservation,
    FailoverPhase,
    FailoverRecord,
    StrEnum,
    _check_last_failover_time,
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
    '_check_last_failover_time',
]
