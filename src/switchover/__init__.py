# encoding: utf-8
"""Switchover package — domain types and state machines (MDB-41951, ADR-0005 §3).

Re-exports public API so ``from .switchover import X`` works after the split
into ``types``, ``primary`` and ``candidate`` submodules.
"""

from .types import (
    StrEnum,
    SwitchoverMachineConfig,
    SwitchoverObservation,
    SwitchoverPhase,
    SwitchoverRecord,
    _check_last_failover_time,
)
from .primary import PrimarySwitchoverMachine
from .candidate import CandidateSwitchoverMachine

__all__ = [
    'StrEnum',
    'SwitchoverMachineConfig',
    'SwitchoverObservation',
    'SwitchoverPhase',
    'SwitchoverRecord',
    'PrimarySwitchoverMachine',
    'CandidateSwitchoverMachine',
    '_check_last_failover_time',
]
