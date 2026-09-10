# encoding: utf-8
"""Switchover package — domain types and state machines (MDB-41951, ADR-0005 §3).

Re-exports public API so ``from .switchover import X`` works after the split
into ``types``, ``primary`` and ``candidate`` submodules.

``StrEnum`` and ``check_last_failover_time`` are re-exported from the shared
``src/types.py`` module for backward compatibility.
"""

from ..types import StrEnum, check_last_failover_time
from .types import (
    SwitchoverMachineConfig,
    SwitchoverObservation,
    SwitchoverPhase,
    SwitchoverRecord,
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
    'check_last_failover_time',
]
