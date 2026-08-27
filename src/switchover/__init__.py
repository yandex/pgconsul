# encoding: utf-8
"""Switchover package — domain types and state machines (MDB-41951, ADR-0005 §3).

Re-exports the switchover types and machines used by the orchestrator.
"""

from .types import (
    SwitchoverMachineConfig,
    SwitchoverObservation,
    SwitchoverPhase,
    SwitchoverRecord,
    SwitchoverRoute,
    decide_switchover_route,
)
from .primary import PrimarySwitchoverMachine
from .candidate import CandidateSwitchoverMachine

__all__ = [
    'SwitchoverMachineConfig',
    'SwitchoverObservation',
    'SwitchoverPhase',
    'SwitchoverRecord',
    'SwitchoverRoute',
    'decide_switchover_route',
    'PrimarySwitchoverMachine',
    'CandidateSwitchoverMachine',
]
