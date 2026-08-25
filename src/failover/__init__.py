# encoding: utf-8
"""Failover package — domain types and state machines (MDB-41951, ADR-0007).

Re-exports the failover types and machines used by the orchestrator.
"""

from .types import (
    FailoverMachineConfig,
    FailoverObservation,
    FailoverPhase,
)
from .participant import FailoverParticipantMachine
from .coordinator import FailoverCoordinatorMachine
from .machine import FailoverMachine

__all__ = [
    'FailoverMachineConfig',
    'FailoverObservation',
    'FailoverPhase',
    'FailoverParticipantMachine',
    'FailoverCoordinatorMachine',
    'FailoverMachine',
]
