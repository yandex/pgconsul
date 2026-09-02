# encoding: utf-8
"""Manager-owned switchover protocol types."""

from .types import (
    DurabilityPinMode,
    SwitchoverPhase,
    SwitchoverRecord,
)
from .machine import SwitchoverMachine, SwitchoverObservation

__all__ = [
    'DurabilityPinMode',
    'SwitchoverPhase',
    'SwitchoverRecord',
    'SwitchoverMachine',
    'SwitchoverObservation',
]
