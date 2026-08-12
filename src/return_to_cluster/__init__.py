# encoding: utf-8
"""
Return-to-cluster package — state machine for cluster re-attachment
(MDB-41951, ADR-0006).

Re-exports the public API so that ``from .return_to_cluster import X`` works
after the module was split into ``types`` and ``machine`` submodules.
"""

from .types import (
    ReturnMachineConfig,
    ReturnObservation,
    ReturnPhase,
    is_op_destructive,
    timelines_match,
)
from .machine import ReturnToClusterMachine

__all__ = [
    'ReturnMachineConfig',
    'ReturnObservation',
    'ReturnPhase',
    'ReturnToClusterMachine',
    'is_op_destructive',
    'timelines_match',
]
