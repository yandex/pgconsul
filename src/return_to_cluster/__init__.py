# encoding: utf-8
"""
Return-to-cluster package — state machine for cluster re-attachment
(MDB-41951, ADR-0006).

Re-exports the public API so that ``from .return_to_cluster import X`` works
after the module was split into ``types`` and ``machine`` submodules.
"""

from ..helpers import is_op_destructive
from .machine import ReturnToClusterMachine
from .types import (
    ReturnObservation,
    ReturnPhase,
    timelines_match,
)

__all__ = [
    'ReturnObservation',
    'ReturnPhase',
    'ReturnToClusterMachine',
    'is_op_destructive',
    'timelines_match',
]
