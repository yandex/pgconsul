# encoding: utf-8
"""
Return-to-cluster package — decision logic for cluster re-attachment
(MDB-41951, ADR-0006).

Re-exports the public API so that ``from .return_to_cluster import X`` works
after the module was split into ``types`` and ``machine`` submodules.
"""

from ..helpers import is_op_destructive
from .machine import (
    ReturnAction,
    ReturnIterationObservation,
    ReturnToClusterMachine,
    decide_return_action,
)
from .types import (
    ReturnObservation,
)
from .timeline_history import (
    TimelineSwitch,
    parse_timeline_history,
    timeline_requires_rewind,
    wal_filename_before_switch,
    wal_filenames_before_switch,
)

__all__ = [
    'ReturnAction',
    'ReturnIterationObservation',
    'ReturnObservation',
    'ReturnToClusterMachine',
    'decide_return_action',
    'is_op_destructive',
    'TimelineSwitch',
    'parse_timeline_history',
    'timeline_requires_rewind',
    'wal_filename_before_switch',
    'wal_filenames_before_switch',
]
