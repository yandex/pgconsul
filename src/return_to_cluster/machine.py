# encoding: utf-8
"""
Return-to-cluster decision logic (MDB-41951, ADR-0006).

Pure decide_return_action(observation) → ReturnAction. Stateless: the action
is re-derived from the observation each call. Distinguishes transient
simple-switch failures from real WAL divergence to avoid unnecessary pg_rewind.
"""

import logging

from ..helpers import is_op_destructive
from ..types import StrEnum
from .types import (
    ReturnObservation,
    timelines_match,
)


class ReturnAction(StrEnum):
    """Action chosen by decide_return_action (not persisted to ZK)."""

    SIMPLE_SWITCH = 'simple_switch'
    REWIND = 'rewind'


def decide_return_action(obs: ReturnObservation) -> ReturnAction:
    """Pure decision: SIMPLE_SWITCH or REWIND.

    Replaces ReturnToClusterMachine._derive_phase + plan_check_divergence.
    """
    # Former primary or destructive op — go straight to rewind.
    # When PG is dead, role is None even for a former primary.
    # Use fallback_role (previous role from dead_iter) to detect
    # former primaries and force REWIND instead of SIMPLE_SWITCH.
    effective_role = obs.role or obs.fallback_role
    if effective_role == 'primary' or is_op_destructive(obs.last_op):
        return ReturnAction.REWIND

    # Simple switch already failed — check divergence.
    if obs.simple_switch_tried:
        if timelines_match(obs.local_timeline, obs.zk_timeline):
            logging.info(
                'Simple switch failed but timelines match (local=%s, zk=%s). '
                'Rewind not needed — will retry.',
                obs.local_timeline, obs.zk_timeline,
            )
            return ReturnAction.SIMPLE_SWITCH  # retry (transient failure)
        logging.info(
            'Timelines diverge (local=%s, zk=%s) — pg_rewind required.',
            obs.local_timeline, obs.zk_timeline,
        )
        return ReturnAction.REWIND  # real divergence

    return ReturnAction.SIMPLE_SWITCH  # first try
