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
from .timeline_history import timeline_requires_rewind


class ReturnAction(StrEnum):
    """Action chosen by decide_return_action (not persisted to ZK)."""

    SIMPLE_SWITCH = 'simple_switch'
    REWIND = 'rewind'
    WAIT_HISTORY = 'wait_history'
    WAIT_ARCHIVE = 'wait_archive'


def decide_return_action(obs: ReturnObservation) -> ReturnAction:
    """Choose whether to wait, switch directly, or rewind.

    Replaces ReturnToClusterMachine._derive_phase + plan_check_divergence.
    """
    effective_role = obs.role or obs.fallback_role
    destructive = is_op_destructive(obs.last_op)
    timelines_differ = (
        obs.local_timeline is not None
        and obs.zk_timeline is not None
        and obs.local_timeline != obs.zk_timeline
    )
    if timelines_differ:
        assert obs.local_timeline is not None
        assert obs.zk_timeline is not None
        if obs.timeline_history is None:
            logging.info(
                'Waiting for timeline %s history in the archive',
                obs.zk_timeline,
            )
            return ReturnAction.WAIT_HISTORY
        needs_rewind = (
            effective_role == 'primary'
            or destructive
            or obs.local_lsn is None
            or timeline_requires_rewind(
            obs.local_timeline,
            obs.local_lsn,
            obs.zk_timeline,
            obs.timeline_history,
            )
        )
        if needs_rewind:
            if obs.required_wal_archived is not True:
                logging.info(
                    'Waiting for required WAL %s in the archive',
                    obs.required_wal_filename,
                )
                return ReturnAction.WAIT_ARCHIVE
            logging.info(
                'Local timeline %s at LSN %s diverges from timeline %s',
                obs.local_timeline,
                obs.local_lsn,
                obs.zk_timeline,
            )
            return ReturnAction.REWIND
        return ReturnAction.SIMPLE_SWITCH

    # Former primary or destructive op — go straight to rewind.
    # When PG is dead, role is None even for a former primary.
    # Use fallback_role (previous role from dead_iter) to detect
    # former primaries and force REWIND instead of SIMPLE_SWITCH.
    if effective_role == 'primary' or destructive:
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
