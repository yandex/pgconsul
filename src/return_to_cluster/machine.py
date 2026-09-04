# encoding: utf-8
"""
Return-to-cluster state machines (MDB-41951, ADR-0006).

The persistent iteration machine owns local phase routing. The stateless WAL
decision distinguishes transient simple-switch failures from divergence.
"""

import logging
from dataclasses import dataclass
from typing import Any, Mapping

from ..commands import Decision, Plan, ReturnIterationAction, ReturnIterationStep
from ..helpers import is_op_destructive
from ..types import StrEnum
from .state import ReturnPhase, ReturnStartSource, ReturnState
from .types import (
    ReturnObservation,
)
from .timeline_history import timeline_requires_rewind


class ReturnAction(StrEnum):
    """Action chosen by decide_return_action (not persisted to ZK)."""

    SIMPLE_SWITCH = 'simple_switch'
    REWIND = 'rewind'
    WAIT_HISTORY = 'wait_history'
    WAIT_ARCHIVE = 'wait_archive'
    ARCHIVE_CATCHUP = 'archive_catchup'


@dataclass(frozen=True)
class ReturnIterationObservation:
    """Immutable snapshot for one host-local return iteration."""

    state: ReturnState | None
    db_state: Mapping[str, Any]
    rewind_flag_set: bool = False
    state_read_failed: bool = False
    target_stale: bool = False
    return_succeeded: bool = False
    previous_primary_unchanged: bool = False
    primary_switch_checks: int = 0
    current_time: float = 0.0
    start_command_running: bool = False
    start_command_exit_code: int | None = None


class ReturnToClusterMachine:
    """Route one bounded host-local return step without performing I/O."""

    @staticmethod
    def _step(
        action: ReturnIterationAction,
        obs: ReturnIterationObservation,
        state: ReturnState | None = None,
    ) -> ReturnIterationStep:
        return ReturnIterationStep(
            action=action,
            state=obs.state if state is None else state,
            db_state=obs.db_state,
            current_time=obs.current_time,
        )

    def decide(self, obs: ReturnIterationObservation) -> Decision:
        state = obs.state
        if obs.state_read_failed:
            return Decision([], True)
        if obs.rewind_flag_set:
            return Decision([self._step('wait_for_resetup', obs)], True)
        if state is None or state.phase == ReturnPhase.BLOCKED:
            return Decision([], False)
        if state.phase == ReturnPhase.RESETUP_REQUIRED:
            return Decision([self._step('resume_after_resetup', obs)], True)
        if obs.target_stale:
            return Decision([self._step('replan_target', obs)], True)
        if obs.return_succeeded:
            return Decision([self._step('complete', obs)], True)
        if state.phase == ReturnPhase.WAITING_ARCHIVE:
            return Decision([self._step('reconcile_requested', obs)], True)

        alive = bool(obs.db_state.get('alive'))
        running = bool(obs.db_state.get('running'))
        if running and not alive:
            return Decision([self._step('track_startup', obs)], True)
        if (
            alive
            and state.phase == ReturnPhase.STARTING
            and state.start_source == ReturnStartSource.PRIMARY
        ):
            return Decision([self._step('track_primary_receive', obs)], True)
        if alive and state.phase in (ReturnPhase.STARTING, ReturnPhase.STARTING_AFTER_REWIND):
            return Decision([self._step('track_replay', obs)], True)
        if alive and state.phase == ReturnPhase.ARCHIVE_CATCHUP:
            return Decision([self._step('track_archive_replay', obs)], True)
        if state.phase in (
            ReturnPhase.STARTING,
            ReturnPhase.STARTING_AFTER_REWIND,
        ) and (obs.start_command_running or obs.start_command_exit_code == 0):
            return Decision([self._step('track_startup', obs)], True)
        if not alive and not running:
            if state.phase == ReturnPhase.REQUESTED and obs.previous_primary_unchanged:
                return Decision([self._step('start_unchanged', obs)], True)
            if (
                state.phase in (
                    ReturnPhase.STARTING,
                    ReturnPhase.STARTING_AFTER_REWIND,
                )
                and state.start_attempts < obs.primary_switch_checks
            ):
                return Decision([self._step('retry_start', obs)], True)
            state = state.evolve(
                phase=ReturnPhase.REWINDING,
                start_source=ReturnStartSource.ARCHIVE,
            )

        if state.phase == ReturnPhase.REQUESTED and alive:
            if state.start_source == ReturnStartSource.PRIMARY:
                return Decision([self._step('simple_remaster', obs, state)], True)
            return Decision([
                self._step('reconcile_requested', obs, state),
            ], True)
        if state.phase == ReturnPhase.REWINDING:
            return Decision([self._step('rewind', obs, state)], True)
        return Decision([], True)

    def plan(self, obs: ReturnIterationObservation) -> Plan:
        """Compatibility projection for callers that only execute commands."""
        return self.decide(obs).plan


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
        if obs.required_wal_archived is not True or obs.fork_lsn is None:
            logging.info(
                'Waiting for target-timeline WAL %s before archive-only catch-up',
                obs.required_wal_filename,
            )
            return ReturnAction.WAIT_ARCHIVE
        return ReturnAction.ARCHIVE_CATCHUP

    # Former primary or destructive op — go straight to rewind.
    # When PG is dead, role is None even for a former primary.
    # Use fallback_role (previous role from dead_iter) to detect
    # former primaries and force REWIND instead of SIMPLE_SWITCH.
    if effective_role == 'primary' or destructive:
        return ReturnAction.REWIND

    return ReturnAction.SIMPLE_SWITCH
