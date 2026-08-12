# encoding: utf-8
"""
Return-to-cluster state machine (MDB-41951, ADR-0006).

Pure plan(observation) API. Stateless: phase is re-derived from the
observation each call. Distinguishes transient simple-switch failures
from real WAL divergence to avoid unnecessary pg_rewind.
"""

import logging
from typing import TYPE_CHECKING

from ..commands import (
    CheckDivergence,
    EnsureRestoringWal,
    Log,
    Plan as CommandPlan,
    RewindFromSource,
    SetSimplePrimarySwitchTry,
    SimplePrimarySwitch,
)
from .types import (
    ReturnMachineConfig,
    ReturnObservation,
    ReturnPhase,
    is_op_destructive,
    timelines_match,
)

if TYPE_CHECKING:
    pass


class ReturnToClusterMachine:
    """Return-to-cluster state machine (ADR-0006). Pure plan(), no I/O."""

    def __init__(self, config: 'ReturnMachineConfig | None' = None) -> None:
        self._cfg = config or ReturnMachineConfig()

    def plan(self, obs: ReturnObservation) -> CommandPlan:
        """Return the Command Plan for the current observation (pure, no I/O)."""
        phase = self._derive_phase(obs)
        planners = {
            ReturnPhase.INIT: self.plan_init,
            ReturnPhase.SIMPLE_SWITCH: self.plan_simple_switch,
            ReturnPhase.CHECK_DIVERGENCE: self.plan_check_divergence,
            ReturnPhase.WAIT_CANDIDATE: self.plan_wait_candidate,
            ReturnPhase.RETRY_SIMPLE: self.plan_retry_simple,
            ReturnPhase.REWIND: self.plan_rewind,
            ReturnPhase.DONE: self.plan_done,
        }
        planner = planners.get(phase)
        if planner is None:
            logging.debug('No planner for return-to-cluster phase %s', phase)
            return []
        return planner(obs)

    def _derive_phase(self, obs: ReturnObservation) -> ReturnPhase:
        """Derive phase from observation (pure)."""
        # Easy way not possible — go straight to rewind.
        if obs.role == 'primary' or is_op_destructive(obs.last_op):
            return ReturnPhase.REWIND

        # Simple switch already failed — check divergence.
        if obs.simple_switch_tried:
            return ReturnPhase.CHECK_DIVERGENCE

        # Try the easy way first.
        return ReturnPhase.SIMPLE_SWITCH

    def plan_init(self, obs: ReturnObservation) -> CommandPlan:
        """INIT: not used directly — _derive_phase always advances past INIT."""
        return []

    def plan_simple_switch(self, obs: ReturnObservation) -> CommandPlan:
        """SIMPLE_SWITCH: attempt simple primary switch, then check divergence."""
        return [
            SimplePrimarySwitch(
                new_primary=obs.new_primary,
                is_dead=obs.is_dead,
                limit=obs.recovery_timeout,
            ),
            CheckDivergence(),
        ]

    def plan_check_divergence(self, obs: ReturnObservation) -> CommandPlan:
        """CHECK_DIVERGENCE: retry if timelines match, rewind if diverged."""
        if timelines_match(obs.local_timeline, obs.zk_timeline):
            logging.info(
                'Simple switch failed but timelines match (local=%s, zk=%s). '
                'Rewind not needed — will retry.',
                obs.local_timeline, obs.zk_timeline,
            )
            plan: CommandPlan = []
            if obs.archive_restore_disabled:
                plan.append(EnsureRestoringWal())
            plan.append(Log(
                message='Return-to-cluster: timelines match, retrying simple switch',
                level='info',
            ))
            return plan

        logging.info(
            'Timelines diverge (local=%s, zk=%s) — pg_rewind required.',
            obs.local_timeline, obs.zk_timeline,
        )
        return self.plan_rewind(obs)

    def plan_wait_candidate(self, obs: ReturnObservation) -> CommandPlan:
        """WAIT_CANDIDATE: no-op, retry next iteration."""
        return [Log(
            message='Return-to-cluster: waiting for candidate %s' % obs.new_primary,
            level='info',
        )]

    def plan_retry_simple(self, obs: ReturnObservation) -> CommandPlan:
        """RETRY_SIMPLE: restore archive recovery, then retry simple switch."""
        plan: CommandPlan = []
        if obs.archive_restore_disabled:
            plan.append(EnsureRestoringWal())
        plan.append(SimplePrimarySwitch(
            new_primary=obs.new_primary,
            is_dead=obs.is_dead,
            limit=obs.recovery_timeout,
        ))
        return plan

    def plan_rewind(self, obs: ReturnObservation) -> CommandPlan:
        """REWIND: mark tried, delegate to RewindFromSource."""
        return [
            SetSimplePrimarySwitchTry(),
            RewindFromSource(
                new_primary=obs.new_primary,
                is_postgresql_dead=obs.is_dead,
                limit=obs.recovery_timeout,
            ),
        ]

    def plan_done(self, obs: ReturnObservation) -> CommandPlan:
        """DONE: terminal."""
        return []
