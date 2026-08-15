# encoding: utf-8
"""
Return-to-cluster state machine (MDB-41951, ADR-0006).

Pure plan(observation) API. Stateless: phase is re-derived from the
observation each call. Distinguishes transient simple-switch failures
from real WAL divergence to avoid unnecessary pg_rewind.
"""

import logging

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


class ReturnToClusterMachine:
    """Return-to-cluster state machine (ADR-0006). Pure plan(), no I/O."""

    def __init__(self, config: 'ReturnMachineConfig | None' = None) -> None:
        self._cfg = config or ReturnMachineConfig()

    def plan(self, obs: ReturnObservation) -> CommandPlan:
        """Return the Command Plan for the current observation (pure, no I/O)."""
        phase = self._derive_phase(obs)
        match phase:
            case ReturnPhase.SIMPLE_SWITCH:
                return self.plan_simple_switch(obs)
            case ReturnPhase.CHECK_DIVERGENCE:
                return self.plan_check_divergence(obs)
            case ReturnPhase.REWIND:
                return self.plan_rewind(obs)
        logging.debug('No planner for return-to-cluster phase %s', phase)
        return []

    def _derive_phase(self, obs: ReturnObservation) -> ReturnPhase:
        """Derive phase from observation (pure)."""
        # Easy way not possible — go straight to rewind.
        # When PG is dead, role is None even for a former primary.
        # Use fallback_role (previous role from dead_iter) to detect
        # former primaries and force REWIND instead of SIMPLE_SWITCH.
        effective_role = obs.role or obs.fallback_role
        if effective_role == 'primary' or is_op_destructive(obs.last_op):
            return ReturnPhase.REWIND

        # Simple switch already failed — check divergence.
        if obs.simple_switch_tried:
            return ReturnPhase.CHECK_DIVERGENCE

        # Try the easy way first.
        return ReturnPhase.SIMPLE_SWITCH

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
