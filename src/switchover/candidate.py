# encoding: utf-8
"""Candidate-side switchover state machine (ADR-0005 §3, ADR-0006).

Pure ``plan(observation)`` API: returns a Command Plan executed by
CommandExecutor. Handles phases: ``initiated`` (create slots, wait for
side replicas), ``candidate_found`` (acquire lock, promote, cleanup).
"""

import logging
from typing import TYPE_CHECKING, Callable

from ..commands import (
    AcquireLock,
    CleanupSwitchover,
    CreateSlots,
    DoFailover,
    Log,
    Plan as CommandPlan,
    ReleaseLock,
    StartTimer,
    StopTimer,
    TransitionTo,
    WriteLastSwitchoverTime,
)
from ..log_formatters import log_event
from .types import (
    SwitchoverMachineConfig,
    SwitchoverObservation,
    SwitchoverPhase,
)

if TYPE_CHECKING:
    from ..zk import Zookeeper


class CandidateSwitchoverMachine:
    """Candidate-side switchover state machine (ADR-0005 §3, ADR-0006)."""

    def __init__(
        self,
        zk: 'Zookeeper',
        config: 'SwitchoverMachineConfig | None' = None,
        debug_failure: Callable[[str], bool] | None = None,
    ) -> None:
        self._zk = zk
        self._cfg = config or SwitchoverMachineConfig()
        self._debug_failure: Callable[[str], bool] = debug_failure or (lambda _: False)

    # --- Core machine API ---

    def transition_to(self, phase: SwitchoverPhase) -> bool:
        """Persist phase to ZK before the action (ADR-0005 §3). False on write fail."""
        if not self._zk.write_switchover_state(phase):
            logging.error('Failed to persist switchover phase %s to ZK', phase)
            return False
        log_event(f'SWITCHOVER PHASE → {phase}', level='warning')
        return True

    # --- Pure plan() API (ADR-0006) ---

    def plan(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """Return Command Plan for current observation (pure, no I/O).

        Empty Plan = nothing to do, retry next iteration (ADR-0006 §2).
        """
        planners: dict = {
            SwitchoverPhase.INITIATED: self.plan_initiated,
            SwitchoverPhase.CANDIDATE_FOUND: self.plan_candidate_found,
            # Old primary shutting down — keep attempting non-blocking lock acquire.
            # AcquireLock timeout=0 is safe (lock still held → just retries).
            SwitchoverPhase.POOLER_STOPPED: self.plan_candidate_found,
            SwitchoverPhase.PG_STOPPED: self.plan_candidate_found,
            # Old primary released lock — acquire and promote.
            SwitchoverPhase.PRIMARY_SHUT: self.plan_candidate_found,
            # Lock held, promote in progress — failed-promote guard detects.
            SwitchoverPhase.CANDIDATE_ACQUIRED: self.plan_candidate_found,
        }
        planner = planners.get(obs.record.phase)  # type: ignore[arg-type]
        if planner is None:
            logging.debug('No candidate-side planner for switchover phase %s', obs.record.phase)
            return []
        return planner(obs)

    def plan_initiated(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """initiated → candidate_found: create slots, check side replicas turned (non-blocking).

        CreateSlots is idempotent (emitted every iteration); TransitionTo(CANDIDATE_FOUND)
        only when all side replicas turned. Returns CreateSlots-only Plan when waiting.
        """
        side_replicas = list(obs.side_replicas)

        if not side_replicas:  # No side replicas → transition immediately.
            return [TransitionTo(SwitchoverPhase.CANDIDATE_FOUND)]

        plan: CommandPlan = [CreateSlots(hosts=side_replicas)]  # Idempotent.

        # None = read error, False = not yet turned — both retry next iteration.
        if obs.all_side_replicas_turned is not True:
            logging.info('Waiting for side replicas to turn to candidate')
            return plan

        logging.info('All side replicas turned to candidate, signaling primary')
        plan.append(TransitionTo(SwitchoverPhase.CANDIDATE_FOUND))
        return plan

    def plan_candidate_found(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """candidate_found → promoted: acquire lock, do_failover, cleanup.

        Non-blocking lock acquisition (timeout=0); if held, executor stops at
        AcquireLock and retries next iteration. DoFailover is opaque — executor
        releases lock on failure (post-condition of the command).
        """
        if self._debug_failure('candidate_switchover_before_acquire'):  # ADR-0006 §6.
            return []

        # Failed promote: we hold the lock but phase is still candidate_found /
        # primary_shut / candidate_acquired — previous DoFailover failed (executor
        # stops on failure, lock never released). Abort to avoid infinite retry.
        if obs.lock_holder == obs.my_hostname:
            logging.error(
                'Switchover %s: lock already held by us but '
                'promote did not succeed — aborting switchover (releasing lock)',
                obs.record.phase,
            )
            return [
                ReleaseLock(),
                TransitionTo(SwitchoverPhase.FAILED),
            ]

        plan: CommandPlan = [AcquireLock(allow_queue=True, timeout=0)]  # Non-blocking.

        if obs.switchover_primary_info is None:
            logging.error('Failed to get switchover primary info from ZK.')
            plan.append(ReleaseLock())
            return plan

        # CANDIDATE_ACQUIRED before promote — MDB-41951 race fix: old primary
        # checks for PROMOTED before rewinding, preventing premature rewind.
        plan.append(TransitionTo(SwitchoverPhase.CANDIDATE_ACQUIRED))

        if not obs.downtime_timer_started:  # Idempotent (old primary may not have started it).
            plan.append(StartTimer('downtime'))

        old_primary = obs.switchover_primary_info.get('hostname')

        plan.append(DoFailover(old_primary=old_primary))  # Opaque; executor releases lock on failure.

        plan.append(TransitionTo(SwitchoverPhase.PROMOTED))  # Observability marker.

        plan.append(CleanupSwitchover())
        plan.append(WriteLastSwitchoverTime())
        plan.append(StopTimer('switchover'))
        return plan
