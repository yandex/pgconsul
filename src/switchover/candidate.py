# encoding: utf-8
"""Candidate-side switchover state machine (ADR-0005 §3, ADR-0006).

Pure ``plan(observation)`` API: returns a Command Plan executed by
CommandExecutor. Handles phases: ``initiated`` (create slots, wait for
side replicas), ``candidate_found`` (acquire lock, promote, cleanup).
"""

import logging
from typing import Callable

from ..commands import (
    AcquireLock,
    ClearLocalState,
    CleanupSwitchover,
    CreateSlots,
    Log,
    Plan as CommandPlan,
    Promote,
    ReleaseLock,
    StartTimer,
    StopTimer,
    TransitionTo,
    WriteLastSwitchoverTime,
)
from ..types import is_timed_out
from .types import (
    SwitchoverMachineConfig,
    SwitchoverObservation,
    SwitchoverPhase,
)

class CandidateSwitchoverMachine:
    """Candidate-side switchover state machine (ADR-0005 §3, ADR-0006)."""

    # Phases where candidate waits for old primary to release the lock —
    # timeout gate short-circuits to FAILED after primary_shut_timeout.
    _PRIMARY_SHUT_WAIT_PHASES = frozenset({
        SwitchoverPhase.CANDIDATE_FOUND,
        SwitchoverPhase.POOLER_STOPPED,
        SwitchoverPhase.PG_STOPPED,
        SwitchoverPhase.PRIMARY_SHUT,
    })

    def __init__(
        self,
        config: 'SwitchoverMachineConfig | None' = None,
        debug_failure: Callable[[str], bool] | None = None,
    ) -> None:
        self._cfg = config or SwitchoverMachineConfig()
        self._debug_failure: Callable[[str], bool] = debug_failure or (lambda _: False)

    # --- Pure plan() API (ADR-0006) ---

    def plan(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """Return Command Plan for current observation (pure, no I/O).

        Empty Plan = nothing to do, retry next iteration (ADR-0006 §2).
        """
        # Timeout gate: if old primary didn't release lock in time → FAILED.
        if obs.record.phase in self._PRIMARY_SHUT_WAIT_PHASES and is_timed_out(
            obs.downtime_started_ts, self._cfg.primary_shut_timeout, 'Old primary lock release'
        ):
            return [TransitionTo(SwitchoverPhase.FAILED)]

        match obs.record.phase:
            case SwitchoverPhase.INITIATED:
                return self.plan_initiated(obs)
            case (
                SwitchoverPhase.CANDIDATE_FOUND
                | SwitchoverPhase.POOLER_STOPPED
                | SwitchoverPhase.PG_STOPPED
                | SwitchoverPhase.PRIMARY_SHUT
            ):
                # Old primary shutting down or released lock — keep attempting
                # non-blocking lock acquire (AcquireLock timeout=0 is safe).
                return self.plan_candidate_found(obs)
            case SwitchoverPhase.CANDIDATE_ACQUIRED:
                return self.plan_candidate_acquired(obs)
            case SwitchoverPhase.PROMOTED:
                return self.plan_promoted(obs)
            case SwitchoverPhase.FAILED:
                return self.plan_failed(obs)
            case _:
                logging.debug('No candidate-side planner for switchover phase %s', obs.record.phase)
                return []

    def plan_initiated(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """initiated → candidate_found: create slots, check side replicas turned (non-blocking).

        Emits 'SWITCHOVER STARTED' as a structured event on first entry — the
        entry-point log expected by behave tests (lost during ADR-0006 migration
        from the old _accept_switchover). CreateSlots is idempotent (emitted
        every iteration); TransitionTo(CANDIDATE_FOUND) only when all side
        replicas turned. Returns CreateSlots-only Plan when waiting.
        """
        started = Log(message='SWITCHOVER STARTED', level='warning', event=True)

        side_replicas = tuple(obs.record.side_replicas)

        if not side_replicas:  # No side replicas → transition immediately.
            return [started, TransitionTo(SwitchoverPhase.CANDIDATE_FOUND)]

        plan: CommandPlan = [started, CreateSlots(hosts=side_replicas)]  # Idempotent.

        # False = not yet turned (or read error), both retry next iteration.
        if not obs.all_side_replicas_turned:
            logging.info('Waiting for side replicas to turn to candidate')
            return plan

        logging.info('All side replicas turned to candidate, signaling primary')
        plan.append(TransitionTo(SwitchoverPhase.CANDIDATE_FOUND))
        return plan

    def plan_candidate_found(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """candidate_found → promoted: acquire lock, promote, cleanup.

        Lock acquisition strategy depends on the current phase (MDB-41951 race fix):
        - CANDIDATE_FOUND / POOLER_STOPPED / PG_STOPPED: non-blocking (timeout=0).
          Old primary is still active; do not block the iteration.
        - PRIMARY_SHUT: blocking (timeout=primary_shut_acquire_timeout).
          Old primary has already released (or is about to release) the lock.
          Using timeout=0 wastes ~7-8 seconds per iteration under network latency,
          which under CLI timeout=60s leaves only 4-5 attempts total.

        Promote is opaque — executor releases lock on failure.
        """
        if self._debug_failure('candidate_switchover_before_acquire'):  # ADR-0006 §6.
            return []

        # In PRIMARY_SHUT the old primary guarantees immediate lock release —
        # use a blocking acquire so we don't waste a full iteration cycle.
        if obs.record.phase == SwitchoverPhase.PRIMARY_SHUT:
            acquire_timeout = self._cfg.primary_shut_acquire_timeout
        else:
            acquire_timeout = 0  # Non-blocking for all pre-shutdown phases.

        plan: CommandPlan = [ClearLocalState('switchover_candidate')]
        if obs.lock_holder != obs.my_hostname:
            plan.append(AcquireLock(allow_queue=True, timeout=acquire_timeout))

        # CANDIDATE_ACQUIRED before promote — MDB-41951 race fix: old primary
        # checks for PROMOTED before rewinding, preventing premature rewind.
        plan.append(TransitionTo(SwitchoverPhase.CANDIDATE_ACQUIRED))

        if obs.downtime_started_ts is None:
            plan.append(StartTimer('downtime'))

        old_primary = obs.record.hostname
        if old_primary is None:
            logging.error(
                'Switchover %s: switchover primary info has no hostname, aborting',
                obs.record.phase,
            )
            plan.append(ReleaseLock())
            return plan

        plan.extend(self._plan_promotion(old_primary))
        return plan

    def plan_candidate_acquired(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """Resume candidate-local promotion while keeping the global fence."""
        plan: CommandPlan = []
        if obs.lock_holder != obs.my_hostname:
            plan.append(AcquireLock(allow_queue=True, timeout=self._cfg.primary_shut_acquire_timeout))

        old_primary = obs.record.hostname
        if old_primary is None:
            return [ReleaseLock(), TransitionTo(SwitchoverPhase.FAILED)]

        plan.extend(self._plan_promotion(old_primary))
        return plan

    @staticmethod
    def _plan_promotion(old_primary: str) -> CommandPlan:
        return [
            Promote(scope='switchover_candidate', old_primary=old_primary),
            TransitionTo(SwitchoverPhase.PROMOTED),
            WriteLastSwitchoverTime(),
            StopTimer('switchover'),
            CleanupSwitchover(),
        ]

    def plan_promoted(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """Finish candidate-side metadata cleanup after a restart."""
        return [
            WriteLastSwitchoverTime(),
            StopTimer('switchover'),
            CleanupSwitchover(),
        ]

    def plan_failed(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """Resolve a failed candidate's primary lock before global cleanup."""
        if obs.lock_holder != obs.my_hostname:
            return []
        if obs.role != 'primary':
            return [
                ReleaseLock(),
                ClearLocalState('switchover_candidate'),
            ]
        return [
            Promote(
                scope='switchover_candidate',
                old_primary=obs.record.hostname,
            ),
            WriteLastSwitchoverTime(),
            StopTimer('switchover'),
            CleanupSwitchover(),
        ]
