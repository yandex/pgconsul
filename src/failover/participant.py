# encoding: utf-8
"""Participant-side failover state machine (ADR-0007, ADR-0006).

Pure ``plan(observation)`` API: returns a Command Plan executed by
CommandExecutor. Handles phases: ``registration``/``voting`` (vote),
``winner_selected`` (winner: acquire lock + promote; loser: wait),
``finished`` (wait for coordinator cleanup).

The promotion pipeline stays opaque and persists its host-local command group.
"""

import logging
from typing import Callable

from ..commands import (
    AcquireLock,
    ClearLocalState,
    DisableWalReceiver,
    FailoverTransitionTo,
    Log,
    Plan as CommandPlan,
    Promote,
    ReleaseLock,
    ReturnToCluster,
    Sleep,
    StopTimer,
    WriteElectionVote,
    WriteLastFailoverTime,
)
from .types import (
    FailoverMachineConfig,
    FailoverObservation,
    FailoverPhase,
)

class FailoverParticipantMachine:
    """Participant-side failover state machine (ADR-0007, ADR-0006).

    Every HA replica runs this machine. The node holding
    ``ELECTION_MANAGER_LOCK_PATH`` runs the coordinator machine instead.
    """

    def __init__(
        self,
        config: 'FailoverMachineConfig | None' = None,
        debug_failure: Callable[[str], bool] | None = None,
    ) -> None:
        self._cfg = config or FailoverMachineConfig()
        self._debug_failure: Callable[[str], bool] = debug_failure or (lambda _: False)

    # --- Pure plan() API (ADR-0006) ---

    def plan(self, obs: 'FailoverObservation') -> CommandPlan:
        """Return Command Plan for current observation (pure, no I/O).

        Empty Plan = nothing to do, retry next iteration (ADR-0006 §2).
        """
        planners: dict = {
            FailoverPhase.WALRECEIVER_DISABLING: self.plan_walreceiver_disabling,
            FailoverPhase.REGISTRATION: self.plan_vote,
            FailoverPhase.VOTING: self.plan_vote,
            FailoverPhase.WINNER_SELECTED: self.plan_winner_selected,
            FailoverPhase.PROMOTING: self.plan_promoting,
            FailoverPhase.FINISHED: self.plan_finished,
            FailoverPhase.FAILED: self.plan_failed,
        }
        planner = planners.get(obs.phase)  # type: ignore[arg-type]
        if planner is None:
            logging.debug('No participant-side planner for failover phase %s', obs.phase)
            return []
        return planner(obs)

    def plan_walreceiver_disabling(self, obs: 'FailoverObservation') -> CommandPlan:
        """walreceiver_disabling: sleep (optional) + disable walreceiver.

        Mirrors coordinator's plan_walreceiver_disabling but without
        FailoverTransitionTo — coordinator owns phase transitions.
        Executes unconditionally: even if primary recovered, failover is
        committed and walreceiver must be disabled before voting.
        """
        plan: CommandPlan = []
        sleep_sec = self._cfg.sleep_before_disable_walreceiver
        if sleep_sec:
            plan.append(Log(
                message=f'Sleep for test purposes before disabling walreceiver: {sleep_sec}',
                level='debug',
            ))
            plan.append(Sleep(seconds=sleep_sec))
        plan.append(DisableWalReceiver(timeout=self._cfg.walreceiver_disable_timeout))
        return plan

    def plan_vote(self, obs: 'FailoverObservation') -> CommandPlan:
        """registration/voting: write election vote (idempotent).

        Empty Plan if host_lsn is unavailable (PG dead — retry next iteration).
        """
        if obs.host_lsn is None:
            logging.debug('Cannot vote: host_lsn unavailable')
            return []
        logging.debug('Voting: lsn=%s priority=%s', obs.host_lsn, obs.host_priority)
        return [WriteElectionVote(lsn=obs.host_lsn, priority=obs.host_priority)]

    def plan_winner_selected(self, obs: 'FailoverObservation') -> CommandPlan:
        """winner_selected: winner acquires lock + transitions to promoting.

        Winner: AcquireLock(timeout=0) → FailoverTransitionTo(PROMOTING).
        The actual promote happens in plan_promoting via Promote.
        Non-blocking lock; if held by another, executor stops and retries.

        Loser: wait until the global failover is cleaned up.
        """
        winner = obs.election_winner
        if winner is None:
            logging.warning('winner_selected but no winner recorded, waiting')
            return []

        if winner != obs.my_hostname:
            return self._plan_loser(obs, winner)

        # --- Winner branch ---

        if self._debug_failure('participant_before_acquire'):
            return []

        # Safety: don't promote while still replaying WAL.
        if obs.is_replaying_wal:
            logging.info('Winner selected but still replaying WAL, waiting')
            return []

        # AcquireLock(timeout=0) is non-blocking. Local promotion progress is
        # reset before acquiring the lock for this new election result.
        return [
            ClearLocalState('failover_participant'),
            AcquireLock(timeout=0),
            FailoverTransitionTo(phase=FailoverPhase.PROMOTING),
        ]

    def plan_promoting(self, obs: 'FailoverObservation') -> CommandPlan:
        """promoting: winner retries Promote (idempotent); loser waits."""
        winner = obs.election_winner
        if winner is None:
            return []
        if winner != obs.my_hostname:
            return self._plan_loser(obs, winner)

        if self._debug_failure('participant_before_promote'):
            return [FailoverTransitionTo(phase=FailoverPhase.FAILED)]

        return self._plan_winner_retry(obs)

    def _plan_winner_retry(self, obs: 'FailoverObservation') -> CommandPlan:
        """Winner: resume its host-local promotion command group."""
        return [
            AcquireLock(timeout=0),
            Promote(
                scope='failover_participant',
                start_postgresql=obs.is_postgresql_dead,
            ),
            WriteLastFailoverTime(),
            StopTimer('failover'),
            FailoverTransitionTo(phase=FailoverPhase.FINISHED),
            ClearLocalState('failover_participant'),
        ]

    def plan_finished(self, obs: 'FailoverObservation') -> CommandPlan:
        """finished: wait for coordinator cleanup.

        Winner: empty Plan (already promoted).
        Loser: log and wait; local reconciliation starts after cleanup.
        """
        winner = obs.election_winner
        if winner is None or winner == obs.my_hostname:
            return []
        return self._plan_loser(obs, winner)

    def plan_failed(self, obs: 'FailoverObservation') -> CommandPlan:
        """failed: resolve the winner's primary lock or wait for cleanup."""
        if obs.election_winner == obs.my_hostname and obs.lock_holder == obs.my_hostname:
            if obs.role != 'primary':
                return [
                    ReleaseLock(),
                    ClearLocalState('failover_participant'),
                ]
            return [
                Promote(scope='failover_participant'),
                WriteLastFailoverTime(),
                StopTimer('failover'),
                FailoverTransitionTo(phase=FailoverPhase.FINISHED),
                ClearLocalState('failover_participant'),
            ]
        return [Log(
            message='FAILOVER: election failed, waiting for cleanup',
            level='warning',
            event=True,
        )]

    def _plan_loser(self, obs: 'FailoverObservation', winner: str) -> CommandPlan:
        """Loser branch: follow the winner while failover still blocks iterations."""
        return_plan = self.plan_return_to_cluster(obs)
        if return_plan:
            return return_plan
        return [Log(
            message=f'FAILOVER: winner is {winner}, waiting for cleanup',
            level='warning',
            event=True,
        )]

    @staticmethod
    def plan_return_to_cluster(obs: 'FailoverObservation') -> CommandPlan:
        """Return a loser to the elected winner once it owns the primary lock."""
        winner = obs.election_winner
        if (
            obs.phase in (
                FailoverPhase.WINNER_SELECTED,
                FailoverPhase.PROMOTING,
                FailoverPhase.FINISHED,
            )
            and winner is not None
            and winner != obs.my_hostname
            and obs.lock_holder == winner
            and not (
                obs.role == 'replica'
                and obs.replication_source == winner
            )
            and (obs.role is not None or obs.is_postgresql_dead)
        ):
            return [ReturnToCluster(
                new_primary=winner,
                role=obs.role or obs.previous_role,
                is_postgresql_dead=obs.is_postgresql_dead,
            )]
        return []
