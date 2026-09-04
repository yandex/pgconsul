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
    Log,
    Plan as CommandPlan,
    PrepareFailoverVote,
    Promote,
    ReleaseLock,
    RequestReturnToCluster,
    Sleep,
    StopPostgresql,
    WriteFailoverParticipantState,
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
            FailoverPhase.WALRECEIVER_DISABLING: self.plan_vote,
            FailoverPhase.GATES_PASSED: self.plan_vote,
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

    def plan_vote(self, obs: 'FailoverObservation') -> CommandPlan:
        """Fence external WAL sources, then publish this epoch's vote."""
        if obs.my_hostname not in obs.electorate:
            logging.debug('Host is outside the immutable failover electorate')
            return []
        if obs.my_hostname in obs.votes:
            return []
        if obs.failover_version is None:
            logging.debug('Cannot vote without a failover epoch')
            return []
        if obs.local_timeline is None:
            logging.warning('Cannot vote from an unknown timeline')
            return []
        source_primary_vote = bool(
            obs.branch_old_primary == obs.my_hostname
        )
        if source_primary_vote and not obs.is_postgresql_dead:
            return [
                Log('Stopping old primary before publishing its branch vote'),
                StopPostgresql(wait=False),
            ]
        timeline_matches = obs.local_timeline == obs.zk_timeline
        if not timeline_matches and not obs.allow_mismatched_timeline_votes:
            logging.warning('Cannot vote from a different timeline')
            return []
        plan: CommandPlan = []
        if self._cfg.sleep_before_disable_walreceiver:
            plan.extend([
                Log(
                    message=(
                        'Sleep for test purposes before disabling walreceiver: '
                        f'{self._cfg.sleep_before_disable_walreceiver}'
                    ),
                    level='debug',
                ),
                Sleep(self._cfg.sleep_before_disable_walreceiver),
            ])
        plan.append(PrepareFailoverVote(
            walreceiver_timeout=self._cfg.walreceiver_disable_timeout,
            failover_version=obs.failover_version,
            lsn_read_sleep=self._cfg.election_lsn_read_sleep,
            timeline_only=source_primary_vote,
            fence_wal_sources=obs.manual_fence_wal_sources,
        ))
        return plan

    def plan_winner_selected(self, obs: 'FailoverObservation') -> CommandPlan:
        """winner_selected: winner acquires lock + transitions to promoting.

        Winner acquires the primary lock. Only the coordinator advances the
        global phase after observing the lock holder.
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
            AcquireLock(
                timeout=0,
                desired_operation_id=obs.failover_version,
                desired_hostname=obs.my_hostname,
            ),
        ]

    def plan_promoting(self, obs: 'FailoverObservation') -> CommandPlan:
        """promoting: winner retries Promote (idempotent); loser waits."""
        winner = obs.election_winner
        if winner is None:
            return []
        if winner != obs.my_hostname:
            return self._plan_loser(obs, winner)
        if obs.failover_version is None:
            return []

        if self._debug_failure('participant_before_promote'):
            return [WriteFailoverParticipantState('failed', obs.failover_version)]

        return self._plan_winner_retry(obs)

    def _plan_winner_retry(self, obs: 'FailoverObservation') -> CommandPlan:
        """Winner: resume its host-local promotion command group."""
        if obs.failover_version is None:
            return []
        return [
            AcquireLock(
                timeout=0,
                desired_operation_id=obs.failover_version,
                desired_hostname=obs.my_hostname,
            ),
            Promote(
                scope='failover_participant',
                start_postgresql=obs.is_postgresql_dead,
                failover_version=obs.failover_version,
            ),
            WriteFailoverParticipantState('promoted', obs.failover_version),
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
            if obs.failover_version is None:
                return []
            if obs.role != 'primary':
                return [
                    ReleaseLock(),
                    ClearLocalState('failover_participant'),
                ]
            return [
                Promote(
                    scope='failover_participant',
                    failover_version=obs.failover_version,
                ),
                WriteFailoverParticipantState('promoted', obs.failover_version),
                ClearLocalState('failover_participant'),
            ]
        return [Log(
            message='FAILOVER: election failed, waiting for cleanup',
            level='warning',
            event=True,
        )]

    def _plan_loser(self, obs: 'FailoverObservation', winner: str) -> CommandPlan:
        """Loser branch: follow the winner while failover still blocks iterations."""
        request_plan = self.plan_request_return_to_cluster(obs)
        if request_plan:
            return request_plan
        return [Log(
            message=f'FAILOVER: winner is {winner}, waiting for cleanup',
            level='warning',
            event=True,
        )]

    @staticmethod
    def plan_request_return_to_cluster(obs: 'FailoverObservation') -> CommandPlan:
        """Request that a loser return once the winner owns the primary lock."""
        winner = obs.election_winner
        if (
            (
                obs.phase == FailoverPhase.FINISHED
                or obs.winner_status == 'promoted'
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
            return [RequestReturnToCluster(
                new_primary=winner,
                role=obs.role or obs.previous_role,
                is_postgresql_dead=obs.is_postgresql_dead,
                start_source='primary',
            )]
        return []
