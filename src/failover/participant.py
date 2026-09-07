# encoding: utf-8
"""Participant-side failover state machine (ADR-0007, ADR-0006).

Pure ``decide(observation)`` API: returns a Decision executed by
CommandExecutor. During ``voting`` it publishes a vote; during ``promoting``
the winner waits for primary ownership and resumes promotion.

The promotion pipeline stays opaque and persists its host-local command group.
"""

import logging
from typing import Callable

from ..commands import (
    ClearFailoverDesiredPrimary,
    ClearLocalState,
    Decision,
    Log,
    Plan as CommandPlan,
    PrepareFailoverVote,
    Promote,
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

    Every HA replica runs this machine, including the coordinator host.
    """

    _POST_ELECTION_PHASES = frozenset({
        FailoverPhase.PROMOTING,
        FailoverPhase.RESOLVING_WINNER,
    })

    def __init__(
        self,
        config: 'FailoverMachineConfig | None' = None,
        debug_failure: Callable[[str], bool] | None = None,
    ) -> None:
        self._cfg = config or FailoverMachineConfig()
        self._debug_failure: Callable[[str], bool] = debug_failure or (lambda _: False)

    # --- Pure decision API (ADR-0006) ---

    def decide(self, obs: 'FailoverObservation') -> Decision:
        """Return the current decision (pure, no I/O)."""
        return Decision(
            self._plan(obs),
            obs.phase is not None,
        )

    def _plan(self, obs: 'FailoverObservation') -> CommandPlan:
        """Build the command plan for the current observation.

        Empty Plan = nothing to do, retry next iteration (ADR-0006 §2).
        """
        winner = obs.election_winner
        # request return to cluster for not-winner hosts.
        if (
            obs.phase in self._POST_ELECTION_PHASES
            and winner is not None
            and winner != obs.my_hostname
        ):
            return self._plan_loser(obs, winner)
        if obs.phase == FailoverPhase.CLEANUP:
            return []

        if obs.phase == FailoverPhase.VOTING:
            return self.plan_vote(obs)
        if obs.phase == FailoverPhase.PROMOTING:
            return self.plan_promoting(obs)
        if obs.phase == FailoverPhase.RESOLVING_WINNER:
            return self.plan_resolving_winner(obs)
        return []

    def plan_vote(self, obs: 'FailoverObservation') -> CommandPlan:
        """Fence external WAL sources, then publish this epoch's vote."""
        # A primary that participates in an active failover must fence itself
        # even though it is outside the immutable electorate.
        if (
            obs.failed_primary == obs.my_hostname
            and not obs.is_postgresql_dead
        ):
            return [
                Log('Stopping old primary before failover'),
                StopPostgresql(wait=False),
            ]
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
            obs.switchover_old_primary == obs.my_hostname
        )
        if source_primary_vote and not obs.is_postgresql_dead:
            return [
                Log('Stopping old primary before publishing its switchover source vote'),
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

    def plan_promoting(self, obs: 'FailoverObservation') -> CommandPlan:
        """promoting: winner retries Promote (idempotent); loser waits."""
        winner = obs.election_winner
        if winner is None:
            return []
        if obs.failover_version is None:
            return []

        if self._debug_failure('participant_before_promote'):
            return [WriteFailoverParticipantState('failed', obs.failover_version)]

        return self._plan_winner_retry(obs)

    def _plan_winner_retry(self, obs: 'FailoverObservation') -> CommandPlan:
        """Winner: resume its host-local promotion command group."""
        failover_version = obs.failover_version
        if failover_version is None or not self._has_primary_ownership(obs):
            return []
        return [
            Promote(
                scope='failover_participant',
                start_postgresql=obs.is_postgresql_dead,
                failover_version=failover_version,
            ),
            WriteFailoverParticipantState('promoted', failover_version),
            ClearLocalState('failover_participant'),
        ]

    def plan_resolving_winner(self, obs: 'FailoverObservation') -> CommandPlan:
        """Resolve the failed winner's primary lock or wait for cleanup."""
        if obs.election_winner == obs.my_hostname and obs.lock_holder == obs.my_hostname:
            if obs.failover_version is None:
                return []
            if obs.role != 'primary':
                return [
                    ClearFailoverDesiredPrimary(
                        obs.my_hostname,
                        obs.failover_version,
                    ),
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
            message='FAILOVER: winner resolution in progress',
            level='warning',
            event=True,
        )]

    @staticmethod
    def _has_primary_ownership(obs: 'FailoverObservation') -> bool:
        """Whether this observation still authorizes the winner to promote."""
        return (
            obs.failover_version is not None
            and obs.lock_holder == obs.my_hostname
            and obs.desired_hostname == obs.my_hostname
            and obs.desired_operation_id == obs.failover_version
            and obs.desired_operation_type == 'failover'
        )

    def _plan_loser(self, obs: 'FailoverObservation', winner: str) -> CommandPlan:
        """Loser branch: follow the winner while failover still blocks iterations."""
        request_plan = self._plan_request_return_to_cluster(obs, winner)
        if request_plan:
            return request_plan
        return [Log(
            message=f'FAILOVER: winner is {winner}, waiting for cleanup',
            level='warning',
            event=True,
        )]

    @staticmethod
    def _plan_request_return_to_cluster(
        obs: 'FailoverObservation',
        winner: str,
    ) -> CommandPlan:
        """Request that a loser return once the winner owns the primary lock."""
        if (
            obs.winner_status == 'promoted'
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
                start_source='primary',
            )]
        return []
