# encoding: utf-8
"""Coordinator-side failover state machine (ADR-0007, ADR-0006).

Pure ``decide(observation)`` API: returns a Decision executed by
CommandExecutor. The coordinator holds ``ELECTION_MANAGER_LOCK_PATH`` and
drives voting, winner selection, promotion-result handling, and cleanup.

Blocking ``sleep`` is replaced by "no condition → empty Plan → retry next
iteration" (ADR-0007 §2).
"""

import logging

from ..commands import (
    ClearFailoverDesiredPrimary,
    CleanupFailover,
    Decision,
    FailoverTransitionTo,
    ForceReleasePrimaryLock,
    Log,
    Plan as CommandPlan,
    StartTimer,
    StopTimer,
    WriteElectionWinner,
    WriteLastFailedFailoverTime,
    WriteLastFailoverTime,
)
from ..types import DurabilityConfig, is_timed_out
from .types import (
    FailoverMachineConfig,
    FailoverObservation,
    FailoverPhase,
)

class FailoverCoordinatorMachine:
    """Coordinator-side failover state machine (ADR-0007, ADR-0006).

    The node holding ``ELECTION_MANAGER_LOCK_PATH`` collects votes, checks
    quorum/promote-safe, and writes the winner.
    """

    def __init__(self, config: 'FailoverMachineConfig | None' = None) -> None:
        self._cfg = config or FailoverMachineConfig()

    # --- Pure decision API (ADR-0006) ---

    def decide(self, obs: 'FailoverObservation') -> Decision:
        """Return one nonblocking coordinator decision (pure, no I/O)."""
        return Decision(
            self._plan(obs),
            False,
        )

    def _plan(self, obs: 'FailoverObservation') -> CommandPlan:
        """Build the command plan for the current observation.

        Empty Plan = nothing to do, retry next iteration.
        """
        if obs.phase == FailoverPhase.VOTING and is_timed_out(
            obs.failover_started_ts, self._cfg.failover_timeout, 'Failover election',
            now=obs.current_time,
        ):
            return [
                WriteLastFailedFailoverTime(),
                FailoverTransitionTo(phase=FailoverPhase.CLEANUP),
            ]
        # Timeout gate: resolve ownership if winner stalls beyond
        # promote_timeout (ADR-0007 §2).
        if obs.phase == FailoverPhase.PROMOTING and is_timed_out(
            obs.promote_started_ts, self._cfg.promote_timeout, 'Winner promote',
            now=obs.current_time,
        ):
            return [FailoverTransitionTo(phase=FailoverPhase.RESOLVING_WINNER)]

        if obs.phase == FailoverPhase.VOTING:
            return self.plan_voting(obs)
        if obs.phase == FailoverPhase.PROMOTING:
            return self.plan_promoting(obs)
        if obs.phase == FailoverPhase.RESOLVING_WINNER:
            return self.plan_resolving_winner(obs)
        if obs.phase == FailoverPhase.CLEANUP:
            return self.plan_cleanup(obs)
        return []

    @staticmethod
    def authorized_timeline(obs: 'FailoverObservation') -> int | None:
        """Choose the only branch on which this election may continue.

        A host that already voted on another timeline is fenced and cannot
        have acknowledged a later commit on new master. An absent vote is treated
        conservatively: that host may still contain such a commit.
        """
        target = obs.switchover_target_timeline
        source = obs.switchover_source_timeline
        if target is None or source is None:
            return obs.zk_timeline
        if not obs.switchover_handoff_committed:
            return source

        members = set(obs.switchover_commit_members)
        target_votes = {
            host for host in members
            if obs.vote_timelines.get(host) == target
        }
        non_voters = members - set(obs.vote_timelines)
        if len(target_votes | non_voters) >= obs.switchover_commit_required:
            return target
        return source

    @classmethod
    def _timeline_votes(
        cls,
        obs: 'FailoverObservation',
    ) -> dict[str, int]:
        timeline = cls.authorized_timeline(obs)
        return {
            host: vote for host, vote in obs.votes.items()
            if obs.vote_timelines.get(host) == timeline
        }

    @classmethod
    def _failed_writer(cls, obs: 'FailoverObservation') -> str | None:
        if (
            obs.switchover_handoff_committed
            and cls.authorized_timeline(obs) == obs.switchover_source_timeline
        ):
            return obs.switchover_old_primary
        if obs.switchover_handoff_committed:
            return obs.switchover_candidate
        return obs.failed_primary

    @classmethod
    def _durability_quorums(
        cls,
        obs: 'FailoverObservation',
    ) -> tuple['DurabilityConfig', ...]:
        if (
            obs.switchover_handoff_committed
            and cls.authorized_timeline(obs) == obs.switchover_source_timeline
        ):
            return obs.switchover_source_durability_quorums
        return obs.durability_quorums

    def _is_election_valid(self, obs: 'FailoverObservation') -> bool:
        """Require a read quorum for every possibly active SSN."""
        authorized_timeline = self.authorized_timeline(obs)
        if (
            obs.switchover_handoff_committed
            and authorized_timeline == obs.switchover_source_timeline
            and self._source_primary_has_vote(obs)
        ):
            return True
        authorized_timeline_votes = self._timeline_votes(obs)
        configs = self._durability_quorums(obs)
        if not configs:
            return False
        failed_writer = self._failed_writer(obs)
        if failed_writer is None:
            return False
        target_timeline_authorized = authorized_timeline == obs.switchover_target_timeline
        for config in configs:
            replicas = set(config.members) - {failed_writer}
            required = len(replicas) - config.required + 1
            # An old-branch vote is still useful: after fencing it proves that
            # this host cannot hide a commit from the target branch.
            counted_votes = obs.votes if target_timeline_authorized else authorized_timeline_votes
            voted = replicas & set(counted_votes)
            if len(voted) < required:
                logging.info(
                    'Waiting for durability read quorum %s: %d < %d',
                    sorted(config.members), len(voted), required,
                )
                return False
        return True

    @staticmethod
    def _source_primary_has_vote(obs: 'FailoverObservation') -> bool:
        """A fenced source vote makes old primary P a special safe winner."""
        return bool(
            obs.switchover_old_primary is not None
            and obs.switchover_old_primary in obs.votes
            and obs.vote_timelines.get(obs.switchover_old_primary)
            == obs.switchover_source_timeline
        )

    @staticmethod
    def _manual_winner_has_vote(obs: 'FailoverObservation') -> bool:
        return bool(
            obs.manual_data_loss
            and obs.manual_winner is not None
            and obs.manual_winner in obs.electorate
            and obs.manual_winner in obs.votes
        )

    @classmethod
    def _candidate_is_safe(cls, obs: 'FailoverObservation', candidate: str) -> bool:
        votes = cls._timeline_votes(obs)
        vote = votes.get(candidate)
        if vote is None:
            return False
        candidate_lsn = vote
        configs = cls._durability_quorums(obs)
        if not configs:
            return False
        failed_writer = cls._failed_writer(obs)
        if failed_writer is None:
            return False
        authorized_timeline = cls.authorized_timeline(obs)
        for config in configs:
            replicas = set(config.members) - {failed_writer}
            required = len(replicas) - config.required + 1
            if authorized_timeline == obs.switchover_target_timeline:
                dominated_current_timeline = sum(
                    1 for host in replicas
                    if host in obs.votes
                    and obs.vote_timelines.get(host) == obs.switchover_target_timeline
                    and obs.votes[host] <= candidate_lsn
                )
                dominated_prev_timeline = sum(
                    1 for host in replicas
                    if host in obs.votes
                    and obs.vote_timelines.get(host) == obs.switchover_source_timeline
                )
                dominated = dominated_current_timeline + dominated_prev_timeline
            else:
                dominated = sum(
                    1 for host in replicas
                    if host in obs.votes
                    and obs.vote_timelines.get(host) == authorized_timeline
                    and obs.votes[host] <= candidate_lsn
                )
            if dominated < required:
                return False
        return True

    def _determine_safe_winner(self, obs: 'FailoverObservation') -> str | None:
        if (
            obs.switchover_handoff_committed
            and self.authorized_timeline(obs) == obs.switchover_source_timeline
            and self._source_primary_has_vote(obs)
        ):
            return obs.switchover_old_primary
        votes = self._timeline_votes(obs)
        candidates = set(obs.electorate)
        configs = self._durability_quorums(obs)
        if configs:
            candidates &= {
                host
                for config in configs
                for host in config.members
            }
        ordered = sorted(
            (host for host in votes if host in candidates),
            key=lambda host: (-votes[host], host),
        )
        for host in ordered:
            if self._candidate_is_safe(obs, host):
                return host
        return None

    # --- Phase planners ---

    def plan_voting(self, obs: 'FailoverObservation') -> CommandPlan:
        """Start timers, wait for safe votes, and persist the winner."""
        plan: CommandPlan = []

        if obs.failover_started_ts is None:
            plan.append(StartTimer('failover'))
        if obs.downtime_started_ts is None:
            plan.append(StartTimer('downtime'))
        if self.authorized_timeline(obs) is None:
            logging.info('Waiting for an authorized failover timeline')
            return plan

        if obs.manual_data_loss:
            if not self._manual_winner_has_vote(obs):
                logging.info('Waiting for the operator to select a voted host')
                return plan
            winner = obs.manual_winner
        else:
            if not self._is_election_valid(obs):
                logging.info('Waiting for every durability read quorum')
                return plan
            winner = self._determine_safe_winner(obs)
        if winner is None:
            logging.info('Waiting for a candidate safe for every durability quorum')
            return plan

        if obs.lock_holder is not None and obs.lock_holder != winner:
            if not self._cfg.force_release_primary_lock:
                logging.info(
                    'Forced leader-lock release is disabled; waiting for old primary %s',
                    obs.lock_holder,
                )
                return plan
            if not is_timed_out(
                obs.failover_started_ts,
                self._cfg.primary_unavailability_timeout,
                'Old primary lock release',
                now=obs.current_time,
            ):
                logging.info(
                    'Waiting for old primary %s to release the leader lock',
                    obs.lock_holder,
                )
                return plan
            logging.warning(
                'Forcing stale primary %s to release the leader lock',
                obs.lock_holder,
            )
            plan.append(ForceReleasePrimaryLock(obs.lock_holder))
            return plan

        logging.info('Elected winner: %s', winner)
        plan.extend([
            WriteElectionWinner(winner=winner),
            FailoverTransitionTo(phase=FailoverPhase.PROMOTING),
        ])
        return plan

    def plan_promoting(self, obs: 'FailoverObservation') -> CommandPlan:
        """Time the ownership/promotion wait and process the winner result."""
        plan: CommandPlan = []
        if obs.promote_started_ts is None:
            plan.append(StartTimer('failover_promote'))
        if obs.winner_status == 'failed':
            plan.append(FailoverTransitionTo(FailoverPhase.RESOLVING_WINNER))
            return plan
        if obs.winner_status == 'promoted':
            plan.extend([
                WriteLastFailoverTime(),
                FailoverTransitionTo(FailoverPhase.CLEANUP),
            ])
            return plan
        logging.debug('Coordinator: waiting for winner promotion status')
        return plan

    def plan_resolving_winner(self, obs: 'FailoverObservation') -> CommandPlan:
        """Wait until a failed winner can no longer own primary state."""
        if obs.winner_status == 'promoted':
            return [
                WriteLastFailoverTime(),
                FailoverTransitionTo(FailoverPhase.CLEANUP),
            ]
        if obs.election_winner is not None and obs.lock_holder == obs.election_winner:
            logging.warning('FAILOVER: waiting for winner %s to resolve primary lock', obs.election_winner)
            return []
        if obs.election_winner is not None and obs.failover_version is not None:
            return [
                ClearFailoverDesiredPrimary(
                    obs.election_winner,
                    obs.failover_version,
                ),
                WriteLastFailedFailoverTime(),
                FailoverTransitionTo(FailoverPhase.CLEANUP),
            ]
        return [
            WriteLastFailedFailoverTime(),
            FailoverTransitionTo(FailoverPhase.CLEANUP),
        ]

    @staticmethod
    def plan_cleanup(obs: 'FailoverObservation') -> CommandPlan:
        plan: CommandPlan = [
            Log(
                message='FAILOVER: cleaning up',
                level='warning',
                event=True,
            ),
        ]
        if obs.downtime_started_ts is not None:
            plan.append(StopTimer('downtime'))
        if obs.failover_started_ts is not None:
            plan.append(StopTimer('failover'))
        if obs.promote_started_ts is not None:
            plan.append(StopTimer('failover_promote'))
        plan.append(CleanupFailover())
        return plan
