# encoding: utf-8
"""Coordinator-side failover state machine (ADR-0007, ADR-0006).

Pure ``plan(observation)`` API: returns a Command Plan executed by
CommandExecutor. The coordinator holds ``ELECTION_MANAGER_LOCK_PATH`` and
drives phases: gate checks, registration, voting, winner selection.

Blocking ``sleep`` is replaced by "no condition → empty Plan → retry next
iteration" (ADR-0007 §2).
"""

import logging
from typing import Callable

from ..commands import (
    CleanupFailover,
    FailoverTransitionTo,
    Log,
    Plan as CommandPlan,
    PrepareFailoverVote,
    Sleep,
    StartTimer,
    StopTimer,
    WriteElectionWinner,
    WriteLastFailoverTime,
)
from ..types import is_timed_out
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

    # Phases where coordinator waits for winner — timeout gate short-circuits
    # to FAILED after promote_timeout (ADR-0007 §2).
    # WINNER_SELECTED is included: if the winner is dead it never acquires the
    # primary lock, so the timer must cover the lock-acquire wait too.
    _PROMOTE_WAIT_PHASES = frozenset({
        FailoverPhase.WINNER_SELECTED,
        FailoverPhase.PROMOTING,
    })

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

        Empty Plan = nothing to do, retry next iteration.
        """
        if obs.must_reset:
            if (
                obs.phase != FailoverPhase.FINISHED
                and obs.election_winner is not None
                and obs.lock_holder == obs.election_winner
            ):
                return []
            return self._plan_cleanup(obs, 'FAILOVER: resuming interrupted cleanup')

        planners: dict = {
            FailoverPhase.WALRECEIVER_DISABLING: self.plan_walreceiver_disabling,
            FailoverPhase.GATES_PASSED: self.plan_gates_passed,
            FailoverPhase.REGISTRATION: self.plan_registration,
            FailoverPhase.VOTING: self.plan_voting,
            FailoverPhase.WINNER_SELECTED: self.plan_winner_selected,
            FailoverPhase.PROMOTING: self.plan_promoting,
            FailoverPhase.FINISHED: self.plan_finished,
            FailoverPhase.FAILED: self.plan_failed,
        }
        # Timeout gate: short-circuit to FAILED if winner stalls beyond
        # promote_timeout (ADR-0007 §2).
        if obs.phase in self._PROMOTE_WAIT_PHASES and is_timed_out(
            obs.promote_started_ts, self._cfg.promote_timeout, 'Winner promote',
            now=obs.current_time,
        ):
            return [FailoverTransitionTo(phase=FailoverPhase.FAILED)]

        planner = planners.get(obs.phase)  # type: ignore[arg-type]
        if planner is None:
            logging.debug('No coordinator-side planner for failover phase %s', obs.phase)
            return []
        return planner(obs)

    # --- Pure gate predicates (analog of _can_do_failover, ADR-0007 §3) ---

    def can_start_failover(self, obs: 'FailoverObservation') -> bool:
        """Legacy non-probe entry used only by explicit recovery paths."""
        return obs.autofailover

    def _gates_pass(self, obs: 'FailoverObservation') -> bool:
        """All _can_do_failover gates as pure predicates over Observation."""
        if not obs.autofailover:
            logging.info('Autofailover is disabled. Not doing anything.')
            return False

        return True

    def _is_election_valid(self, obs: 'FailoverObservation') -> bool:
        """Require a read quorum for every possibly active SSN."""
        configs = obs.durability_quorums
        if not configs:
            voted = set(obs.electorate) & set(obs.votes)
            return len(voted) >= obs.quorum_size
        if obs.failed_primary is None:
            return False
        for config in configs:
            replicas = set(config.members) - {obs.failed_primary}
            required = len(replicas) - config.required + 1
            voted = replicas & set(obs.votes)
            if len(voted) < required:
                logging.info(
                    'Waiting for durability read quorum %s: %d < %d',
                    sorted(config.members), len(voted), required,
                )
                return False
        return True

    @staticmethod
    def _candidate_is_safe(obs: 'FailoverObservation', candidate: str) -> bool:
        vote = obs.votes.get(candidate)
        if vote is None:
            return False
        candidate_lsn = vote[0]
        if not obs.durability_quorums:
            return True
        if obs.failed_primary is None:
            return False
        for config in obs.durability_quorums:
            replicas = set(config.members) - {obs.failed_primary}
            required = len(replicas) - config.required + 1
            dominated = sum(
                1 for host, host_vote in obs.votes.items()
                if host in replicas and host_vote[0] <= candidate_lsn
            )
            if dominated < required:
                return False
        return True

    def _determine_safe_winner(self, obs: 'FailoverObservation') -> str | None:
        candidates = set(obs.electorate)
        if obs.durability is not None:
            candidates &= set(obs.durability.members)
        ordered = sorted(
            (
                (vote, host) for host, vote in obs.votes.items()
                if host in candidates
            ),
            reverse=True,
        )
        for _, host in ordered:
            if self._candidate_is_safe(obs, host):
                return host
        return None

    @staticmethod
    def _determine_winner(votes: dict[str, tuple[int, int]]) -> str | None:
        """Pick the winner: highest (lsn, priority) tuple."""
        best_vote = None
        winner = None
        for host, vote in votes.items():
            if vote is None:
                continue
            if best_vote is None or vote > best_vote:
                best_vote = vote
                winner = host
        return winner

    # --- Phase planners ---

    def plan_walreceiver_disabling(self, obs: 'FailoverObservation') -> CommandPlan:
        """Prepare the coordinator's vote and wait for a fenced read-quorum."""
        plan: CommandPlan = []

        if obs.failover_started_ts is None:
            plan.append(StartTimer('failover'))
        if obs.downtime_started_ts is None:
            plan.append(StartTimer('downtime'))

        timeline_matches = obs.local_timeline == obs.zk_timeline
        if (
            obs.my_hostname in obs.electorate
            and obs.my_hostname not in obs.votes
            and obs.failover_version is not None
            and obs.zk_timeline is not None
            and (timeline_matches or obs.fence_mismatched_timelines)
        ):
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
                priority=obs.host_priority,
                walreceiver_timeout=self._cfg.walreceiver_disable_timeout,
                failover_version=obs.failover_version,
                timeline=obs.zk_timeline,
                lsn_read_sleep=self._cfg.election_lsn_read_sleep,
                publish_vote=timeline_matches,
            ))
        if self._is_election_valid(obs):
            plan.append(FailoverTransitionTo(phase=FailoverPhase.GATES_PASSED))
        return plan

    def plan_gates_passed(self, obs: 'FailoverObservation') -> CommandPlan:
        """gates_passed → registration: cleanup votes, open registration, vote.

        Coordinator votes too (it is an HA replica itself).
        """
        return [FailoverTransitionTo(phase=FailoverPhase.REGISTRATION)]

    def plan_registration(self, obs: 'FailoverObservation') -> CommandPlan:
        """registration → voting: wait for participants to vote (non-blocking).

        Empty Plan until the frozen durability read-quorum has voted.
        """
        if not self._is_election_valid(obs):
            logging.debug('Waiting for durability read-quorum votes: %s', list(obs.votes))
            return []

        logging.info('Durability read-quorum voted, proceeding to selection')
        return [FailoverTransitionTo(phase=FailoverPhase.VOTING)]

    def plan_voting(self, obs: 'FailoverObservation') -> CommandPlan:
        """voting → winner_selected: tally votes, check quorum, write winner.

        TransitionTo(FAILED) if quorum not met or no winner.
        """
        if not self._is_election_valid(obs):
            logging.info('Waiting for every durability read quorum')
            return []

        winner = self._determine_safe_winner(obs)
        if winner is None:
            logging.info('Waiting for a candidate safe for every durability quorum')
            return []

        logging.info('Elected winner: %s', winner)
        return [
            WriteElectionWinner(winner=winner),
            FailoverTransitionTo(phase=FailoverPhase.WINNER_SELECTED),
        ]

    def plan_winner_selected(self, obs: 'FailoverObservation') -> CommandPlan:
        """winner_selected: start promote timer, wait for winner lock, then → PROMOTING.

        The timer is started on entry (not on lock acquire) so the timeout
        gate covers the lock-acquire wait — if the winner is dead it never
        acquires the lock and the gate fires after promote_timeout.
        """
        plan: CommandPlan = []
        if obs.promote_started_ts is None:
            plan.append(StartTimer('failover_promote'))
        if obs.election_winner is not None and obs.lock_holder == obs.election_winner:
            logging.info('Winner %s acquired the lock, failover proceeding', obs.election_winner)
            plan.append(FailoverTransitionTo(phase=FailoverPhase.PROMOTING))
            return plan
        return plan

    def plan_promoting(self, obs: 'FailoverObservation') -> CommandPlan:
        """Advance only after the winner publishes its local promotion result."""
        if obs.winner_status == 'failed':
            return [FailoverTransitionTo(FailoverPhase.FAILED)]
        if obs.winner_status == 'promoted':
            return [
                WriteLastFailoverTime(),
                FailoverTransitionTo(FailoverPhase.FINISHED),
            ]
        logging.debug('Coordinator: waiting for winner promotion status')
        return []

    def plan_finished(self, obs: 'FailoverObservation') -> CommandPlan:
        """finished: clean failover metadata and stop promote timer."""
        return self._plan_cleanup(obs, 'FAILOVER: finished, cleaning up')

    def plan_failed(self, obs: 'FailoverObservation') -> CommandPlan:
        """failed: wait for the winner's lock resolution, then clean up."""
        if obs.winner_status == 'promoted':
            return [
                WriteLastFailoverTime(),
                FailoverTransitionTo(FailoverPhase.FINISHED),
            ]
        if obs.election_winner is not None and obs.lock_holder == obs.election_winner:
            logging.warning('FAILOVER: waiting for failed winner %s to resolve primary lock', obs.election_winner)
            return []
        return self._plan_cleanup(obs, 'FAILOVER: coordinator failed, cleaning up')

    @staticmethod
    def _plan_cleanup(obs: 'FailoverObservation', message: str) -> CommandPlan:
        plan: CommandPlan = [
            Log(
                message=message,
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
