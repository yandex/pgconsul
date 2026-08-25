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
    CleanupVotes,
    CleanupFailover,
    DisableWalReceiver,
    FailoverTransitionTo,
    Log,
    Plan as CommandPlan,
    Sleep,
    StartTimer,
    StopTimer,
    WriteElectionVote,
    WriteElectionWinner,
)
from ..helpers import make_current_replics_quorum
from ..types import is_timed_out, is_transition_allowed
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
        """Return whether failover may cross its persistent entry boundary."""
        if not self._gates_pass(obs):
            return False
        return obs.allow_data_loss or self._is_promote_safe(obs)

    def _gates_pass(self, obs: 'FailoverObservation') -> bool:
        """All _can_do_failover gates as pure predicates over Observation."""
        if not obs.autofailover:
            logging.info('Autofailover is disabled. Not doing anything.')
            return False

        # Timeline sync gate.
        if obs.zk_timeline is not None and obs.local_timeline is not None:
            if obs.zk_timeline != obs.local_timeline:
                logging.warning(
                    'Timeline mismatch: local=%s zk=%s',
                    obs.local_timeline, obs.zk_timeline,
                )
                return False

        # Last failover timeout gate.
        if not is_transition_allowed(
            obs.last_failover_ts, self._cfg.min_failover_timeout, now=obs.current_time,
        ):
            logging.info('Last failover too recent, waiting')
            return False

        if not obs.is_primary_unreachable:
            logging.warning('Primary still accessible through libpq, not doing failover')
            return False

        # Primary unavailability timeout gate.
        if obs.last_primary_availability_ts is not None:
            elapsed = obs.current_time - obs.last_primary_availability_ts
            if elapsed < self._cfg.primary_unavailability_timeout:
                logging.info('Primary seen %.1fs ago, waiting', elapsed)
                return False

        # WAL replaying gate.
        if obs.is_replaying_wal:
            logging.info('Still replaying WAL, cannot promote')
            return False

        # Replics info available.
        if obs.replics_info is None:
            logging.error('No replics_info available')
            return False

        # No alive hosts — failover is impossible.
        if not obs.alive_hosts:
            logging.error('No alive hosts — failover cannot proceed')
            return False

        return True

    def _is_promote_safe(self, obs: 'FailoverObservation') -> bool:
        """Pure predicate: enough alive streaming replicas in ZK sync quorum.

        Faithful port of ``replication_manager.is_promote_safe``: checks
        ``len(set(sync_quorum) & alive_replics) >= len(sync_quorum) // 2 + 1``.
        Empty sync_quorum (async mode) → failover blocked.
        """
        sync_quorum = obs.sync_quorum
        if sync_quorum is None:
            sync_quorum = []
        alive_replics = make_current_replics_quorum(
            obs.replics_info or [], obs.alive_hosts or [],
        )
        hosts_in_quorum = len(set(sync_quorum) & alive_replics)
        required = len(sync_quorum) // 2 + 1
        logging.info(
            'Promote-safe check: sync_quorum=%s alive_replics=%s '
            'hosts_in_quorum=%d required=%d',
            sync_quorum, alive_replics, hosts_in_quorum, required,
        )
        if hosts_in_quorum < required:
            logging.error(
                'Not enough alive hosts in sync quorum: %d < %d',
                hosts_in_quorum, required,
            )
            return False
        return True

    def _is_election_valid(self, obs: 'FailoverObservation') -> bool:
        """Pure predicate: quorum of votes collected (checks actual votes)."""
        alive = obs.alive_hosts or []
        voted_alive = set(alive) & set(obs.votes.keys())
        if len(voted_alive) < obs.quorum_size:
            logging.error(
                'Not enough votes for quorum: %d < %d',
                len(voted_alive), obs.quorum_size,
            )
            return False
        return True

    def _all_alive_voted(self, obs: 'FailoverObservation') -> bool:
        """True if all alive HA hosts have recorded their votes."""
        alive = obs.alive_hosts or []
        if not alive:
            return False
        voted = set(obs.votes.keys())
        return set(alive).issubset(voted)

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
        """Start timers, disable walreceiver, then enter gates_passed.

        Runs unconditionally once entered. Disabling walreceiver before voting
        ensures the old primary can't get a sync write acknowledged (MDB-41951).
        """
        plan: CommandPlan = []

        if obs.failover_started_ts is None:
            plan.append(StartTimer('failover'))
        if obs.downtime_started_ts is None:
            plan.append(StartTimer('downtime'))

        sleep_sec = self._cfg.sleep_before_disable_walreceiver
        if sleep_sec:
            plan.append(Log(
                message=f'Sleep for test purposes before disabling walreceiver: {sleep_sec}',
                level='debug',
            ))
            plan.append(Sleep(seconds=sleep_sec))

        plan.append(DisableWalReceiver(timeout=self._cfg.walreceiver_disable_timeout))
        plan.append(FailoverTransitionTo(phase=FailoverPhase.GATES_PASSED))
        return plan

    def plan_gates_passed(self, obs: 'FailoverObservation') -> CommandPlan:
        """gates_passed → registration: cleanup votes, open registration, vote.

        Coordinator votes too (it is an HA replica itself).
        """
        plan: CommandPlan = [
            CleanupVotes(),
            FailoverTransitionTo(phase=FailoverPhase.REGISTRATION),
        ]

        # Coordinator votes too (idempotent).
        if obs.host_lsn is not None:
            plan.append(WriteElectionVote(lsn=obs.host_lsn, priority=obs.host_priority))

        return plan

    def plan_registration(self, obs: 'FailoverObservation') -> CommandPlan:
        """registration → voting: wait for participants to vote (non-blocking).

        Empty Plan if not all alive hosts have voted.
        """
        if not self._all_alive_voted(obs):
            logging.debug('Waiting for all alive hosts to vote (votes: %s)', list(obs.votes.keys()))
            return []

        logging.info('All alive hosts voted, proceeding to selection')
        return [FailoverTransitionTo(phase=FailoverPhase.VOTING)]

    def plan_voting(self, obs: 'FailoverObservation') -> CommandPlan:
        """voting → winner_selected: tally votes, check quorum, write winner.

        TransitionTo(FAILED) if quorum not met or no winner.
        """
        if not self._is_election_valid(obs):
            logging.error('Quorum not met or promote unsafe, failing failover')
            return [FailoverTransitionTo(phase=FailoverPhase.FAILED)]

        winner = self._determine_winner(obs.votes)
        if winner is None:
            logging.error('No winner determined from votes, failing failover')
            return [FailoverTransitionTo(phase=FailoverPhase.FAILED)]

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
        if obs.lock_holder is not None:
            logging.info('Winner %s acquired the lock, failover proceeding', obs.lock_holder)
            plan.append(FailoverTransitionTo(phase=FailoverPhase.PROMOTING))
            return plan
        return plan

    def plan_promoting(self, obs: 'FailoverObservation') -> CommandPlan:
        """promoting: wait for winner (participant runs Promote)."""
        logging.debug('Coordinator: waiting for winner to finish promote (phase=%s)', obs.phase)
        return []

    def plan_finished(self, obs: 'FailoverObservation') -> CommandPlan:
        """finished: clean failover metadata and stop promote timer."""
        return self._plan_cleanup(obs, 'FAILOVER: finished, cleaning up')

    def plan_failed(self, obs: 'FailoverObservation') -> CommandPlan:
        """failed: clean failover metadata and stop promote timer."""
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
