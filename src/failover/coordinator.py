# encoding: utf-8
"""Coordinator-side failover state machine (ADR-0007, ADR-0006).

Pure ``plan(observation)`` API: returns a Command Plan executed by
CommandExecutor. The coordinator holds ``ELECTION_MANAGER_LOCK_PATH`` and
drives phases: gate checks, registration, voting, winner selection.

Blocking ``sleep`` is replaced by "no condition → empty Plan → retry next
iteration" (ADR-0007 §2).
"""

import logging
import time
from typing import TYPE_CHECKING, Callable

from ..commands import (
    CleanupVotes,
    DisableWalReceiver,
    FailoverTransitionTo,
    Log,
    Plan as CommandPlan,
    ReleaseLock,
    ResetFailoverNode,
    Sleep,
    StartTimer,
    StopTimer,
    WriteElectionStatus,
    WriteElectionVote,
    WriteElectionWinner,
)
# Election status constants (ADR-0007 §7).
STATUS_REGISTRATION = 'registration'
STATUS_SELECTION = 'selection'
STATUS_DONE = 'done'

from ..helpers import make_current_replics_quorum
from ..types import check_last_failover_time, is_timed_out
from .types import (
    FailoverMachineConfig,
    FailoverObservation,
    FailoverPhase,
)

if TYPE_CHECKING:
    pass


class FailoverCoordinatorMachine:
    """Coordinator-side failover state machine (ADR-0007, ADR-0006).

    The node holding ``ELECTION_MANAGER_LOCK_PATH`` collects votes, checks
    quorum/promote-safe, and writes the winner.
    """

    # Phases where coordinator waits for winner — timeout gate short-circuits
    # to FAILED after promote_timeout (ADR-0007 §2).
    _PROMOTE_WAIT_PHASES = frozenset({
        FailoverPhase.PROMOTING,
        FailoverPhase.CHECKPOINTING,
        FailoverPhase.CREATING_SLOTS,
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
        planners: dict = {
            FailoverPhase.DETECTED: self.plan_detected,
            FailoverPhase.WALRECEIVER_DISABLING: self.plan_walreceiver_disabling,
            FailoverPhase.GATES_PASSED: self.plan_gates_passed,
            FailoverPhase.REGISTRATION: self.plan_registration,
            FailoverPhase.VOTING: self.plan_voting,
            FailoverPhase.WINNER_SELECTED: self.plan_winner_selected,
            FailoverPhase.PROMOTING: self.plan_promoting,
            FailoverPhase.CHECKPOINTING: self.plan_checkpointing,
            FailoverPhase.CREATING_SLOTS: self.plan_creating_slots,
            FailoverPhase.FINISHED: self.plan_finished,
            FailoverPhase.FAILED: self.plan_failed,
        }
        # Timeout gate: short-circuit to FAILED if winner stalls beyond
        # promote_timeout (ADR-0007 §2).
        if obs.record.phase in self._PROMOTE_WAIT_PHASES and is_timed_out(
            obs.promote_started_ts, self._cfg.promote_timeout, 'Winner promote'
        ):
            return [FailoverTransitionTo(phase=FailoverPhase.FAILED)]

        planner = planners.get(obs.record.phase)  # type: ignore[arg-type]
        if planner is None:
            logging.debug('No coordinator-side planner for failover phase %s', obs.record.phase)
            return []
        return planner(obs)

    # --- Pure gate predicates (analog of _can_do_failover, ADR-0007 §3) ---

    def can_start_failover(self, obs: 'FailoverObservation') -> bool:
        """Pre-check before writing 'detected': async mode + no data loss → False (MDB-41951)."""
        if obs.allow_data_loss:
            return True
        return self._is_promote_safe(obs)

    def _gates_pass(self, obs: 'FailoverObservation') -> bool:
        """All _can_do_failover gates as pure predicates over Observation."""
        if not (obs.autofailover or obs.switchover_in_progress):
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
        if not check_last_failover_time(obs.last_failover_ts, self._cfg.min_failover_timeout):
            logging.info('Last failover too recent, waiting')
            return False

        # Primary unreachable gate (skipped on switchover_in_progress).
        if not obs.switchover_in_progress and not obs.is_primary_unreachable:
            logging.warning('Primary still accessible through libpq, not doing failover')
            return False

        # Primary unavailability timeout gate.
        if obs.last_primary_availability_ts is not None:
            elapsed = time.time() - obs.last_primary_availability_ts
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

        # Promote-safe gate is checked separately in plan_detected because
        # its failure may be permanent (async mode) or transient (sync mode
        # with minority alive).  See plan_detected for the branching logic
        # (MDB-41951, async.feature:47).
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
            obs.replics_info, obs.alive_hosts or [],
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

    def plan_detected(self, obs: 'FailoverObservation') -> CommandPlan:
        """detected → walreceiver_disabling: run gates, no walreceiver ops.

        Gates checked once here. On success → WALRECEIVER_DISABLING.
        Sleep + DisableWalReceiver run in plan_walreceiver_disabling without
        gate recheck — prevents "primary returned" deadlock. Walreceiver
        disabled before voting (MDB-41951).

        Transient gate failures (primary still reachable, WAL replaying,
        last failover too recent) → empty Plan (retry next iteration).

        Permanent gate failures → TransitionTo(FAILED):
        - No alive hosts at all.
        - Promote-safe gate fails in async mode (empty sync_quorum):
          ``len([]) // 2 + 1 = 1`` required but ``0`` in quorum — always
          False.  Retry cannot fix a permanent condition; without FAILED
          the coordinator loops forever and ``failover_state`` stays
          'detected' (MDB-41951, async.feature:47).
        """
        if not self._gates_pass(obs):
            # If no alive hosts at all, fail immediately.
            if not obs.alive_hosts:
                logging.error('No alive hosts — failover cannot proceed')
                return [FailoverTransitionTo(phase=FailoverPhase.FAILED)]
            return []

        # Promote-safe gate: checked after transient gates pass.
        # In async mode (empty sync_quorum) the condition is permanent —
        # retry cannot fix it.  Transition to FAILED so plan_failed →
        # ResetFailoverNode clears failover_state (MDB-41951).
        if not obs.allow_data_loss and not self._is_promote_safe(obs):
            logging.warning(
                'Promote is not allowed with given configuration — '
                'permanent failure, transitioning to FAILED'
            )
            return [FailoverTransitionTo(phase=FailoverPhase.FAILED)]

        logging.info('Failover gates passed OK')

        plan: CommandPlan = []
        if not obs.failover_timer_started:
            plan.append(StartTimer('failover'))
        if not obs.downtime_timer_started:
            plan.append(StartTimer('downtime'))

        plan.append(FailoverTransitionTo(phase=FailoverPhase.WALRECEIVER_DISABLING))
        return plan

    def plan_walreceiver_disabling(self, obs: 'FailoverObservation') -> CommandPlan:
        """walreceiver_disabling → gates_passed: sleep + disable walreceiver, no gate recheck.

        Runs unconditionally once entered. Disabling walreceiver before voting
        ensures the old primary can't get a sync write acknowledged (MDB-41951).
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
        plan.append(FailoverTransitionTo(phase=FailoverPhase.GATES_PASSED))
        return plan

    def plan_gates_passed(self, obs: 'FailoverObservation') -> CommandPlan:
        """gates_passed → registration: cleanup votes, open registration, vote.

        Coordinator votes too (it is an HA replica itself).
        """
        plan: CommandPlan = [
            CleanupVotes(),
            WriteElectionStatus(status=STATUS_REGISTRATION),
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
        return [
            WriteElectionStatus(status=STATUS_SELECTION),
            FailoverTransitionTo(phase=FailoverPhase.VOTING),
        ]

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
            WriteElectionStatus(status=STATUS_DONE),
            FailoverTransitionTo(phase=FailoverPhase.WINNER_SELECTED),
        ]

    def plan_winner_selected(self, obs: 'FailoverObservation') -> CommandPlan:
        """winner_selected: wait for winner to acquire lock, then → PROMOTING.

        Starts ``failover_promote`` timer for the timeout gate.
        """
        if obs.lock_holder is not None:
            logging.info('Winner %s acquired the lock, failover proceeding', obs.lock_holder)
            plan: CommandPlan = [FailoverTransitionTo(phase=FailoverPhase.PROMOTING)]
            if obs.promote_started_ts is None:
                plan.append(StartTimer('failover_promote'))
            return plan
        return []

    def plan_promoting(self, obs: 'FailoverObservation') -> CommandPlan:
        """promoting: wait for winner (participant runs DoFailover)."""
        logging.debug('Coordinator: waiting for winner to finish promote (phase=%s)', obs.record.phase)
        return []

    def plan_checkpointing(self, obs: 'FailoverObservation') -> CommandPlan:
        """checkpointing: coordinator waits for winner to finish checkpointing."""
        logging.debug('Coordinator: waiting for winner to finish checkpointing')
        return []

    def plan_creating_slots(self, obs: 'FailoverObservation') -> CommandPlan:
        """creating_slots: coordinator waits for winner to finish slot creation."""
        logging.debug('Coordinator: waiting for winner to finish creating slots')
        return []

    def plan_finished(self, obs: 'FailoverObservation') -> CommandPlan:
        """finished: release lock, reset failover node, stop promote timer."""
        plan: CommandPlan = [
            Log(
                message='FAILOVER: finished, coordinator releasing election lock',
                level='warning',
                event=True,
            ),
        ]
        if obs.promote_started_ts is not None:
            plan.append(StopTimer('failover_promote'))
        plan.extend([ReleaseLock(), ResetFailoverNode()])
        return plan

    def plan_failed(self, obs: 'FailoverObservation') -> CommandPlan:
        """failed: release lock, reset failover node, stop promote timer."""
        plan: CommandPlan = [
            Log(
                message='FAILOVER: coordinator failed, resetting',
                level='warning',
                event=True,
            ),
        ]
        if obs.promote_started_ts is not None:
            plan.append(StopTimer('failover_promote'))
        plan.extend([ReleaseLock(), ResetFailoverNode()])
        return plan
