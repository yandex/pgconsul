# encoding: utf-8
"""Participant-side failover state machine (ADR-0007, ADR-0006).

Pure ``plan(observation)`` API: returns a Command Plan executed by
CommandExecutor. Handles phases: ``registration``/``voting`` (vote),
``winner_selected`` (winner: acquire lock + promote; loser: return-to-cluster),
``finished`` (loser: return-to-cluster).

``DoFailover`` runs the promote logic (``_do_failover``/``_promote``/
``_promote_handle_slots``) directly inside CommandExecutor (ADR-0007 §2.3).
Full reification into explicit phases is deferred to stage 7.
"""

import logging
from typing import TYPE_CHECKING, Callable

from ..commands import (
    AcquireLock,
    DisableWalReceiver,
    DoFailover,
    FailoverTransitionTo,
    Log,
    Plan as CommandPlan,
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

if TYPE_CHECKING:
    pass


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
            FailoverPhase.DETECTED: self.plan_detected,
            FailoverPhase.WALRECEIVER_DISABLING: self.plan_walreceiver_disabling,
            FailoverPhase.REGISTRATION: self.plan_vote,
            FailoverPhase.VOTING: self.plan_vote,
            FailoverPhase.WINNER_SELECTED: self.plan_winner_selected,
            FailoverPhase.PROMOTING: self.plan_promoting,
            FailoverPhase.CHECKPOINTING: self.plan_checkpointing,
            FailoverPhase.CREATING_SLOTS: self.plan_creating_slots,
            FailoverPhase.FINISHED: self.plan_finished,
            FailoverPhase.FAILED: self.plan_failed,
        }
        planner = planners.get(obs.record.phase)  # type: ignore[arg-type]
        if planner is None:
            logging.debug('No participant-side planner for failover phase %s', obs.record.phase)
            return []
        return planner(obs)

    def plan_detected(self, obs: 'FailoverObservation') -> CommandPlan:
        """detected: participant waits for coordinator to advance to WALRECEIVER_DISABLING.

        Coordinator checks gates in plan_detected and transitions to
        WALRECEIVER_DISABLING. Participant must not act here — doing
        Sleep+DisableWalReceiver on every iteration caused an infinite loop
        because the phase never changed (coordinator was blocked by gates).
        """
        return []

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
        The actual promote happens in plan_promoting via DoFailover.
        Non-blocking lock; if held by another, executor stops and retries.

        Loser: empty Plan — the shell delegates to decide_return_action.
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

        # AcquireLock(timeout=0) is non-blocking. If the lock is already held
        # by us (previous attempt failed mid-way), it succeeds immediately
        # and plan_promoting retries DoFailover (idempotent via delete_failover_state).
        return [
            AcquireLock(timeout=0),
            FailoverTransitionTo(phase=FailoverPhase.PROMOTING),
        ]

    def plan_promoting(self, obs: 'FailoverObservation') -> CommandPlan:
        """promoting: winner retries DoFailover (idempotent); loser waits."""
        winner = obs.election_winner
        if winner is None:
            return []
        if winner != obs.my_hostname:
            return self._plan_loser(obs, winner)

        if self._debug_failure('participant_before_promote'):
            return [FailoverTransitionTo(phase=FailoverPhase.FAILED)]

        return self._plan_winner_retry()

    def plan_checkpointing(self, obs: 'FailoverObservation') -> CommandPlan:
        """checkpointing: winner retries DoFailover; loser waits."""
        winner = obs.election_winner
        if winner is None or winner == obs.my_hostname:
            # Winner: DoFailover is idempotent — retry to finish checkpointing.
            return self._plan_winner_retry()
        return self._plan_loser(obs, winner)

    def plan_creating_slots(self, obs: 'FailoverObservation') -> CommandPlan:
        """creating_slots: winner retries DoFailover; loser waits."""
        winner = obs.election_winner
        if winner is None or winner == obs.my_hostname:
            return self._plan_winner_retry()
        return self._plan_loser(obs, winner)

    def _plan_winner_retry(self) -> CommandPlan:
        """Winner: retry DoFailover (idempotent). Shared by promoting/checkpointing/creating_slots."""
        return [
            DoFailover(old_primary=None),
            WriteLastFailoverTime(),
            StopTimer('failover'),
        ]

    def plan_finished(self, obs: 'FailoverObservation') -> CommandPlan:
        """finished: winner is done; losers return to cluster.

        Winner: empty Plan (already promoted).
        Loser: empty Plan — the shell delegates to decide_return_action.
        """
        winner = obs.election_winner
        if winner is None or winner == obs.my_hostname:
            return []
        return self._plan_loser(obs, winner)

    def plan_failed(self, obs: 'FailoverObservation') -> CommandPlan:
        """failed: coordinator aborted. Shell handles reset + return-to-cluster."""
        return [Log(
            message='FAILOVER: election failed, returning to cluster',
            level='warning',
            event=True,
        )]

    def _plan_loser(self, obs: 'FailoverObservation', winner: str) -> CommandPlan:
        """Loser branch: emit event log; shell delegates to decide_return_action."""
        return [Log(
            message=f'FAILOVER: winner is {winner}, returning to cluster',
            level='warning',
            event=True,
        )]
