# encoding: utf-8
"""
Command executor — the shared imperative shell for cluster-operation state
machines (ADR-0006/ADR-0007).

Owns the infra objects (zk, db, durability manager, timings) and the bound
opaque composite callbacks. Dispatches each command type to its effect,
stopping on the first failing command (fail-fast). Concentrates all I/O in
one place, aligning with ADR-0002 exception handling.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any, Callable, Protocol

from . import helpers
from .commands import (
    AcquireLock,
    ClearLocalState,
    CleanupFailover,
    Command,
    FailoverTransitionTo,
    ForceReleasePrimaryLock,
    Log,
    Plan,
    PrepareFailoverVote,
    ReleaseLock,
    Promote,
    PromotionResult,
    ReturnToCluster,
    Sleep,
    StartTimer,
    StopTimer,
    SwitchoverStep,
    WriteElectionWinner,
    WriteFailoverParticipantState,
    WriteLastFailoverTime,
)
from .exceptions import PostgresConnectionError
from .log_formatters import log_event
from .local_state import LocalStateError
from .zk import ZookeeperException

if TYPE_CHECKING:
    from .failover import FailoverPhase
    from .local_state import LocalStateStore
    from .pg import Postgres
    from .timings import TimingTracker
    from .zk import Zookeeper

class PlanMachine(Protocol):
    """Protocol for state machines that produce a Command Plan (ADR-0006 §5).

    ``observation`` is ``Any`` because each machine defines its own dataclass.
    """

    def plan(self, observation: Any) -> Plan:
        """Return the ordered Plan for the current observation (pure, no I/O)."""
        ...


class CommandExecutor:
    """
    Imperative shell interpreting cluster-operation Command Plans.

    Owns infra objects and opaque composite callbacks. ``run()`` calls
    ``machine.plan(observation)`` (pure, no I/O) and executes the returned Plan
    command-by-command, stopping on the first failing command (fail-fast).
    """

    def __init__(
        self,
        zk: Zookeeper,
        db: Postgres,
        timings: TimingTracker,
        *,
        promote: Callable[..., PromotionResult],
        return_to_cluster: Callable[..., Any],
        local_states: 'dict[str, LocalStateStore]',
        switchover_step: Callable[[SwitchoverStep], bool] | None = None,
    ) -> None:
        self._zk = zk
        self._db = db
        self._timings = timings
        # Opaque composite callbacks (delegated to pgconsul methods, ADR-0006 §3).
        self._promote = promote
        self._return_to_cluster = return_to_cluster
        self._local_states = local_states
        self._switchover_step = switchover_step
        self._local_operation_id: str | None = None

    def run(self, machine: PlanMachine, observation: Any) -> None:
        """Execute one step: call machine.plan(obs), run the returned Plan.

        Stops on the first failing command (fail-fast: retry next iteration).
        Empty plan = nothing to do (retry next time).

        The local operation identity is cleared after each run so state from a
        previous failover cannot be reused.
        """
        try:
            plan = machine.plan(observation)
        except Exception:
            logging.exception(
                'State machine %s raised an unexpected exception in plan()',
                type(machine).__name__,
            )
            return
        self.execute(plan, observation)

    def execute(self, plan: Plan, observation: Any | None = None) -> None:
        """Execute an already-decided plan exactly once.

        Callers that need to serialize an effect with a ZooKeeper lock decide
        first, acquire the lock, then execute this immutable plan.  Replanning
        after lock acquisition could accidentally choose a different protocol
        transition within the same iteration.
        """
        try:
            self._local_operation_id = (
                getattr(observation, 'failover_version', None)
                or getattr(getattr(observation, 'record', None), 'operation_id', None)
            )
            for cmd in plan:
                if not self._dispatch(cmd):
                    return
        finally:
            self._local_operation_id = None

    def _dispatch(self, cmd: Command) -> bool:
        """Execute a single command. Returns False on failure (fail-fast).

        Catches PostgresConnectionError / ZookeeperException per-command
        (ADR-0002): logs and returns False so run() stops and retries next.
        """
        try:
            return self._exec(cmd)
        except (LocalStateError, PostgresConnectionError, ZookeeperException):
            logging.warning(
                'Command %s failed with I/O error, will retry next iteration',
                type(cmd).__name__,
                exc_info=True,
            )
            return False

    def _exec(self, cmd: Command) -> bool:
        """Dispatch by command type to the corresponding infra call."""
        match cmd:
            # --- Common commands ---
            case AcquireLock():
                if cmd.desired_operation_id is not None:
                    desired, _ = self._zk.get_desired_primary()
                    if (
                        desired is None
                        or desired.operation_id != cmd.desired_operation_id
                        or desired.hostname != cmd.desired_hostname
                    ):
                        logging.warning('Refusing leader lock: desired primary changed')
                        return False
                return self._zk.try_acquire_lock(
                    lock_type=cmd.lock_type,
                    allow_queue=cmd.allow_queue,
                    timeout=cmd.timeout,
                )
            case ReleaseLock():
                return self._zk.release_lock(lock_type=cmd.lock_type, wait=cmd.wait)
            case StartTimer():
                operation_id = self._current_local_operation_id()
                if operation_id is None:
                    logging.error('Timing start without an operation id')
                    return False
                return self._timings.start(cmd.name, operation_id, cmd.ts)
            case StopTimer():
                operation_id = self._current_local_operation_id()
                if operation_id is None:
                    logging.error('Timing stop without an operation id')
                    return False
                return self._timings.stop(cmd.name, operation_id, cmd.track_as)
            case Sleep():
                time.sleep(cmd.seconds)
                return True
            case Log():
                self._exec_log(cmd)
                return True
            case ClearLocalState():
                return self._exec_clear_local_state(cmd.scope)
            # --- Opaque commands (delegated to pgconsul methods, ADR-0006 §3) ---
            case Promote():
                operation_id = cmd.failover_version or self._current_local_operation_id()
                if operation_id is None:
                    logging.error('Promotion without an operation id')
                    return False
                promotion_result = self._promote(
                    scope=cmd.scope,
                    operation_id=operation_id,
                    old_primary=cmd.old_primary,
                    start_postgresql=cmd.start_postgresql,
                )
                if promotion_result == PromotionResult.SUCCESS:
                    return True
                if promotion_result == PromotionResult.REJECTED and cmd.scope == 'failover_participant':
                    version = cmd.failover_version
                    if version is not None:
                        self._zk.write_failover_participant_state('failed', version)
                    self._exec_clear_local_state('failover_participant')
                    self._zk.release_lock()
                return False
            case ReturnToCluster():
                self._return_to_cluster(
                    cmd.new_primary,
                    cmd.role,
                    is_dead=cmd.is_postgresql_dead,
                )
                return True
            case SwitchoverStep():
                if self._switchover_step is None:
                    logging.error('Switchover command executor is not configured')
                    return False
                return self._switchover_step(cmd)
            case WriteLastFailoverTime():
                if not self._zk.is_lock_holder(self._zk.ELECTION_MANAGER_LOCK_PATH):
                    return False
                return self._zk.write_last_failover_time()
            case PrepareFailoverVote():
                return self._exec_prepare_failover_vote(cmd)
            case WriteFailoverParticipantState():
                return self._zk.write_failover_participant_state(cmd.state, cmd.failover_version)
            case WriteElectionWinner():
                if not self._zk.is_lock_holder(self._zk.ELECTION_MANAGER_LOCK_PATH):
                    return False
                return self._zk.write_election_winner(cmd.winner)
            case ForceReleasePrimaryLock():
                if not self._zk.is_lock_holder(self._zk.ELECTION_MANAGER_LOCK_PATH):
                    return False
                return self._zk.force_release_primary_lock(cmd.expected_holder)
            case CleanupFailover():
                return self._exec_cleanup_failover()
            case FailoverTransitionTo():
                return self._exec_failover_transition_to(cmd.phase)
            case _:
                logging.error('Unknown command type: %s', type(cmd).__name__)
                return False

    # --- Command implementations ---

    def _exec_clear_local_state(self, scope: str) -> bool:
        store = self._local_states.get(scope)
        if store is None:
            logging.error('Local state store %s is not configured', scope)
            return False
        operation_id = self._current_local_operation_id()
        if operation_id is None:
            logging.error('Local state cleanup without an operation id')
            return False
        store.clear(operation_id)
        return True

    def _current_local_operation_id(self) -> str | None:
        return self._local_operation_id

    def _exec_log(self, cmd: Log) -> None:
        if cmd.event:
            log_event(cmd.message, level=cmd.level)
        else:
            level = getattr(logging, cmd.level.upper(), logging.INFO)
            logging.log(level, cmd.message)

    def _exec_prepare_failover_vote(self, cmd: PrepareFailoverVote) -> bool:
        if cmd.timeline_only:
            timeline = self._db.get_timeline()
            if timeline != cmd.timeline:
                return False
            return self._zk.write_election_vote(
                0,
                cmd.priority,
                failover_version=cmd.failover_version,
                timeline=timeline,
            )
        if cmd.fence_wal_sources:
            if not self._db.stop_restoring_wal():
                return False
            if not self._db.disable_wal_receiver(cmd.walreceiver_timeout):
                return False
        else:
            logging.warning(
                'Collecting an unfenced failover vote: restore_command and '
                'walreceiver may still advance the local WAL position'
            )
        timeline = self._db.get_timeline()
        if timeline != cmd.timeline:
            logging.error(
                'Cannot vote from timeline %s; failover timeline is %s',
                timeline,
                cmd.timeline,
            )
            return False
        lsn = self._db.get_wal_flush_lsn()
        if lsn is None:
            logging.error('Cannot vote without a local WAL flush LSN')
            return False
        if cmd.lsn_read_sleep:
            logging.debug(
                'Read LSN for election vote: %s. Sleep for test purposes: %s',
                lsn,
                cmd.lsn_read_sleep,
            )
            time.sleep(cmd.lsn_read_sleep)
        return self._zk.write_election_vote(
            lsn,
            cmd.priority,
            failover_version=cmd.failover_version,
            timeline=cmd.timeline,
        )

    def _exec_cleanup_failover(self) -> bool:
        if not self._zk.is_lock_holder(self._zk.ELECTION_MANAGER_LOCK_PATH):
            return False
        logging.info('Resetting failover metadata')
        if not self._zk.ensure_failover_must_be_reset():
            return False
        if not self._zk.cleanup_failover():
            logging.info('Resetting failover failed, will try on next iteration.')
            return False
        if not self._zk.release_lock(self._zk.ELECTION_MANAGER_LOCK_PATH):
            logging.info('Releasing failover coordinator lock failed, will retry.')
            return False
        if not self._zk.delete_failover_must_be_reset():
            logging.info('Removing failover reset marker failed, will retry.')
            return False
        logging.info('Failover cleanup finished')
        return True

    def _exec_failover_transition_to(self, phase: 'FailoverPhase') -> bool:
        """Persist failover phase to ZK before the action (ADR-0007 §2 fence)."""
        if not self._zk.is_lock_holder(self._zk.ELECTION_MANAGER_LOCK_PATH):
            logging.error('Only the failover coordinator may change the global phase')
            return False
        if not self._zk.write_failover_state(phase):
            logging.error('Failed to persist failover phase %s to ZK', phase)
            return False
        log_event(f'FAILOVER PHASE → {phase}', level='warning')
        return True
