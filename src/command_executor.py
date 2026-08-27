# encoding: utf-8
"""
Command executor — the single imperative shell for all cluster-op state
machines (ADR-0006 §5).

Owns the infra objects (zk, db, replication_manager, timings) and the bound
opaque composite callbacks. Dispatches each command type to its effect,
stopping on the first failing command (fail-fast). Concentrates all I/O in
one place, aligning with ADR-0002 exception handling.
"""

from __future__ import annotations

import logging
import time
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Callable, Protocol

from .commands import (
    AcquireLock,
    Checkpoint,
    ClearLocalState,
    CleanupSwitchover,
    CleanupFailover,
    Command,
    CreateSlots,
    DeleteHostOp,
    FailoverTransitionTo,
    InitializeFailover,
    Log,
    Plan,
    PrepareFailoverVote,
    ReleaseLock,
    RewindFromSource,
    SetSimplePrimarySwitchTry,
    SetSyncReplication,
    Promote,
    PromotionResult,
    ReturnToCluster,
    Sleep,
    StartTimer,
    StartPostgresql,
    StopPooler,
    StopPostgresql,
    StopTimer,
    StoreReplicsInfo,
    TransitionTo,
    WriteCandidate,
    WriteElectionWinner,
    WriteFailoverParticipantState,
    WriteLastFailoverTime,
    WriteLastSwitchoverTime,
    WriteLocalState,
    WriteSideReplicas,
    WriteSwitchoverAck,
)
from .exceptions import PostgresConnectionError
from . import helpers
from .log_formatters import log_event
from .local_state import LocalStateError
from .switchover.types import SwitchoverPhase, SwitchoverRecord
from .zk import ZookeeperException

if TYPE_CHECKING:
    from .failover import FailoverPhase
    from .local_state import LocalStateStore
    from .pg import Postgres
    from .replication_manager import ReplicationManager
    from .switchover import SwitchoverPhase
    from .timings import TimingTracker
    from .zk import Zookeeper

# Default timeout (seconds) for StopPostgresql when cmd.timeout is None.
_DEFAULT_STOP_PG_TIMEOUT: float = 60


class PlanMachine(Protocol):
    """Protocol for state machines that produce a Command Plan (ADR-0006 §5).

    ``observation`` is ``Any`` because each machine defines its own dataclass.
    """

    def plan(self, observation: Any) -> Plan:
        """Return the ordered Plan for the current observation (pure, no I/O)."""
        ...


class CommandExecutor:
    """
    Single imperative shell interpreting Command Plans for all cluster-op machines.

    Owns infra objects and opaque composite callbacks. ``run()`` calls
    ``machine.plan(observation)`` (pure, no I/O) and executes the returned Plan
    command-by-command, stopping on the first failing command (fail-fast).
    """

    def __init__(
        self,
        zk: Zookeeper,
        db: Postgres,
        replication_manager: ReplicationManager,
        timings: TimingTracker,
        *,
        stop_postgresql: Callable[..., int],
        store_replics_info: Callable[[dict, dict], bool],
        rewind_from_source: Callable[..., bool | None],
        promote: Callable[..., PromotionResult],
        return_to_cluster: Callable[..., Any],
        set_simple_primary_switch_try: Callable[[str], None],
        create_slots_for_hosts: Callable[[list[str]], bool],
        initialize_failover: Callable[[dict, dict], bool],
        local_states: 'dict[str, LocalStateStore]',
    ) -> None:
        self._zk = zk
        self._db = db
        self._replication_manager = replication_manager
        self._timings = timings
        # Opaque composite callbacks (delegated to pgconsul methods, ADR-0006 §3).
        self._stop_postgresql = stop_postgresql
        self._store_replics_info = store_replics_info
        self._rewind_from_source = rewind_from_source
        self._promote = promote
        self._return_to_cluster = return_to_cluster
        self._set_simple_primary_switch_try = set_simple_primary_switch_try
        self._create_slots_for_hosts = create_slots_for_hosts
        self._initialize_failover = initialize_failover
        self._local_states = local_states
        # Iteration context for commands needing raw state dicts (StoreReplicsInfo).
        self._db_state: dict | None = None
        self._zk_state: dict | None = None
        self._switchover_record: SwitchoverRecord | None = None

    def set_iteration_state(self, db_state: dict, zk_state: dict) -> None:
        """Set raw db/zk state dicts for the current iteration.

        Needed only by StoreReplicsInfo (delegates to pgconsul._store_replics_info
        which expects raw dicts). Removed once fully reified (Stage 6).
        """
        self._db_state = db_state
        self._zk_state = zk_state

    def run(self, machine: PlanMachine, observation: Any) -> None:
        """Execute one step: call machine.plan(obs), run the returned Plan.

        Stops on the first failing command (fail-fast: retry next iteration).
        Empty plan = nothing to do (retry next time).

        Iteration state (``_db_state`` / ``_zk_state``) is cleared after each
        ``run()`` so a stale dict from a previous iteration is never reused.
        """
        try:
            record = getattr(observation, 'record', None)
            if isinstance(record, SwitchoverRecord):
                self._switchover_record = record
            try:
                plan = machine.plan(observation)
            except Exception:
                logging.exception(
                    'State machine %s raised an unexpected exception in plan()',
                    type(machine).__name__,
                )
                return
            if not plan:
                return
            for cmd in plan:
                if not self._dispatch(cmd):
                    return
        finally:
            # Clear iteration state so a stale dict is never reused.
            self._db_state = None
            self._zk_state = None
            self._switchover_record = None

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
                return self._zk.try_acquire_lock(
                    lock_type=cmd.lock_type,
                    allow_queue=cmd.allow_queue,
                    timeout=cmd.timeout,
                )
            case ReleaseLock():
                return self._zk.release_lock(lock_type=cmd.lock_type, wait=cmd.wait)
            case StartTimer():
                if self._timings.get_start(cmd.name) is None:
                    self._timings.start(cmd.name, cmd.ts)
                return True
            case StopTimer():
                self._timings.stop(cmd.name, cmd.track_as)
                return True
            case WriteLastSwitchoverTime():
                return self._zk.write_last_switchover_time()
            case StopPooler():
                return self._db.pgpooler('stop')
            case StopPostgresql():
                timeout = cmd.timeout if cmd.timeout is not None else _DEFAULT_STOP_PG_TIMEOUT
                return self._stop_postgresql(
                    timeout=timeout, wait=cmd.wait, force_async=cmd.force_async
                ) == 0
            case StartPostgresql():
                return self._db.start_postgresql() == 0
            case Checkpoint():
                return bool(self._db.checkpoint())
            case StoreReplicsInfo():
                return self._exec_store_replics_info()
            case Sleep():
                time.sleep(cmd.seconds)
                return True
            case Log():
                self._exec_log(cmd)
                return True
            case WriteLocalState():
                return self._exec_write_local_state(cmd.scope, cmd.phase)
            case ClearLocalState():
                return self._exec_clear_local_state(cmd.scope)
            # --- Switchover commands ---
            case TransitionTo():
                return self._exec_transition_to(cmd.phase)
            case WriteCandidate():
                if not self._zk.is_lock_holder():
                    return False
                return self._write_switchover_record(candidate=cmd.candidate)
            case WriteSideReplicas():
                if not self._zk.is_lock_holder():
                    return False
                return self._write_switchover_record(side_replicas=list(cmd.side_replicas))
            case SetSyncReplication():
                return self._replication_manager.change_replication_to_sync_host(cmd.host)
            case CleanupSwitchover():
                if self._switchover_record is None or self._switchover_record.version is None:
                    return False
                if not self._zk.cleanup_switchover(self._switchover_record.version):
                    return False
                for scope in ('switchover_primary', 'switchover_candidate'):
                    store = self._local_states.get(scope)
                    if store is not None:
                        store.clear()
                return True
            case WriteSwitchoverAck():
                return self._zk.write_switchover_ack(
                    helpers.get_hostname(), cmd.operation_id, cmd.state
                )
            case InitializeFailover():
                return self._exec_initialize_failover()
            # --- Opaque commands (delegated to pgconsul methods, ADR-0006 §3) ---
            case Promote():
                promotion_result = self._promote(
                    scope=cmd.scope,
                    old_primary=cmd.old_primary,
                    start_postgresql=cmd.start_postgresql,
                )
                if promotion_result == PromotionResult.SUCCESS:
                    return True
                if promotion_result == PromotionResult.REJECTED and cmd.scope == 'switchover_candidate':
                    self._exec_transition_to(SwitchoverPhase.FAILED)
                    self._exec_clear_local_state('switchover_candidate')
                    self._zk.release_lock()
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
            case RewindFromSource():
                result = self._rewind_from_source(
                    is_postgresql_dead=cmd.is_postgresql_dead,
                    limit=cmd.limit,
                    new_primary=cmd.new_primary,
                )
                return bool(result)
            case SetSimplePrimarySwitchTry():
                self._set_simple_primary_switch_try(cmd.new_primary)
                return True
            case DeleteHostOp():
                self._zk.delete_host_op()
                return True
            case CreateSlots():
                return self._create_slots_for_hosts(list(cmd.hosts))
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
            case CleanupFailover():
                return self._exec_cleanup_failover()
            case FailoverTransitionTo():
                return self._exec_failover_transition_to(cmd.phase)
            case _:
                logging.error('Unknown command type: %s', type(cmd).__name__)
                return False

    # --- Command implementations ---

    def _exec_store_replics_info(self) -> bool:
        if self._db_state is None or self._zk_state is None:
            logging.error('StoreReplicsInfo: iteration state not set')
            return False
        return bool(self._store_replics_info(self._db_state, self._zk_state))

    def _exec_initialize_failover(self) -> bool:
        if self._db_state is None or self._zk_state is None:
            logging.error('InitializeFailover: iteration state not set')
            return False
        return self._initialize_failover(self._db_state, self._zk_state)

    def _exec_write_local_state(self, scope: str, phase: str) -> bool:
        store = self._local_states.get(scope)
        if store is None:
            logging.error('Local state store %s is not configured', scope)
            return False
        store.write(phase)
        return True

    def _exec_clear_local_state(self, scope: str) -> bool:
        store = self._local_states.get(scope)
        if store is None:
            logging.error('Local state store %s is not configured', scope)
            return False
        store.clear()
        return True

    def _exec_transition_to(self, phase: SwitchoverPhase) -> bool:
        if not self._write_switchover_record(phase=phase):
            logging.error('Failed to persist switchover phase %s to ZK', phase)
            return False
        log_event(f'SWITCHOVER PHASE → {phase}', level='warning')
        return True

    def _write_switchover_record(self, **changes: Any) -> bool:
        record = self._switchover_record
        if record is None:
            logging.error('Switchover command executed without an observation record')
            return False
        updated = replace(record, **changes)
        version = self._zk.write_switchover_record(updated.to_dict(), record.version)
        if version is None:
            logging.info('Switchover record changed concurrently; retrying next iteration')
            return False
        self._switchover_record = replace(updated, version=version)
        return True

    def _exec_log(self, cmd: Log) -> None:
        if cmd.event:
            log_event(cmd.message, level=cmd.level)
        else:
            level = getattr(logging, cmd.level.upper(), logging.INFO)
            logging.log(level, cmd.message)

    def _exec_prepare_failover_vote(self, cmd: PrepareFailoverVote) -> bool:
        if not self._db.stop_restoring_wal():
            return False
        if not self._db.disable_wal_receiver(cmd.walreceiver_timeout):
            return False
        if not cmd.publish_vote:
            return True
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
