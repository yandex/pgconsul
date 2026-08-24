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
from typing import TYPE_CHECKING, Any, Callable, Protocol

from .commands import (
    AcquireLock,
    CheckDivergence,
    Checkpoint,
    ClearLocalState,
    CleanupSwitchover,
    CleanupVotes,
    Command,
    CreateSlots,
    DeleteHostOp,
    DisableWalReceiver,
    DoFailover,
    EnsureRestoringWal,
    FailoverTransitionTo,
    InitializeFailover,
    LeaveSyncGroup,
    Log,
    Plan,
    ReleaseLock,
    ResetFailoverNode,
    RewindFromSource,
    SetSSNBeforePromote,
    SetSimplePrimarySwitchTry,
    SetSyncReplication,
    SimplePrimarySwitch,
    Sleep,
    StartTimer,
    StopPooler,
    StopPostgresql,
    StopTimer,
    StoreReplicsInfo,
    TransitionTo,
    WriteCandidate,
    WriteCurrentPromotingHost,
    WriteElectionStatus,
    WriteElectionVote,
    WriteElectionWinner,
    WriteFailoverState,
    WriteLastFailoverTime,
    WriteLastSwitchoverTime,
    WriteLocalState,
    WriteSideReplicas,
    WriteTimeline,
)
from .exceptions import PostgresConnectionError
from .log_formatters import log_event
from .local_state import LocalStateError
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

    Generic over the observation type so both switchover and return-to-cluster
    machines satisfy the protocol. ``observation`` is ``Any`` because each
    machine defines its own observation dataclass.
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
        do_failover: Callable[..., bool],
        set_simple_primary_switch_try: Callable[[], None],
        create_slots_for_hosts: Callable[[list[str]], bool],
        simple_primary_switch: Callable[..., bool] | None = None,
        ensure_restoring_wal: Callable[[], None] | None = None,
        # Failover opaque callback (ADR-0007 §4).
        set_ssn_before_promote: Callable[..., bool] | None = None,
        initialize_failover: Callable[[dict, dict], bool] | None = None,
        local_states: 'dict[str, LocalStateStore] | None' = None,
    ) -> None:
        self._zk = zk
        self._db = db
        self._replication_manager = replication_manager
        self._timings = timings
        # Opaque composite callbacks (delegated to pgconsul methods, ADR-0006 §3).
        self._stop_postgresql = stop_postgresql
        self._store_replics_info = store_replics_info
        self._rewind_from_source = rewind_from_source
        self._do_failover = do_failover
        self._set_simple_primary_switch_try = set_simple_primary_switch_try
        self._create_slots_for_hosts = create_slots_for_hosts
        # Return-to-cluster callbacks (MDB-41951).
        self._simple_primary_switch = simple_primary_switch
        self._ensure_restoring_wal = ensure_restoring_wal
        # Failover opaque callback (ADR-0007 §4).
        self._set_ssn_before_promote = set_ssn_before_promote
        self._initialize_failover = initialize_failover
        self._local_states = local_states or {}
        # Iteration context for commands needing raw state dicts (StoreReplicsInfo).
        self._db_state: dict | None = None
        self._zk_state: dict | None = None

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
            case WriteFailoverState():
                return self._zk.write_failover_state(cmd.value)
            case WriteTimeline():
                return self._zk.write_timeline(cmd.timeline)
            case WriteLastSwitchoverTime():
                return self._zk.write_last_switchover_time()
            case StopPooler():
                return self._db.pgpooler('stop')
            case StopPostgresql():
                timeout = cmd.timeout if cmd.timeout is not None else _DEFAULT_STOP_PG_TIMEOUT
                return self._stop_postgresql(
                    timeout=timeout, wait=cmd.wait, force_async=cmd.force_async
                ) == 0
            case Checkpoint():
                return bool(self._db.checkpoint())
            case StoreReplicsInfo():
                return self._exec_store_replics_info()
            case LeaveSyncGroup():
                self._replication_manager.leave_sync_group()
                return True
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
                return self._zk.write_switchover_candidate(cmd.candidate)
            case WriteSideReplicas():
                return self._zk.write_switchover_side_replicas(list(cmd.side_replicas))
            case SetSyncReplication():
                return self._replication_manager.change_replication_to_sync_host(cmd.host)
            case CleanupSwitchover():
                for scope in ('switchover_primary', 'switchover_candidate'):
                    store = self._local_states.get(scope)
                    if store is not None:
                        store.clear()
                return self._zk.cleanup_switchover()
            case InitializeFailover():
                return self._exec_initialize_failover()
            # --- Opaque commands (delegated to pgconsul methods, ADR-0006 §3) ---
            case DoFailover():
                return bool(self._do_failover(old_primary=cmd.old_primary, operation=cmd.operation))
            case RewindFromSource():
                result = self._rewind_from_source(
                    is_postgresql_dead=cmd.is_postgresql_dead,
                    limit=cmd.limit,
                    new_primary=cmd.new_primary,
                )
                return bool(result)
            case SetSimplePrimarySwitchTry():
                self._set_simple_primary_switch_try()
                return True
            case DeleteHostOp():
                self._zk.delete_host_op()
                return True
            case CreateSlots():
                return self._create_slots_for_hosts(list(cmd.hosts))
            # --- Return-to-cluster commands (MDB-41951) ---
            case SimplePrimarySwitch():
                if self._simple_primary_switch is None:
                    logging.error('SimplePrimarySwitch: callback not configured')
                    return False
                return bool(self._simple_primary_switch(
                    limit=cmd.limit,
                    new_primary=cmd.new_primary,
                    is_dead=cmd.is_dead,
                ))
            case EnsureRestoringWal():
                # Silent skip: restore may already be enabled, missing callback is not an error.
                if self._ensure_restoring_wal is not None:
                    self._ensure_restoring_wal()
                return True
            case CheckDivergence():
                # No-op marker: the machine re-derives divergence from the
                # next observation. Always succeeds.
                return True
            # --- Failover commands (ADR-0007 §4) ---
            case SetSSNBeforePromote():
                return self._exec_set_ssn_before_promote(cmd)
            case WriteCurrentPromotingHost():
                return self._zk.write_current_promoting_host()
            case WriteLastFailoverTime():
                return self._zk.write_last_failover_time()
            case CleanupVotes():
                return self._exec_cleanup_votes()
            case WriteElectionStatus():
                return self._zk.write_election_status(cmd.status)
            case WriteElectionVote():
                return self._zk.write_election_vote(cmd.lsn, cmd.priority)
            case WriteElectionWinner():
                return self._zk.write_election_winner(cmd.winner)
            case ResetFailoverNode():
                return self._exec_reset_failover_node()
            case FailoverTransitionTo():
                return self._exec_failover_transition_to(cmd.phase)
            case DisableWalReceiver():
                return self._db.disable_wal_receiver(cmd.timeout)
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
        if self._initialize_failover is None:
            logging.error('InitializeFailover: callback not configured')
            return False
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
        if not self._zk.write_switchover_state(phase):
            logging.error('Failed to persist switchover phase %s to ZK', phase)
            return False
        log_event(f'SWITCHOVER PHASE → {phase}', level='warning')
        return True

    def _exec_log(self, cmd: Log) -> None:
        if cmd.event:
            log_event(cmd.message, level=cmd.level)
        else:
            level = getattr(logging, cmd.level.upper(), logging.INFO)
            logging.log(level, cmd.message)

    # --- Failover command implementations (ADR-0007 §4) ---

    def _exec_set_ssn_before_promote(self, cmd: SetSSNBeforePromote) -> bool:
        if self._set_ssn_before_promote is None:
            logging.error('SetSSNBeforePromote: callback not configured')
            return False
        return bool(self._set_ssn_before_promote(old_primary=cmd.old_primary))

    def _exec_cleanup_votes(self) -> bool:
        """Delete election vote nodes for all HA hosts."""
        ha_hosts = self._zk.get_ha_hosts() or []
        ok = True
        for host in ha_hosts:
            if not self._zk.delete_election_vote(host):
                ok = False
        return ok

    def _exec_reset_failover_node(self) -> bool:
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
        if not self._zk.write_failover_state(phase):
            logging.error('Failed to persist failover phase %s to ZK', phase)
            return False
        log_event(f'FAILOVER PHASE → {phase}', level='warning')
        return True
