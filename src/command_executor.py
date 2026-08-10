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
from typing import TYPE_CHECKING, Callable, Protocol

from .commands import (
    AcquireLock,
    Checkpoint,
    CleanupSwitchover,
    Command,
    CreateSlots,
    DeleteHostOp,
    DoFailover,
    LeaveSyncGroup,
    Log,
    Plan,
    ReleaseLock,
    RewindFromSource,
    SetSimplePrimarySwitchTry,
    SetSyncReplication,
    Sleep,
    StartTimer,
    StopPooler,
    StopPostgresql,
    StopTimer,
    StoreReplicsInfo,
    TransitionTo,
    WriteCandidate,
    WriteFailoverState,
    WriteLastSwitchoverTime,
    WriteSideReplicas,
    WriteTimeline,
)
from .exceptions import PostgresConnectionError
from .log_formatters import log_event
from .zk import ZookeeperException

if TYPE_CHECKING:
    from .pg import Postgres
    from .replication_manager import ReplicationManager
    from .switchover import SwitchoverObservation, SwitchoverPhase
    from .timings import TimingTracker
    from .zk import Zookeeper


class PlanMachine(Protocol):
    """Protocol for state machines that produce a Command Plan (ADR-0006 §5)."""

    def plan(self, observation: SwitchoverObservation) -> Plan:
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

    def run(self, machine: PlanMachine, observation: SwitchoverObservation) -> bool:
        """Execute one step: call machine.plan(obs), run the returned Plan.

        Returns True if a non-empty Plan was produced (iteration budget consumed),
        False if the Plan is empty (nothing to do — retry next time).
        Stops on the first failing command (fail-fast: return True, retry next).
        """
        plan = machine.plan(observation)
        if not plan:
            return False
        for cmd in plan:
            if not self._dispatch(cmd):
                return True
        return True

    def _dispatch(self, cmd: Command) -> bool:
        """Execute a single command. Returns False on failure (fail-fast).

        Catches PostgresConnectionError / ZookeeperException per-command
        (ADR-0002): logs and returns False so run() stops and retries next.
        """
        try:
            return self._exec(cmd)
        except (PostgresConnectionError, ZookeeperException):
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
                self._zk.release_lock(lock_type=cmd.lock_type, wait=cmd.wait)
                return True
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
                self._db.pgpooler('stop')
                return True
            case StopPostgresql():
                timeout = cmd.timeout if cmd.timeout is not None else 60
                return self._stop_postgresql(
                    timeout=timeout, wait=cmd.wait, force_async=cmd.force_async
                ) == 0
            case Checkpoint():
                self._db.checkpoint()
                return True
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
            # --- Switchover commands ---
            case TransitionTo():
                return self._exec_transition_to(cmd.phase)
            case WriteCandidate():
                return self._zk.write_switchover_candidate(cmd.candidate)
            case WriteSideReplicas():
                return self._zk.write_switchover_side_replicas(cmd.side_replicas)
            case SetSyncReplication():
                return self._replication_manager.change_replication_to_sync_host(cmd.host)
            case CleanupSwitchover():
                self._zk.cleanup_switchover()
                return True
            # --- Opaque commands (delegated to pgconsul methods, ADR-0006 §3) ---
            case DoFailover():
                return bool(self._do_failover(old_primary=cmd.old_primary))
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
                return self._create_slots_for_hosts(cmd.hosts)
            case _:
                logging.error('Unknown command type: %s', type(cmd).__name__)
                return False

    # --- Command implementations ---

    def _exec_store_replics_info(self) -> bool:
        if self._db_state is None or self._zk_state is None:
            logging.error('StoreReplicsInfo: iteration state not set')
            return False
        return bool(self._store_replics_info(self._db_state, self._zk_state))

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
