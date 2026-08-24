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

from . import helpers
from .commands import (
    AcquireLock,
    Checkpoint,
    CleanupSwitchover,
    CleanupVotes,
    Command,
    CreateSlots,
    DeleteHostOp,
    DisableWalReceiver,
    DoFailover,
    FailoverTransitionTo,
    LeaveSyncGroup,
    Log,
    Plan,
    ReleaseLock,
    ResetFailoverNode,
    RewindFromSource,
    SetSSNBeforePromote,
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
    WriteCurrentPromotingHost,
    WriteHostStat,
    WriteElectionStatus,
    WriteElectionVote,
    WriteElectionWinner,
    WriteFailoverState,
    WriteLastFailoverTime,
    WriteLastSwitchoverTime,
    WriteSideReplicas,
    WriteTimeline,
)
from .debug import DebugFailure
from .exceptions import PostgresConnectionError
from .log_formatters import log_event
from .zk import ZookeeperException

if TYPE_CHECKING:
    from .failover import FailoverPhase
    from .pg import Postgres
    from .replication_manager import ReplicationManager
    from .slot_manager import ReplicationSlotManager
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
        slot_manager: ReplicationSlotManager,
        *,
        rewind_from_source: Callable[..., bool | None],
        debug_failure: DebugFailure,
        promote_checkpoint_sql: str | None,
    ) -> None:
        self._zk = zk
        self._db = db
        self._replication_manager = replication_manager
        self._timings = timings
        self._slot_manager = slot_manager
        # Opaque composite callback (delegated to pgconsul method, ADR-0006 §3).
        self._rewind_from_source = rewind_from_source
        # Failover promote logic (moved from Pgconsul, ADR-0007 §2.3).
        self._debug_failure = debug_failure
        self._promote_checkpoint_sql = promote_checkpoint_sql

    def run(self, machine: PlanMachine, observation: Any) -> None:
        """Execute one step: call machine.plan(obs), run the returned Plan.

        Stops on the first failing command (fail-fast: retry next iteration).
        Empty plan = nothing to do (retry next time).
        """
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
                if cmd.force_async:
                    try:
                        self._replication_manager.change_replication_to_async(
                            reset_sync_replication_in_zk=False
                        )
                    except (PostgresConnectionError, ZookeeperException):
                        logging.warning(
                            'StopPostgresql: failed to switch to async, continuing',
                            exc_info=True,
                        )
                return self._db.stop_postgresql(timeout=timeout, wait=cmd.wait) == 0
            case Checkpoint():
                return bool(self._db.checkpoint())
            case StoreReplicsInfo():
                return self._exec_store_replics_info(cmd)
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
                return self._zk.write_switchover_side_replicas(list(cmd.side_replicas))
            case SetSyncReplication():
                return self._replication_manager.change_replication_to_sync_host(cmd.host)
            case CleanupSwitchover():
                self._zk.cleanup_switchover()
                return True
            # --- Opaque commands (delegated to pgconsul methods, ADR-0006 §3) ---
            case DoFailover():
                return self._do_failover(old_primary=cmd.old_primary)
            case RewindFromSource():
                result = self._rewind_from_source(
                    is_postgresql_dead=cmd.is_postgresql_dead,
                    limit=cmd.limit,
                    new_primary=cmd.new_primary,
                )
                return bool(result)
            case SetSimplePrimarySwitchTry():
                self._zk.set_simple_primary_switch_tried(cmd.hostname)
                return True
            case DeleteHostOp():
                self._zk.delete_host_op()
                return True
            case CreateSlots():
                return self._slot_manager.create_slots_for_hosts(list(cmd.hosts))
            case WriteHostStat():
                return bool(
                    self._zk.write_host_stat(cmd.hostname, cmd.db_state, cmd.stream_from)
                )
            # --- Failover commands (ADR-0007 §4) ---
            case SetSSNBeforePromote():
                ha_replicas = self._zk.get_quorum_replics_for_promote()
                return bool(
                    self._replication_manager.set_ssn_before_promote(
                        ha_replicas, old_primary=cmd.old_primary
                    )
                )
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

    def _exec_store_replics_info(self, cmd: StoreReplicsInfo) -> bool:
        if not cmd.timeline_match or cmd.replics_info is None:
            return False
        return bool(self._zk.write_replics_info(cmd.replics_info))

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

    def _exec_cleanup_votes(self) -> bool:
        """Delete election vote nodes for all HA hosts."""
        ha_hosts = self._zk.get_ha_hosts() or []
        ok = True
        for host in ha_hosts:
            if not self._zk.delete_election_vote(host):
                ok = False
        return ok

    def _exec_reset_failover_node(self) -> bool:
        """Reset failover ZK node to 'finished' and clean up (ADR-0007 §4)."""
        if (self._zk.get_failover_state() == 'finished'
                or self._zk.write_failover_state('finished')
        ) and self._zk.delete_current_promoting_host():
            self._zk.delete_failover_must_be_reset()
            return True
        self._zk.ensure_failover_must_be_reset()
        logging.warning('ResetFailoverNode: could not reset, will retry next iteration')
        return False

    def _exec_failover_transition_to(self, phase: 'FailoverPhase') -> bool:
        """Persist failover phase to ZK before the action (ADR-0007 §2 fence)."""
        if not self._zk.write_failover_state(phase):
            logging.error('Failed to persist failover phase %s to ZK', phase)
            return False
        log_event(f'FAILOVER PHASE → {phase}', level='warning')
        return True

    # --- Failover promote logic (moved from Pgconsul, ADR-0007 §2.3) ---

    def _do_failover(self, old_primary: str | None = None) -> bool:
        # Critical section (ADR-0002 §2): DB loss here is caught and returned
        # as False so the caller releases the leader lock. _do_failover owns
        # only the promote logic; the lock is managed by its callers.
        try:
            # Resume WAL replay after acquiring the primary lock (was in
            # _accept_failover before ADR-0007 integration).
            self._db.pg_wal_replay_resume()

            if not self._zk.delete_failover_state():
                logging.error('Could not remove previous failover state.')
                return False

            if not self._promote_handle_slots():
                return False

            if self._debug_failure('before_promote'):
                return False

            if not self._replication_manager.set_ssn_before_promote(
                self._zk.get_quorum_replics_for_promote(), old_primary=old_primary
            ):
                logging.error('Failed to set SSN before promote, aborting promote')
                return False

            if not self._promote():
                return False

            self._replication_manager.leave_sync_group()
            self._replication_manager.remove_self_from_quorum_after_promote()
            return True
        except PostgresConnectionError:
            logging.warning('DB connection lost during failover.', exc_info=True)
            return False

    def _promote(self) -> bool:
        if not self._zk.write_failover_state('promoting'):
            logging.error('Could not write failover state to ZK.')
            return False

        if not self._zk.write_current_promoting_host():
            logging.error('Could not write self as last promoted host.')
            return False

        if not self._db.promote():
            logging.error('Could not promote me as a new primary. We should release the lock in ZK here.')
            # We need to close here and recheck postgres role. If it was no actual
            # promote, we need too delete self as last promoted host, mark failover "finished"
            # and return to cluster. If self primary we need to continue promote despite on exit code
            # because self already accepted some data modification which will be loss if
            # we simply return False here.
            if self._db.get_role() != 'primary':
                self._db.pgpooler('stop')
                if not self._zk.delete_current_promoting_host():
                    logging.error('Could not remove self as current promoting host.')
                if not self._zk.write_failover_state('finished'):
                    logging.error('Could not write failover state to ZK.')
                return False

            logging.info('Promote command failed but we are current primary. Continue')

        self._timings.stop('downtime')

        self._slot_manager.reset_on_promote()

        if not self._zk.write_failover_state('checkpointing'):
            logging.warning('Could not write failover state to ZK.')

        logging.debug('Doing checkpoint after promoting.')
        # Post-promote critical section (ADR-0002 §2): cosmetic — promote already succeeded.
        try:
            self._db.checkpoint(query=self._promote_checkpoint_sql)
        except PostgresConnectionError:
            logging.warning('Could not checkpoint after failover.', exc_info=True)

        my_tli = self._db.get_timeline()

        if not self._zk.write_timeline(my_tli):
            logging.warning('Could not write timeline to ZK.')

        if not self._zk.write_failover_state('finished'):
            logging.error('Could not write failover state to ZK.')

        if not self._zk.delete_current_promoting_host():
            logging.error('Could not remove self as current promoting host.')

        return True

    def _promote_handle_slots(self) -> bool:
        if not self._zk.write_failover_state('creating_slots'):
            logging.warning('Could not write failover state to ZK.')
        hosts = self._zk.get_ha_replics(helpers.get_hostname())
        if hosts is None:
            logging.error(
                'Could not get all hosts list from ZK. '
                'Replication slots should be created but we '
                'are unable to do it. Releasing the lock.'
            )
            return False
        return self._slot_manager.create_slots_for_hosts(list(hosts))
