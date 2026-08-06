# encoding: utf-8
"""
Switchover state machine types and primary-side machine (MDB-41951, ADR-0005 §3).

Phase values are persisted in ZK node ``switchover/state``. New values
(``sync_set``, ``primary_shut``, ``promoted``) are chosen so that old pgconsul
versions treat them as "not scheduled" and do not start a parallel switchover
(ADR-0005 §5 — two-phase rollout).
"""

import logging
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Callable

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:

    class StrEnum(str, Enum):
        """Backport of enum.StrEnum for Python 3.10.

        str() returns the member value (not "Class.NAME"), matching the
        stdlib StrEnum behaviour used for ZK serialization.
        """

        def __str__(self) -> str:
            return self.value

from .exceptions import PostgresConnectionError
from .log_formatters import log_event
from .types import ReplicaInfos

if TYPE_CHECKING:
    from .pg import Postgres
    from .replication_manager import ReplicationManager
    from .timings import TimingTracker
    from .zk import Zookeeper


class SwitchoverPhase(StrEnum):
    """Persistent phases of the switchover state machine."""

    # External entry point — written by dbaas_worker / pgconsul-util.
    SCHEDULED = 'scheduled'
    # Primary has switched replication to sync on the candidate.
    SYNC_SET = 'sync_set'
    # Primary has fixed candidate + side replicas and announced switchover.
    INITIATED = 'initiated'
    # Candidate has created slots, turned side replicas, and is ready.
    CANDIDATE_FOUND = 'candidate_found'
    # Old primary stopped pooler + PG and released the leader lock.
    PRIMARY_SHUT = 'primary_shut'
    # Candidate took the lock and promoted itself.
    PROMOTED = 'promoted'
    # Switchover failed — rollback / cleanup needed.
    FAILED = 'failed'

    @classmethod
    def from_str(cls, value: str | None) -> 'SwitchoverPhase | None':
        """Parse a ZK state string into a phase, or None if absent/unknown."""
        if value is None:
            return None
        try:
            return cls(value)
        except ValueError:
            logging.warning('Unknown switchover state value: %s', value)
            return None


@dataclass
class SwitchoverRecord:
    """
    Typed view of the switchover ZK nodes.

    Mirrors the JSON stored in ``switchover/master`` (hostname, timeline,
    destination) plus the scalar ``switchover/state`` and the list
    ``switchover/side_replicas``.
    """

    hostname: str | None = None
    timeline: int | None = None
    destination: str | None = None
    phase: SwitchoverPhase | None = None
    candidate: str | None = None
    side_replicas: list[str] = field(default_factory=list)

    @classmethod
    def from_zk_state(cls, zk_state: dict, zk) -> 'SwitchoverRecord':
        """Build a record from a ``zk.get_state()`` snapshot.

        ``zk`` is the ``Zookeeper`` instance (used only for path constants).
        """
        info = zk_state.get(zk.SWITCHOVER_ROOT_PATH) or {}
        state_str = zk_state.get(zk.SWITCHOVER_STATE_PATH)
        side = zk_state.get(zk.SWITCHOVER_SIDE_REPLICAS) or []
        candidate = zk_state.get(zk.SWITCHOVER_CANDIDATE)
        return cls(
            hostname=info.get('hostname'),
            timeline=info.get(zk.TIMELINE_INFO_PATH),
            destination=info.get('destination'),
            phase=SwitchoverPhase.from_str(state_str),
            candidate=candidate,
            side_replicas=list(side) if side else [],
        )

    def belongs_to(self, hostname: str) -> bool:
        """True if this switchover record targets the given hostname."""
        return self.hostname == hostname

    def is_active(self) -> bool:
        """True if the record represents an in-progress (resumable) switchover."""
        return self.phase in (
            SwitchoverPhase.SCHEDULED,
            SwitchoverPhase.SYNC_SET,
            SwitchoverPhase.INITIATED,
            SwitchoverPhase.CANDIDATE_FOUND,
            SwitchoverPhase.PRIMARY_SHUT,
            SwitchoverPhase.PROMOTED,
        )

    def is_failed(self) -> bool:
        return self.phase == SwitchoverPhase.FAILED


@dataclass
class SwitchoverMachineConfig:
    """Configuration values consumed by PrimarySwitchoverMachine (ADR-0004)."""

    catchup_timeout: float = 60.0
    rollback_timeout: float = 60.0
    max_allowed_lag_ms: int = 10


@dataclass
class PrimaryContext:
    """
    Operational dependencies injected into PrimarySwitchoverMachine (ADR-0004).

    Groups the infra objects and callbacks that the machine needs, keeping the
    constructor signature short and the class testable via mocks.
    """

    zk: 'Zookeeper'
    db: 'Postgres'
    replication_manager: 'ReplicationManager'
    timings: 'TimingTracker'
    # Callbacks delegating to pgconsul methods.
    stop_postgresql: Callable[..., int]
    get_streaming_replicas: Callable[[], list[str]]
    candidate_is_sync: Callable[[ReplicaInfos, str], bool]
    store_replics_info: Callable[[dict, dict], None]
    rewind_from_source: Callable[..., None]
    set_simple_primary_switch_try: Callable[[], None]
    get_hostname: Callable[[], str]


class PrimarySwitchoverMachine:
    """
    Primary-side switchover state machine (ADR-0005 §3).

    One ``step()`` call per iteration; phase is persisted to ZK
    before the action executes so that restarts resume from the same phase.

    Dependencies are injected via :class:`PrimaryContext` (ADR-0004).
    Pass ``context=None`` to create a stub-only machine (for tests that only
    check ``transition_to`` / dispatch logic).
    """

    def __init__(
        self,
        zk: 'Zookeeper',
        context: 'PrimaryContext | None' = None,
        config: 'SwitchoverMachineConfig | None' = None,
        debug_failure: Callable[[str], bool] | None = None,
    ) -> None:
        self._zk = zk
        self._ctx = context
        self._cfg = config or SwitchoverMachineConfig()
        self._debug_failure: Callable[[str], bool] = debug_failure or (lambda _: False)

    # --- Core machine API ---

    def transition_to(self, phase: SwitchoverPhase) -> bool:
        """Persist phase to ZK before executing the phase action (ADR-0005 §3).

        Returns False if the ZK write fails.
        """
        if not self._zk.write_switchover_state(phase):
            logging.error('Failed to persist switchover phase %s to ZK', phase)
            return False
        log_event(f'SWITCHOVER PHASE → {phase}', level='warning')
        return True

    def step(self, record: 'SwitchoverRecord', db_state: dict, zk_state: dict) -> bool:
        """Execute one step for the current phase.

        Returns True if a handler was invoked (iteration budget consumed),
        False if no handler is registered for the current phase (e.g. FAILED, None).
        Callers must skip repairs and stale-cleanup when True is returned.
        """
        handlers = {
            SwitchoverPhase.SYNC_SET: self._handle_sync_set,
            SwitchoverPhase.INITIATED: self._handle_initiated,
            SwitchoverPhase.CANDIDATE_FOUND: self._handle_candidate_found,
            SwitchoverPhase.PRIMARY_SHUT: self._handle_primary_shut,
        }
        phase = record.phase
        handler = handlers.get(phase)  # type: ignore[arg-type]
        if handler is None:
            logging.debug('No primary-side handler for switchover phase %s', phase)
            return False
        if self._ctx is None:
            logging.debug('PrimarySwitchoverMachine: no context — step skipped for phase %s', phase)
            return False
        return handler(record, db_state, zk_state)

    # --- Phase handlers (ADR-0005 §3, step 14c/14d) ---

    def _handle_sync_set(self, record: 'SwitchoverRecord', db_state: dict, zk_state: dict) -> bool:
        """sync_set → initiated: idempotently fix candidate + side replicas, write initiated.

        Idempotent (14d): writes to ZK only if not already present.
        """
        assert self._ctx is not None
        ctx = self._ctx

        candidate = record.candidate or record.destination
        if candidate is None:
            logging.error('Switchover sync_set: candidate is None, aborting')
            self.transition_to(SwitchoverPhase.FAILED)
            return True

        # Idempotent: write candidate to ZK (safe to repeat on restart).
        if not ctx.zk.write_switchover_candidate(candidate):
            logging.error('Switchover sync_set: failed to write candidate to ZK')
            return True

        # Compute side replicas: all streaming replicas except the candidate.
        try:
            side_replicas = [r for r in ctx.get_streaming_replicas() if r != candidate]
        except Exception:
            logging.exception('Switchover sync_set: failed to get streaming replicas')
            return True

        if not ctx.zk.write_switchover_side_replicas(side_replicas):
            logging.error('Switchover sync_set: failed to write side replicas')
            return True

        logging.info('Switchover sync_set: candidate=%s side_replicas=%s', candidate, side_replicas)

        # Transition to initiated (persisted before the action per ADR-0005 §3).
        if not self.transition_to(SwitchoverPhase.INITIATED):
            return True

        # Backwards compatibility: replicate old failover_state marker.
        ctx.zk.write_failover_state('switchover_initiated')
        return True

    def _handle_initiated(self, record: 'SwitchoverRecord', db_state: dict, zk_state: dict) -> bool:
        """initiated: wait (non-blocking) for candidate to set candidate_found.

        Per ADR-0005 §1: no blocking wait inside iteration.

        If the candidate's alive lock disappears (ZK session expired due to network
        disconnect), the switchover can never complete. Transition to FAILED so that
        _drop_stale_switchover cleans up the switchover record and failover_state,
        allowing side replicas to return to the cluster.
        """
        assert self._ctx is not None
        ctx = self._ctx

        candidate = record.candidate or record.destination
        if candidate is None:
            logging.error('Switchover initiated: candidate is None, aborting')
            self.transition_to(SwitchoverPhase.FAILED)
            return True

        # Re-read fresh ZK state to detect transition by the candidate.
        current_state = ctx.zk.get_switchover_state()
        if current_state == SwitchoverPhase.CANDIDATE_FOUND:
            # Candidate is ready — store replics info and checkpoint before shutdown.
            log_event('SWITCHOVER: candidate_found detected, proceeding to shutdown', level='warning')
            try:
                db_state['replics_info'] = ctx.db.get_replics_info('primary')
                ctx.store_replics_info(db_state, zk_state)
            except PostgresConnectionError:
                logging.warning('Could not update replics info before shutdown, continuing', exc_info=True)
            try:
                ctx.db.checkpoint()
            except PostgresConnectionError:
                logging.warning('Could not checkpoint before switchover, continuing', exc_info=True)
            return True

        # Candidate dead check: if its alive lock is gone, it can never write
        # candidate_found. Abort the switchover immediately.
        if not ctx.zk.is_host_alive(candidate):
            logging.warning(
                'Switchover initiated: candidate %s is no longer alive, aborting switchover', candidate
            )
            self.transition_to(SwitchoverPhase.FAILED)
            return True

        logging.debug('Switchover initiated: waiting for candidate_found (current=%s)', current_state)
        return True

    def _handle_candidate_found(self, record: 'SwitchoverRecord', db_state: dict, zk_state: dict) -> bool:
        """candidate_found → primary_shut: stop pooler, check sync, stop PG, release lock.

        Idempotent (14d):
        - downtime timer started only if not already started.
        - pooler stop: safe to call multiple times.
        - sync check: returns early if candidate not yet in sync (next iteration retries).
        - PG stop + lock release + PRIMARY_SHUT: executed once; transition acts as fence.
        """
        assert self._ctx is not None
        ctx = self._ctx

        candidate = record.candidate or record.destination
        if candidate is None:
            logging.error('Switchover candidate_found: candidate is None, aborting')
            self.transition_to(SwitchoverPhase.FAILED)
            return True

        # Start downtime timer only if not already started (idempotent).
        if ctx.timings.get_start('downtime') is None:
            ctx.timings.start('downtime')

        ctx.db.pgpooler('stop')
        logging.warning('Cluster closed from user requests (pooler stopped)')

        if self._debug_failure('primary_switchover_before_catchup'):
            self.transition_to(SwitchoverPhase.FAILED)
            return True

        # Non-blocking sync check: verify once per iteration, retry next if not synced.
        try:
            replics_info = ctx.db.get_replics_info('primary')
        except PostgresConnectionError:
            logging.warning('Could not get replics_info for sync check, retrying next iteration', exc_info=True)
            return True

        if not ctx.candidate_is_sync(replics_info, candidate):
            logging.info('Switchover candidate_found: candidate %s not yet in sync, waiting', candidate)
            return True

        logging.warning('Candidate %s is in sync, stopping PostgreSQL', candidate)

        # Stop PG without blocking wait.
        if ctx.stop_postgresql(wait=False, force_async=False) != 0:
            logging.error('Switchover candidate_found: unable to initiate PostgreSQL stop')
            return True

        # Give sync replica a chance to consume last WAL records.
        time.sleep(5)

        if self._debug_failure('primary_switchover_before_release'):
            self.transition_to(SwitchoverPhase.FAILED)
            return True

        # Backwards compatibility marker.
        ctx.zk.write_failover_state('switchover_master_shut')

        # Persist PRIMARY_SHUT before releasing the lock (idempotency fence for restarts).
        if not self.transition_to(SwitchoverPhase.PRIMARY_SHUT):
            return True

        # Release leader lock.
        ctx.zk.release_lock(lock_type=ctx.zk.PRIMARY_LOCK_PATH, wait=5)

        # Final blocking PG stop (best-effort).
        if ctx.stop_postgresql(force_async=False) != 0:
            if ctx.db.get_postgresql_status() == 0:
                logging.warning('Switchover: unable to confirm PostgreSQL stopped')

        if self._debug_failure('primary_switchover_after_release'):
            return True

        # Signal return-to-cluster.
        ctx.set_simple_primary_switch_try()
        return True

    def _handle_primary_shut(self, record: 'SwitchoverRecord', db_state: dict, zk_state: dict) -> bool:
        """primary_shut: idempotent recovery handler for restarts mid-shutdown.

        After _handle_candidate_found, PG is stopped and the lock released.
        If pgconsul restarts while in primary_shut, primary_iter may re-enter.
        This handler:
        1. Releases the lock if we somehow hold it again.
        2. Finds the new primary and rewinds to it.
        """
        assert self._ctx is not None
        ctx = self._ctx

        my_hostname = ctx.get_hostname()

        # Safety: if we hold the lock again (unexpected restart), release it.
        if ctx.zk.get_current_lock_holder() == my_hostname:
            logging.warning('Switchover primary_shut: unexpectedly holding the lock — releasing')
            ctx.db.pgpooler('stop')
            ctx.zk.release_lock(lock_type=ctx.zk.PRIMARY_LOCK_PATH, wait=5)
            return True

        # Find the new primary and rewind.
        new_primary = ctx.zk.get_current_lock_holder(ctx.zk.PRIMARY_LOCK_PATH)
        if new_primary is not None:
            log_event('SWITCHOVER: new primary found, returning to cluster', level='warning')
            ctx.zk.delete_host_op()
            ctx.set_simple_primary_switch_try()
            ctx.rewind_from_source(
                is_postgresql_dead=True,
                limit=self._cfg.rollback_timeout,
                new_primary=new_primary,
            )
            return True

        logging.info('Switchover primary_shut: waiting for new primary to take over')
        return True
