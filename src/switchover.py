# encoding: utf-8
"""
Switchover state machine types and primary-side machine (MDB-41951, ADR-0005 §3).

Phase values are persisted in ZK node ``switchover/state``. New values
(``sync_set``, ``primary_shut``, ``promoted``) are chosen so that old pgconsul
versions treat them as "not scheduled" and do not start a parallel switchover
(ADR-0005 §5 — two-phase rollout).
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Callable

from .exceptions import PostgresConnectionError
from .log_formatters import log_event
from .types import ReplicaInfos

if TYPE_CHECKING:
    from .pg import Postgres
    from .replication_manager import ReplicationManager
    from .timings import TimingTracker
    from .zk import Zookeeper


def _check_last_failover_time(last: float | None, min_timeout: float) -> bool:
    """Return True if last failover was long enough ago (or never happened).

    Local copy of ``helpers.check_last_failover_time`` that takes a scalar
    ``min_timeout`` instead of a config object, so the state machine does not
    depend on the full pgconsul config.
    """
    if not last:
        return True
    return (time.time() - last) > min_timeout


class StrEnum(str, Enum):
    """Minimal StrEnum compatible with any Python version.

    str() returns the member value (not "Class.NAME"), matching the
    stdlib enum.StrEnum behaviour used for ZK serialization.
    """

    def __str__(self) -> str:
        return self.value


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
    min_failover_timeout: float = 0.0
    # Seconds to wait after stopping PG so the sync replica drains last WAL.
    wal_drain_delay: float = 5.0


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
    get_switchover_candidate: Callable[[], str | None]
    get_replics_info: Callable[[str], ReplicaInfos]
    get_ha_replics: Callable[[str], set[str] | None]
    get_last_failover_time: Callable[[], float | None]
    get_last_switchover_time: Callable[[], float | None]
    get_failover_state: Callable[[], str | None]
    get_timeline: Callable[[], int | None]


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
            SwitchoverPhase.SCHEDULED: self._handle_scheduled,
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

    def _handle_scheduled(self, record: 'SwitchoverRecord', db_state: dict, zk_state: dict) -> bool:
        """scheduled → sync_set: sanity-check, choose candidate, set sync replication.

        Replaces the legacy ``_check_primary_switchover`` + first part of
        ``_do_primary_switchover`` (step 14h). All sanity gates are ported 1:1;
        each gate that fails returns True (iteration consumed, retry next time).

        Idempotent (ADR-0005 §3): ``timings.start('switchover')`` only if not
        already started; ``change_replication_to_sync_host`` is safe to repeat.
        """
        assert self._ctx is not None
        ctx = self._ctx

        # --- Sanity gates (ported from _check_primary_switchover) ---

        # The node contains hostname of current instance.
        if record.hostname != ctx.get_hostname():
            logging.warning(
                'Switchover scheduled: hostname %s differs from current %s, ignoring',
                record.hostname, ctx.get_hostname(),
            )
            return True

        # Current instance is primary.
        if ctx.db.get_role() != 'primary':
            logging.error(
                'Switchover scheduled: current role is %s, ignoring switchover',
                ctx.db.get_role(),
            )
            return True

        # Timeline of the current instance matches the timeline in switchover node.
        zk_tli = ctx.get_timeline()
        sw_tli = record.timeline
        if zk_tli != sw_tli:
            logging.warning(
                'Switchover scheduled: ZK timeline %s differs from switchover timeline %s, ignoring',
                zk_tli, sw_tli,
            )
            return True

        # Ensure there is no other failover in progress.
        failover_state = ctx.get_failover_state()
        if failover_state not in ('finished', None):
            logging.error(
                'Switchover scheduled: current failover state is %s, ignoring switchover',
                failover_state,
            )
            return True

        # Last role transition was more than min_failover_timeout ago
        # (or enough replicas are alive).
        last_failover_ts = ctx.get_last_failover_time()
        last_switchover_ts = ctx.get_last_switchover_time()
        last_role_transition_ts: float = 0.0
        if last_failover_ts is not None or last_switchover_ts is not None:
            last_role_transition_ts = max(
                x for x in (last_switchover_ts, last_failover_ts) if x is not None
            )

        ha_replics = ctx.get_ha_replics(ctx.get_hostname())
        if ha_replics is None:
            logging.warning('Switchover scheduled: HA replicas are empty, ignoring switchover')
            return True

        # Check last failover time only if we have replics_info in db_state.
        replics_info = db_state.get('replics_info', [])
        alive_replics_number = len([i for i in replics_info if i.get('state') == 'streaming'])
        if not _check_last_failover_time(last_role_transition_ts, self._cfg.min_failover_timeout) and (
            alive_replics_number < len(ha_replics)
        ):
            logging.warning(
                'Switchover scheduled: last role transition was %.1f seconds ago,'
                ' and alive host count less than HA hosts (HA: %d, alive: %d) ignoring switchover.',
                time.time() - last_role_transition_ts,
                len(ha_replics),
                alive_replics_number,
            )
            return True

        # --- Choose candidate (ported from _get_switchover_candidate) ---

        candidate = ctx.get_switchover_candidate()
        if candidate is None:
            logging.info('Switchover scheduled: no eligible candidate, waiting')
            return True

        # --- Check candidate is in sync (ported from _candidate_is_sync_with_primary) ---

        if not ctx.candidate_is_sync(replics_info, candidate):
            logging.info('Switchover scheduled: candidate %s not yet in sync, waiting', candidate)
            return True

        # --- Action: set sync replication and transition to sync_set ---

        logging.info('Scheduled switchover checks passed OK.')

        # Idempotent: start timer only if not already started.
        if ctx.timings.get_start('switchover') is None:
            ctx.timings.start('switchover')

        logging.warning('Starting sync replication %s', candidate)
        if not ctx.replication_manager.change_replication_to_sync_host(candidate):
            logging.error('Switchover scheduled: failed to make switchover candidate single sync host')
            self.transition_to(SwitchoverPhase.FAILED)
            return True

        # Persist SYNC_SET before the action (ADR-0005 §3).
        if not self.transition_to(SwitchoverPhase.SYNC_SET):
            return True

        return True

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
        if not ctx.zk.is_host_alive(candidate, timeout=1):
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
        if self._cfg.wal_drain_delay > 0:
            time.sleep(self._cfg.wal_drain_delay)

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


# --- Candidate-side state machine (ADR-0005 §3, step 15a) ---


@dataclass
class CandidateContext:
    """
    Operational dependencies injected into CandidateSwitchoverMachine (ADR-0004).

    Groups the infra objects and callbacks that the candidate machine needs.
    """

    zk: 'Zookeeper'
    db: 'Postgres'
    timings: 'TimingTracker'
    # Callbacks delegating to pgconsul methods.
    create_slots_for_hosts: Callable[[list[str]], bool]
    all_side_replicas_turned: Callable[[list[str]], bool]
    do_failover: Callable[..., bool]
    get_hostname: Callable[[], str]


class CandidateSwitchoverMachine:
    """
    Candidate-side switchover state machine (ADR-0005 §3, step 15a).

    One ``step()`` call per iteration; phase is persisted to ZK before the
    phase action executes so that restarts resume from the same phase.

    Handles phases: ``initiated`` (create slots, wait for side replicas),
    ``candidate_found`` (acquire lock, promote, cleanup).

    Dependencies are injected via :class:`CandidateContext` (ADR-0004).
    Pass ``context=None`` to create a stub-only machine (for tests that only
    check dispatch logic).
    """

    def __init__(
        self,
        zk: 'Zookeeper',
        context: 'CandidateContext | None' = None,
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
        False if no handler is registered for the current phase.
        """
        handlers = {
            SwitchoverPhase.INITIATED: self._handle_initiated,
            SwitchoverPhase.CANDIDATE_FOUND: self._handle_candidate_found,
            # primary_shut: old primary released the lock — candidate must
            # acquire it and promote. Same handler as candidate_found.
            SwitchoverPhase.PRIMARY_SHUT: self._handle_candidate_found,
        }
        phase = record.phase
        handler = handlers.get(phase)  # type: ignore[arg-type]
        if handler is None:
            logging.debug('No candidate-side handler for switchover phase %s', phase)
            return False
        if self._ctx is None:
            logging.debug('CandidateSwitchoverMachine: no context — step skipped for phase %s', phase)
            return False
        return handler(record, db_state, zk_state)

    # --- Phase handlers (ADR-0005 §3, step 15a) ---

    def _handle_initiated(self, record: 'SwitchoverRecord', db_state: dict, zk_state: dict) -> bool:
        """initiated → candidate_found: create slots, check side replicas turned (non-blocking).

        Idempotent (ADR-0005 §3):
        - Slot creation is safe to repeat.
        - Side replica check is a single non-blocking probe per iteration
          (replaces the former blocking ``helpers.await_for``).
        """
        assert self._ctx is not None
        ctx = self._ctx

        side_replicas = record.side_replicas

        # Create slots before promote to allow side replicas to turn.
        if side_replicas and not ctx.create_slots_for_hosts(side_replicas):
            logging.warning('Failed to create slots for side replicas, retrying next iteration')
            return True

        # Non-blocking check: are all side replicas streaming from us?
        # Replaces the former blocking helpers.await_for (ADR-0005 §1).
        if side_replicas:
            try:
                if not ctx.all_side_replicas_turned(side_replicas):
                    logging.info('Waiting for side replicas to turn to candidate')
                    return True
            except PostgresConnectionError:
                logging.warning('Could not check side replicas, retrying next iteration', exc_info=True)
                return True

        logging.info('All side replicas turned to candidate, signaling primary')
        if not self.transition_to(SwitchoverPhase.CANDIDATE_FOUND):
            return True
        return True

    def _handle_candidate_found(self, record: 'SwitchoverRecord', db_state: dict, zk_state: dict) -> bool:
        """candidate_found → promoted: acquire lock, do_failover, cleanup.

        Step 15b: non-blocking lock acquisition — one attempt per iteration.
        If the lock is still held by the old primary, returns True and retries
        on the next iteration (ADR-0005 §1: no blocking wait inside iteration).
        """
        assert self._ctx is not None
        ctx = self._ctx

        if self._debug_failure('candidate_switchover_before_acquire'):
            return True

        # Non-blocking lock acquisition: timeout=0 means a single attempt.
        # Retries happen on subsequent iterations while phase stays candidate_found.
        logging.info('Attempting to acquire the lock (non-blocking)')
        if not ctx.zk.try_acquire_lock(allow_queue=True, timeout=0):
            logging.info('Could not acquire lock in ZK, will retry next iteration.')
            return True

        switchover_info = ctx.zk.get_switchover_primary_info()
        if switchover_info is None:
            logging.error('Failed to get switchover primary info from ZK.')
            ctx.zk.release_lock()
            return True

        # Start downtime timer if not already started (idempotent).
        # The old primary normally starts it in candidate_found phase, but if it
        # was killed (e.g. kill -9) before reaching that phase, the candidate must
        # start it here so that stop('downtime') in _promote records a value.
        if ctx.timings.get_start('downtime') is None:
            ctx.timings.start('downtime')

        if not ctx.do_failover(old_primary=switchover_info.get('hostname')):
            ctx.zk.release_lock()
            return True

        # Write promoted phase as observability marker before cleanup.
        self.transition_to(SwitchoverPhase.PROMOTED)

        # Cleanup switchover nodes and finalize.
        ctx.zk.cleanup_switchover()
        ctx.zk.write_last_switchover_time()
        ctx.timings.stop('switchover')

        return True
