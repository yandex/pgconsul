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

from .commands import (
    AcquireLock,
    Checkpoint,
    CleanupSwitchover,
    CreateSlots,
    DeleteHostOp,
    DoFailover,
    Log,
    Plan as CommandPlan,
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
)
from .exceptions import PostgresConnectionError
from .helpers import app_name_from_fqdn
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
    # Primary stopped the pooler — granular kill-9 recovery point (ADR-0006 §4).
    POOLER_STOPPED = 'pooler_stopped'
    # Primary stopped PG — granular kill-9 recovery point (ADR-0006 §4).
    PG_STOPPED = 'pg_stopped'
    # Old primary stopped pooler + PG and released the leader lock.
    PRIMARY_SHUT = 'primary_shut'
    # Candidate acquired the leader lock but has not promoted yet.
    # Prevents the old primary from rewinding to the candidate before
    # promote completes (MDB-41951 race condition fix).
    CANDIDATE_ACQUIRED = 'candidate_acquired'
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
            SwitchoverPhase.POOLER_STOPPED,
            SwitchoverPhase.PG_STOPPED,
            SwitchoverPhase.PRIMARY_SHUT,
            SwitchoverPhase.CANDIDATE_ACQUIRED,
            SwitchoverPhase.PROMOTED,
        )

    def is_failed(self) -> bool:
        return self.phase == SwitchoverPhase.FAILED


@dataclass(frozen=True)
class SwitchoverObservation:
    """Immutable snapshot assembled once per step — the sole handler input (ADR-0006 §1).

    The shell (main.py / Executor) builds this before calling ``machine.plan()``;
    handlers perform no I/O. Fields are extended incrementally, one per read
    the handlers need.
    """

    record: SwitchoverRecord
    my_hostname: str
    role: str | None
    zk_timeline: int | None
    failover_state: str | None
    last_failover_ts: float | None
    last_switchover_ts: float | None
    ha_replics: frozenset[str] | None
    replics_info: ReplicaInfos
    streaming_replicas: tuple[str, ...]
    # Re-read for candidate-transition detection (initiated handler).
    live_switchover_state: 'SwitchoverPhase | None'
    candidate_alive: bool | None
    lock_holder: str | None
    switchover_timer_started: bool
    downtime_timer_started: bool
    # Candidate-side reads.
    candidate: str | None
    side_replicas: tuple[str, ...]
    all_side_replicas_turned: bool | None
    switchover_primary_info: dict | None
    # Pre-computed switchover candidate (from _get_switchover_candidate, I/O in builder).
    switchover_candidate: str | None = None

    @classmethod
    def build(
        cls,
        record: 'SwitchoverRecord',
        zk: 'Zookeeper',
        db: 'Postgres',
        timings: 'TimingTracker',
        my_hostname: str,
        db_state: dict,
        zk_state: dict,
        *,
        streaming_replicas: tuple[str, ...] = (),
        all_side_replicas_turned: bool | None = None,
        is_candidate_side: bool = False,
        switchover_candidate: str | None = None,
    ) -> 'SwitchoverObservation':
        """Assemble the observation — the sole I/O read point for a step (ADR-0006 §1).

        Called by the shell (main.py / Executor) before ``machine.plan()``.
        All phase-specific reads happen here so handlers stay pure.

        ``streaming_replicas`` and ``all_side_replicas_turned`` are passed by the
        shell because they require shell-specific helpers (app_name_from_fqdn,
        get_members) that don't belong in the observation module.
        """
        # Common reads.
        # When local PG is dead (dead_iter path), db.get_role() raises
        # PostgresConnectionError. Fall back to the cached role from db_state
        # so the state machine can still advance (pg_stopped → primary_shut).
        role: str | None
        try:
            role = db.get_role()
        except PostgresConnectionError:
            role = db_state.get('role')
        zk_timeline = zk_state.get(zk.TIMELINE_INFO_PATH)
        failover_state = zk.get_failover_state()
        last_failover_ts = zk.get_last_failover_time()
        last_switchover_ts = zk.get_last_switchover_time()
        ha_replics_raw = zk.get_ha_replics(my_hostname)
        ha_replics = frozenset(ha_replics_raw) if ha_replics_raw is not None else None
        replics_info = db_state.get('replics_info', [])
        switchover_timer_started = timings.get_start('switchover') is not None
        downtime_timer_started = timings.get_start('downtime') is not None
        lock_holder = zk.get_current_lock_holder(zk.PRIMARY_LOCK_PATH)

        # Phase-specific reads.
        live_switchover_state = SwitchoverPhase.from_str(zk.get_switchover_state())
        candidate = record.candidate or record.destination
        candidate_alive: bool | None = None
        if candidate is not None:
            candidate_alive = zk.is_host_alive(candidate, timeout=1)

        # Candidate-side reads.
        switchover_primary_info: dict | None = None
        if is_candidate_side:
            switchover_primary_info = zk.get_switchover_primary_info()

        return cls(
            record=record,
            my_hostname=my_hostname,
            role=role,
            zk_timeline=zk_timeline,
            failover_state=failover_state,
            last_failover_ts=last_failover_ts,
            last_switchover_ts=last_switchover_ts,
            ha_replics=ha_replics,
            replics_info=replics_info,
            streaming_replicas=streaming_replicas,
            live_switchover_state=live_switchover_state,
            candidate_alive=candidate_alive,
            lock_holder=lock_holder,
            switchover_timer_started=switchover_timer_started,
            downtime_timer_started=downtime_timer_started,
            candidate=candidate,
            side_replicas=tuple(record.side_replicas),
            all_side_replicas_turned=all_side_replicas_turned,
            switchover_primary_info=switchover_primary_info,
            switchover_candidate=switchover_candidate,
        )


@dataclass
class SwitchoverMachineConfig:
    """Configuration values consumed by PrimarySwitchoverMachine (ADR-0004)."""

    catchup_timeout: float = 60.0
    rollback_timeout: float = 60.0
    max_allowed_lag_ms: int = 10
    min_failover_timeout: float = 0.0
    # Seconds to wait after stopping PG so the sync replica drains last WAL.
    wal_drain_delay: float = 5.0
    # Whether data loss is allowed during switchover candidate selection.
    allow_potential_data_loss: bool = False



class PrimarySwitchoverMachine:
    """
    Primary-side switchover state machine (ADR-0005 §3, ADR-0006).

    Pure ``plan(observation)`` API: returns a Command Plan executed by
    CommandExecutor. Phase is persisted to ZK via TransitionTo commands
    before the phase action executes so that restarts resume from the same phase.
    """

    def __init__(
        self,
        zk: 'Zookeeper',
        config: 'SwitchoverMachineConfig | None' = None,
        debug_failure: Callable[[str], bool] | None = None,
    ) -> None:
        self._zk = zk
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


    # --- Pure plan() API (ADR-0006) ---

    def plan(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """Return the Command Plan for the current observation (pure, no I/O).

        Dispatches to phase-specific plan_* methods. Returns an empty Plan
        to mean "nothing to do — retry next iteration" (ADR-0006 §2).
        """
        planners: dict = {
            SwitchoverPhase.SCHEDULED: self.plan_scheduled,
            SwitchoverPhase.SYNC_SET: self.plan_sync_set,
            SwitchoverPhase.INITIATED: self.plan_initiated,
            SwitchoverPhase.CANDIDATE_FOUND: self.plan_candidate_found,
            SwitchoverPhase.POOLER_STOPPED: self.plan_pooler_stopped,
            SwitchoverPhase.PG_STOPPED: self.plan_pg_stopped,
            SwitchoverPhase.PRIMARY_SHUT: self.plan_primary_shut,
            # PROMOTED: candidate has promoted — old primary must rewind.
            # Same handler as primary_shut (checks phase == PROMOTED for rewind).
            SwitchoverPhase.PROMOTED: self.plan_primary_shut,
        }
        planner = planners.get(obs.record.phase)  # type: ignore[arg-type]
        if planner is None:
            logging.debug('No primary-side planner for switchover phase %s', obs.record.phase)
            return []
        return planner(obs)

    def _candidate_is_sync(self, replics_info: ReplicaInfos, candidate: str) -> bool:
        """Pure predicate: is the candidate in sync with the primary?

        Uses config values (max_allowed_lag_ms, allow_potential_data_loss)
        instead of pgconsul config — the machine is self-contained.
        """
        candidate_appname = app_name_from_fqdn(candidate)
        replica = next(
            (r for r in replics_info if r.get('application_name') == candidate_appname),
            None,
        )
        if replica is None:
            logging.warning('Could not find replica info for %s', candidate)
            return False
        replay_lag = replica.get('replay_lag_msec')
        logging.info('Replica %s has replay lag %sms', candidate, replay_lag)
        if replay_lag is None:
            logging.warning('Could not get replay lag for replica %s', candidate)
            return False
        replay_lag_ms = int(replay_lag)
        if replay_lag_ms > self._cfg.max_allowed_lag_ms:
            if not self._cfg.allow_potential_data_loss:
                logging.warning(
                    'Replica %s cannot be primary for switchover, max allowed lag %sms',
                    candidate, self._cfg.max_allowed_lag_ms,
                )
                return False
            logging.warning('Replica %s has replay lag %s and allow data loss', candidate, replay_lag)
        return True

    def plan_scheduled(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """scheduled → sync_set: sanity-check, choose candidate, set sync replication.

        Pure version of _handle_scheduled (ADR-0006). All reads come from the
        observation; effects are emitted as commands. Returns an empty Plan
        when a gate fails (retry next iteration).
        """
        # --- Sanity gates (ported 1:1 from _handle_scheduled) ---

        if obs.record.hostname != obs.my_hostname:
            logging.warning(
                'Switchover scheduled: hostname %s differs from current %s, ignoring',
                obs.record.hostname, obs.my_hostname,
            )
            return []

        if obs.role != 'primary':
            logging.error(
                'Switchover scheduled: current role is %s, ignoring switchover',
                obs.role,
            )
            return []

        if obs.zk_timeline != obs.record.timeline:
            logging.warning(
                'Switchover scheduled: ZK timeline %s differs from switchover timeline %s, ignoring',
                obs.zk_timeline, obs.record.timeline,
            )
            return []

        if obs.failover_state not in ('finished', None):
            logging.error(
                'Switchover scheduled: current failover state is %s, ignoring switchover',
                obs.failover_state,
            )
            return []

        # Last role transition was more than min_failover_timeout ago
        # (or enough replicas are alive).
        last_role_transition_ts: float = 0.0
        if obs.last_failover_ts is not None or obs.last_switchover_ts is not None:
            last_role_transition_ts = max(
                x for x in (obs.last_switchover_ts, obs.last_failover_ts) if x is not None
            )

        if obs.ha_replics is None:
            logging.warning('Switchover scheduled: HA replicas are empty, ignoring switchover')
            return []

        alive_replics_number = len([i for i in obs.replics_info if i.get('state') == 'streaming'])
        if not _check_last_failover_time(last_role_transition_ts, self._cfg.min_failover_timeout) and (
            alive_replics_number < len(obs.ha_replics)
        ):
            logging.warning(
                'Switchover scheduled: last role transition was %.1f seconds ago,'
                ' and alive host count less than HA hosts (HA: %d, alive: %d) ignoring switchover.',
                time.time() - last_role_transition_ts,
                len(obs.ha_replics),
                alive_replics_number,
            )
            return []

        # --- Choose candidate ---

        candidate = obs.switchover_candidate
        if candidate is None:
            logging.info('Switchover scheduled: no eligible candidate, waiting')
            return []

        # --- Check candidate is in sync ---

        if not self._candidate_is_sync(obs.replics_info, candidate):
            logging.info('Switchover scheduled: candidate %s not yet in sync, waiting', candidate)
            return []

        # --- Action: set sync replication and transition to sync_set ---

        logging.info('Scheduled switchover checks passed OK.')

        plan: CommandPlan = []
        # Idempotent: start timer only if not already started.
        if not obs.switchover_timer_started:
            plan.append(StartTimer('switchover'))

        # Persist candidate to ZK before transitioning so plan_sync_set can
        # read it from obs.candidate (record.candidate) in the next iteration.
        # Without this, anywhere-switchover (no destination) fails immediately
        # in plan_sync_set because obs.candidate is None.
        plan.append(WriteCandidate(candidate=candidate))

        logging.warning('Starting sync replication %s', candidate)
        plan.append(SetSyncReplication(host=candidate))

        # Persist SYNC_SET before the action (ADR-0005 §3 fence).
        plan.append(TransitionTo(SwitchoverPhase.SYNC_SET))

        return plan

    def plan_sync_set(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """sync_set → initiated: idempotently fix candidate + side replicas, write initiated.

        Pure version of _handle_sync_set (ADR-0006). Side replicas are computed
        from obs.streaming_replicas (pre-read by the shell). Returns an empty
        Plan when a gate fails; emits TransitionTo(FAILED) if candidate is None.
        """
        candidate = obs.candidate
        if candidate is None:
            logging.error('Switchover sync_set: candidate is None, aborting')
            return [TransitionTo(SwitchoverPhase.FAILED)]

        # Compute side replicas: all streaming replicas except the candidate.
        side_replicas = [r for r in obs.streaming_replicas if r != candidate]

        logging.info('Switchover sync_set: candidate=%s side_replicas=%s', candidate, side_replicas)

        # Fence: TransitionTo(INITIATED) before the backwards-compat marker.
        return [
            WriteCandidate(candidate=candidate),
            WriteSideReplicas(side_replicas=side_replicas),
            TransitionTo(SwitchoverPhase.INITIATED),
            WriteFailoverState(value='switchover_initiated'),
        ]

    def plan_initiated(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """initiated: wait (non-blocking) for candidate to set candidate_found.

        Pure version of _handle_initiated (ADR-0006). Detects the candidate's
        transition to candidate_found via obs.live_switchover_state (fresh re-read
        in the observation builder). Emits pre-shutdown prep (StoreReplicsInfo +
        Checkpoint) when detected; aborts if the candidate is dead.

        No phase transition is emitted — the candidate writes candidate_found to ZK;
        the primary detects it and the next iteration enters plan_candidate_found.
        """
        candidate = obs.candidate
        if candidate is None:
            logging.error('Switchover initiated: candidate is None, aborting')
            return [TransitionTo(SwitchoverPhase.FAILED)]

        # Detect candidate's transition to candidate_found.
        if obs.live_switchover_state == SwitchoverPhase.CANDIDATE_FOUND:
            logging.warning('SWITCHOVER: candidate_found detected, proceeding to shutdown')
            return [
                Log(
                    message='SWITCHOVER: candidate_found detected, proceeding to shutdown',
                    level='warning',
                    event=True,
                ),
                StoreReplicsInfo(),
                Checkpoint(),
            ]

        # Candidate dead: abort the switchover.
        if obs.candidate_alive is not True:
            logging.warning(
                'Switchover initiated: candidate %s is no longer alive, aborting switchover',
                candidate,
            )
            return [TransitionTo(SwitchoverPhase.FAILED)]

        logging.debug(
            'Switchover initiated: waiting for candidate_found (current=%s)',
            obs.live_switchover_state,
        )
        return []

    def plan_candidate_found(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """candidate_found → pooler_stopped: stop pooler, start downtime timer.

        Pure version of the first part of _handle_candidate_found (ADR-0006 §4).
        Splits the old monolithic handler into read-at-start sub-phases so that
        kill-9 recovery is more granular. This phase: stop pooler + start timer
        + persist POOLER_STOPPED. The sync check moves to plan_pooler_stopped.
        """
        candidate = obs.candidate
        if candidate is None:
            logging.error('Switchover candidate_found: candidate is None, aborting')
            return [TransitionTo(SwitchoverPhase.FAILED)]

        plan: CommandPlan = []

        # Idempotent: start downtime timer only if not already started.
        if not obs.downtime_timer_started:
            plan.append(StartTimer('downtime'))

        plan.append(StopPooler())
        plan.append(Log(
            message='Cluster closed from user requests (pooler stopped)',
            level='warning',
        ))

        # debug_failure injection point (ADR-0006 §6: pure predicate).
        if self._debug_failure('primary_switchover_before_catchup'):
            plan.append(TransitionTo(SwitchoverPhase.FAILED))
            return plan

        # Persist POOLER_STOPPED before the next phase (idempotency fence).
        plan.append(TransitionTo(SwitchoverPhase.POOLER_STOPPED))
        return plan

    def plan_pooler_stopped(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """pooler_stopped → pg_stopped: non-blocking sync check, stop PG.

        Pure version of the middle part of _handle_candidate_found (ADR-0006 §4).
        Reads replics_info (pre-read by the observation builder) and checks sync.
        If not in sync, returns empty Plan (retry next iteration). If in sync,
        stops PG (non-blocking) and persists PG_STOPPED.
        """
        candidate = obs.candidate
        if candidate is None:
            logging.error('Switchover pooler_stopped: candidate is None, aborting')
            return [TransitionTo(SwitchoverPhase.FAILED)]

        # Non-blocking sync check: verify candidate is in sync before stopping PG.
        if not self._candidate_is_sync(obs.replics_info, candidate):
            logging.info('Switchover pooler_stopped: candidate %s not yet in sync, waiting', candidate)
            return []

        logging.warning('Candidate %s is in sync, stopping PostgreSQL', candidate)

        # Stop PG without blocking wait (non-blocking first stop).
        return [
            StopPostgresql(wait=False, force_async=False),
            TransitionTo(SwitchoverPhase.PG_STOPPED),
        ]

    def plan_pg_stopped(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """pg_stopped → primary_shut: drain WAL, release lock, final PG stop.

        Pure version of the last part of _handle_candidate_found (ADR-0006 §4).
        After PG is stopped (PG_STOPPED persisted), drain WAL, write the
        backwards-compat failover_state marker, persist PRIMARY_SHUT, release
        the lock, do the final blocking PG stop, and signal return-to-cluster.
        """
        candidate = obs.candidate
        if candidate is None:
            logging.error('Switchover pg_stopped: candidate is None, aborting')
            return [TransitionTo(SwitchoverPhase.FAILED)]

        plan: CommandPlan = []

        # Give sync replica a chance to consume last WAL records.
        if self._cfg.wal_drain_delay > 0:
            plan.append(Sleep(seconds=self._cfg.wal_drain_delay))

        # debug_failure injection point before lock release.
        if self._debug_failure('primary_switchover_before_release'):
            plan.append(TransitionTo(SwitchoverPhase.FAILED))
            return plan

        # Backwards compatibility marker.
        plan.append(WriteFailoverState(value='switchover_master_shut'))

        # Persist PRIMARY_SHUT before releasing the lock (idempotency fence).
        plan.append(TransitionTo(SwitchoverPhase.PRIMARY_SHUT))

        # Release leader lock.
        plan.append(ReleaseLock(wait=5))

        # Final blocking PG stop (best-effort).
        plan.append(StopPostgresql(wait=True, force_async=False))

        # debug_failure injection point after release.
        if self._debug_failure('primary_switchover_after_release'):
            return plan

        # Signal return-to-cluster.
        plan.append(SetSimplePrimarySwitchTry())
        return plan

    def plan_primary_shut(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """primary_shut: idempotent recovery handler for restarts mid-shutdown.

        Pure version of _handle_primary_shut (ADR-0006). After candidate_found,
        PG is stopped and the lock released. If pgconsul restarts while in
        primary_shut, this handler either releases a re-acquired lock or rewinds
        to the new primary.

        MDB-41951 race fix: only rewind when the candidate has successfully
        promoted (phase == PROMOTED). If the candidate holds the lock but
        hasn't promoted yet (phase == CANDIDATE_ACQUIRED or PRIMARY_SHUT),
        wait — rewinding to a non-primary candidate causes a stuck cluster
        when promote fails.
        """
        # Safety: if we hold the lock again (unexpected restart), release it.
        if obs.lock_holder == obs.my_hostname:
            logging.warning('Switchover primary_shut: unexpectedly holding the lock — releasing')
            return [
                StopPooler(),
                ReleaseLock(wait=5),
            ]

        # Only rewind when the candidate has successfully promoted.
        # If phase is PRIMARY_SHUT or CANDIDATE_ACQUIRED, the candidate may
        # still fail promote — rewinding now is a race condition (MDB-41951).
        new_primary = obs.lock_holder
        if new_primary is not None and obs.record.phase == SwitchoverPhase.PROMOTED:
            logging.warning('SWITCHOVER: new primary found, returning to cluster')
            return [
                Log(
                    message='SWITCHOVER: new primary found, returning to cluster',
                    level='warning',
                    event=True,
                ),
                DeleteHostOp(),
                SetSimplePrimarySwitchTry(),
                RewindFromSource(
                    new_primary=new_primary,
                    is_postgresql_dead=True,
                    limit=self._cfg.rollback_timeout,
                ),
            ]

        logging.info('Switchover primary_shut: waiting for candidate to promote (phase=%s)', obs.record.phase)
        return []



# --- Candidate-side state machine (ADR-0005 §3, step 15a) ---



class CandidateSwitchoverMachine:
    """
    Candidate-side switchover state machine (ADR-0005 §3, ADR-0006).

    Pure ``plan(observation)`` API: returns a Command Plan executed by
    CommandExecutor. Handles phases: ``initiated`` (create slots, wait for
    side replicas), ``candidate_found`` (acquire lock, promote, cleanup).
    """

    def __init__(
        self,
        zk: 'Zookeeper',
        config: 'SwitchoverMachineConfig | None' = None,
        debug_failure: Callable[[str], bool] | None = None,
    ) -> None:
        self._zk = zk
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


    # --- Pure plan() API (ADR-0006) ---

    def plan(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """Return the Command Plan for the current observation (pure, no I/O).

        Dispatches to phase-specific plan_* methods. Returns an empty Plan
        to mean "nothing to do — retry next iteration" (ADR-0006 §2).
        """
        planners: dict = {
            SwitchoverPhase.INITIATED: self.plan_initiated,
            SwitchoverPhase.CANDIDATE_FOUND: self.plan_candidate_found,
            # pooler_stopped / pg_stopped: old primary is shutting down —
            # candidate must keep attempting non-blocking lock acquisition.
            # Same planner as candidate_found (AcquireLock timeout=0 is safe:
            # the lock is still held by the old primary, so it just retries).
            SwitchoverPhase.POOLER_STOPPED: self.plan_candidate_found,
            SwitchoverPhase.PG_STOPPED: self.plan_candidate_found,
            # primary_shut: old primary released the lock — candidate must
            # acquire it and promote. Same planner as candidate_found.
            SwitchoverPhase.PRIMARY_SHUT: self.plan_candidate_found,
            # candidate_acquired: candidate holds the lock, promote in progress.
            # Same planner — the failed-promote guard detects lock-already-held.
            SwitchoverPhase.CANDIDATE_ACQUIRED: self.plan_candidate_found,
        }
        planner = planners.get(obs.record.phase)  # type: ignore[arg-type]
        if planner is None:
            logging.debug('No candidate-side planner for switchover phase %s', obs.record.phase)
            return []
        return planner(obs)

    def plan_initiated(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """initiated → candidate_found: create slots, check side replicas turned (non-blocking).

        Pure version of _handle_initiated (ADR-0006). Side replicas and their
        turned-status come from the observation. CreateSlots is idempotent and
        emitted every iteration; TransitionTo(CANDIDATE_FOUND) only when all
        side replicas have turned. Returns a CreateSlots-only Plan when waiting.
        """
        side_replicas = list(obs.side_replicas)

        # No side replicas → transition immediately.
        if not side_replicas:
            return [TransitionTo(SwitchoverPhase.CANDIDATE_FOUND)]

        # Create slots (idempotent, safe to repeat on restart).
        plan: CommandPlan = [CreateSlots(hosts=side_replicas)]

        # Non-blocking check: are all side replicas streaming from us?
        # None = read error, False = not yet turned — both retry next iteration.
        if obs.all_side_replicas_turned is not True:
            logging.info('Waiting for side replicas to turn to candidate')
            return plan

        logging.info('All side replicas turned to candidate, signaling primary')
        plan.append(TransitionTo(SwitchoverPhase.CANDIDATE_FOUND))
        return plan

    def plan_candidate_found(self, obs: 'SwitchoverObservation') -> CommandPlan:
        """candidate_found → promoted: acquire lock, do_failover, cleanup.

        Pure version of _handle_candidate_found (ADR-0006). Non-blocking lock
        acquisition (timeout=0); if the lock is held, executor stops at
        AcquireLock and retries next iteration. switchover_primary_info is
        pre-read in the observation; if missing, the plan acquires then releases
        the lock. DoFailover is opaque — the executor releases the lock on
        failure (post-condition of the command).
        """
        # debug_failure injection point (ADR-0006 §6: pure predicate).
        if self._debug_failure('candidate_switchover_before_acquire'):
            return []

        # Detect a failed promote: if we already hold the lock but the phase
        # is still candidate_found / primary_shut / candidate_acquired, the
        # previous DoFailover failed (the executor stops on failure and the
        # lock is never released). Without this check the candidate retries
        # promote in an infinite loop. Abort: release the lock and transition
        # to FAILED so the old primary can reclaim the lock and resume serving.
        if obs.lock_holder == obs.my_hostname:
            logging.error(
                'Switchover %s: lock already held by us but '
                'promote did not succeed — aborting switchover (releasing lock)',
                obs.record.phase,
            )
            return [
                ReleaseLock(),
                TransitionTo(SwitchoverPhase.FAILED),
            ]

        # Acquire lock (non-blocking: timeout=0, one attempt per iteration).
        plan: CommandPlan = [AcquireLock(allow_queue=True, timeout=0)]

        # If switchover primary info is missing, release the lock and wait.
        if obs.switchover_primary_info is None:
            logging.error('Failed to get switchover primary info from ZK.')
            plan.append(ReleaseLock())
            return plan

        # Persist CANDIDATE_ACQUIRED before promote — race condition fix
        # (MDB-41951). The old primary checks for PROMOTED before rewinding;
        # without this intermediate phase, the old primary sees lock_holder
        # != None and rewinds to a candidate that hasn't promoted yet.
        plan.append(TransitionTo(SwitchoverPhase.CANDIDATE_ACQUIRED))

        # Start downtime timer if not already started (idempotent).
        # The old primary normally starts it, but if it was killed before
        # reaching that phase, the candidate must start it here.
        if not obs.downtime_timer_started:
            plan.append(StartTimer('downtime'))

        old_primary = obs.switchover_primary_info.get('hostname')

        # Do failover (opaque; executor releases lock on failure).
        plan.append(DoFailover(old_primary=old_primary))

        # Write promoted phase as observability marker before cleanup.
        plan.append(TransitionTo(SwitchoverPhase.PROMOTED))

        # Cleanup switchover nodes and finalize.
        plan.append(CleanupSwitchover())
        plan.append(WriteLastSwitchoverTime())
        plan.append(StopTimer('switchover'))
        return plan

