# encoding: utf-8
"""Switchover domain types and phases (MDB-41951, ADR-0005 §3).

Phase values persisted in ZK ``switchover/state``. New values (``sync_set``,
``primary_shut``, ``promoted``) are unrecognized by old pgconsul versions,
preventing parallel switchovers (ADR-0005 §5 — two-phase rollout).
"""

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ..exceptions import PostgresConnectionError
from ..types import ReplicaInfos, StrEnum

if TYPE_CHECKING:
    from ..pg import Postgres
    from ..timings import TimingTracker
    from ..zk import Zookeeper


class SwitchoverPhase(StrEnum):
    """Persistent phases of the switchover state machine."""

    SCHEDULED = 'scheduled'          # Written by dbaas_worker / pgconsul-util.
    SYNC_SET = 'sync_set'            # Primary set sync replication on candidate.
    INITIATED = 'initiated'          # Primary fixed candidate + side replicas.
    CANDIDATE_FOUND = 'candidate_found'  # Candidate ready (slots, side replicas).
    POOLER_STOPPED = 'pooler_stopped'    # kill-9 recovery point (ADR-0006 §4).
    PG_STOPPED = 'pg_stopped'            # kill-9 recovery point (ADR-0006 §4).
    PRIMARY_SHUT = 'primary_shut'        # Old primary released the lock.
    # Candidate holds the lock but hasn't promoted — prevents premature rewind
    # by the old primary (MDB-41951 race fix).
    CANDIDATE_ACQUIRED = 'candidate_acquired'
    PROMOTED = 'promoted'            # Candidate promoted itself.
    FAILED = 'failed'                # Rollback / cleanup needed.

    @classmethod
    def from_str(cls, value: str | None) -> 'SwitchoverPhase | None':
        """Parse ZK state string, or None if absent/unknown."""
        if value is None:
            return None
        try:
            return cls(value)
        except ValueError:
            logging.warning('Unknown switchover state value: %s', value)
            return None


@dataclass
class SwitchoverRecord:
    """Typed view of switchover ZK nodes (master JSON + state + side_replicas)."""

    hostname: str | None = None
    timeline: int | None = None
    destination: str | None = None
    phase: SwitchoverPhase | None = None
    candidate: str | None = None
    side_replicas: list[str] = field(default_factory=list)

    @classmethod
    def from_zk_state(cls, zk_state: dict, zk) -> 'SwitchoverRecord':
        """Build from ``zk.get_state()`` snapshot (zk used for path constants)."""
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
        """True if this switchover targets the given hostname."""
        return self.hostname == hostname

    def is_active(self) -> bool:
        """True if in-progress (resumable) switchover."""
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
    """Immutable snapshot — sole handler input (ADR-0006 §1).

    Built by the shell before ``machine.plan()``; handlers perform no I/O.
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
    live_switchover_state: 'SwitchoverPhase | None'  # Fresh re-read for transition detection.
    candidate_alive: bool | None
    lock_holder: str | None
    switchover_timer_started: bool
    downtime_timer_started: bool
    downtime_started_ts: float | None  # Actual start ts for timeout gates.
    # Candidate-side reads.
    candidate: str | None
    side_replicas: tuple[str, ...]
    all_side_replicas_turned: bool
    switchover_primary_info: dict | None
    # Pre-computed candidate (I/O done in builder).
    switchover_candidate: str | None = None
    # Timeline match: zk_timeline == db_state['timeline'] (reified for StoreReplicsInfo).
    timeline_match: bool = False
    # Raw db_state for WriteHostStat (technical debt — full reification deferred).
    db_state: dict | None = None
    # stream_from config for WriteHostStat.
    stream_from: str | None = None

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
        all_side_replicas_turned: bool = False,
        is_candidate_side: bool = False,
        switchover_candidate: str | None = None,
        stream_from: str | None = None,
    ) -> 'SwitchoverObservation':
        """Assemble observation — sole I/O read point per step (ADR-0006 §1).

        streaming_replicas / all_side_replicas_turned passed by the shell
        (require shell-specific helpers).
        """
        # When local PG is dead, db.get_role() raises — fall back to cached role
        # so the machine can still advance (pg_stopped → primary_shut).
        role: str | None
        try:
            role = db.get_role()
        except PostgresConnectionError:
            role = db_state.get('role')
        zk_timeline = zk_state.get(zk.TIMELINE_INFO_PATH)
        timeline_match = bool(zk_timeline) and zk_timeline == db_state.get('timeline')
        failover_state = zk.get_failover_state()
        last_failover_ts = zk.get_last_failover_time()
        last_switchover_ts = zk.get_last_switchover_time()
        ha_replics_raw = zk.get_ha_replics(my_hostname)
        ha_replics = frozenset(ha_replics_raw) if ha_replics_raw is not None else None
        replics_info = db_state.get('replics_info', [])
        switchover_timer_started = timings.get_start('switchover') is not None
        downtime_timer_started = timings.get_start('downtime') is not None
        downtime_started_ts = timings.get_start('downtime')
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
            timeline_match=timeline_match,
            streaming_replicas=streaming_replicas,
            live_switchover_state=live_switchover_state,
            candidate_alive=candidate_alive,
            lock_holder=lock_holder,
            switchover_timer_started=switchover_timer_started,
            downtime_timer_started=downtime_timer_started,
            downtime_started_ts=downtime_started_ts,
            candidate=candidate,
            side_replicas=tuple(record.side_replicas),
            all_side_replicas_turned=all_side_replicas_turned,
            switchover_primary_info=switchover_primary_info,
            switchover_candidate=switchover_candidate,
            db_state=db_state,
            stream_from=stream_from,
        )


@dataclass(frozen=True)
class SwitchoverMachineConfig:
    """Config consumed by switchover machines (ADR-0004).

    Frozen for immutability, mirroring ``FailoverMachineConfig``.
    """

    catchup_timeout: float = 60.0
    rollback_timeout: float = 60.0
    max_allowed_lag_ms: int = 10
    min_failover_timeout: float = 0.0
    allow_potential_data_loss: bool = False  # Allow data loss in candidate selection.
    # Max wait for old primary to release lock before FAILED (candidate side).
    primary_shut_timeout: float = 300.0
    # Max wait for candidate to promote before FAILED (primary side).
    promote_timeout: float = 300.0
    # Blocking AcquireLock timeout used in PRIMARY_SHUT phase (MDB-41951 race fix).
    # In PRIMARY_SHUT the old primary guarantees immediate lock release, so the candidate
    # can block for up to this many seconds instead of returning to the next iteration.
    # Default=30s: enough to cover ReleaseLock(wait=5) plus network latency overhead
    # without blocking indefinitely. Set to 0 to restore the original non-blocking behavior.
    primary_shut_acquire_timeout: float = 30.0
