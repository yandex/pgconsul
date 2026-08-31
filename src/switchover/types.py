# encoding: utf-8
"""Switchover domain types and phases (MDB-41951, ADR-0005 §3).

Cross-host state is persisted in the versioned ZK ``switchover/record``. Host-local
command groups reuse the same enum but are persisted on the local filesystem.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ..types import ReplicaInfos, StrEnum

if TYPE_CHECKING:
    from ..timings import TimingTracker
    from ..zk import Zookeeper


class SwitchoverPhase(StrEnum):
    """Global phases and host-local command groups of switchover."""

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
    FALLBACK = 'fallback'            # Waiting for fallback recovery.

    # Manager-owned bridge protocol (ADR-0014).  Kept distinct from the
    # legacy phases so an old record remains readable during upgrade.
    PREPARING_DURABILITY = 'preparing_durability'
    TURNING_SIDES = 'turning_sides'
    PREPARING_BRIDGE = 'preparing_bridge'
    HANDOFF_READY = 'handoff_ready'
    # The manager has durably committed the handoff to the candidate's next
    # timeline.  Old-primary rollback is forbidden from this point.
    HANDOFF_COMMITTED = 'handoff_committed'
    WAITING_ARCHIVE = 'waiting_archive'

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


class SwitchoverRoute(StrEnum):
    GLOBAL = 'global'
    PRIMARY = 'primary'
    CANDIDATE = 'candidate'
    REPLICA = 'replica'
    WAIT = 'wait'


class DurabilityPinMode(StrEnum):
    """Owner and allowed direction of the switchover durability pin."""

    CONTRACTING = 'contracting'
    EXPANDING = 'expanding'

    @classmethod
    def from_str(cls, value: str | None) -> 'DurabilityPinMode | None':
        if value is None:
            return None
        try:
            return cls(value)
        except ValueError:
            logging.warning('Unknown switchover durability pin mode: %s', value)
            return None


@dataclass
class SwitchoverRecord:
    """Typed view of the versioned switchover JSON record."""

    hostname: str | None = None
    timeline: int | None = None
    destination: str | None = None
    phase: SwitchoverPhase | None = None
    candidate: str | None = None
    side_replicas: list[str] = field(default_factory=list)
    protocol_version: int = 1
    operation_id: str | None = None
    durability_pin_mode: DurabilityPinMode | None = None
    durability_pin_owner: str | None = None
    bridge_member: str | None = None
    bridge_source: str | None = None
    handoff_lsn: int | None = None
    side_wait_started_at: float | None = None
    required_side_replicas: int | None = None
    original_durability_members: list[str] = field(default_factory=list)
    expected_timeline: int | None = None
    promoted_timeline: int | None = None
    started_at: float | None = None
    deadline_at: float | None = None
    failure_reason: str | None = None
    version: int | None = None

    @classmethod
    def from_zk_state(cls, zk_state: dict, zk) -> 'SwitchoverRecord':
        """Build from ``zk.get_state()`` snapshot (zk used for path constants)."""
        info = zk_state.get(zk.SWITCHOVER_RECORD_PATH)
        version = zk_state.get(zk.SWITCHOVER_VERSION_KEY)
        if info is None:
            # An existing node with invalid JSON is failed and cleaned via CAS.
            return cls(
                phase=SwitchoverPhase.FAILED if version is not None else None,
                version=version,
            )
        state_str = info.get('phase')
        phase = SwitchoverPhase.from_str(state_str)
        if info and phase is None:
            phase = SwitchoverPhase.FAILED
        return cls(
            hostname=info.get('hostname'),
            timeline=info.get(zk.TIMELINE_INFO_PATH),
            destination=info.get('destination'),
            phase=phase,
            candidate=info.get('candidate'),
            side_replicas=list(info.get('side_replicas') or []),
            protocol_version=int(info.get('protocol_version', 1)),
            operation_id=info.get('operation_id'),
            durability_pin_mode=DurabilityPinMode.from_str(info.get('durability_pin_mode')),
            durability_pin_owner=info.get('durability_pin_owner'),
            bridge_member=info.get('bridge_member'),
            bridge_source=info.get('bridge_source'),
            handoff_lsn=info.get('handoff_lsn'),
            side_wait_started_at=info.get('side_wait_started_at'),
            required_side_replicas=info.get('required_side_replicas'),
            original_durability_members=list(info.get('original_durability_members') or []),
            expected_timeline=info.get('expected_timeline'),
            promoted_timeline=info.get('promoted_timeline'),
            started_at=info.get('started_at'),
            deadline_at=info.get('deadline_at'),
            failure_reason=info.get('failure_reason'),
            version=version,
        )

    def to_dict(self) -> dict:
        """Serialize without the transport-only ZK version."""
        if self.phase is None:
            return {}
        record: dict[str, object] = {
            'hostname': self.hostname,
            'timeline': self.timeline,
            'destination': self.destination,
            'phase': self.phase.value,
            'candidate': self.candidate,
            'side_replicas': self.side_replicas,
        }
        if self.protocol_version != 1:
            record['protocol_version'] = self.protocol_version
        optional: dict[str, object | None] = {
            'operation_id': self.operation_id,
            'durability_pin_mode': self.durability_pin_mode.value if self.durability_pin_mode is not None else None,
            'durability_pin_owner': self.durability_pin_owner,
            'bridge_member': self.bridge_member,
            'bridge_source': self.bridge_source,
            'handoff_lsn': self.handoff_lsn,
            'side_wait_started_at': self.side_wait_started_at,
            'required_side_replicas': self.required_side_replicas,
            'original_durability_members': self.original_durability_members or None,
            'expected_timeline': self.expected_timeline,
            'promoted_timeline': self.promoted_timeline,
            'started_at': self.started_at,
            'deadline_at': self.deadline_at,
            'failure_reason': self.failure_reason,
        }
        record.update({key: value for key, value in optional.items() if value is not None})
        return record

    @property
    def selected_candidate(self) -> str | None:
        return self.candidate or self.destination

    @property
    def local_operation_id(self) -> str:
        """Stable key for host-local progress, including legacy records."""
        if self.operation_id is not None:
            return self.operation_id
        return f'legacy:{self.hostname}:{self.timeline}:{self.destination}'

    def requires_primary_lock(self) -> bool:
        """True while the planned handoff still requires the old primary."""
        return self.phase in (
            SwitchoverPhase.SCHEDULED,
            SwitchoverPhase.SYNC_SET,
            SwitchoverPhase.INITIATED,
            SwitchoverPhase.CANDIDATE_FOUND,
            SwitchoverPhase.POOLER_STOPPED,
            SwitchoverPhase.PG_STOPPED,
        )

    def can_follow_candidate(self) -> bool:
        """True after the candidate starts preparing side replicas."""
        return self.phase in (
            SwitchoverPhase.INITIATED,
            SwitchoverPhase.CANDIDATE_FOUND,
            SwitchoverPhase.POOLER_STOPPED,
            SwitchoverPhase.PG_STOPPED,
            SwitchoverPhase.PRIMARY_SHUT,
            SwitchoverPhase.CANDIDATE_ACQUIRED,
            SwitchoverPhase.PROMOTED,
        )

    def handoff_is_committed(self) -> bool:
        """True once recovery must never automatically restore old primary."""
        return self.phase in (
            SwitchoverPhase.HANDOFF_COMMITTED,
            SwitchoverPhase.WAITING_ARCHIVE,
        )


def decide_switchover_route(
    record: SwitchoverRecord,
    hostname: str,
    role: str | None,
    lock_holder: str | None,
) -> SwitchoverRoute:
    """Choose the local switchover actor without performing I/O."""
    if (
        record.phase == SwitchoverPhase.FAILED
        and record.selected_candidate == hostname
        and lock_holder == hostname
    ):
        return SwitchoverRoute.CANDIDATE
    if record.phase in (SwitchoverPhase.FAILED, SwitchoverPhase.FALLBACK):
        return SwitchoverRoute.GLOBAL
    if record.selected_candidate == hostname:
        return SwitchoverRoute.CANDIDATE
    if record.hostname == hostname:
        return SwitchoverRoute.PRIMARY
    if role == 'replica':
        return SwitchoverRoute.REPLICA
    return SwitchoverRoute.WAIT


@dataclass(frozen=True)
class SwitchoverObservation:
    """Immutable snapshot — sole handler input (ADR-0006 §1).

    Built by the shell before ``machine.plan()``; handlers perform no I/O.
    """

    record: SwitchoverRecord
    my_hostname: str
    role: str | None
    zk_timeline: int | None
    last_role_transition_ts: float | None
    ha_replics: frozenset[str] | None
    replics_info: ReplicaInfos
    streaming_replicas: tuple[str, ...]
    candidate_alive: bool | None
    lock_holder: str | None
    switchover_started_ts: float | None
    downtime_started_ts: float | None  # Actual start ts for timeout gates.
    all_side_replicas_turned: bool
    current_time: float
    switchover_candidate: str | None = None
    # Host-local primary-side command group.
    local_phase: 'SwitchoverPhase | None' = None
    primary_alive: bool | None = None

    @classmethod
    def build(
        cls,
        record: 'SwitchoverRecord',
        zk: 'Zookeeper',
        timings: 'TimingTracker',
        my_hostname: str,
        db_state: dict,
        zk_state: dict,
        *,
        streaming_replicas: tuple[str, ...] = (),
        all_side_replicas_turned: bool = False,
        switchover_candidate: str | None = None,
        local_phase: 'SwitchoverPhase | None' = None,
    ) -> 'SwitchoverObservation':
        """Assemble observation — sole I/O read point per step (ADR-0006 §1).

        streaming_replicas / all_side_replicas_turned passed by the shell
        (require shell-specific helpers).
        """
        role = db_state.get('role')
        zk_timeline = zk_state.get(zk.TIMELINE_INFO_PATH)
        last_role_transition_ts = zk.get_last_role_transition_time()
        ha_replics_raw = zk.get_ha_replics(my_hostname)
        ha_replics = frozenset(ha_replics_raw) if ha_replics_raw is not None else None
        replics_info = db_state.get('replics_info', [])
        switchover_started_ts = timings.get_start('switchover')
        downtime_started_ts = timings.get_start('downtime')
        lock_holder = zk.get_current_lock_holder(zk.PRIMARY_LOCK_PATH)

        primary_alive: bool | None = None
        if (
            record.phase == SwitchoverPhase.FAILED
            and lock_holder is None
            and record.hostname is not None
        ):
            primary_alive = zk.is_host_alive(record.hostname, timeout=1)

        candidate_alive: bool | None = None
        if record.phase == SwitchoverPhase.INITIATED and record.selected_candidate is not None:
            candidate_alive = zk.is_host_alive(record.selected_candidate, timeout=1)

        return cls(
            record=record,
            my_hostname=my_hostname,
            role=role,
            zk_timeline=zk_timeline,
            last_role_transition_ts=last_role_transition_ts,
            ha_replics=ha_replics,
            replics_info=replics_info,
            streaming_replicas=streaming_replicas,
            candidate_alive=candidate_alive,
            lock_holder=lock_holder,
            switchover_started_ts=switchover_started_ts,
            downtime_started_ts=downtime_started_ts,
            all_side_replicas_turned=all_side_replicas_turned,
            current_time=time.time(),
            switchover_candidate=switchover_candidate,
            local_phase=local_phase,
            primary_alive=primary_alive,
        )


@dataclass(frozen=True)
class SwitchoverMachineConfig:
    """Config consumed by switchover machines (ADR-0004)."""

    catchup_timeout: float = 60.0
    max_allowed_lag_ms: int = 10
    min_role_transition_timeout: float = 0.0
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
