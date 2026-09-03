# encoding: utf-8
"""Switchover domain types and phases (MDB-41951, ADR-0005 §3).

Cross-host state is persisted in the versioned ZK ``switchover/record``. Host-local
command groups reuse the same enum but are persisted on the local filesystem.
"""

import logging
from dataclasses import dataclass, field

from ..types import StrEnum


class SwitchoverPhase(StrEnum):
    """Global phases and host-local command groups of switchover."""

    SCHEDULED = 'scheduled'          # Written by dbaas_worker / pgconsul-util.
    FAILED = 'failed'                # Rollback / cleanup needed.
    FALLBACK = 'fallback'            # Waiting for fallback recovery.
    PREPARING_DURABILITY = 'preparing_durability'
    PREPARING_CANDIDATE = 'preparing_candidate'
    TURNING_SIDES = 'turning_sides'
    # The manager has durably committed the handoff to the candidate's next
    # timeline. Rollback now requires the fenced-vote proof from ADR-0014.
    HANDOFF_COMMITTED = 'handoff_committed'
    WAITING_ARCHIVE = 'waiting_archive'
    # Terminal fence: no switchover work remains.  The manager lock must be
    # released before the record may be CAS-cleared.
    CLEANUP = 'cleanup'

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


class DurabilityPinMode(StrEnum):
    """Owner and allowed direction of the switchover durability pin."""

    CONTRACTING = 'contracting'
    MANDATORY = 'mandatory'
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
    side_turn_permitted: list[str] = field(default_factory=list)
    operation_id: str | None = None
    durability_pin_mode: DurabilityPinMode | None = None
    durability_pin_owner: str | None = None
    side_wait_started_at: float | None = None
    required_side_replicas: int | None = None
    original_durability_members: list[str] = field(default_factory=list)
    expected_timeline: int | None = None
    use_pg_patches: bool = False
    use_target_promote: bool = False
    promoted_timeline: int | None = None
    target_may_have_commits: bool = False
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
        operation_id = info.get('operation_id')
        if phase is not None and not isinstance(operation_id, str):
            raise ValueError('active switchover record has no operation_id')
        return cls(
            hostname=info.get('hostname'),
            timeline=info.get(zk.TIMELINE_INFO_PATH),
            destination=info.get('destination'),
            phase=phase,
            candidate=info.get('candidate'),
            side_replicas=list(info.get('side_replicas') or []),
            side_turn_permitted=list(info.get('side_turn_permitted') or []),
            operation_id=operation_id,
            durability_pin_mode=DurabilityPinMode.from_str(info.get('durability_pin_mode')),
            durability_pin_owner=info.get('durability_pin_owner'),
            side_wait_started_at=info.get('side_wait_started_at'),
            required_side_replicas=info.get('required_side_replicas'),
            original_durability_members=list(info.get('original_durability_members') or []),
            expected_timeline=info.get('expected_timeline'),
            use_pg_patches=info.get('use_pg_patches') is True,
            use_target_promote=info.get('use_target_promote') is True,
            promoted_timeline=info.get('promoted_timeline'),
            target_may_have_commits=info.get('target_may_have_commits') is True,
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
            'side_turn_permitted': self.side_turn_permitted,
        }
        record['use_target_promote'] = self.use_target_promote
        optional: dict[str, object | None] = {
            'operation_id': self.operation_id,
            'durability_pin_mode': self.durability_pin_mode.value if self.durability_pin_mode is not None else None,
            'durability_pin_owner': self.durability_pin_owner,
            'side_wait_started_at': self.side_wait_started_at,
            'required_side_replicas': self.required_side_replicas,
            'original_durability_members': self.original_durability_members or None,
            'expected_timeline': self.expected_timeline,
            'use_pg_patches': self.use_pg_patches or None,
            'promoted_timeline': self.promoted_timeline,
            'target_may_have_commits': self.target_may_have_commits or None,
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
        """Stable key for host-local progress."""
        if self.operation_id is None:
            raise ValueError('switchover record has no operation_id')
        return self.operation_id

    def handoff_is_committed(self) -> bool:
        """True once recovery must never automatically restore old primary."""
        return self.phase in (
            SwitchoverPhase.HANDOFF_COMMITTED,
            SwitchoverPhase.WAITING_ARCHIVE,
        )
