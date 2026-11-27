"""Persistent cluster authority shared by durability, failover and switchover."""

import json
from dataclasses import dataclass, field, replace
from typing import Any
from uuid import UUID, uuid4


@dataclass(frozen=True)
class WriteConfiguration:
    timeline: int
    primary: str
    members: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.timeline < 1 or not self.primary or self.primary not in self.members or tuple(sorted(set(self.members))) != self.members:
            raise ValueError('Invalid write configuration')

    @property
    def replicas(self) -> tuple[str, ...]:
        return tuple(host for host in self.members if host != self.primary)

    @property
    def write_quorum(self) -> int:
        return len(self.replicas) // 2 + len(self.replicas) % 2

    @property
    def read_quorum(self) -> int:
        return len(self.replicas) - self.write_quorum + 1

    def to_dict(self) -> dict[str, Any]:
        return {'timeline': self.timeline, 'primary': self.primary, 'members': list(self.members)}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'WriteConfiguration':
        return cls(timeline=int(data['timeline']), primary=data['primary'], members=tuple(data['members']))


@dataclass(frozen=True)
class ClusterState:
    state_id: str
    desired_primary: str | None
    operation: dict[str, Any]
    possible_write_configurations: tuple[WriteConfiguration, ...]
    next_failover_after: float = 0.0
    version: int | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        if UUID(self.state_id).version != 4 or not isinstance(self.operation, dict):
            raise ValueError('Invalid cluster state revision')
        if not self.possible_write_configurations:
            raise ValueError('Cluster state has no write configurations')
        kind = self.operation.get('type')
        if kind not in ('idle', 'durability_transition', 'failover', 'switchover'):
            raise ValueError('Unknown cluster operation')
        if kind == 'idle' and (
            len(self.possible_write_configurations) != 1
            or self.operation.get('phase') is not None
            or self.possible_write_configurations[-1].primary != self.desired_primary
        ):
            raise ValueError('Invalid idle cluster state')
        if kind == 'durability_transition' and (
            self.operation.get('phase') != 'in_progress'
            or len(self.possible_write_configurations) != 2
            or any(config.primary != self.desired_primary for config in self.possible_write_configurations)
        ):
            raise ValueError('Invalid durability transition')
        if self.operation.get('phase') == 'promoting' and self.possible_write_configurations[-1].primary != self.desired_primary:
            raise ValueError('Promotion target differs from desired primary')
        if kind == 'failover' and self.operation.get('phase') not in ('voting', 'promoting'):
            raise ValueError('Invalid failover phase')
        if kind == 'failover' and self.operation.get('phase') == 'promoting' and len(self.possible_write_configurations) < 2:
            raise ValueError('Failover target is missing its source')
        if kind == 'switchover' and self.operation.get('phase') not in ('tuning_ssn', 'promoting'):
            raise ValueError('Invalid switchover phase')
        if kind == 'switchover' and (
            len(self.possible_write_configurations) not in (1, 2)
            or (self.operation.get('phase') == 'promoting' and len(self.possible_write_configurations) != 2)
        ):
            raise ValueError('Invalid switchover configurations')
        if kind == 'failover' and self.operation['phase'] == 'voting':
            if not all(key in self.operation for key in ('previous_primary', 'source_operation', 'electorate', 'deadline')):
                raise ValueError('Incomplete failover vote')
        if kind == 'switchover':
            if not all(key in self.operation for key in ('request_id', 'source', 'target', 'original_members', 'deadline')):
                raise ValueError('Incomplete switchover')

    @classmethod
    def initial(cls, primary: str, timeline: int, members: set[str]) -> 'ClusterState':
        config = WriteConfiguration(timeline, primary, tuple(sorted(members | {primary})))
        return cls(str(uuid4()), primary, {'type': 'idle', 'phase': None}, (config,))

    @classmethod
    def from_json(cls, data: str, version: int) -> 'ClusterState':
        parsed = json.loads(data)
        return cls(
            state_id=parsed['state_id'],
            desired_primary=parsed['desired_primary'],
            operation=parsed['operation'],
            possible_write_configurations=tuple(
                WriteConfiguration.from_dict(item) for item in parsed['possible_write_configurations']
            ),
            next_failover_after=float(parsed.get('next_failover_after', 0)),
            version=version,
        )

    def to_json(self) -> str:
        return json.dumps({
            'state_id': self.state_id,
            'desired_primary': self.desired_primary,
            'operation': self.operation,
            'possible_write_configurations': [config.to_dict() for config in self.possible_write_configurations],
            'next_failover_after': self.next_failover_after,
        }, sort_keys=True)

    def evolve(self, **changes: Any) -> 'ClusterState':
        return replace(self, state_id=str(uuid4()), **changes)
