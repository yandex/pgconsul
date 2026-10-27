"""Typed domain records and explicit conversions at SQL/JSON boundaries."""
from __future__ import annotations

from dataclasses import dataclass, field, fields
from typing import Any, Mapping, TypeVar

Record = TypeVar('Record', bound='SparseRecord')

@dataclass
class SparseRecord:
    # Preserve absent keys separately from explicit nulls at legacy boundaries.
    _present_fields: set[str] | None = field(default=None, init=False, repr=False, compare=False)
    _extra_fields: dict[str, Any] = field(default_factory=dict, init=False, repr=False)

    def __setattr__(self, name: str, value: Any) -> None:
        present = self.__dict__.get('_present_fields')
        if present is not None and not name.startswith('_'):
            present.add(name)
        object.__setattr__(self, name, value)

    @classmethod
    def from_dict(cls: type[Record], data: Mapping[str, Any]) -> Record:
        names = {item.name for item in fields(cls) if item.init}
        record = cls(**{key: value for key, value in data.items() if key in names})
        record._present_fields = set(data)
        record._extra_fields = {key: value for key, value in data.items() if key not in names}
        return record

    def require(self, name: str) -> Any:
        if self._present_fields is not None and name not in self._present_fields:
            raise KeyError(name)
        return getattr(self, name)

    def __bool__(self) -> bool:
        return self._present_fields is None or bool(self._present_fields)

    def to_dict(self) -> dict[str, Any]:
        result = dict(self._extra_fields)
        for item in fields(self):
            if item.name.startswith('_') or (self._present_fields is not None and item.name not in self._present_fields):
                continue
            value = getattr(self, item.name)
            if hasattr(value, 'to_dict'):
                value = value.to_dict()
            elif isinstance(value, list):
                value = [row.to_dict() if hasattr(row, 'to_dict') else row for row in value]
            result[item.name] = value
        return result

@dataclass(frozen=True)
class ReplicationState:
    mode: str
    synchronous_standby_names: str | None

    @classmethod
    def from_data(cls, data: Mapping[str, Any] | list[Any] | tuple[str, str | None]) -> ReplicationState:
        if isinstance(data, Mapping):
            return cls(mode=data['mode'], synchronous_standby_names=data['synchronous_standby_names'])
        mode, ssn = data
        return cls(mode=mode, synchronous_standby_names=ssn)

    def to_dict(self) -> tuple[str, str | None]:
        return (self.mode, self.synchronous_standby_names)

@dataclass(frozen=True)
class SsnInfo:
    value: str | None
    last_update: str | None

    def to_dict(self) -> tuple[str | None, str | None]:
        return (self.value, self.last_update)

@dataclass(frozen=True, order=True)
class ElectionVote:
    lsn: int
    priority: int

@dataclass
class ReplicaInfo(SparseRecord):
    pid: int = 0
    application_name: str | None = ''
    client_hostname: str | None = ''
    state: str = ''
    primary_location: str | None = None
    write_location_diff: int | None = None
    replay_lag_msec: int | None = None
    reply_time_ms: int | None = None
    sync_state: str = ''
    priority: int | None = None
    client_addr: str | None = None
    sent_location_diff: int | None = None
    replay_location_diff: int | None = None
    backend_start_ts: int | None = None


ReplicaInfos = list[ReplicaInfo]

@dataclass
class WalReceiverInfo(SparseRecord):
    pid: int = 0
    status: str = ''
    slot_name: str = ''
    last_msg_receipt_time_msec: int = 0
    conninfo: str = ''


@dataclass
class DbState(SparseRecord):
    alive: bool = False
    running: bool = False
    role: str | None = None
    pgdata: str = ''
    opened: bool = False
    timeline: int | None = None
    wal_receiver: WalReceiverInfo | None = None
    replics_info: ReplicaInfos | None = None
    replication_state: ReplicationState | None = None
    primary_fqdn: str | None = None
    connection_timed_out: bool = False
    sessions_ratio: float | None = None
    prev_state: Any = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DbState:
        converted = dict(data)
        if converted.get('wal_receiver') is not None:
            converted['wal_receiver'] = WalReceiverInfo.from_dict(converted['wal_receiver'])
        if converted.get('replication_state') is not None:
            converted['replication_state'] = ReplicationState.from_data(converted['replication_state'])
        if converted.get('replics_info') is not None:
            converted['replics_info'] = [ReplicaInfo.from_dict(row) for row in converted['replics_info']]
        return super().from_dict(converted)

@dataclass
class MaintenanceState:
    status: str | None = None
    ts: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            'status': self.status,
            'ts': self.ts,
        }

@dataclass
class SwitchoverPrimaryInfo(SparseRecord):
    hostname: str | None = None
    timeline: Any = None
    destination: str | None = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> SwitchoverPrimaryInfo | None:  # type: ignore[override]
        if data is None:
            return None
        return super().from_dict(data)

@dataclass
class ZkState:
    alive: bool = False
    replics_info: ReplicaInfos | None = None
    last_failover_time: float | None = None
    last_switchover_time: float | None = None
    failover_state: str | None = None
    failover_must_be_reset: bool = False
    current_promoting_host: str | None = None
    lock_holder: str | None = None
    single_node: bool = False
    timeline: int | None = None
    switchover: SwitchoverPrimaryInfo | None = None
    switchover_candidate: str | None = None
    switchover_side_replicas: list[str] | None = None
    switchover_state: str | None = None
    maintenance: MaintenanceState = field(default_factory=MaintenanceState)
    last_leader: str | None = None
    lock_version: str | None = None
    replics_info_written: bool | None = field(default=None, repr=False)
    synchronous_standby_names: dict[str, SsnInfo] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {
            'alive': self.alive,
            'replics_info': [row.to_dict() for row in self.replics_info] if self.replics_info is not None else None,
            'last_failover_time': self.last_failover_time,
            'last_switchover_time': self.last_switchover_time,
            'failover_state': self.failover_state,
            'failover_must_be_reset': self.failover_must_be_reset,
            'current_promoting_host': self.current_promoting_host,
            'lock_version': self.lock_version,
            'lock_holder': self.lock_holder,
            'single_node': self.single_node,
            'timeline': self.timeline,
            'switchover': self.switchover.to_dict() if self.switchover is not None else None,
            'switchover/candidate': self.switchover_candidate,
            'switchover/side_replicas': self.switchover_side_replicas,
            'switchover/state': self.switchover_state,
            'maintenance': self.maintenance.to_dict(),
            'last_leader': self.last_leader,
            'synchronous_standby_names': {host: info.to_dict() for host, info in self.synchronous_standby_names.items()},
        }

@dataclass
class SwitchoverPlan:
    primary: str
    timeline: int | None = None

@dataclass
class SwitchoverState:
    progress: str | None = None
    info: SwitchoverPrimaryInfo | None = None
    failover: str | None = None
    replicas: ReplicaInfos = field(default_factory=list)
