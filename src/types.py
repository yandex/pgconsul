"""Typed domain records and explicit conversions at SQL/JSON boundaries."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, cast

ReplicationState = tuple[str, str | None]
SsnInfo = tuple[str | None, str | None]
ElectionVote = tuple[int, int]


@dataclass
class ReplicaInfo:
    pid: int = 0
    application_name: str | None = ''
    client_hostname: str | None = None
    state: str | None = ''
    primary_location: str | None = None
    write_location_diff: int | None = None
    replay_lag_msec: int | None = None
    reply_time_ms: int | None = None
    sync_state: str | None = ''
    priority: int | None = None
    _present_fields: set[str] = field(default_factory=set, repr=False, compare=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> ReplicaInfo:
        return cls(
            pid=cast(int, data.get('pid', 0)),
            application_name=cast(str | None, data.get('application_name', '')),
            client_hostname=cast(str | None, data.get('client_hostname', None)),
            state=cast(str | None, data.get('state', '')),
            primary_location=cast(str | None, data.get('primary_location', None)),
            write_location_diff=cast(int | None, data.get('write_location_diff', None)),
            replay_lag_msec=cast(int | None, data.get('replay_lag_msec', None)),
            reply_time_ms=cast(int | None, data.get('reply_time_ms', None)),
            sync_state=cast(str | None, data.get('sync_state', '')),
            priority=cast(int | None, data.get('priority', None)),
            _present_fields=set(data),
        )

    def to_dict(self) -> dict[str, object]:
        data: dict[str, object] = {
            'pid': self.pid,
            'application_name': self.application_name,
            'client_hostname': self.client_hostname,
            'state': self.state,
            'primary_location': self.primary_location,
            'write_location_diff': self.write_location_diff,
            'replay_lag_msec': self.replay_lag_msec,
            'reply_time_ms': self.reply_time_ms,
            'sync_state': self.sync_state,
            'priority': self.priority,
        }
        return {key: value for key, value in data.items() if key in self._present_fields}


ReplicaInfos = list[ReplicaInfo]


@dataclass
class WalReceiverInfo:
    pid: int = 0
    status: str = ''
    slot_name: str | None = None
    last_msg_receipt_time_msec: int = 0
    conninfo: str | None = None
    _present_fields: set[str] = field(default_factory=set, repr=False, compare=False)

    def __bool__(self) -> bool:
        return bool(self._present_fields)

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> WalReceiverInfo:
        return cls(
            pid=cast(int, data.get('pid', 0)),
            status=cast(str, data.get('status', '')),
            slot_name=cast(str | None, data.get('slot_name', None)),
            last_msg_receipt_time_msec=cast(int, data.get('last_msg_receipt_time_msec', 0)),
            conninfo=cast(str | None, data.get('conninfo', None)),
            _present_fields=set(data),
        )

    def to_dict(self) -> dict[str, object]:
        data: dict[str, object] = {
            'pid': self.pid,
            'status': self.status,
            'slot_name': self.slot_name,
            'last_msg_receipt_time_msec': self.last_msg_receipt_time_msec,
            'conninfo': self.conninfo,
        }
        return {key: value for key, value in data.items() if key in self._present_fields}


@dataclass
class DbState:
    alive: bool = False
    running: bool = False
    role: str | None = None
    pgdata: str = ''
    opened: bool = False
    timeline: int | None = None
    wal_receiver: WalReceiverInfo | None = None
    replics_info: ReplicaInfos | None = None
    replication_state: ReplicationState | None = None
    sessions_ratio: float | None = None
    primary_fqdn: str | None = None
    connection_timed_out: bool = False
    prev_state: DbState | None = None
    _present_fields: set[str] = field(default_factory=set, repr=False, compare=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> DbState:
        return cls(
            alive=cast(bool, data.get('alive', False)),
            running=cast(bool, data.get('running', False)),
            role=cast(str | None, data.get('role', None)),
            pgdata=cast(str, data.get('pgdata', '')),
            opened=cast(bool, data.get('opened', False)),
            timeline=cast(int | None, data.get('timeline', None)),
            wal_receiver=WalReceiverInfo.from_dict(cast(Mapping[str, object], data['wal_receiver'])) if data.get('wal_receiver') is not None else None,
            replics_info=[ReplicaInfo.from_dict(row) for row in cast(list[Mapping[str, object]], data['replics_info'])] if data.get('replics_info') is not None else None,
            replication_state=cast(ReplicationState, tuple(cast(list[object], data['replication_state']))) if data.get('replication_state') is not None else None,
            sessions_ratio=cast(float | None, data.get('sessions_ratio', None)),
            primary_fqdn=cast(str | None, data.get('primary_fqdn', None)),
            connection_timed_out=cast(bool, data.get('connection_timed_out', False)),
            prev_state=DbState.from_dict(cast(Mapping[str, object], data['prev_state'])) if data.get('prev_state') else None,
            _present_fields=set(data),
        )

    def to_dict(self) -> dict[str, object]:
        data: dict[str, object] = {
            'alive': self.alive,
            'running': self.running,
            'role': self.role,
            'pgdata': self.pgdata,
            'opened': self.opened,
            'timeline': self.timeline,
            'wal_receiver': self.wal_receiver.to_dict() if self.wal_receiver is not None else None,
            'replics_info': [row.to_dict() for row in self.replics_info] if self.replics_info is not None else None,
            'replication_state': self.replication_state,
            'sessions_ratio': self.sessions_ratio,
            'primary_fqdn': self.primary_fqdn,
            'connection_timed_out': self.connection_timed_out,
            'prev_state': self.prev_state.to_dict() if self.prev_state is not None else {},
        }
        return {key: value for key, value in data.items() if key in self._present_fields}


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
class SwitchoverPrimaryInfo:
    hostname: str | None = None
    timeline: int | str | None = None
    destination: str | None = None
    primary: str | None = None
    _present_fields: set[str] = field(default_factory=set, repr=False, compare=False)

    def __bool__(self) -> bool:
        return bool(self._present_fields)

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> SwitchoverPrimaryInfo:
        return cls(
            hostname=cast(str | None, data.get('hostname', None)),
            timeline=cast(int | str | None, data.get('timeline', None)),
            destination=cast(str | None, data.get('destination', None)),
            primary=cast(str | None, data.get('primary', None)),
            _present_fields=set(data),
        )

    def to_dict(self) -> dict[str, object]:
        data: dict[str, object] = {
            'hostname': self.hostname,
            'timeline': self.timeline,
            'destination': self.destination,
            'primary': self.primary,
        }
        return {key: value for key, value in data.items() if key in self._present_fields}


@dataclass
class ZkState:
    alive: bool = False
    replics_info: ReplicaInfos | None = None
    last_failover_time: float | None = None
    last_switchover_time: float | None = None
    failover_state: str | None = None
    failover_must_be_reset: bool = False
    current_promoting_host: str | None = None
    lock_version: str | None = None
    lock_holder: str | None = None
    single_node: bool = False
    timeline: int | None = None
    switchover: SwitchoverPrimaryInfo | None = None
    switchover_candidate: str | None = None
    switchover_side_replicas: list[str] | None = None
    switchover_state: str | None = None
    maintenance: MaintenanceState = field(default_factory=MaintenanceState)
    last_leader: str | None = None
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
            'synchronous_standby_names': self.synchronous_standby_names,
        }


@dataclass
class SwitchoverPlan:
    primary: str | None = None
    timeline: int | str | None = None


@dataclass
class SwitchoverState:
    progress: str | None = None
    info: SwitchoverPrimaryInfo = field(default_factory=SwitchoverPrimaryInfo)
    failover: str | None = None
    replicas: ReplicaInfos = field(default_factory=list)
