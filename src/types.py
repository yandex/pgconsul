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
    client_addr: str | None = None
    state: str | None = ''
    primary_location: str | None = None
    sent_location_diff: int | None = None
    write_location_diff: int | None = None
    replay_location_diff: int | None = None
    replay_lag_msec: int | None = None
    backend_start_ts: int | None = None
    reply_time_ms: int | None = None
    sync_state: str | None = ''
    priority: int | None = None
    sent_lsn: str | None = None
    write_lsn: str | None = None
    replay_lsn: str | None = None
    _present_fields: set[str] = field(default_factory=set, repr=False, compare=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> ReplicaInfo:
        return cls(
            pid=cast(int, data.get('pid', 0)),
            application_name=cast(str | None, data.get('application_name', '')),
            client_hostname=cast(str | None, data.get('client_hostname', None)),
            client_addr=cast(str | None, data.get('client_addr', None)),
            state=cast(str | None, data.get('state', '')),
            primary_location=cast(str | None, data.get('primary_location', None)),
            sent_location_diff=cast(int | None, data.get('sent_location_diff', None)),
            write_location_diff=cast(int | None, data.get('write_location_diff', None)),
            replay_location_diff=cast(int | None, data.get('replay_location_diff', None)),
            replay_lag_msec=cast(int | None, data.get('replay_lag_msec', None)),
            backend_start_ts=cast(int | None, data.get('backend_start_ts', None)),
            reply_time_ms=cast(int | None, data.get('reply_time_ms', None)),
            sync_state=cast(str | None, data.get('sync_state', '')),
            priority=cast(int | None, data.get('priority', None)),
            sent_lsn=cast(str | None, data.get('sent_lsn')),
            write_lsn=cast(str | None, data.get('write_lsn')),
            replay_lsn=cast(str | None, data.get('replay_lsn')),
            _present_fields=set(data),
        )

    def to_dict(self) -> dict[str, object]:
        data: dict[str, object] = {
            'pid': self.pid,
            'application_name': self.application_name,
            'client_hostname': self.client_hostname,
            'client_addr': self.client_addr,
            'state': self.state,
            'primary_location': self.primary_location,
            'sent_location_diff': self.sent_location_diff,
            'write_location_diff': self.write_location_diff,
            'replay_location_diff': self.replay_location_diff,
            'replay_lag_msec': self.replay_lag_msec,
            'backend_start_ts': self.backend_start_ts,
            'reply_time_ms': self.reply_time_ms,
            'sync_state': self.sync_state,
            'priority': self.priority,
            'sent_lsn': self.sent_lsn,
            'write_lsn': self.write_lsn,
            'replay_lsn': self.replay_lsn,
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
    lsn: str | int | None = None
    archive_command: str | None = None
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
            lsn=cast(str | int | None, data.get('lsn', None)),
            archive_command=cast(str | None, data.get('archive_command', None)),
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
            'lsn': self.lsn,
            'archive_command': self.archive_command,
        }
        return {key: value for key, value in data.items() if key in self._present_fields}


@dataclass
class MaintenanceState:
    status: str | None = None
    ts: str | None = None
    _present_fields: set[str] = field(default_factory=lambda: {'status', 'ts'}, repr=False, compare=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> MaintenanceState:
        return cls(
            status=cast(str | None, data.get('status', None)),
            ts=cast(str | None, data.get('ts', None)),
            _present_fields=set(data),
        )

    def to_dict(self) -> dict[str, object]:
        data: dict[str, object] = {
            'status': self.status,
            'ts': self.ts,
        }
        return {key: value for key, value in data.items() if key in self._present_fields}


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
    single_node: bool | None = None
    timeline: int | None = None
    switchover: SwitchoverPrimaryInfo | None = None
    switchover_candidate: str | None = None
    switchover_side_replicas: list[str] | None = None
    switchover_state: str | None = None
    maintenance: MaintenanceState | None = None
    last_leader: str | None = None
    synchronous_standby_names: dict[str, SsnInfo] = field(default_factory=dict)
    replics_info_written: bool | None = None
    _present_fields: set[str] = field(default_factory=lambda: {
        'alive', 'replics_info', 'last_failover_time', 'last_switchover_time', 'failover_state',
        'failover_must_be_reset', 'current_promoting_host', 'lock_version', 'lock_holder',
        'single_node', 'timeline', 'switchover', 'switchover/candidate', 'switchover/side_replicas',
        'switchover/state', 'maintenance', 'last_leader', 'synchronous_standby_names',
    }, repr=False, compare=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> ZkState:
        return cls(
            alive=cast(bool, data.get('alive', False)),
            replics_info=[ReplicaInfo.from_dict(row) for row in cast(list[Mapping[str, object]], data['replics_info'])] if data.get('replics_info') is not None else None,
            last_failover_time=cast(float | None, data.get('last_failover_time', None)),
            last_switchover_time=cast(float | None, data.get('last_switchover_time', None)),
            failover_state=cast(str | None, data.get('failover_state', None)),
            failover_must_be_reset=cast(bool, data.get('failover_must_be_reset', False)),
            current_promoting_host=cast(str | None, data.get('current_promoting_host', None)),
            lock_version=cast(str | None, data.get('lock_version', None)),
            lock_holder=cast(str | None, data.get('lock_holder', None)),
            single_node=cast(bool | None, data.get('single_node', None)),
            timeline=cast(int | None, data.get('timeline', None)),
            switchover=SwitchoverPrimaryInfo.from_dict(cast(Mapping[str, object], data['switchover'])) if data.get('switchover') is not None else None,
            switchover_candidate=cast(str | None, data.get('switchover/candidate', None)),
            switchover_side_replicas=cast(list[str] | None, data.get('switchover/side_replicas', None)),
            switchover_state=cast(str | None, data.get('switchover/state', None)),
            maintenance=MaintenanceState.from_dict(cast(Mapping[str, object], data['maintenance'])) if data.get('maintenance') is not None else None,
            last_leader=cast(str | None, data.get('last_leader', None)),
            synchronous_standby_names={host: cast(SsnInfo, tuple(value)) for host, value in cast(Mapping[str, list[object]], data.get('synchronous_standby_names') or {}).items()},
            replics_info_written=cast(bool | None, data.get('replics_info_written', None)),
            _present_fields=set(data),
        )

    def to_dict(self) -> dict[str, object]:
        data: dict[str, object] = {
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
            'maintenance': self.maintenance.to_dict() if self.maintenance is not None else None,
            'last_leader': self.last_leader,
            'synchronous_standby_names': self.synchronous_standby_names,
            'replics_info_written': self.replics_info_written,
        }
        return {key: value for key, value in data.items() if key in self._present_fields}


@dataclass
class SwitchoverPlan:
    primary: str | None = None
    timeline: int | str | None = None
    _present_fields: set[str] = field(default_factory=lambda: {'primary', 'timeline'}, repr=False, compare=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> SwitchoverPlan:
        return cls(
            primary=cast(str | None, data.get('primary', None)),
            timeline=cast(int | str | None, data.get('timeline', None)),
            _present_fields=set(data),
        )

    def to_dict(self) -> dict[str, object]:
        data: dict[str, object] = {
            'primary': self.primary,
            'timeline': self.timeline,
        }
        return {key: value for key, value in data.items() if key in self._present_fields}


@dataclass
class SwitchoverState:
    progress: str | None = None
    info: SwitchoverPrimaryInfo = field(default_factory=SwitchoverPrimaryInfo)
    failover: str | None = None
    replicas: ReplicaInfos = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> SwitchoverState:
        return cls(
            progress=cast(str | None, data.get('progress')),
            info=SwitchoverPrimaryInfo.from_dict(cast(Mapping[str, object], data.get('info') or {})),
            failover=cast(str | None, data.get('failover')),
            replicas=[ReplicaInfo.from_dict(row) for row in cast(list[Mapping[str, object]], data.get('replicas') or [])],
        )

    def to_dict(self) -> dict[str, object]:
        return {
            'progress': self.progress,
            'info': self.info.to_dict(),
            'failover': self.failover,
            'replicas': [row.to_dict() for row in self.replicas] if self.replicas else {},
        }
