"""Typed domain records and explicit conversions at SQL/JSON boundaries."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

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

    def to_dict(self) -> dict[str, object]:
        return {'mode': self.mode, 'synchronous_standby_names': self.synchronous_standby_names}


@dataclass(frozen=True)
class SsnInfo:
    value: str
    last_update: str

    def to_dict(self) -> dict[str, str]:
        return {'value': self.value, 'last_update': self.last_update}


@dataclass(frozen=True, order=True)
class ElectionVote:
    lsn: int
    priority: int


@dataclass
class ReplicaInfo:
    pid: int = 0
    application_name: str = ''
    client_hostname: str = ''
    state: str = ''
    primary_location: str | None = None
    write_location_diff: int | None = None
    replay_lag_msec: int | None = None
    reply_time_ms: int | None = None
    sync_state: str = ''
    priority: int = 0

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReplicaInfo:
        return cls(
            pid=data.get('pid', 0),
            application_name=data.get('application_name') or '',
            client_hostname=data.get('client_hostname') or '',
            state=data.get('state') or '',
            primary_location=data.get('primary_location', None),
            write_location_diff=data.get('write_location_diff', None),
            replay_lag_msec=data.get('replay_lag_msec', None),
            reply_time_ms=data.get('reply_time_ms', None),
            sync_state=data.get('sync_state') or '',
            priority=data.get('priority') or 0,
        )

    def to_dict(self) -> dict[str, object]:
        return {
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


ReplicaInfos = list[ReplicaInfo]


@dataclass
class WalReceiverInfo:
    pid: int = 0
    status: str = ''
    slot_name: str = ''
    last_msg_receipt_time_msec: int = 0
    conninfo: str = ''

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> WalReceiverInfo:
        # dict.get defaults do not replace explicit nulls in legacy records.
        return cls(
            pid=data.get('pid') or 0,
            status=data.get('status') or '',
            slot_name=data.get('slot_name') or '',
            last_msg_receipt_time_msec=data.get('last_msg_receipt_time_msec') or 0,
            conninfo=data.get('conninfo') or '',
        )

    def to_dict(self) -> dict[str, object]:
        return {
            'pid': self.pid,
            'status': self.status,
            'slot_name': self.slot_name,
            'last_msg_receipt_time_msec': self.last_msg_receipt_time_msec,
            'conninfo': self.conninfo,
        }


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
    primary_fqdn: str | None = None
    connection_timed_out: bool = False

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DbState:
        wal_receiver = data.get('wal_receiver')
        if wal_receiver == {}:
            wal_receiver = None
        replication_state = data.get('replication_state')
        if replication_state is not None:
            replication_state = ReplicationState.from_data(replication_state)
        # dict.get defaults do not replace explicit nulls in legacy records.
        return cls(
            alive=data['alive'] if data.get('alive') is not None else False,
            running=data['running'] if data.get('running') is not None else False,
            role=data.get('role', None),
            pgdata=data.get('pgdata') or '',
            opened=data['opened'] if data.get('opened') is not None else False,
            timeline=data.get('timeline', None),
            wal_receiver=WalReceiverInfo.from_dict(wal_receiver) if wal_receiver is not None else None,
            replics_info=[ReplicaInfo.from_dict(row) for row in data['replics_info']] if data.get('replics_info') is not None else None,
            replication_state=replication_state,
            primary_fqdn=data.get('primary_fqdn', None),
            connection_timed_out=data['connection_timed_out'] if data.get('connection_timed_out') is not None else False,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            'alive': self.alive,
            'running': self.running,
            'role': self.role,
            'pgdata': self.pgdata,
            'opened': self.opened,
            'timeline': self.timeline,
            'wal_receiver': self.wal_receiver.to_dict() if self.wal_receiver is not None else None,
            'replics_info': [row.to_dict() for row in self.replics_info] if self.replics_info is not None else None,
            'replication_state': self.replication_state.to_dict() if self.replication_state is not None else None,
            'primary_fqdn': self.primary_fqdn,
            'connection_timed_out': self.connection_timed_out,
        }


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
    timeline: int | None = None
    destination: str | None = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> SwitchoverPrimaryInfo | None:
        if data is None or data == {}:
            return None
        timeline = data.get('timeline')
        if timeline is not None:
            try:
                if isinstance(timeline, bool) or not isinstance(timeline, (int, str)):
                    raise ValueError
                timeline = int(timeline)
            except ValueError as exc:
                raise ValueError(
                    f'Invalid timeline in ZooKeeper switchover/master: {timeline!r}; '
                    'expected an integer, an integer string, or null'
                ) from exc
        return cls(
            hostname=data.get('hostname'),
            timeline=timeline,
            destination=data.get('destination'),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            'hostname': self.hostname,
            'timeline': self.timeline,
            'destination': self.destination,
        }


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
    last_leader: str = ''
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
