"""Shared domain types and helpers (MDB-41951).

Hosts ``ReplicaInfos`` plus utilities shared by cluster-operation modules.
"""

import logging
import uuid
import time
from dataclasses import dataclass
from enum import Enum

ReplicaInfo = dict[str, int | str]
ReplicaInfos = list[ReplicaInfo]


@dataclass(frozen=True)
class DurabilityConfig:
    """Full durability group stored in ZK, including the primary."""

    members: tuple[str, ...]

    def __post_init__(self) -> None:
        members = tuple(sorted(set(self.members)))
        object.__setattr__(self, 'members', members)

    @property
    def required(self) -> int:
        """Derived SSN ANY threshold; members include the primary."""
        return len(self.members) // 2

    @classmethod
    def build(cls, members) -> 'DurabilityConfig':
        return cls(tuple(members))

    @classmethod
    def from_dict(cls, value: dict) -> 'DurabilityConfig':
        return cls.build(value.get('members') or [])

    def to_dict(self) -> dict:
        return {'members': list(self.members)}

    def replicas_for(self, hostname: str) -> list[str]:
        if hostname not in self.members:
            raise ValueError(f'Host {hostname} is absent from durability members')
        return [host for host in self.members if host != hostname]


class StrEnum(str, Enum):
    """StrEnum for any Python version: str() returns value, not "Class.NAME"."""

    def __str__(self) -> str:
        return self.value


class DurabilityTransitionOrder(StrEnum):
    SSN_FIRST = 'ssn_first'
    ZK_FIRST = 'zk_first'


@dataclass(frozen=True)
class DurabilityTransition:
    source: DurabilityConfig | None
    target: DurabilityConfig
    order: DurabilityTransitionOrder
    lsn: int | None = None

    @classmethod
    def from_dict(cls, value: dict) -> 'DurabilityTransition':
        source_members = value.get('from_members') or []
        lsn = value.get('lsn')
        return cls(
            source=DurabilityConfig.build(source_members) if source_members else None,
            target=DurabilityConfig.build(value['to_members']),
            order=DurabilityTransitionOrder(value['order']),
            lsn=int(lsn) if lsn is not None else None,
        )

    def to_dict(self) -> dict:
        value: dict[str, object] = {
            'from_members': list(self.source.members) if self.source else [],
            'to_members': list(self.target.members),
            'order': self.order.value,
        }
        if self.lsn is not None:
            value['lsn'] = self.lsn
        return value


@dataclass(frozen=True)
class DurabilityState:
    stable: DurabilityConfig | None
    transition: DurabilityTransition | None = None

    @classmethod
    def from_dict(cls, value: dict) -> 'DurabilityState':
        members = value.get('members') or []
        transition = value.get('transition')
        return cls(
            stable=DurabilityConfig.build(members) if members else None,
            transition=DurabilityTransition.from_dict(transition) if transition else None,
        )

    def to_dict(self) -> dict:
        value: dict = {'members': list(self.stable.members) if self.stable else []}
        if self.transition is not None:
            value['transition'] = self.transition.to_dict()
        return value


@dataclass(frozen=True)
class DesiredPrimary:
    """Persistent owner intended to hold the PostgreSQL leader lock."""

    hostname: str | None
    operation_id: str
    operation_type: str

    @classmethod
    def steady(cls, hostname: str) -> 'DesiredPrimary':
        return cls(hostname, uuid.uuid4().hex, 'steady')

    @classmethod
    def from_dict(cls, value: dict) -> 'DesiredPrimary':
        hostname = value.get('hostname')
        if hostname is not None and not isinstance(hostname, str):
            raise ValueError('desired primary hostname must be a string or null')
        operation_id = value.get('operation_id')
        operation_type = value.get('operation_type')
        if not isinstance(operation_id, str) or not isinstance(operation_type, str):
            raise ValueError('desired primary operation metadata is missing')
        return cls(hostname, operation_id, operation_type)

    def to_dict(self) -> dict:
        return {
            'hostname': self.hostname,
            'operation_id': self.operation_id,
            'operation_type': self.operation_type,
        }


def is_transition_allowed(last: float | None, min_timeout: float, *, now: float | None = None) -> bool:
    """True if the previous role transition was long enough ago.

    ``now`` lets pure handlers pass the snapshot time from the observation
    instead of reading the system clock (ADR-0006). Defaults to ``time.time()``
    when no snapshot is available.
    """
    if not last:
        return True
    current = now if now is not None else time.time()
    return (current - last) > min_timeout


def is_timed_out(started_ts: float | None, timeout: float, what: str, *, now: float | None = None) -> bool:
    """True if ``started_ts`` is set and the elapsed time exceeded ``timeout``.

    Pure timeout predicate shared by failover/switchover timeout gates
    (de-duplicated from ``_is_promote_timed_out`` / ``_is_primary_shut_timed_out``).
    Logs an error with ``what`` describing the timed-out operation.

    ``now`` lets pure handlers pass the snapshot time from the observation
    instead of reading the system clock (ADR-0006). Defaults to ``time.time()``
    for backward compatibility with callers outside the state machines.
    """
    if started_ts is None:
        return False
    current = now if now is not None else time.time()
    elapsed = current - started_ts
    if elapsed > timeout:
        logging.error('%s timed out after %.1fs (timeout=%.1fs)', what, elapsed, timeout)
        return True
    return False
