"""Shared domain types and helpers (MDB-41951).

Hosts ``ReplicaInfos`` plus utilities shared by cluster-operation modules.
"""

import logging
import time
from dataclasses import dataclass
from enum import Enum

ReplicaInfo = dict[str, int | str]
ReplicaInfos = list[ReplicaInfo]


@dataclass(frozen=True)
class DurabilityConfig:
    """Full durability group stored in ZK, including the primary."""

    members: tuple[str, ...]
    required: int

    def __post_init__(self) -> None:
        members = tuple(sorted(set(self.members)))
        object.__setattr__(self, 'members', members)
        if self.required < 0 or self.required > max(0, len(members) - 1):
            raise ValueError(f'Invalid durability required={self.required} for {len(members)} members')

    @classmethod
    def build(cls, members, required: int | None = None) -> 'DurabilityConfig':
        unique_members = tuple(sorted(set(members)))
        if required is None:
            required = len(unique_members) // 2
        return cls(unique_members, required)

    @classmethod
    def from_dict(cls, value: dict) -> 'DurabilityConfig':
        return cls.build(value.get('members') or [], required=int(value.get('required', 0)))

    def to_dict(self) -> dict:
        return {'members': list(self.members), 'required': self.required}

    def replicas_for(self, hostname: str) -> list[str]:
        if hostname not in self.members:
            raise ValueError(f'Host {hostname} is absent from durability members')
        return [host for host in self.members if host != hostname]


class StrEnum(str, Enum):
    """StrEnum for any Python version: str() returns value, not "Class.NAME"."""

    def __str__(self) -> str:
        return self.value


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
