# encoding: utf-8
"""
Command vocabulary for cluster-op state machines (ADR-0006).

Each effect a handler can request is a frozen dataclass with no behaviour.
A handler returns an ordered ``Plan`` (a list of commands, executed in order;
execution stops at the first failing command).

Commands are grouped by scope so that switchover and failover machines draw
from the same namespace. Composite operations (do_failover, rewind_from_source)
stay opaque — full reification is deferred to Stage 6.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from .failover import FailoverPhase
    from .switchover import SwitchoverPhase

# --- Common commands (used by every cluster-op machine) ---


@dataclass(frozen=True)
class AcquireLock:
    """Acquire the leader lock (or a named lock)."""

    lock_type: str | None = None
    allow_queue: bool = True
    timeout: float = 0


@dataclass(frozen=True)
class ReleaseLock:
    """Release the leader lock (or a named lock)."""

    lock_type: str | None = None
    wait: float = 0


@dataclass(frozen=True)
class StartTimer:
    """Start a named timing (idempotent: skipped if already started)."""

    name: str
    ts: float | None = None


@dataclass(frozen=True)
class StopTimer:
    """Stop a named timing and log the duration."""

    name: str
    track_as: str | None = None


@dataclass(frozen=True)
class WriteFailoverState:
    """Write a value to the failover_state ZK node."""

    value: str


@dataclass(frozen=True)
class WriteTimeline:
    """Write the current timeline to ZK."""

    timeline: int


@dataclass(frozen=True)
class StopPooler:
    """Stop the connection pooler (pgbouncer)."""


@dataclass(frozen=True)
class StopPostgresql:
    """Stop PostgreSQL via the external command manager."""

    wait: bool = True
    force_async: bool = False
    timeout: float | None = None


@dataclass(frozen=True)
class Checkpoint:
    """Issue a CHECKPOINT on the local PostgreSQL."""


@dataclass(frozen=True)
class StoreReplicsInfo:
    """Persist replics_info to ZK for the current primary."""


@dataclass(frozen=True)
class WriteLastSwitchoverTime:
    """Write the current time to the last_switchover_time ZK node."""


@dataclass(frozen=True)
class LeaveSyncGroup:
    """Remove the local host from the sync standby names group."""


@dataclass(frozen=True)
class Sleep:
    """Sleep for the given number of seconds (WAL-drain delay only)."""

    seconds: float


@dataclass(frozen=True)
class Log:
    """Emit a log message (optionally as a structured event)."""

    message: str
    level: str = 'info'
    event: bool = False


# --- Switchover-specific commands ---


@dataclass(frozen=True)
class TransitionTo:
    """Persist a new switchover phase to ZK (the idempotency fence)."""

    phase: SwitchoverPhase


@dataclass(frozen=True)
class WriteCandidate:
    """Write the switchover candidate hostname to ZK."""

    candidate: str


@dataclass(frozen=True)
class WriteSideReplicas:
    """Write the side-replica list to ZK."""

    side_replicas: list[str]


@dataclass(frozen=True)
class SetSyncReplication:
    """Switch replication to sync on the given host."""

    host: str


@dataclass(frozen=True)
class CleanupSwitchover:
    """Delete all switchover-related ZK nodes."""


# --- Opaque commands (composite operations, delegated to pgconsul) ---


@dataclass(frozen=True)
class DoFailover:
    """Delegate to pgconsul.do_failover (multi-step mini-procedure)."""

    old_primary: str | None


@dataclass(frozen=True)
class RewindFromSource:
    """Delegate to pgconsul.rewind_from_source."""

    new_primary: str
    is_postgresql_dead: bool
    limit: float


@dataclass(frozen=True)
class SetSimplePrimarySwitchTry:
    """Signal return-to-cluster via the simple primary switch flag."""


@dataclass(frozen=True)
class DeleteHostOp:
    """Delete the host_op ZK node for the local host."""


@dataclass(frozen=True)
class CreateSlots:
    """Create replication slots for the given side-replica hosts (opaque)."""

    hosts: list[str]


# --- Return-to-cluster commands (MDB-41951, ADR-0006) ---


@dataclass(frozen=True)
class SimplePrimarySwitch:
    """Delegate to pgconsul._simple_primary_switch (opaque)."""

    new_primary: str
    is_dead: bool
    limit: float


@dataclass(frozen=True)
class EnsureRestoringWal:
    """Restore archive recovery (undo restore_command=/bin/false)."""


@dataclass(frozen=True)
class CheckDivergence:
    """No-op marker: machine re-derives divergence from next observation."""


# --- Failover-specific commands (ADR-0007) ---


@dataclass(frozen=True)
class SetSSNBeforePromote:
    """Set sync standby names before promotion."""

    old_primary: str | None


@dataclass(frozen=True)
class WriteCurrentPromotingHost:
    """Write the current promoting host to ZK."""


# --- Failover-specific commands (ADR-0007, stage 2) ---


@dataclass(frozen=True)
class WriteLastFailoverTime:
    """Write the current time to the last_failover_time ZK node."""


@dataclass(frozen=True)
class CleanupVotes:
    """Delete all election vote nodes for HA hosts."""


@dataclass(frozen=True)
class WriteElectionStatus:
    """Write the election status (registration/selection/done/failed)."""

    status: str


@dataclass(frozen=True)
class WriteElectionVote:
    """Write the local host's election vote (lsn, priority)."""

    lsn: int | str
    priority: int


@dataclass(frozen=True)
class WriteElectionWinner:
    """Write the election winner hostname to ZK."""

    winner: str


@dataclass(frozen=True)
class ResetFailoverNode:
    """Reset the failover ZK node to 'finished' (opaque, ADR-0007 §4)."""


@dataclass(frozen=True)
class FailoverTransitionTo:
    """Persist a new failover phase to ZK (the idempotency fence, ADR-0007 §2)."""

    phase: 'FailoverPhase'


@dataclass(frozen=True)
class DisableWalReceiver:
    """Disable wal receiver on the local PostgreSQL (ADR-0007 §4)."""

    timeout: float


# --- Type aliases ---


Command = Union[
    # Common
    AcquireLock,
    ReleaseLock,
    StartTimer,
    StopTimer,
    WriteFailoverState,
    WriteTimeline,
    WriteLastSwitchoverTime,
    StopPooler,
    StopPostgresql,
    Checkpoint,
    StoreReplicsInfo,
    LeaveSyncGroup,
    Sleep,
    Log,
    # Switchover
    TransitionTo,
    WriteCandidate,
    WriteSideReplicas,
    SetSyncReplication,
    CleanupSwitchover,
    # Opaque
    DoFailover,
    RewindFromSource,
    SetSimplePrimarySwitchTry,
    DeleteHostOp,
    CreateSlots,
    # Return-to-cluster
    SimplePrimarySwitch,
    EnsureRestoringWal,
    CheckDivergence,
    # Failover (ADR-0007)
    SetSSNBeforePromote,
    WriteCurrentPromotingHost,
    # Failover (ADR-0007, stage 2)
    WriteLastFailoverTime,
    CleanupVotes,
    WriteElectionStatus,
    WriteElectionVote,
    WriteElectionWinner,
    ResetFailoverNode,
    FailoverTransitionTo,
    DisableWalReceiver,
]

Plan = list[Command]
