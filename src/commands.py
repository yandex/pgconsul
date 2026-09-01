# encoding: utf-8
"""
Command vocabulary for cluster-op state machines (ADR-0006).

Each effect a handler can request is a frozen dataclass with no behaviour.
A handler returns an ordered ``Plan`` (a list of commands, executed in order;
execution stops at the first failing command).

Commands are grouped by scope so cluster-operation machines can share the
same executor. Composite operations stay opaque.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Union

from .types import StrEnum

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
    desired_operation_id: str | None = None
    desired_hostname: str | None = None


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
class StopPooler:
    """Stop the connection pooler (pgbouncer)."""


@dataclass(frozen=True)
class StopPostgresql:
    """Stop PostgreSQL via the external command manager."""

    wait: bool = True
    timeout: float | None = None


@dataclass(frozen=True)
class StartPostgresql:
    """Start the local PostgreSQL service."""


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
class Sleep:
    """Sleep for the given number of seconds (WAL-drain delay only)."""

    seconds: float


@dataclass(frozen=True)
class Log:
    """Emit a log message (optionally as a structured event)."""

    message: str
    level: Literal['debug', 'info', 'warning', 'error'] = 'info'
    event: bool = False


LocalStateScope = Literal[
    'switchover_primary',
    'switchover_candidate',
    'failover_participant',
]


@dataclass(frozen=True)
class WriteLocalState:
    """Persist the current host-local command group."""

    scope: LocalStateScope
    phase: str


@dataclass(frozen=True)
class ClearLocalState:
    """Discard host-local progress for an operation side."""

    scope: LocalStateScope


# --- Switchover-specific commands ---


@dataclass(frozen=True)
class TransitionTo:
    """CAS-persist a new switchover phase (the idempotency fence)."""

    phase: SwitchoverPhase


@dataclass(frozen=True)
class WriteCandidate:
    """Write the switchover candidate hostname to ZK."""

    candidate: str


@dataclass(frozen=True)
class WriteSideReplicas:
    """Write the side-replica list to ZK."""

    side_replicas: tuple[str, ...]


@dataclass(frozen=True)
class SetSyncReplication:
    """Switch replication to sync on the given host."""

    host: str


@dataclass(frozen=True)
class CleanupSwitchover:
    """CAS-clear the versioned switchover record."""


@dataclass(frozen=True)
class WriteSwitchoverAck:
    """Publish host-local switchover progress without changing its phase."""

    operation_id: str
    state: dict


@dataclass(frozen=True)
class InitializeFailover:
    """Initialize failover as a switchover fallback."""


# --- Opaque commands (composite operations, delegated to pgconsul) ---


class PromotionResult(StrEnum):
    """Outcome of one resumable promotion pipeline attempt."""

    SUCCESS = 'success'
    RETRY = 'retry'
    REJECTED = 'rejected'


@dataclass(frozen=True)
class Promote:
    """Resume a host-local promotion pipeline."""

    scope: LocalStateScope
    old_primary: str | None = None
    start_postgresql: bool = False
    failover_version: str | None = None


@dataclass(frozen=True)
class ReturnToCluster:
    """Reconcile the local PostgreSQL with the new primary."""

    new_primary: str
    role: str | None
    is_postgresql_dead: bool


@dataclass(frozen=True)
class RewindFromSource:
    """Delegate to pgconsul.rewind_from_source."""

    new_primary: str
    is_postgresql_dead: bool
    limit: float


@dataclass(frozen=True)
class SetSimplePrimarySwitchTry:
    """Remember a failed switch to the given primary."""

    new_primary: str


@dataclass(frozen=True)
class DeleteHostOp:
    """Delete the host_op ZK node for the local host."""


@dataclass(frozen=True)
class CreateSlots:
    """Create replication slots for the given side-replica hosts (opaque)."""

    hosts: tuple[str, ...]


# --- Failover-specific commands (ADR-0007, stage 2) ---


@dataclass(frozen=True)
class WriteLastFailoverTime:
    """Write the current time to the last_failover_time ZK node."""


@dataclass(frozen=True)
class PrepareFailoverVote:
    """Fence external WAL sources, then publish an actual-timeline vote."""

    priority: int
    walreceiver_timeout: float
    failover_version: str
    timeline: int
    lsn_read_sleep: float = 0.0
    timeline_only: bool = False
    fence_wal_sources: bool = True


@dataclass(frozen=True)
class WriteFailoverParticipantState:
    """Publish winner-local progress for the coordinator."""

    state: str
    failover_version: str


@dataclass(frozen=True)
class WriteElectionWinner:
    """Write the election winner hostname to ZK."""

    winner: str


@dataclass(frozen=True)
class ForceReleasePrimaryLock:
    """Delete the exact stale primary-lock contender after fencing."""

    expected_holder: str


@dataclass(frozen=True)
class CleanupFailover:
    """Delete failover metadata and release coordinator ownership."""


@dataclass(frozen=True)
class FailoverTransitionTo:
    """Persist a new failover phase to ZK (the idempotency fence, ADR-0007 §2)."""

    phase: FailoverPhase


# --- Type aliases ---


Command = Union[
    # Common
    AcquireLock,
    ReleaseLock,
    StartTimer,
    StopTimer,
    WriteLastSwitchoverTime,
    StopPooler,
    StopPostgresql,
    StartPostgresql,
    Checkpoint,
    StoreReplicsInfo,
    Sleep,
    Log,
    WriteLocalState,
    ClearLocalState,
    # Switchover
    TransitionTo,
    WriteCandidate,
    WriteSideReplicas,
    SetSyncReplication,
    CleanupSwitchover,
    WriteSwitchoverAck,
    InitializeFailover,
    # Opaque
    Promote,
    ReturnToCluster,
    RewindFromSource,
    SetSimplePrimarySwitchTry,
    DeleteHostOp,
    CreateSlots,
    # Failover (ADR-0007, stage 2)
    WriteLastFailoverTime,
    PrepareFailoverVote,
    WriteFailoverParticipantState,
    WriteElectionWinner,
    ForceReleasePrimaryLock,
    CleanupFailover,
    FailoverTransitionTo,
]

Plan = list[Command]
