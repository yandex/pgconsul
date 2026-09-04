# encoding: utf-8
"""
Command vocabulary for cluster-operation state machines (ADR-0006/ADR-0007).

Each effect a handler can request is a frozen dataclass with no behaviour.
A handler returns an ordered ``Plan`` (a list of commands, executed in order;
execution stops at the first failing command).

Commands are grouped by scope so cluster-operation machines can share the
same executor. Composite operations stay opaque.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING, Literal, Mapping, Union

from .types import StrEnum

if TYPE_CHECKING:
    from .failover import FailoverPhase
    from .return_to_cluster.state import ReturnState
    from .switchover.types import SwitchoverRecord

# --- Common failover commands ---


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
    'switchover_candidate',
    'failover_participant',
]

SwitchoverAction = Literal[
    'cleanup_invalid',
    'cleanup',
    'initialize_deadline',
    'rollback_pre_handoff_timeout',
    'recover_committed_handoff_timeout',
    'schedule_cleanup',
    'recover_pre_handoff',
    'primary_schedule',
    'primary_prepare_durability',
    'primary_prepare_candidate',
    'primary_turn_sides',
    'primary_confirm_promotion',
    'primary_fence_return',
    'candidate_prepare',
    'candidate_promote',
    'candidate_wait_archive',
    'side_turn',
    'side_wait_archive',
]

ReturnIterationAction = Literal[
    'wait_for_resetup',
    'resume_after_resetup',
    'replan_target',
    'complete',
    'track_startup',
    'track_replay',
    'track_archive_replay',
    'start_unchanged',
    'retry_start',
    'reconcile_requested',
    'rewind',
]


@dataclass(frozen=True)
class ClearLocalState:
    """Discard host-local progress for an operation side."""

    scope: LocalStateScope


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
class SwitchoverStep:
    """Execute one named idempotent switchover effect selected by the machine."""

    action: SwitchoverAction
    record: 'SwitchoverRecord'
    db_state: Mapping[str, Any]
    zk_state: Mapping[str, Any]


@dataclass(frozen=True)
class ReturnIterationStep:
    """Execute one idempotent host-local return-to-cluster effect."""

    action: ReturnIterationAction
    state: 'ReturnState | None'
    db_state: Mapping[str, Any]
    current_time: float = 0.0


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
    Sleep,
    Log,
    ClearLocalState,
    # Opaque
    Promote,
    ReturnToCluster,
    ReturnIterationStep,
    SwitchoverStep,
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


@dataclass(frozen=True)
class Decision:
    """A pure machine decision and its iteration-ownership contract."""

    plan: Plan
    owns_iteration: bool
