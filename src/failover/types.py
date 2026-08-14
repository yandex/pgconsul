# encoding: utf-8
"""Failover domain types and phases (MDB-41951, ADR-0007).

Phase values persisted in ZK ``failover_state``. Existing values
(``promoting``, ``checkpointing``, ``creating_slots``, ``finished``) are
kept verbatim — they are already written by ``_do_failover``/``_promote``
and read by old pgconsul versions. New values (``detected``,
``gates_passed``, ``registration``, ``voting``, ``winner_selected``,
``failed``) are unrecognized by old versions, preventing parallel
promotes (ADR-0007 §5 — two-phase rollout, same technique as ADR-0005 §5).
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from ..exceptions import PostgresConnectionError
from ..helpers import make_current_replics_quorum
from ..types import ReplicaInfos

if TYPE_CHECKING:
    from ..pg import Postgres
    from ..timings import TimingTracker
    from ..zk import Zookeeper


def _check_last_failover_time(last: float | None, min_timeout: float) -> bool:
    """True if last failover was long enough ago (or never happened).

    Scalar version of ``helpers.check_last_failover_time`` — no config dep.
    """
    if not last:
        return True
    return (time.time() - last) > min_timeout


class StrEnum(str, Enum):
    """StrEnum for any Python version: str() returns value, not "Class.NAME"."""

    def __str__(self) -> str:
        return self.value


class FailoverPhase(StrEnum):
    """Persistent phases of the failover state machine (ADR-0007 §2).

    Existing values (PROMOTING, CHECKPOINTING, CREATING_SLOTS, FINISHED) are
    written by the legacy ``_do_failover``/``_promote`` path and read by old
    pgconsul versions. New values are added by the state machine.
    """

    # --- New coordinator/election phases ---
    DETECTED = 'detected'                          # Replica sees holder is None.
    WALRECEIVER_DISABLING = 'walreceiver_disabling'  # Sleep + disable walreceiver (no gate recheck).
    GATES_PASSED = 'gates_passed'                  # Coordinator gates passed.
    REGISTRATION = 'registration'                  # Coordinator opened voting.
    VOTING = 'voting'                              # Participants recorded votes.
    WINNER_SELECTED = 'winner_selected'            # Coordinator wrote the winner.
    FAILED = 'failed'                              # Gates/quorum/lock failed — reset.

    # --- Existing phases (written by _do_failover/_promote) ---
    PROMOTING = 'promoting'
    CHECKPOINTING = 'checkpointing'
    CREATING_SLOTS = 'creating_slots'
    FINISHED = 'finished'

    @classmethod
    def from_str(cls, value: str | None) -> 'FailoverPhase | None':
        """Parse ZK state string, or None if absent/unknown."""
        if value is None:
            return None
        try:
            return cls(value)
        except ValueError:
            logging.warning('Unknown failover state value: %s', value)
            return None


@dataclass
class FailoverRecord:
    """Typed view of failover ZK nodes (failover_state + election nodes)."""

    phase: FailoverPhase | None = None
    winner: str | None = None
    election_status: str | None = None

    @classmethod
    def from_zk_state(cls, failover_state: str | None, zk: 'Zookeeper') -> 'FailoverRecord':
        """Build from a fresh ZK read (zk used for election reads)."""
        return cls(
            phase=FailoverPhase.from_str(failover_state),
            winner=zk.get_election_winner(),
            election_status=zk.get_election_status(),
        )

    def is_active(self) -> bool:
        """True if in-progress (resumable) failover."""
        return self.phase in (
            FailoverPhase.DETECTED,
            FailoverPhase.WALRECEIVER_DISABLING,
            FailoverPhase.GATES_PASSED,
            FailoverPhase.REGISTRATION,
            FailoverPhase.VOTING,
            FailoverPhase.WINNER_SELECTED,
            FailoverPhase.PROMOTING,
            FailoverPhase.CHECKPOINTING,
            FailoverPhase.CREATING_SLOTS,
        )

    def is_failed(self) -> bool:
        return self.phase == FailoverPhase.FAILED

    def is_finished(self) -> bool:
        return self.phase == FailoverPhase.FINISHED


@dataclass(frozen=True)
class FailoverObservation:
    """Immutable snapshot — sole handler input (ADR-0007 §3, ADR-0006 §1).

    Built by the shell before ``machine.plan()``; handlers perform no I/O.
    All gates of ``_can_do_failover`` become pure predicates over this.
    """

    record: FailoverRecord
    my_hostname: str
    role: str | None
    fallback_role: str | None
    lock_holder: str | None
    is_coordinator: bool
    election_status: str | None
    election_winner: str | None
    votes: dict[str, tuple[int, int]]
    ha_replics: frozenset[str] | None
    alive_hosts: list[str] | None
    replics_info: ReplicaInfos
    host_lsn: int | str | None
    host_priority: int
    last_failover_ts: float | None
    last_primary_availability_ts: float | None
    is_primary_unreachable: bool
    is_replaying_wal: bool
    switchover_in_progress: bool
    failover_timer_started: bool
    downtime_timer_started: bool
    zk_timeline: int | None
    local_timeline: int | None
    allow_data_loss: bool
    quorum_size: int
    autofailover: bool = True
    promote_started_ts: float | None = None

    @classmethod
    def build(
        cls,
        record: 'FailoverRecord',
        zk: 'Zookeeper',
        db: 'Postgres',
        timings: 'TimingTracker',
        my_hostname: str,
        db_state: dict,
        *,
        switchover_in_progress: bool = False,
        fallback_role: str | None = None,
        host_priority: int = 0,
        allow_data_loss: bool = False,
        quorum_size: int = 0,
        autofailover: bool = True,
    ) -> 'FailoverObservation':
        """Assemble observation — sole I/O read point per step (ADR-0006 §1).

        All gates of ``_can_do_failover`` read their inputs here; the I/O
        side effects (``is_host_unreachable``, ``is_replaying_wal``,
        ``disable_wal_receiver``) run in this builder so handlers stay pure.
        """
        # When local PG is dead, db.get_role() raises — fall back to cached role.
        role: str | None
        try:
            role = db.get_role()
        except PostgresConnectionError:
            role = db_state.get('role')

        local_timeline = db_state.get('timeline')
        zk_timeline = zk.get_timeline()
        lock_holder = zk.get_current_lock_holder(zk.PRIMARY_LOCK_PATH)
        is_coordinator = zk.get_current_lock_holder(zk.ELECTION_MANAGER_LOCK_PATH) == my_hostname

        election_status = zk.get_election_status()
        election_winner = zk.get_election_winner()

        # Collect votes for all HA hosts (coordinator tallies; participant votes).
        ha_replics_raw = zk.get_ha_replics(my_hostname)
        ha_replics = frozenset(ha_replics_raw) if ha_replics_raw is not None else None
        votes: dict[str, tuple[int, int]] = {}
        ha_hosts = zk.get_ha_hosts() or []
        for host in ha_hosts:
            vote = zk.get_election_host_vote(host)
            if vote is not None:
                votes[host] = vote

        alive_hosts = zk.get_alive_hosts()

        replics_info = zk.noexcept_get_replics_info() or []

        # Compute quorum_size if not provided (analog of _make_election).
        computed_quorum = quorum_size
        if computed_quorum == 0 and replics_info and alive_hosts is not None:
            computed_quorum = len(make_current_replics_quorum(replics_info, alive_hosts))

        # Local WAL position for the vote (best-effort; None if PG dead).
        host_lsn: int | str | None = None
        try:
            host_lsn = db.get_wal_receive_lsn() or '0'
        except PostgresConnectionError:
            host_lsn = None

        last_failover_ts = zk.get_last_failover_time()
        last_primary_availability_ts = zk.get_last_primary_availability_time()

        # I/O gates moved into the builder so handlers stay pure.
        is_primary_unreachable = False
        if not switchover_in_progress:
            try:
                is_primary_unreachable = db.is_host_unreachable(check_primary=False)
            except PostgresConnectionError:
                is_primary_unreachable = True

        is_replaying_wal = False
        try:
            is_replaying_wal = db.is_replaying_wal(1)
        except PostgresConnectionError:
            is_replaying_wal = False

        failover_timer_started = timings.get_start('failover') is not None
        downtime_timer_started = timings.get_start('downtime') is not None
        promote_started_ts = timings.get_start('failover_promote')

        return cls(
            record=record,
            my_hostname=my_hostname,
            role=role,
            fallback_role=fallback_role,
            lock_holder=lock_holder,
            is_coordinator=is_coordinator,
            election_status=election_status,
            election_winner=election_winner,
            votes=votes,
            ha_replics=ha_replics,
            alive_hosts=alive_hosts,
            replics_info=replics_info,
            host_lsn=host_lsn,
            host_priority=host_priority,
            last_failover_ts=last_failover_ts,
            last_primary_availability_ts=last_primary_availability_ts,
            is_primary_unreachable=is_primary_unreachable,
            is_replaying_wal=is_replaying_wal,
            switchover_in_progress=switchover_in_progress,
            failover_timer_started=failover_timer_started,
            downtime_timer_started=downtime_timer_started,
            promote_started_ts=promote_started_ts,
            zk_timeline=zk_timeline,
            local_timeline=local_timeline,
            allow_data_loss=allow_data_loss,
            quorum_size=computed_quorum,
            autofailover=autofailover,
        )


@dataclass
class FailoverMachineConfig:
    """Config consumed by failover machines (ADR-0004)."""

    election_timeout: float = 10.0
    min_failover_timeout: float = 0.0
    primary_unavailability_timeout: float = 30.0
    allow_potential_data_loss: bool = False
    iteration_timeout: float = 1.0
    walreceiver_disable_timeout: float = 30.0
    # Max wait for winner to finish promote before FAILED (ADR-0007 §2).
    promote_timeout: float = 300.0
    # Debug-only: sleep before disabling walreceiver (mirrors old _can_do_failover).
    sleep_before_disable_walreceiver: float = 0.0
