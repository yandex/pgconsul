# encoding: utf-8
"""Failover domain types and phases (MDB-41951, ADR-0007)."""

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..exceptions import PostgresConnectionError
from ..types import DurabilityConfig, ReplicaInfos, StrEnum

if TYPE_CHECKING:
    from ..pg import Postgres
    from ..timings import TimingTracker
    from ..zk import Zookeeper


class FailoverPhase(StrEnum):
    """Persistent phases of the failover state machine (ADR-0007 §2).

    Only phases required for coordination between hosts are stored in ZK.
    """

    WALRECEIVER_DISABLING = 'walreceiver_disabling'  # Fence WAL sources and collect votes.
    GATES_PASSED = 'gates_passed'                  # Coordinator gates passed.
    REGISTRATION = 'registration'                  # Coordinator opened voting.
    VOTING = 'voting'                              # Participants recorded votes.
    WINNER_SELECTED = 'winner_selected'            # Coordinator wrote the winner.
    FAILED = 'failed'                              # Gates/quorum/lock failed — reset.

    PROMOTING = 'promoting'
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


@dataclass(frozen=True)
class FailoverProbe:
    """One bounded request for simultaneous primary-health observations."""

    probe_id: int
    primary: str
    durability_members: tuple[str, ...]
    durability_version: int
    operation_id: str
    durability_quorums: tuple[tuple[str, ...], ...] = ()

    @classmethod
    def from_dict(cls, value: dict) -> 'FailoverProbe':
        members = value['durability_members']
        if not isinstance(members, list) or not members or not all(
            isinstance(member, str) for member in members
        ):
            raise ValueError('probe durability members must be a non-empty string list')
        quorums = value.get('durability_quorums') or [members]
        if not isinstance(quorums, list) or not all(
            isinstance(quorum, list)
            and quorum
            and all(isinstance(member, str) for member in quorum)
            for quorum in quorums
        ):
            raise ValueError('probe durability quorums must be non-empty string lists')
        return cls(
            probe_id=int(value['probe_id']),
            primary=str(value['primary']),
            durability_members=tuple(members),
            durability_version=int(value['durability_version']),
            operation_id=str(value['operation_id']),
            durability_quorums=tuple(tuple(quorum) for quorum in quorums),
        )

    def to_dict(self) -> dict:
        return {
            'probe_id': self.probe_id,
            'primary': self.primary,
            'durability_members': list(self.durability_members),
            'durability_version': self.durability_version,
            'operation_id': self.operation_id,
            'durability_quorums': [list(quorum) for quorum in self.quorum_memberships],
        }

    @property
    def quorum_memberships(self) -> tuple[tuple[str, ...], ...]:
        return self.durability_quorums or (self.durability_members,)


@dataclass(frozen=True)
class FailoverHealthReport:
    """Replica response to exactly one failover probe."""

    probe_id: int
    primary: str
    durability_version: int
    primary_unreachable: bool
    wal_stalled: bool
    wal_position: int | None

    @classmethod
    def from_dict(cls, value: dict) -> 'FailoverHealthReport':
        position = value.get('wal_position')
        return cls(
            probe_id=int(value['probe_id']),
            primary=str(value['primary']),
            durability_version=int(value['durability_version']),
            primary_unreachable=value.get('primary_unreachable') is True,
            wal_stalled=value.get('wal_stalled') is True,
            wal_position=int(position) if position is not None else None,
        )

    def to_dict(self) -> dict:
        return {
            'probe_id': self.probe_id,
            'primary': self.primary,
            'durability_version': self.durability_version,
            'primary_unreachable': self.primary_unreachable,
            'wal_stalled': self.wal_stalled,
            'wal_position': self.wal_position,
        }


@dataclass(frozen=True)
class FailoverObservation:
    """Immutable snapshot — sole handler input (ADR-0007 §3, ADR-0006 §1).

    Built by the shell before ``machine.plan()``; handlers perform no I/O.
    """

    phase: FailoverPhase | None
    my_hostname: str
    role: str | None
    lock_holder: str | None
    is_coordinator: bool
    election_winner: str | None
    votes: dict[str, tuple[int, int]]
    alive_hosts: list[str] | None
    replics_info: ReplicaInfos | None
    host_priority: int
    last_failover_ts: float | None
    last_primary_availability_ts: float | None
    is_primary_unreachable: bool
    is_replaying_wal: bool
    failover_started_ts: float | None
    downtime_started_ts: float | None
    zk_timeline: int | None
    local_timeline: int | None
    allow_data_loss: bool
    quorum_size: int
    autofailover: bool = True
    must_reset: bool = False
    durability: DurabilityConfig | None = None
    durability_quorums: tuple[DurabilityConfig, ...] = ()
    failed_primary: str | None = None
    promote_started_ts: float | None = None
    replication_source: str | None = None
    is_postgresql_dead: bool = False
    previous_role: str | None = None
    electorate: tuple[str, ...] = ()
    winner_status: str | None = None
    failover_version: str | None = None
    # A committed bridge handoff fences old-timeline receivers but never lets
    # them vote for the new branch.
    fence_mismatched_timelines: bool = False
    # Snapshot of system clock — sole time source for pure handlers (ADR-0006).
    current_time: float = 0.0

    @classmethod
    def build(
        cls,
        phase: FailoverPhase | None,
        zk: 'Zookeeper',
        db: 'Postgres',
        timings: 'TimingTracker',
        my_hostname: str,
        db_state: dict,
        *,
        check_primary_unreachable: bool = True,
        check_wal_replay: bool = True,
        host_priority: int = 0,
        allow_data_loss: bool = False,
        autofailover: bool = True,
        must_reset: bool = False,
        fence_mismatched_timelines: bool = False,
    ) -> 'FailoverObservation':
        """Assemble observation — sole I/O read point per step (ADR-0006 §1).

        All I/O side effects run here so handlers stay pure.
        """
        local_timeline = db_state.get('timeline')
        zk_timeline = zk.get_timeline()
        lock_holder = zk.get_current_lock_holder(zk.PRIMARY_LOCK_PATH)
        is_coordinator = zk.get_current_lock_holder(zk.ELECTION_MANAGER_LOCK_PATH) == my_hostname

        election_winner = zk.get_election_winner()

        electorate = tuple(zk.get_failover_members() or ())
        failover_version = zk.get_failover_version()

        # Votes are accepted only from the immutable failover electorate.
        votes: dict[str, tuple[int, int]] = {}
        for host in electorate:
            if failover_version is None or zk_timeline is None:
                continue
            vote = zk.get_election_host_vote(
                host,
                failover_version=failover_version,
                timeline=zk_timeline,
            )
            if vote is not None:
                votes[host] = vote

        alive_hosts = zk.get_alive_hosts()

        replics_info = zk.noexcept_get_replics_info()

        durability_state, _ = zk.get_durability_state()
        durability = durability_state.stable
        durability_quorums = durability_state.failover_configs()
        failed_primary = (
            db_state.get('primary_fqdn')
            or lock_holder
            or zk.get(zk.LAST_PRIMARY_PATH)
        )

        quorum_size = 0
        if electorate:
            replica_count = len(electorate)
            write_quorum = (replica_count + 1) // 2
            quorum_size = replica_count - write_quorum + 1

        winner_status = (
            zk.get_failover_participant_state(election_winner, failover_version)
            if election_winner is not None and failover_version is not None
            else None
        )

        last_failover_ts = zk.get_last_failover_time()
        last_primary_availability_ts = None

        # Snapshot the system clock once so pure handlers never call time.time()
        # (ADR-0006: handlers must not read the system clock).
        current_time = time.time()

        # I/O gates run here so handlers stay pure.
        is_primary_unreachable = not check_primary_unreachable
        if check_primary_unreachable:
            try:
                is_primary_unreachable = db.is_host_unreachable(check_primary=False)
            except PostgresConnectionError:
                is_primary_unreachable = True

        is_replaying_wal = False
        if check_wal_replay and db_state.get('role') == 'replica':
            try:
                is_replaying_wal = db.is_replaying_wal(1)
            except PostgresConnectionError:
                is_replaying_wal = False

        failover_started_ts = timings.get_start('failover')
        downtime_started_ts = timings.get_start('downtime')
        promote_started_ts = timings.get_start('failover_promote')

        return cls(
            phase=phase,
            my_hostname=my_hostname,
            role=db_state.get('role'),
            lock_holder=lock_holder,
            is_coordinator=is_coordinator,
            election_winner=election_winner,
            votes=votes,
            alive_hosts=alive_hosts,
            replics_info=replics_info,
            host_priority=host_priority,
            last_failover_ts=last_failover_ts,
            last_primary_availability_ts=last_primary_availability_ts,
            is_primary_unreachable=is_primary_unreachable,
            is_replaying_wal=is_replaying_wal,
            failover_started_ts=failover_started_ts,
            downtime_started_ts=downtime_started_ts,
            promote_started_ts=promote_started_ts,
            replication_source=db_state.get('primary_fqdn'),
            is_postgresql_dead=db_state.get('running') is False,
            previous_role=db.role,
            zk_timeline=zk_timeline,
            local_timeline=local_timeline,
            allow_data_loss=allow_data_loss,
            quorum_size=quorum_size,
            durability=durability,
            durability_quorums=durability_quorums,
            failed_primary=failed_primary,
            autofailover=autofailover,
            must_reset=must_reset,
            electorate=electorate,
            winner_status=winner_status,
            failover_version=failover_version,
            fence_mismatched_timelines=fence_mismatched_timelines,
            current_time=current_time,
        )


@dataclass
class FailoverMachineConfig:
    """Config consumed by failover machines (ADR-0004)."""

    min_failover_timeout: float = 0.0
    primary_unavailability_timeout: float = 30.0
    walreceiver_disable_timeout: float = 30.0
    # Max wait for winner to finish promote before FAILED (ADR-0007 §2).
    promote_timeout: float = 300.0
    # Debug-only: sleep before disabling walreceiver.
    sleep_before_disable_walreceiver: float = 0.0
    # Debug-only: sleep after reading the vote LSN.
    election_lsn_read_sleep: float = 0.0
