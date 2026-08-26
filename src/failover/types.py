# encoding: utf-8
"""Failover domain types and phases (MDB-41951, ADR-0007)."""

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..exceptions import PostgresConnectionError
from ..helpers import make_current_replics_quorum
from ..types import DurabilityConfig, ReplicaInfos, StrEnum

if TYPE_CHECKING:
    from ..pg import Postgres
    from ..timings import TimingTracker
    from ..zk import Zookeeper


class FailoverPhase(StrEnum):
    """Persistent phases of the failover state machine (ADR-0007 §2).

    Only phases required for coordination between hosts are stored in ZK.
    """

    WALRECEIVER_DISABLING = 'walreceiver_disabling'  # Sleep + disable walreceiver (no gate recheck).
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
    host_lsn: int | str | None
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
    promote_started_ts: float | None = None
    replication_source: str | None = None
    is_postgresql_dead: bool = False
    previous_role: str | None = None
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
        host_priority: int = 0,
        allow_data_loss: bool = False,
        autofailover: bool = True,
        must_reset: bool = False,
    ) -> 'FailoverObservation':
        """Assemble observation — sole I/O read point per step (ADR-0006 §1).

        All I/O side effects run here so handlers stay pure.
        """
        local_timeline = db_state.get('timeline')
        zk_timeline = zk.get_timeline()
        lock_holder = zk.get_current_lock_holder(zk.PRIMARY_LOCK_PATH)
        is_coordinator = zk.get_current_lock_holder(zk.ELECTION_MANAGER_LOCK_PATH) == my_hostname

        election_winner = zk.get_election_winner()

        # Collect votes for all HA hosts (coordinator tallies; participant votes).
        votes: dict[str, tuple[int, int]] = {}
        ha_hosts = zk.get_ha_hosts() or []
        for host in ha_hosts:
            vote = zk.get_election_host_vote(host)
            if vote is not None:
                votes[host] = vote

        alive_hosts = zk.get_alive_hosts()

        replics_info = zk.noexcept_get_replics_info()

        durability = zk.get_durability_config()

        quorum_size = len(make_current_replics_quorum(replics_info or [], alive_hosts or []))

        # Local WAL position for the vote (best-effort; None if PG dead).
        host_lsn: int | str | None = None
        try:
            host_lsn = db.get_wal_receive_lsn() or '0'
        except PostgresConnectionError:
            host_lsn = None

        last_failover_ts = zk.get_last_failover_time()
        last_primary_availability_ts = zk.get_last_primary_availability_time()

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
        if db_state.get('role') == 'replica':
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
            host_lsn=host_lsn,
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
            autofailover=autofailover,
            must_reset=must_reset,
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
