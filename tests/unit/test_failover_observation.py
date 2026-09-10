# encoding: utf-8
"""Unit tests for FailoverObservation.build() (ADR-0007, stage 1).

Verifies that the observation builder — the sole I/O read point for a step —
correctly assembles all fields from db/zk/timings/record.
"""

from unittest.mock import MagicMock

from src.exceptions import PostgresConnectionError
from src.failover import (
    FailoverObservation,
    FailoverPhase,
    FailoverRecord,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_zk():
    """Create a MagicMock with ZK path constants set to real values."""
    zk = MagicMock()
    zk.PRIMARY_LOCK_PATH = 'leader'
    zk.ELECTION_MANAGER_LOCK_PATH = 'epoch_manager'
    zk.TIMELINE_INFO_PATH = 'timeline'
    # Default returns
    zk.get_timeline.return_value = 5
    zk.get_current_lock_holder.return_value = None
    zk.get_election_status.return_value = None
    zk.get_election_winner.return_value = None
    zk.get_ha_replics.return_value = {'host2', 'host3'}
    zk.get_ha_hosts.return_value = ['host1', 'host2', 'host3']
    zk.get_election_host_vote.return_value = None
    zk.get_alive_hosts.return_value = ['host2', 'host3']
    zk.noexcept_get_replics_info.return_value = []
    zk.get_last_failover_time.return_value = None
    zk.get_last_primary_availability_time.return_value = None
    return zk


def _make_db():
    db = MagicMock()
    db.get_role.return_value = 'replica'
    db.get_wal_receive_lsn.return_value = 100
    db.is_host_unreachable.return_value = True
    db.is_replaying_wal.return_value = False
    return db


def _make_timings():
    timings = MagicMock()
    timings.get_start.return_value = None
    return timings


def _make_record(phase=FailoverPhase.DETECTED):
    return FailoverRecord(phase=phase)


# ---------------------------------------------------------------------------
# Role / timeline
# ---------------------------------------------------------------------------


class TestObservationBuildRole:
    def test_builds_role_from_db(self):
        zk = _make_zk()
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.role == 'replica'
        db.get_role.assert_called_once()

    def test_falls_back_to_db_state_role_on_pg_error(self):
        zk = _make_zk()
        db = _make_db()
        db.get_role.side_effect = PostgresConnectionError('dead')
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={'role': 'primary'},
        )
        assert obs.role == 'primary'

    def test_fallback_role_passed_through(self):
        zk = _make_zk()
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={}, fallback_role='primary',
        )
        assert obs.fallback_role == 'primary'

    def test_local_timeline_from_db_state(self):
        zk = _make_zk()
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={'timeline': 7},
        )
        assert obs.local_timeline == 7

    def test_zk_timeline_from_zk(self):
        zk = _make_zk()
        zk.get_timeline.return_value = 9
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.zk_timeline == 9


# ---------------------------------------------------------------------------
# Lock / coordinator
# ---------------------------------------------------------------------------


class TestObservationBuildLock:
    def test_lock_holder_from_zk(self):
        zk = _make_zk()
        zk.get_current_lock_holder.return_value = 'host2'
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.lock_holder == 'host2'

    def test_is_coordinator_true_when_holding_manager_lock(self):
        zk = _make_zk()
        # First call: PRIMARY_LOCK_PATH holder; second: ELECTION_MANAGER_LOCK_PATH.
        zk.get_current_lock_holder.side_effect = lambda path=None: 'host1' if path == 'epoch_manager' else None
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.is_coordinator is True

    def test_is_coordinator_false_when_not_holding_manager_lock(self):
        zk = _make_zk()
        zk.get_current_lock_holder.side_effect = lambda path=None: 'host2' if path == 'epoch_manager' else None
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.is_coordinator is False


# ---------------------------------------------------------------------------
# Election / votes
# ---------------------------------------------------------------------------


class TestObservationBuildElection:
    def test_election_status_from_zk(self):
        zk = _make_zk()
        zk.get_election_status.return_value = 'registration'
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.election_status == 'registration'

    def test_election_winner_from_zk(self):
        zk = _make_zk()
        zk.get_election_winner.return_value = 'host2'
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.election_winner == 'host2'

    def test_votes_collected_from_ha_hosts(self):
        zk = _make_zk()
        zk.get_ha_hosts.return_value = ['host2', 'host3']
        zk.get_election_host_vote.side_effect = lambda h: (100, 1) if h == 'host2' else (200, 2)
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.votes == {'host2': (100, 1), 'host3': (200, 2)}

    def test_votes_skip_none(self):
        zk = _make_zk()
        zk.get_ha_hosts.return_value = ['host2', 'host3']
        zk.get_election_host_vote.side_effect = lambda h: (100, 1) if h == 'host2' else None
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.votes == {'host2': (100, 1)}

    def test_votes_empty_when_no_ha_hosts(self):
        zk = _make_zk()
        zk.get_ha_hosts.return_value = []
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.votes == {}


# ---------------------------------------------------------------------------
# HA replics / alive hosts / replics_info
# ---------------------------------------------------------------------------


class TestObservationBuildHosts:
    def test_ha_replics_as_frozenset(self):
        zk = _make_zk()
        zk.get_ha_replics.return_value = {'host2', 'host3'}
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.ha_replics == frozenset({'host2', 'host3'})

    def test_ha_replics_none_when_zk_returns_none(self):
        zk = _make_zk()
        zk.get_ha_replics.return_value = None
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.ha_replics is None

    def test_alive_hosts_from_zk(self):
        zk = _make_zk()
        zk.get_alive_hosts.return_value = ['host2', 'host3']
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.alive_hosts == ['host2', 'host3']

    def test_replics_info_from_zk(self):
        zk = _make_zk()
        zk.noexcept_get_replics_info.return_value = [
            {'application_name': 'host2', 'state': 'streaming'},
        ]
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.replics_info == [{'application_name': 'host2', 'state': 'streaming'}]

    def test_replics_info_defaults_to_empty(self):
        zk = _make_zk()
        zk.noexcept_get_replics_info.return_value = None
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.replics_info == []


# ---------------------------------------------------------------------------
# Host LSN / priority
# ---------------------------------------------------------------------------


class TestObservationBuildLsn:
    def test_host_lsn_from_db(self):
        zk = _make_zk()
        db = _make_db()
        db.get_wal_receive_lsn.return_value = 42
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.host_lsn == 42

    def test_host_lsn_none_on_pg_error(self):
        zk = _make_zk()
        db = _make_db()
        db.get_wal_receive_lsn.side_effect = PostgresConnectionError('dead')
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.host_lsn is None

    def test_host_priority_passed_through(self):
        zk = _make_zk()
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={}, host_priority=5,
        )
        assert obs.host_priority == 5


# ---------------------------------------------------------------------------
# Timestamps
# ---------------------------------------------------------------------------


class TestObservationBuildTimestamps:
    def test_last_failover_ts_from_zk(self):
        zk = _make_zk()
        zk.get_last_failover_time.return_value = 1000.0
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.last_failover_ts == 1000.0

    def test_last_primary_availability_ts_from_zk(self):
        zk = _make_zk()
        zk.get_last_primary_availability_time.return_value = 2000.0
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.last_primary_availability_ts == 2000.0


# ---------------------------------------------------------------------------
# I/O gates (moved into builder)
# ---------------------------------------------------------------------------


class TestObservationBuildGates:
    def test_is_primary_unreachable_from_db(self):
        zk = _make_zk()
        db = _make_db()
        db.is_host_unreachable.return_value = True
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.is_primary_unreachable is True

    def test_is_primary_unreachable_skipped_on_switchover_in_progress(self):
        zk = _make_zk()
        db = _make_db()
        db.is_host_unreachable.return_value = True
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={}, switchover_in_progress=True,
        )
        assert obs.is_primary_unreachable is False
        db.is_host_unreachable.assert_not_called()

    def test_is_primary_unreachable_true_on_pg_error(self):
        zk = _make_zk()
        db = _make_db()
        db.is_host_unreachable.side_effect = PostgresConnectionError('dead')
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.is_primary_unreachable is True

    def test_is_replaying_wal_from_db(self):
        zk = _make_zk()
        db = _make_db()
        db.is_replaying_wal.return_value = True
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.is_replaying_wal is True

    def test_is_replaying_wal_false_on_pg_error(self):
        zk = _make_zk()
        db = _make_db()
        db.is_replaying_wal.side_effect = PostgresConnectionError('dead')
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={},
        )
        assert obs.is_replaying_wal is False


# ---------------------------------------------------------------------------
# Timers / flags / config
# ---------------------------------------------------------------------------


class TestObservationBuildTimers:
    def test_failover_timer_started_true(self):
        zk = _make_zk()
        db = _make_db()
        timings = _make_timings()
        timings.get_start.side_effect = lambda name: 100.0 if name == 'failover' else None
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={},
        )
        assert obs.failover_timer_started is True
        assert obs.downtime_timer_started is False

    def test_downtime_timer_started_true(self):
        zk = _make_zk()
        db = _make_db()
        timings = _make_timings()
        timings.get_start.side_effect = lambda name: 200.0 if name == 'downtime' else None
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=timings,
            my_hostname='host1', db_state={},
        )
        assert obs.downtime_timer_started is True
        assert obs.failover_timer_started is False

    def test_switchover_in_progress_passed_through(self):
        zk = _make_zk()
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={}, switchover_in_progress=True,
        )
        assert obs.switchover_in_progress is True

    def test_allow_data_loss_passed_through(self):
        zk = _make_zk()
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={}, allow_data_loss=True,
        )
        assert obs.allow_data_loss is True

    def test_autofailover_passed_through(self):
        zk = _make_zk()
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={}, autofailover=False,
        )
        assert obs.autofailover is False

    def test_quorum_size_passed_through(self):
        zk = _make_zk()
        db = _make_db()
        obs = FailoverObservation.build(
            record=_make_record(), zk=zk, db=db, timings=_make_timings(),
            my_hostname='host1', db_state={}, quorum_size=3,
        )
        assert obs.quorum_size == 3
