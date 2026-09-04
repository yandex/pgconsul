# encoding: utf-8
"""Tests for the failover observation I/O boundary."""

from unittest.mock import MagicMock

from src.exceptions import PostgresConnectionError
from src.failover import FailoverObservation, FailoverPhase, FailoverRequest
from src.types import DurabilityConfig, DurabilityState


def _dependencies():
    zk = MagicMock()
    zk.PRIMARY_LOCK_PATH = 'primary'
    zk.ELECTION_MANAGER_LOCK_PATH = 'manager'
    zk.get_timeline.return_value = 5
    zk.get_current_lock_holder.side_effect = lambda path: {
        'primary': 'old-primary',
        'manager': 'host1',
    }[path]
    zk.get_election_winner.return_value = 'host2'
    zk.get_failover_version.return_value = 'version-1'
    zk.get_failover_request.return_value = (None, None)
    zk.get_failover_participant_state.return_value = 'promoting'
    zk.get_election_host_vote_with_timeline.side_effect = lambda host, **kwargs: {
        'old-primary': None,
        'host1': (100, 5),
        'host2': None,
    }[host]
    zk.noexcept_get_replics_info.return_value = [
        {'application_name': 'host1', 'state': 'streaming'},
    ]
    durability = DurabilityConfig.build(['old-primary', 'host1', 'host2'])
    zk.get_durability_state.return_value = (DurabilityState(durability), 4)
    zk.get_last_failover_time.return_value = 10.0
    zk.get_last_primary_availability_time.return_value = 20.0

    db = MagicMock()
    db.is_host_unreachable.return_value = True
    db.is_replaying_wal.return_value = False

    timings = MagicMock()
    timings.get_start.side_effect = lambda name, operation_id: {
        'failover': 30.0,
        'downtime': 31.0,
        'failover_promote': 32.0,
    }[name] if operation_id == 'version-1' else None
    return zk, db, timings


def _build(**kwargs):
    zk, db, timings = _dependencies()
    db.role = kwargs.pop('db_role', None)
    arguments = dict(
        phase=FailoverPhase.VOTING,
        zk=zk,
        db=db,
        timings=timings,
        my_hostname='host1',
        db_state={'role': 'replica', 'timeline': 6},
    )
    arguments.update(kwargs)
    return FailoverObservation.build(**arguments), zk, db, timings


def test_builds_identity_role_timelines_and_locks():
    obs, _, _, _ = _build()
    assert obs.phase == FailoverPhase.VOTING
    assert obs.my_hostname == 'host1'
    assert obs.role == 'replica'
    assert obs.local_timeline == 6
    assert obs.zk_timeline == 5
    assert obs.lock_holder == 'old-primary'
    assert obs.is_coordinator


def test_builds_election_snapshot():
    obs, zk, _, _ = _build()
    assert obs.election_winner == 'host2'
    assert obs.votes == {'host1': 100}
    assert obs.vote_timelines == {'host1': 5}
    zk.get_alive_hosts.assert_not_called()
    assert obs.quorum_size == 2
    assert obs.electorate == ('host1', 'host2')
    assert obs.failover_version == 'version-1'
    assert obs.winner_status == 'promoting'
    assert obs.durability == DurabilityConfig.build(
        ['old-primary', 'host1', 'host2'],
    )
    assert obs.durability_quorums == (obs.durability,)
    assert obs.failed_primary == 'old-primary'


def test_builds_postgres_fields():
    obs, _, _, _ = _build()
    assert obs.is_primary_unreachable
    assert not obs.is_replaying_wal


def test_builds_local_reconciliation_fields():
    obs, _, _, _ = _build(db_state={
        'role': None,
        'timeline': 6,
        'running': False,
        'primary_fqdn': 'host2',
    }, db_role='replica')

    assert obs.replication_source == 'host2'
    assert obs.is_postgresql_dead
    assert obs.previous_role == 'replica'


def test_builds_timestamps():
    obs, zk, _, _ = _build()
    assert obs.last_failover_ts == 10.0
    assert obs.last_primary_availability_ts is None
    zk.get_last_primary_availability_time.assert_not_called()
    assert obs.failover_started_ts == 30.0
    assert obs.downtime_started_ts == 31.0
    assert obs.promote_started_ts == 32.0
    assert obs.current_time > 0


def test_observation_does_not_read_lsn_before_fencing():
    zk, db, timings = _dependencies()
    obs = FailoverObservation.build(
        FailoverPhase.REGISTRATION,
        zk,
        db,
        timings,
        'host1',
        {'role': 'dead'},
    )
    db.get_wal_receive_lsn.assert_not_called()
    db.get_wal_flush_lsn.assert_not_called()


def test_primary_unreachable_connection_error_is_treated_as_unreachable():
    zk, db, timings = _dependencies()
    db.is_host_unreachable.side_effect = PostgresConnectionError('dead')
    obs = FailoverObservation.build(
        FailoverPhase.GATES_PASSED,
        zk,
        db,
        timings,
        'host1',
        {'role': 'replica'},
    )
    assert obs.is_primary_unreachable


def test_primary_check_can_be_skipped():
    obs, _, db, _ = _build(check_primary_unreachable=False)
    assert obs.is_primary_unreachable
    db.is_host_unreachable.assert_not_called()


def test_replay_connection_error_is_treated_as_not_replaying():
    zk, db, timings = _dependencies()
    db.is_replaying_wal.side_effect = PostgresConnectionError('dead')
    obs = FailoverObservation.build(
        FailoverPhase.GATES_PASSED,
        zk,
        db,
        timings,
        'host1',
        {'role': 'dead'},
    )
    assert not obs.is_replaying_wal


def test_primary_does_not_probe_wal_replay():
    """pgconsul_util.feature:504: replay positions are undefined on primary."""
    obs, _, db, _ = _build(db_state={'role': 'primary', 'timeline': 6})

    assert not obs.is_replaying_wal
    db.is_replaying_wal.assert_not_called()


def test_must_reset_is_passed_through_directly():
    obs, _, _, _ = _build(must_reset=True)
    assert obs.must_reset


def test_manual_data_loss_request_accepts_actual_vote_timelines():
    zk, db, timings = _dependencies()
    zk.get_failover_request.return_value = (
        FailoverRequest('old-primary', 'version-1', True, 'host1'), 3,
    )

    obs = FailoverObservation.build(
        FailoverPhase.WALRECEIVER_DISABLING,
        zk,
        db,
        timings,
        'host1',
        {'role': 'replica', 'timeline': 6},
    )

    assert obs.manual_data_loss is True
    assert obs.manual_winner == 'host1'
    assert obs.allow_mismatched_timeline_votes is True


def test_manual_data_loss_request_can_leave_wal_sources_unfenced():
    zk, db, timings = _dependencies()
    zk.get_failover_request.return_value = (
        FailoverRequest(
            'old-primary', 'version-1', True,
            fence_wal_sources=False,
        ), 3,
    )

    obs = FailoverObservation.build(
        FailoverPhase.WALRECEIVER_DISABLING,
        zk,
        db,
        timings,
        'host1',
        {'role': 'replica', 'timeline': 6},
    )

    assert obs.manual_fence_wal_sources is False
