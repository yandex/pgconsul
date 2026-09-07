"""Regression tests for quorum-based automatic failover probing."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.failover import FailoverHealthReport, FailoverProbe
from src.main import Pgconsul
from src.switchover import SwitchoverPhase, SwitchoverRecord
from src.types import DesiredPrimary, DurabilityConfig, DurabilityState, DurabilityTransition


def _instance() -> Pgconsul:
    inst = Pgconsul.__new__(Pgconsul)
    inst.db = MagicMock()
    inst.zk = MagicMock()
    inst.config = SimpleNamespace(
        stream_from=None,
        autofailover=True,
        primary_unavailability_timeout=5.0,
        min_failover_timeout=30.0,
        iteration_timeout=1.0,
    )
    inst._is_single_node = False
    inst._health_primary = None
    inst._health_unreachable_since = None
    inst._health_receive_position = None
    inst._health_receive_unchanged_since = None
    inst._health_receiver_missing = False
    inst._health_leader_lock_missing_since = None
    inst.zk.LAST_PRIMARY_PATH = 'last_primary'
    inst.zk.LAST_FAILOVER_TIME_PATH = 'last_failover_time'
    inst.zk.ELECTION_MANAGER_LOCK_PATH = 'manager'
    inst.zk.FAILOVER_PROBE_PATH = 'probe'
    inst.zk.SWITCHOVER_RECORD_PATH = 'switchover_record'
    inst.zk.SWITCHOVER_VERSION_KEY = 'switchover_version'
    inst.zk.PRIMARY_LOCK_PATH = 'leader'
    inst.zk.DESIRED_PRIMARY_PATH = 'desired_primary'
    inst.zk.get_ha_hosts.return_value = [
        'primary', 'candidate', 'a', 'b', 'c', 'd',
    ]
    return inst


def test_health_becomes_eligible_only_after_both_intervals_stop():
    inst = _instance()
    inst.db.is_host_unreachable.return_value = True
    inst.db.get_receive_diff.side_effect = [100, 100]
    state = {'lock_holder': 'primary', 'last_primary': 'primary'}

    with patch('src.main.time.time', side_effect=[10.0, 14.0, 16.0, 16.0]):
        inst._update_failover_health({'role': 'replica'}, state)
        assert inst._local_health_ready('primary') == (False, False)
        inst._update_failover_health({'role': 'replica'}, state)
        assert inst._local_health_ready('primary') == (True, True)

    assert inst.db.get_receive_diff.call_count == 2
    inst.db.get_replay_diff.assert_not_called()


def test_non_ha_primary_becomes_failover_eligible_without_network_probe():
    inst = _instance()
    inst.zk.get_ha_hosts.return_value = ['replica']
    state = {'lock_holder': 'primary', 'last_primary': 'primary'}

    with patch('src.main.time.time', side_effect=[10.0, 16.0, 16.0]), \
         patch('src.main.logging.warning'):
        inst._update_failover_health({'role': 'replica'}, state)
        inst._update_failover_health({'role': 'replica'}, state)
        assert inst._local_health_ready('primary') == (True, True)

    inst.db.is_host_unreachable.assert_not_called()
    inst.db.get_receive_diff.assert_not_called()


def test_probe_quorum_counts_only_matching_negative_stalled_reports():
    inst = _instance()
    probe = FailoverProbe(7, 'primary', ('primary', 'a', 'b', 'c'), 4, 'op')
    good = FailoverHealthReport(7, 'primary', 4, True, True, 100)
    reachable = FailoverHealthReport(7, 'primary', 4, False, True, 100)
    inst.zk.get_failover_health.side_effect = lambda host, _: {
        'a': good,
        'b': reachable,
        'c': good,
    }[host]

    assert inst._probe_has_quorum(probe)
    assert inst._probe_quorum_size(probe) == 2


def test_probe_counts_network_failure_from_replica_without_wal_receiver():
    inst = _instance()
    probe = FailoverProbe(7, 'primary', ('primary', 'a', 'b'), 4, 'op')
    receiverless = FailoverHealthReport(
        7, 'primary', 4, True, False, None, receiver_missing=True,
    )
    inst.zk.get_failover_health.return_value = receiverless

    assert inst._probe_has_quorum(probe)


def test_probe_counts_leader_lock_lost_after_grace_period():
    inst = _instance()
    probe = FailoverProbe(7, 'primary', ('primary', 'a', 'b'), 4, 'op')
    lock_lost = FailoverHealthReport(
        7, 'primary', 4, False, False, None, leader_lock_missing=True,
    )
    inst.zk.get_failover_health.return_value = lock_lost

    assert inst._probe_has_quorum(probe)


def test_leader_lock_loss_becomes_a_signal_only_after_the_grace_period():
    inst = _instance()
    inst.db.is_host_unreachable.return_value = False
    inst.db.get_receive_diff.return_value = 100
    state = {'lock_holder': None, 'last_primary': 'primary', 'wal_receiver': None}
    probe = FailoverProbe(7, 'primary', ('primary', 'a'), 4, 'op')

    with patch('src.main.helpers.get_hostname', return_value='a'), \
         patch('src.main.time.time', side_effect=[10.0, 14.0, 14.0, 16.0, 16.0]):
        inst._update_failover_health({'role': 'replica', 'wal_receiver': None}, state)
        assert not inst._health_report(probe).leader_lock_missing
        assert inst._health_report(probe).leader_lock_missing


def test_expected_switchover_lock_transfer_is_not_a_failover_signal():
    inst = _instance()
    inst.db.is_host_unreachable.return_value = False
    inst.db.get_receive_diff.return_value = 100
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate',
        phase=SwitchoverPhase.TURNING_SIDES, operation_id='switch', version=3,
    )
    state = {
        'lock_holder': None,
        'last_primary': 'primary',
        'switchover_record': record.to_dict(),
        'switchover_version': 3,
        'desired_primary': DesiredPrimary(
            'candidate', 'switch', 'switchover',
        ).to_dict(),
    }
    probe = FailoverProbe(7, 'primary', ('primary', 'a'), 4, 'op')

    with patch('src.main.helpers.get_hostname', return_value='a'), \
         patch('src.main.time.time', side_effect=[10.0, 20.0, 20.0]):
        inst._update_failover_health({'role': 'replica', 'wal_receiver': None}, state)
        assert not inst._health_report(probe).leader_lock_missing


def test_pre_handoff_health_still_observes_the_old_primary():
    inst = _instance()
    inst.db.is_host_unreachable.return_value = True
    inst.db.get_receive_diff.return_value = 100
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate',
        phase=SwitchoverPhase.TURNING_SIDES, operation_id='switch', version=3,
    )
    state = {
        'lock_holder': 'candidate',
        'last_primary': 'primary',
        'switchover_record': record.to_dict(),
        'switchover_version': 3,
    }

    with patch('src.main.time.time', return_value=10.0):
        inst._update_failover_health(
            {'role': 'replica', 'wal_receiver': {'status': 'streaming'}}, state,
        )

    assert inst._health_primary == 'primary'
    inst.db.is_host_unreachable.assert_called_once_with(
        primary='primary', check_primary=False,
    )


def test_committed_handoff_health_observes_the_candidate_without_a_lock():
    inst = _instance()
    inst.db.is_host_unreachable.return_value = True
    inst.db.get_receive_diff.return_value = 100
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate',
        phase=SwitchoverPhase.HANDOFF_COMMITTED,
        operation_id='switch', expected_timeline=2, version=4,
    )
    state = {
        'lock_holder': None,
        'last_primary': 'primary',
        'switchover_record': record.to_dict(),
        'switchover_version': 4,
    }

    with patch('src.main.time.time', return_value=10.0):
        inst._update_failover_health(
            {'role': 'replica', 'wal_receiver': {'status': 'streaming'}}, state,
        )

    assert inst._health_primary == 'candidate'
    inst.db.is_host_unreachable.assert_called_once_with(
        primary='candidate', check_primary=False,
    )


def test_probe_requires_health_quorum_for_source_and_target():
    inst = _instance()
    source = DurabilityConfig.build(['primary', 'a', 'b', 'c'])
    target = DurabilityConfig.build(['primary', 'a', 'b', 'd'])
    probe = FailoverProbe(
        7,
        'primary',
        ('primary', 'a', 'b', 'c', 'd'),
        4,
        'op',
        (source.members, target.members),
    )
    good = FailoverHealthReport(7, 'primary', 4, True, True, 100)
    inst.zk.get_failover_health.side_effect = lambda host, _: (
        good if host in ('a', 'c') else None
    )

    assert not inst._probe_has_quorum(probe)

    inst.zk.get_failover_health.side_effect = lambda host, _: (
        good if host in ('a', 'b', 'c') else None
    )
    assert inst._probe_has_quorum(probe)


def test_start_failover_allows_persisted_transition_and_probes_both_quorums():
    inst = _instance()
    inst._health_primary = 'primary'
    inst._health_unreachable_since = 1.0
    inst._health_receive_unchanged_since = 1.0
    inst._health_receive_position = 100
    inst._try_acquire_failover_coordinator = MagicMock(return_value=True)
    source = DurabilityConfig.build(['primary', 'a', 'b', 'c'])
    target = DurabilityConfig.build(['primary', 'a', 'b', 'd'])
    state = DurabilityState(source, DurabilityTransition(source, target, 'change'))
    inst.zk.get_durability_state.return_value = (state, 5)
    probe = FailoverProbe(
        2, 'primary', ('primary', 'a', 'b', 'c', 'd'), 5, 'failover',
        (source.members, target.members),
    )
    inst.zk.start_failover_probe.return_value = probe
    inst._initialize_failover = MagicMock(return_value=True)
    zk_state = {'lock_holder': 'primary', 'last_primary': 'primary', 'last_failover_time': None}

    with patch('src.main.time.time', return_value=10.0), \
         patch('src.main.helpers.await_for_value', return_value=True):
        assert inst._start_failover({'role': 'replica'}, zk_state)

    inst.zk.start_failover_probe.assert_called_once_with(
        'primary', (source, target), 5, 30.0,
    )


def test_failed_probe_releases_manager_and_does_not_start_failover():
    inst = _instance()
    inst._health_primary = 'primary'
    inst._health_unreachable_since = 1.0
    inst._health_receive_unchanged_since = 1.0
    inst._health_receive_position = 100
    inst._try_acquire_failover_coordinator = MagicMock(return_value=True)
    durability = DurabilityConfig.build(['primary', 'a', 'b'])
    inst.zk.get_durability_state.return_value = (DurabilityState(durability), 5)
    probe = FailoverProbe(2, 'primary', durability.members, 5, 'op')
    inst.zk.start_failover_probe.return_value = probe
    inst.zk.write_failover_health.return_value = True
    inst._initialize_failover = MagicMock()
    inst._probe_has_quorum = MagicMock(return_value=False)
    state = {'lock_holder': 'primary', 'last_primary': 'primary', 'last_failover_time': None}

    with patch('src.main.time.time', return_value=10.0), \
         patch('src.main.helpers.await_for_value', return_value=None):
        assert inst._start_failover({'role': 'replica'}, state)

    inst.zk.release_lock.assert_called_once_with('manager')
    inst._initialize_failover.assert_not_called()


def test_probe_checks_quorum_once_more_after_wait_timeout():
    inst = _instance()
    inst._health_primary = 'primary'
    inst._health_unreachable_since = 1.0
    inst._health_receive_unchanged_since = 1.0
    inst._try_acquire_failover_coordinator = MagicMock(return_value=True)
    durability = DurabilityConfig.build(['primary', 'a', 'b'])
    inst.zk.get_durability_state.return_value = (DurabilityState(durability), 5)
    probe = FailoverProbe(2, 'primary', durability.members, 5, 'op')
    inst.zk.start_failover_probe.return_value = probe
    inst._probe_has_quorum = MagicMock(return_value=True)
    inst._initialize_failover = MagicMock()
    state = {'lock_holder': 'primary', 'last_primary': 'primary', 'last_failover_time': None}

    with patch('src.main.time.time', return_value=10.0), \
         patch('src.main.helpers.await_for_value', return_value=None):
        assert inst._start_failover({'role': 'replica'}, state)

    inst._initialize_failover.assert_called_once()
    inst.zk.release_lock.assert_not_called()


def test_undesired_primary_releases_lock_without_host_side_fencing():
    inst = _instance()
    state = {
        'lock_holder': 'host1',
        'desired_primary': DesiredPrimary(None, 'failover-1', 'failover').to_dict(),
    }

    with patch('src.main.helpers.get_hostname', return_value='host1'):
        assert inst._reconcile_primary_ownership({'role': 'primary'}, state)

    inst.db.pgpooler.assert_not_called()
    inst.db.stop_archiving_wal.assert_not_called()
    inst.zk.release_if_hold.assert_called_once_with('leader')


def test_non_owner_does_not_touch_lock_held_by_the_desired_host():
    inst = _instance()
    state = {
        'lock_holder': 'candidate',
        'desired_primary': DesiredPrimary(
            'candidate', 'failover-1', 'failover',
        ).to_dict(),
    }

    with patch('src.main.helpers.get_hostname', return_value='old-primary'):
        assert not inst._reconcile_primary_ownership({'role': 'primary'}, state)

    inst.db.pgpooler.assert_not_called()
    inst.db.stop_archiving_wal.assert_not_called()
    inst.zk.release_if_hold.assert_not_called()


def test_materialized_operation_winner_acquires_free_leader_lock():
    inst = _instance()
    for operation_type in ('failover', 'switchover'):
        inst.zk.try_acquire_lock.reset_mock()
        state = {
            'lock_holder': None,
            'desired_primary': DesiredPrimary(
                'host1', f'{operation_type}-1', operation_type,
            ).to_dict(),
        }

        with patch('src.main.helpers.get_hostname', return_value='host1'):
            assert inst._reconcile_primary_ownership({'role': 'replica'}, state)

        inst.zk.try_acquire_lock.assert_called_once_with(
            'leader', allow_queue=False, timeout=0,
        )


def test_switchover_desired_owner_transfers_only_the_leader_lock():
    inst = _instance()
    inst._timings = MagicMock()
    inst.stop_postgresql = MagicMock()
    events = []
    inst.db.stop_pooler_async.side_effect = lambda: events.append('pooler-stop')
    inst.zk.release_if_hold.side_effect = lambda _: events.append('lock-release')
    inst.stop_postgresql.side_effect = lambda **_: events.append('postgres-stop')
    state = {
        'lock_holder': 'host1',
        'desired_primary': DesiredPrimary('host2', 'switch-1', 'switchover').to_dict(),
    }

    with patch('src.main.helpers.get_hostname', return_value='host1'):
        assert inst._reconcile_primary_ownership({'role': 'primary'}, state)

    assert events == ['lock-release']
    inst.db.pgpooler.assert_not_called()
    inst.db.stop_archiving_wal.assert_not_called()
    inst.db.stop_pooler_async.assert_not_called()
    inst.stop_postgresql.assert_not_called()

    inst.zk.release_if_hold.reset_mock()
    state['lock_holder'] = 'host2'
    with patch('src.main.helpers.get_hostname', return_value='host1'):
        assert not inst._reconcile_primary_ownership({'role': 'primary'}, state)


    inst.zk.release_if_hold.assert_not_called()


def test_dead_postgres_releases_undesired_lock_it_still_holds():
    inst = _instance()
    state = {
        'lock_holder': 'host1',
        'desired_primary': DesiredPrimary(None, 'failover-1', 'failover').to_dict(),
    }

    with patch('src.main.helpers.get_hostname', return_value='host1'):
        assert inst._reconcile_primary_ownership({'role': None}, state)

    inst.db.pgpooler.assert_not_called()
    inst.db.stop_archiving_wal.assert_not_called()
    inst.zk.release_if_hold.assert_called_once_with('leader')
