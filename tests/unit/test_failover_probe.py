"""Regression tests for quorum-based automatic failover probing."""

from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

from src.failover import FailoverHealthReport, FailoverProbe
from src.main import Pgconsul
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
    inst._health_wal_position = None
    inst._health_wal_unchanged_since = None
    inst.zk.LAST_PRIMARY_PATH = 'last_primary'
    inst.zk.LAST_FAILOVER_TIME_PATH = 'last_failover_time'
    inst.zk.ELECTION_MANAGER_LOCK_PATH = 'manager'
    inst.zk.FAILOVER_PROBE_PATH = 'probe'
    inst.zk.PRIMARY_LOCK_PATH = 'leader'
    inst.zk.DESIRED_PRIMARY_PATH = 'desired_primary'
    return inst


def test_health_becomes_eligible_only_after_both_intervals_stop():
    inst = _instance()
    inst.db.is_host_unreachable.return_value = True
    inst.db.get_replay_diff.side_effect = [100, 100]
    state = {'lock_holder': 'primary', 'last_primary': 'primary'}

    with patch('src.main.time.time', side_effect=[10.0, 14.0, 16.0, 16.0]):
        inst._update_failover_health({'role': 'replica'}, state)
        assert inst._local_health_ready('primary') == (False, False)
        inst._update_failover_health({'role': 'replica'}, state)
        assert inst._local_health_ready('primary') == (True, True)


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
    inst._health_wal_unchanged_since = 1.0
    inst._health_wal_position = 100
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
        'primary', (source, target), 5,
    )


def test_failed_probe_releases_manager_and_does_not_start_failover():
    inst = _instance()
    inst._health_primary = 'primary'
    inst._health_unreachable_since = 1.0
    inst._health_wal_unchanged_since = 1.0
    inst._health_wal_position = 100
    inst._try_acquire_failover_coordinator = MagicMock(return_value=True)
    durability = DurabilityConfig.build(['primary', 'a', 'b'])
    inst.zk.get_durability_state.return_value = (DurabilityState(durability), 5)
    probe = FailoverProbe(2, 'primary', durability.members, 5, 'op')
    inst.zk.start_failover_probe.return_value = probe
    inst.zk.write_failover_health.return_value = True
    inst._initialize_failover = MagicMock()
    state = {'lock_holder': 'primary', 'last_primary': 'primary', 'last_failover_time': None}

    with patch('src.main.time.time', return_value=10.0), \
         patch('src.main.helpers.await_for_value', return_value=None):
        assert inst._start_failover({'role': 'replica'}, state)

    inst.zk.release_lock.assert_called_once_with('manager')
    inst._initialize_failover.assert_not_called()


def test_undesired_primary_is_fenced_before_releasing_lock():
    inst = _instance()
    state = {
        'desired_primary': DesiredPrimary(None, 'failover-1', 'failover').to_dict(),
    }

    assert inst._reconcile_desired_primary({'role': 'primary'}, state)

    assert inst.db.method_calls[:2] == [
        call.pgpooler('stop'),
        call.stop_archiving_wal(),
    ]
    inst.zk.release_if_hold.assert_called_once_with('leader')


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
            assert not inst._reconcile_desired_primary({'role': 'replica'}, state)

        inst.zk.try_acquire_lock.assert_called_once_with(
            'leader', allow_queue=False, timeout=0,
        )


def test_switchover_desired_owner_fences_old_primary_without_blocking_on_pooler():
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
        assert inst._reconcile_desired_primary({'role': 'primary'}, state)

    assert events == ['pooler-stop', 'lock-release', 'postgres-stop']
    inst.db.pgpooler.assert_not_called()
    inst.db.stop_archiving_wal.assert_not_called()
    inst.stop_postgresql.assert_called_once_with(wait=False, force_async=False)


def test_legacy_switchover_is_not_fenced_by_desired_primary_reconciliation():
    inst = _instance()
    inst.zk.SWITCHOVER_RECORD_PATH = 'switchover/record'
    state = {
        'lock_holder': 'host1',
        'desired_primary': DesiredPrimary('host2', 'switch-1', 'switchover').to_dict(),
        'switchover/record': {'phase': 'initiated', 'protocol_version': 1},
    }

    with patch('src.main.helpers.get_hostname', return_value='host1'):
        assert not inst._reconcile_desired_primary({'role': 'primary'}, state)

    inst.zk.release_if_hold.assert_not_called()


def test_dead_postgres_releases_undesired_lock_it_still_holds():
    inst = _instance()
    state = {
        'lock_holder': 'host1',
        'desired_primary': DesiredPrimary(None, 'failover-1', 'failover').to_dict(),
    }

    with patch('src.main.helpers.get_hostname', return_value='host1'):
        assert inst._reconcile_desired_primary({'role': None}, state)

    inst.db.pgpooler.assert_called_once_with('stop')
    inst.db.stop_archiving_wal.assert_not_called()
    inst.zk.release_if_hold.assert_called_once_with('leader')
