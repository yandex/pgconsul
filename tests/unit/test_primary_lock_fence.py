from unittest.mock import MagicMock, call, patch

from src.main import Pgconsul
from src.types import DbState, ZkState


def make_pgconsul():
    instance = Pgconsul.__new__(Pgconsul)
    instance.db = MagicMock()
    instance.zk = MagicMock()
    instance.zk.PRIMARY_LOCK_PATH = 'leader'
    instance.zk.get_current_promoting_host.return_value = None
    instance.zk.get_timeline.return_value = 1
    instance.db.get_timeline.return_value = 1
    return instance


def test_primary_lock_is_not_acquired_for_another_promoting_host():
    instance = make_pgconsul()
    instance.zk.get_current_promoting_host.return_value = 'new-primary'

    with patch('src.main.helpers.get_hostname', return_value='old-primary'):
        assert instance._try_acquire_primary_lock() is False

    instance.zk.try_acquire_lock.assert_not_called()
    instance.zk.release_if_hold.assert_called_once_with('leader')


def test_primary_lock_is_not_acquired_on_an_older_local_timeline():
    instance = make_pgconsul()
    instance.db.get_timeline.return_value = 4
    instance.zk.get_timeline.return_value = 5

    assert instance._try_acquire_primary_lock() is False

    instance.zk.try_acquire_lock.assert_not_called()
    instance.zk.release_if_hold.assert_called_once_with('leader')


def test_primary_lock_is_released_when_promoting_host_changes_during_acquire():
    instance = make_pgconsul()
    instance.zk.try_acquire_lock.return_value = True
    instance.zk.get_current_promoting_host.side_effect = [None, 'new-primary']

    with patch('src.main.helpers.get_hostname', return_value='old-primary'):
        assert instance._try_acquire_primary_lock(timeout=10) is False

    instance.zk.release_lock.assert_called_once_with('leader')


def test_primary_lock_is_released_when_zk_timeline_advances_during_acquire():
    instance = make_pgconsul()
    instance.zk.try_acquire_lock.return_value = True
    instance.zk.get_timeline.side_effect = [4, 5]
    instance.db.get_timeline.return_value = 4

    assert instance._try_acquire_primary_lock() is False

    instance.zk.release_lock.assert_called_once_with('leader')


def test_primary_lock_checks_fence_before_and_after_acquire():
    instance = make_pgconsul()
    instance.zk.try_acquire_lock.return_value = True

    with patch('src.main.helpers.get_hostname', return_value='primary'):
        assert instance._try_acquire_primary_lock(allow_queue=True, timeout=10) is True

    assert instance.zk.get_current_promoting_host.call_count == 2
    assert instance.db.get_timeline.call_count == 2
    assert instance.zk.get_timeline.call_count == 2
    instance.zk.try_acquire_lock.assert_called_once_with('leader', allow_queue=True, timeout=10)


def test_timeline_verification_fails_when_new_timeline_cannot_be_stored():
    instance = make_pgconsul()
    instance.db.role = 'primary'
    instance.zk.write_timeline.return_value = False
    db_state = DbState.from_dict({'timeline': 2})
    zk_state = ZkState(timeline=1)

    assert instance._verify_timeline(db_state, zk_state) is False


def test_promote_keeps_failover_fence_for_primary_iteration():
    instance = make_pgconsul()
    instance._timings = MagicMock()
    instance._slot_manager = MagicMock()
    instance.config = MagicMock(promote_checkpoint_sql=None)
    instance.zk.write_failover_state.return_value = True
    instance.zk.write_current_promoting_host.return_value = True
    instance.zk.write_timeline.return_value = True
    instance.db.promote.return_value = True
    instance.db.get_timeline.return_value = 2

    assert instance._promote() is True

    assert instance.zk.write_failover_state.call_args_list == [call('promoting'), call('checkpointing')]
    instance.zk.delete_current_promoting_host.assert_not_called()


def test_failed_promote_keeps_promotion_fence():
    instance = make_pgconsul()
    instance.zk.write_failover_state.return_value = True
    instance.zk.write_current_promoting_host.return_value = True
    instance.db.promote.return_value = False
    instance.db.get_role.return_value = 'replica'

    assert instance._promote() is False

    instance.zk.delete_current_promoting_host.assert_not_called()


def test_switchover_candidate_does_not_cleanup_in_accept_path():
    instance = make_pgconsul()
    instance.config = MagicMock(switchover_rollback_timeout=10)
    instance._timings = MagicMock()
    instance._try_acquire_primary_lock = MagicMock(return_value=True)
    instance._do_failover = MagicMock(return_value=True)
    instance._cleanup_switchover = MagicMock()
    instance.zk.get_switchover_primary_info.return_value = MagicMock(hostname='old-primary')
    zk_state = MagicMock(switchover_state='candidate_found', switchover_candidate='new-primary')

    with patch('src.main.helpers.get_hostname', return_value='new-primary'):
        assert instance._accept_switchover(zk_state) is True

    instance._cleanup_switchover.assert_not_called()


def test_old_primary_publishes_candidate_before_releasing_primary_lock():
    instance = make_pgconsul()
    instance.config = MagicMock(
        switchover_replica_turn_timeout=10,
        switchover_catchup_timeout=10,
        switchover_rollback_timeout=10,
    )
    instance._timings = MagicMock()
    instance._replication_manager = MagicMock()
    instance._replication_manager.change_replication_to_sync_host.return_value = True
    instance.zk.write_switchover_candidate.return_value = True
    instance.zk.write_switchover_side_replicas.return_value = True
    instance.zk.write_current_promoting_host.return_value = True
    instance._get_streaming_replicas = MagicMock(return_value=[])
    instance._store_replics_info = MagicMock()
    instance._wait_candidate_is_sync_with_primary = MagicMock(return_value=True)
    instance._wait_for_new_master_and_return_to_cluster = MagicMock(return_value=True)
    instance._debug_failure = MagicMock(return_value=False)
    instance.stop_postgresql = MagicMock(return_value=0)

    with patch('src.main.helpers.await_for', return_value=True), patch('src.main.time.sleep'):
        assert instance._do_primary_switchover(
            'new-primary',
            DbState.from_dict({'replics_info': []}),
            ZkState(),
        ) is True

    marker_call = call.write_current_promoting_host('new-primary')
    release_call = call.release_lock(lock_type='leader', wait=5)
    assert marker_call in instance.zk.mock_calls
    assert release_call in instance.zk.mock_calls
    assert instance.zk.mock_calls.index(marker_call) < instance.zk.mock_calls.index(release_call)
