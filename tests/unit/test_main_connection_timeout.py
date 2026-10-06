# encoding: utf-8
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.types import DbState, ReplicationState, ZkState
from src.exceptions import PostgresConnectionTimeout
from src.zk_client import ZkClientError
from tests.unit.state_fixtures import state_cluster, state_consul  # noqa: F401
from src.main import Pgconsul
from tests.unit.state_samples import DB_DEFAULTS


def _make_instance() -> Pgconsul:
    instance = Pgconsul.__new__(Pgconsul)
    instance.config = SimpleNamespace(
        working_dir='/tmp',
        stream_from=None,
        iteration_timeout=0.0,
    )
    instance.db = MagicMock()
    instance.zk = MagicMock()
    instance.notifier = MagicMock()
    instance._pg_conn_grace = MagicMock()
    instance._replication_manager = MagicMock()
    instance._is_single_node = False
    instance.is_in_maintenance = False
    instance.is_rewind_flag_set = MagicMock(return_value=False)
    instance.update_maintenance_status = MagicMock()
    instance._zk_alive_refresh = MagicMock()
    instance.dead_iter = MagicMock()
    instance.re_init_db = MagicMock()
    instance.finish_iteration = MagicMock()
    instance.zk.get_state.return_value = ZkState(alive=True)
    instance.zk.get_members.return_value = []
    return instance


def test_run_iteration_turns_timeout_into_dead_state_with_process_status():
    instance = _make_instance()
    timeout = PostgresConnectionTimeout(1)
    instance.db.is_alive_and_in_terminal_state.side_effect = timeout
    instance.db.is_postgresql_running.return_value = True
    instance.db.get_prev_state.side_effect = AssertionError('timeout snapshot must not read the cache')

    with patch('src.main.helpers.write_status_file'):
        instance.run_iteration('100')

    instance.db.get_prev_state.assert_not_called()
    instance._pg_conn_grace.record_failure.assert_called_once_with()
    instance.dead_iter.assert_called_once_with(
        DbState.from_dict({
            'alive': False,
            'running': True,
            'role': None,
            'connection_timed_out': True,
        }),
        ZkState(alive=True),
        is_in_terminal_state=True,
    )


def test_run_iteration_treats_failed_process_status_as_not_running():
    instance = _make_instance()
    instance.db.is_alive_and_in_terminal_state.side_effect = PostgresConnectionTimeout(1)
    instance.db.is_postgresql_running.side_effect = RuntimeError('status unavailable')

    with patch('src.main.helpers.write_status_file'):
        instance.run_iteration('100')

    db_state = instance.dead_iter.call_args.args[0]
    assert db_state.running is False
    assert db_state.to_dict() == {
        **DB_DEFAULTS, 'connection_timed_out': True,
    }


def test_successful_probe_resets_timeout_grace_period():
    instance = _make_instance()
    instance.db.is_alive_and_in_terminal_state.return_value = (False, True)
    instance.db.get_state.return_value = DbState.from_dict({'alive': False, 'role': None})

    with patch('src.main.helpers.write_status_file'):
        instance.run_iteration('100')

    instance._pg_conn_grace.reset.assert_called_once_with()
    instance._pg_conn_grace.record_failure.assert_not_called()


def test_run_iteration_finishes_once_when_rewind_flag_is_set():
    instance = _make_instance()
    instance.is_rewind_flag_set.return_value = True

    instance.run_iteration('100')

    instance.finish_iteration.assert_called_once()
    instance.db.is_alive_and_in_terminal_state.assert_not_called()


def test_dead_iter_has_no_side_effects_while_timeout_is_protected():
    instance = _make_instance()
    instance._pg_conn_grace.should_act.return_value = False

    result = Pgconsul.dead_iter(
        instance,
        DbState.from_dict({'alive': False, 'running': True, 'connection_timed_out': True}),
        ZkState(alive=True),
        is_in_terminal_state=True,
    )

    assert result is None
    instance.db.pgpooler.assert_not_called()
    instance._replication_manager.leave_sync_group.assert_not_called()
    instance.zk.release_if_hold.assert_not_called()
    instance.db.start_postgresql.assert_not_called()


def test_dead_iter_restarts_after_timeout_grace_period_expires():
    instance = _make_instance()
    instance._is_single_node = True
    instance._pg_conn_grace.should_act.return_value = True
    instance.db.start_postgresql.return_value = 0

    result = Pgconsul.dead_iter(
        instance,
        DbState.from_dict({'alive': False, 'running': True, 'connection_timed_out': True}),
        ZkState(alive=True),
        is_in_terminal_state=True,
    )

    assert result == 0
    instance.db.pgpooler.assert_called_once_with('stop')
    instance.db.start_postgresql.assert_called_once_with()


def test_dead_iter_reaches_cluster_recovery_after_grace_period_expires():
    instance = _make_instance()
    instance.db.role = 'primary'
    instance.db.get_timeline.return_value = 7
    instance.db.start_postgresql.return_value = 0
    instance.zk.get_current_lock_holder.return_value = None
    instance.zk.TIMELINE_INFO_PATH = 'timeline_info'
    instance._pg_conn_grace.should_act.return_value = True

    result = Pgconsul.dead_iter(
        instance,
        DbState.from_dict({'alive': False, 'running': True, 'connection_timed_out': True}),
        ZkState(alive=True, timeline=7),
        is_in_terminal_state=True,
    )

    assert result == 0
    instance.db.pgpooler.assert_called_once_with('stop')
    instance._replication_manager.leave_sync_group.assert_called_once_with()
    instance.zk.release_if_hold.assert_called_once_with(instance.zk.PRIMARY_LOCK_PATH)
    instance.db.stop_archiving_wal_stopped.assert_called_once_with()
    instance.db.start_postgresql.assert_called_once_with()


def test_dead_iter_preserves_zookeeper_safety_check_before_grace_period():
    instance = _make_instance()

    result = Pgconsul.dead_iter(
        instance,
        DbState.from_dict({'alive': False, 'running': True, 'connection_timed_out': True}),
        ZkState(alive=False),
        is_in_terminal_state=True,
    )

    assert result is None
    instance._pg_conn_grace.should_act.assert_not_called()
    instance.db.pgpooler.assert_not_called()


@pytest.mark.parametrize('role', ['primary', 'replica', None])
def test_run_iteration_fences_primary_or_detached_replica_when_zk_read_fails(state_consul, state_cluster, tmp_path, role):
    cluster = state_cluster
    state_consul.config.close_detached_after = 10
    state_consul.db.get_state.return_value = DbState(alive=role is not None, role=role)
    state_consul.db.get_replication_state.return_value = ReplicationState('sync', 'ANY 1(replica)')
    cluster.records.update({'replics_info': '[]', 'maintenance': 'enable'})
    before = dict(cluster.records)

    def disconnect(path):
        cluster.transport.is_alive.return_value = False
        raise ZkClientError('connection lost during state read')

    cluster.transport.get.side_effect = disconnect
    status = tmp_path / 'pgconsul.status'
    status.write_text('previous status')
    state_consul.run_iteration('100')
    if role is None:
        state_consul.db.pgpooler.assert_not_called()
    else:
        state_consul.db.pgpooler.assert_called_with('stop')
    if role == 'primary':
        state_consul.db.stop_archiving_wal.assert_called()
    else:
        state_consul.db.stop_archiving_wal.assert_not_called()
    cluster.transport.reconnect.assert_called()
    state_consul.db.start_postgresql.assert_not_called()
    assert cluster.records == before
    assert status.read_text() == 'previous status'


@pytest.mark.parametrize('cached', [{'role': 'primary'}, {'pgdata': '/var/lib/postgresql/data'}])
def test_re_init_db_exits_when_nonempty_cache_misses_required_field(cached):
    instance = _make_instance()
    instance.db.is_alive.return_value = False
    instance.db.get_prev_state.return_value = DbState.from_dict(cached)
    with pytest.raises(SystemExit) as error:
        Pgconsul.re_init_db(instance)
    assert error.value.code == 1
    instance.db.reconnect.assert_not_called()


def test_startup_checks_reject_cache_without_pgdata_before_rewind_check():
    instance = _make_instance()
    instance.config.quorum_commit = False
    instance.db.is_alive.return_value = False
    instance.db.get_prev_state.return_value = DbState.from_dict({'role': 'primary'})
    with pytest.raises(KeyError, match='pgdata'):
        instance.startup_checks()
    instance.db.is_ready_for_pg_rewind.assert_not_called()
