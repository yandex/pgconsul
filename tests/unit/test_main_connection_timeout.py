# encoding: utf-8
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.exceptions import PostgresConnectionTimeout
from src.main import Pgconsul


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
    instance.zk.get_state.return_value = {'alive': True}
    instance.zk.get_members.return_value = []
    return instance


def test_run_iteration_turns_timeout_into_dead_state_with_process_status():
    instance = _make_instance()
    timeout = PostgresConnectionTimeout(1)
    instance.db.is_alive_and_in_terminal_state.side_effect = timeout
    instance.db.is_postgresql_running.return_value = True
    instance.db.get_prev_state.return_value = {'role': 'primary', 'timeline': 7}

    with patch('src.main.helpers.write_status_file'):
        instance.run_iteration('100')

    instance._pg_conn_grace.record_failure.assert_called_once_with()
    instance.dead_iter.assert_called_once_with(
        {
            'alive': False,
            'running': True,
            'role': None,
            'prev_state': {'role': 'primary', 'timeline': 7},
            'connection_timed_out': True,
        },
        {'alive': True},
        is_in_terminal_state=True,
    )


def test_run_iteration_treats_failed_process_status_as_not_running():
    instance = _make_instance()
    instance.db.is_alive_and_in_terminal_state.side_effect = PostgresConnectionTimeout(1)
    instance.db.is_postgresql_running.side_effect = RuntimeError('status unavailable')
    instance.db.get_prev_state.return_value = {}

    with patch('src.main.helpers.write_status_file'):
        instance.run_iteration('100')

    db_state = instance.dead_iter.call_args.args[0]
    assert db_state['running'] is False


def test_successful_probe_resets_timeout_grace_period():
    instance = _make_instance()
    instance.db.is_alive_and_in_terminal_state.return_value = (False, True)
    instance.db.get_state.return_value = {'alive': False, 'role': None}

    with patch('src.main.helpers.write_status_file'):
        instance.run_iteration('100')

    instance._pg_conn_grace.reset.assert_called_once_with()
    instance._pg_conn_grace.record_failure.assert_not_called()


def test_dead_iter_has_no_side_effects_while_timeout_is_protected():
    instance = _make_instance()
    instance._pg_conn_grace.should_act.return_value = False

    result = Pgconsul.dead_iter(
        instance,
        {'alive': False, 'running': True, 'connection_timed_out': True},
        {'alive': True},
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
        {'alive': False, 'running': True, 'connection_timed_out': True},
        {'alive': True},
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
        {'alive': False, 'running': True, 'connection_timed_out': True},
        {'alive': True, 'timeline_info': 7},
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
        {'alive': False, 'running': True, 'connection_timed_out': True},
        {'alive': False},
        is_in_terminal_state=True,
    )

    assert result is None
    instance._pg_conn_grace.should_act.assert_not_called()
    instance.db.pgpooler.assert_not_called()
