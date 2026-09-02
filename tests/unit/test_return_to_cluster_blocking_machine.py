from unittest.mock import MagicMock, patch

from src.main import Pgconsul, ReturnTarget
from src.return_to_cluster.state import ReturnPhase, ReturnState
from src.types import DesiredPrimary


def _instance(state):
    instance = Pgconsul.__new__(Pgconsul)
    instance.db = MagicMock()
    instance.zk = MagicMock()
    instance.zk.get_desired_primary.return_value = (
        DesiredPrimary('primary-2', 'failover-1', 'failover'),
        1,
    )
    instance.zk.get_timeline.return_value = 2
    instance._return_state = MagicMock()
    instance._return_state.read.return_value = state
    instance.config = MagicMock(
        return_startup_stall_timeout=300.0,
        return_lsn_stall_timeout=60.0,
        primary_switch_checks=3,
        max_rewind_retries=2,
    )
    instance.checks = {'primary_switch': 0, 'rewind': 0}
    instance._active_return_target = None
    instance._acquire_replication_source_slot_lock = MagicMock()
    instance._run_return_action = MagicMock()
    instance._rewind_return_once = MagicMock(return_value=True)
    instance.is_rewind_flag_set = MagicMock(return_value=False)
    instance.set_rewind_flag = MagicMock()
    return instance


def test_return_request_is_persisted_without_touching_postgres():
    instance = _instance(None)
    instance._capture_return_target = MagicMock(
        return_value=ReturnTarget('primary-2', 'failover-4', 7),
    )

    instance._return_to_cluster('primary-2', 'replica', is_dead=True)

    written = instance._return_state.write.call_args.args[0]
    assert written == ReturnState(
        operation_id='failover-4',
        phase=ReturnPhase.REQUESTED,
        target_host='primary-2',
        target_timeline=7,
        role='replica',
        is_postgresql_dead=True,
        target_operation_id='failover-4',
    )
    instance.db.assert_not_called()
    assert instance.zk.release_if_hold.call_count == 2


def test_return_waits_until_failover_winner_is_materialized():
    instance = _instance(None)
    instance.zk.get_desired_primary.return_value = (
        DesiredPrimary(None, 'failover-4', 'failover'),
        2,
    )

    instance._return_to_cluster('primary-1', 'replica', is_dead=True)

    instance._return_state.write.assert_not_called()
    instance.zk.release_if_hold.assert_not_called()


def test_return_rejects_target_from_an_older_primary_epoch():
    instance = _instance(None)
    instance.zk.get_desired_primary.return_value = (
        DesiredPrimary('primary-2', 'failover-4', 'failover'),
        2,
    )

    instance._return_to_cluster('primary-1', 'replica', is_dead=True)

    instance._return_state.write.assert_not_called()
    instance.zk.release_if_hold.assert_not_called()


def test_blocked_return_state_does_not_claim_iteration():
    instance = _instance(ReturnState('switchover-1', ReturnPhase.BLOCKED))

    assert instance._run_return_to_cluster_machine({'alive': False}) is False


def test_rewind_flag_without_local_state_is_owned_by_return_machine():
    instance = _instance(None)
    instance.is_rewind_flag_set.return_value = True

    assert instance._run_return_to_cluster_machine({'alive': False}) is True

    instance._return_state.write.assert_not_called()


def test_rewind_flag_moves_active_return_to_resetup_required():
    state = ReturnState(
        'failover-1', ReturnPhase.REWINDING, 'primary-2', 2,
        progress_signature='old', progress_since=100.0,
    )
    instance = _instance(state)
    instance.is_rewind_flag_set.return_value = True

    assert instance._run_return_to_cluster_machine({'alive': False}) is True

    written = instance._return_state.write.call_args.args[0]
    assert written.phase == ReturnPhase.RESETUP_REQUIRED
    assert written.progress_signature is None
    assert written.progress_since is None


def test_resetup_required_waits_while_rewind_flag_exists():
    state = ReturnState(
        'failover-1', ReturnPhase.RESETUP_REQUIRED, 'primary-2', 2,
    )
    instance = _instance(state)
    instance.is_rewind_flag_set.return_value = True

    assert instance._run_return_to_cluster_machine({'alive': False}) is True

    instance._return_state.write.assert_not_called()


def test_starting_progress_refreshes_stall_deadline():
    state = ReturnState(
        'failover-1', ReturnPhase.STARTING, 'primary-2', 2,
        progress_signature='old', progress_since=100.0,
    )
    instance = _instance(state)
    instance.db.get_startup_progress_signature.return_value = ('wal', 42)

    with patch('src.main.time.time', return_value=200.0):
        assert instance._run_return_to_cluster_machine({
            'alive': False,
            'running': True,
            'role': None,
        }) is True

    written = instance._return_state.write.call_args.args[0]
    assert written.progress_signature == '["wal", 42]'
    assert written.progress_since == 200.0
    instance.set_rewind_flag.assert_not_called()


def test_starting_without_progress_requires_resetup_after_five_minutes():
    state = ReturnState(
        'failover-1', ReturnPhase.STARTING, 'primary-2', 2,
        progress_signature='same', progress_since=100.0,
    )
    instance = _instance(state)
    instance.db.get_startup_progress_signature.return_value = 'same'

    with patch('src.main.time.time', return_value=401.0):
        assert instance._run_return_to_cluster_machine({
            'alive': False,
            'running': True,
            'role': None,
        }) is True

    instance.set_rewind_flag.assert_called_once_with()
    written = instance._return_state.write.call_args.args[0]
    assert written.phase == ReturnPhase.RESETUP_REQUIRED


def test_removing_rewind_flag_restarts_return_from_requested():
    state = ReturnState(
        'failover-1', ReturnPhase.RESETUP_REQUIRED,
        'primary-2', 2, role='replica',
        start_attempts=3, rewind_attempts=2,
    )
    instance = _instance(state)
    instance.is_rewind_flag_set.return_value = False

    assert instance._run_return_to_cluster_machine({
        'alive': False,
        'running': False,
        'role': None,
    }) is True

    written = instance._return_state.write.call_args.args[0]
    assert written.phase == ReturnPhase.REQUESTED
    assert written.start_attempts == 0
    assert written.rewind_attempts == 0


def test_changed_primary_goes_directly_to_rewind_when_postgres_is_down():
    state = ReturnState(
        'failover-1', ReturnPhase.REQUESTED,
        'primary-2', 2, role='replica', is_postgresql_dead=True,
    )
    instance = _instance(state)
    instance.db.get_prev_state.return_value = {
        'role': 'replica',
        'primary_fqdn': 'primary-1',
    }

    assert instance._run_return_to_cluster_machine({
        'alive': False,
        'running': False,
        'role': None,
    }) is True

    rewind_state = instance._rewind_return_once.call_args.args[0]
    assert rewind_state.phase == ReturnPhase.REWINDING


def test_rewind_is_refused_while_postgres_is_still_alive():
    state = ReturnState(
        'failover-1', ReturnPhase.REWINDING, 'primary-2', 2,
    )
    instance = _instance(state)
    instance.db.is_host_unreachable.return_value = False
    instance.zk.write_host_op.return_value = True
    instance.db.is_alive.return_value = True
    instance.stop_postgresql = MagicMock(return_value=0)
    instance.db.is_alive_and_in_terminal_state.return_value = (True, True)

    assert Pgconsul._rewind_return_once(instance, state) is None

    instance.db.do_rewind.assert_not_called()


def test_rewind_is_refused_while_postgres_is_starting_or_shutting_down():
    state = ReturnState(
        'failover-1', ReturnPhase.REWINDING, 'primary-2', 2,
    )
    instance = _instance(state)
    instance.db.is_host_unreachable.return_value = False
    instance.zk.write_host_op.return_value = True
    instance.db.is_alive.return_value = False
    instance.db.is_alive_and_in_terminal_state.return_value = (False, False)

    assert Pgconsul._rewind_return_once(instance, state) is None

    instance.db.do_rewind.assert_not_called()


def test_rewind_is_refused_while_postgres_process_is_still_running():
    state = ReturnState(
        'failover-1', ReturnPhase.REWINDING, 'primary-2', 2,
    )
    instance = _instance(state)
    instance.db.is_host_unreachable.return_value = False
    instance.zk.write_host_op.return_value = True
    instance.db.is_alive.return_value = False
    instance.db.is_alive_and_in_terminal_state.return_value = (False, True)
    instance.db.get_postgresql_status.return_value = 0

    assert Pgconsul._rewind_return_once(instance, state) is None

    instance.db.do_rewind.assert_not_called()


def test_rewind_runs_after_postgres_is_confirmed_stopped():
    state = ReturnState(
        'failover-1', ReturnPhase.REWINDING, 'primary-2', 2,
    )
    instance = _instance(state)
    instance.db.is_host_unreachable.return_value = False
    instance.zk.write_host_op.return_value = True
    instance.db.is_alive.return_value = False
    instance.db.is_alive_and_in_terminal_state.return_value = (False, True)
    instance.db.get_postgresql_status.return_value = 1
    instance.db.resume_restoring_wal_stopped.return_value = True
    instance.db.do_rewind.return_value = 0
    instance._start_return_postgresql = MagicMock()

    assert Pgconsul._rewind_return_once(instance, state) is True

    instance.db.do_rewind.assert_called_once_with('primary-2')
