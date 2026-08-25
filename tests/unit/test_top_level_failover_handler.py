from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.failover import FailoverPhase
from src.main import Pgconsul


def _make_instance():
    inst = Pgconsul.__new__(Pgconsul)
    inst.db = MagicMock()
    inst.zk = MagicMock()
    inst.config = SimpleNamespace(
        stream_from=None,
        autofailover=True,
        priority='100',
        iteration_timeout=0,
        working_dir='/tmp',
    )
    inst._maintenance = MagicMock()
    inst._maintenance.is_in_maintenance = False
    inst._is_single_node = False
    inst._master_lost_ts = None
    inst._run_failover_step = MagicMock()
    inst._start_failover = MagicMock()
    inst.zk.FAILOVER_STATE_PATH = 'failover_state'
    inst.zk.FAILOVER_MUST_BE_RESET = 'failover_must_be_reset'
    inst.zk.SWITCHOVER_RECORD_PATH = 'switchover_record'
    inst.zk.SWITCHOVER_VERSION_KEY = 'switchover_version'
    inst.zk.TIMELINE_INFO_PATH = 'timeline_info'
    inst.zk.ELECTION_MANAGER_LOCK_PATH = 'epoch_manager'
    inst.zk.ELECTION_ENTER_LOCK_PATH = 'epoch_enter'
    inst.zk.PRIMARY_LOCK_PATH = 'leader'
    return inst


def _zk_state(*, failover_state=None, lock_holder='primary'):
    return {
        'failover_state': failover_state,
        'failover_must_be_reset': False,
        'lock_holder': lock_holder,
        'switchover_state': None,
        'switchover_root': None,
        'switchover_side_replicas': None,
        'switchover_candidate': None,
        'timeline_info': 1,
    }


@pytest.mark.parametrize('phase', list(FailoverPhase))
def test_every_failover_phase_claims_iteration(phase):
    inst = _make_instance()
    db_state = {'role': 'replica', 'timeline': 1}
    zk_state = _zk_state(failover_state=phase)

    assert inst.handle_failover(db_state, zk_state) is True

    inst._run_failover_step.assert_called_once_with(
        phase,
        db_state,
        zk_state,
        must_reset=False,
    )


def test_no_failover_does_not_claim_healthy_iteration():
    inst = _make_instance()

    assert inst.handle_failover(
        {'role': 'replica', 'timeline': 1},
        _zk_state(),
    ) is False

    inst._run_failover_step.assert_not_called()
    inst._start_failover.assert_not_called()


def test_missing_primary_does_not_make_active_handler_claim_iteration():
    inst = _make_instance()
    db_state = {'role': 'replica', 'timeline': 1}
    zk_state = _zk_state(lock_holder=None)

    assert inst.handle_failover(db_state, zk_state) is False
    inst._run_failover_step.assert_not_called()


def test_autofailover_disabled_does_not_claim_missing_primary_iteration():
    inst = _make_instance()
    inst.config.autofailover = False

    assert inst.handle_failover(
        {'role': 'replica', 'timeline': 1},
        _zk_state(lock_holder=None),
    ) is False


def test_run_iteration_maintenance_blocks_active_failover():
    inst = _make_instance()
    inst.notifier = MagicMock()
    inst.is_rewind_flag_set = MagicMock(return_value=False)
    inst.db.is_alive_and_in_terminal_state.return_value = (True, True)
    db_state = {'role': 'replica', 'replication_state': None}
    inst.db.get_state.return_value = db_state
    inst.zk.get_state.return_value = _zk_state(failover_state=FailoverPhase.VOTING)
    inst._maintenance.is_in_maintenance = True
    inst.handle_failover = MagicMock()
    inst.write_iteration_state = MagicMock()
    inst._zk_alive_refresh = MagicMock()
    inst.replica_iter = MagicMock()
    inst.finish_iteration = MagicMock()

    with patch('src.main.helpers.write_status_file'):
        inst.run_iteration('100')

    inst.write_iteration_state.assert_called_once_with(db_state, 'replica', '100')
    inst.handle_failover.assert_not_called()
    inst.replica_iter.assert_not_called()
    inst.finish_iteration.assert_called_once()


def test_write_iteration_state_updates_ssn_maintenance_and_priority():
    inst = _make_instance()
    inst._maintenance.is_in_maintenance = True
    inst.zk.get_members.return_value = ['host1']
    inst.zk.get_host_prio.return_value = None

    inst.write_iteration_state(
        {'replication_state': ('sync', 'ANY 1(host1)')},
        'replica',
        '100',
    )

    inst.zk.write_ssn_on_changes.assert_called_once_with('ANY 1(host1)')
    inst.zk.write_host_maintenance_enabled.assert_called_once_with()
    inst.zk.write_host_prio.assert_called_once_with('100')


def test_run_iteration_does_not_dispatch_role_logic_when_failover_claims_it():
    inst = _make_instance()
    inst.notifier = MagicMock()
    inst.is_rewind_flag_set = MagicMock(return_value=False)
    inst.db.is_alive_and_in_terminal_state.return_value = (True, True)
    db_state = {'role': 'primary', 'replication_state': None}
    inst.db.get_state.return_value = db_state
    zk_state = _zk_state(failover_state=FailoverPhase.VOTING)
    inst.zk.get_state.return_value = zk_state
    inst.handle_failover = MagicMock(return_value=True)
    inst.write_iteration_state = MagicMock()
    inst._zk_alive_refresh = MagicMock()
    inst.primary_iter = MagicMock()
    inst.single_node_primary_iter = MagicMock()
    inst.re_init_db = MagicMock()
    inst.zk.get_members.return_value = []
    inst.finish_iteration = MagicMock()

    with patch('src.main.helpers.write_status_file'):
        inst.run_iteration('100')

    inst.handle_failover.assert_called_once_with(db_state, zk_state)
    inst.primary_iter.assert_not_called()
    inst.single_node_primary_iter.assert_not_called()
    inst.write_iteration_state.assert_called_once_with(db_state, 'primary', '100')
    inst.re_init_db.assert_called_once_with()
    inst.finish_iteration.assert_called_once()


def test_reset_marker_is_dispatched_through_failover_machine():
    inst = _make_instance()
    db_state = {'role': 'replica', 'timeline': 1}
    zk_state = _zk_state()
    zk_state['failover_must_be_reset'] = True

    assert inst.handle_failover(db_state, zk_state) is True

    inst._run_failover_step.assert_called_once_with(
        None,
        db_state,
        zk_state,
        must_reset=True,
    )


def test_invalid_failover_phase_requests_reset_without_mutating_snapshot():
    inst = _make_instance()
    db_state = {'role': 'replica', 'timeline': 1}
    zk_state = _zk_state(failover_state='broken')
    original_state = zk_state.copy()

    assert inst.handle_failover(db_state, zk_state) is True

    assert zk_state == original_state
    inst._run_failover_step.assert_called_once_with(
        None,
        db_state,
        zk_state,
        must_reset=True,
    )


def test_run_failover_step_routes_reset_marker_to_coordinator_machine():
    inst = _make_instance()
    inst._try_acquire_failover_coordinator = MagicMock(return_value=True)
    observation = SimpleNamespace(must_reset=True, phase=None)
    inst._build_failover_observation = MagicMock(return_value=observation)
    inst._executor = MagicMock()
    inst._failover_machine = MagicMock()
    inst.zk.get_current_lock_holder.return_value = None
    db_state = {'role': 'replica', 'timeline': 1}
    zk_state = _zk_state()
    zk_state['failover_must_be_reset'] = True

    Pgconsul._run_failover_step(
        inst,
        None,
        db_state,
        zk_state,
        must_reset=True,
    )

    inst._try_acquire_failover_coordinator.assert_called_once_with()
    inst._build_failover_observation.assert_called_once_with(
        None,
        db_state,
        must_reset=True,
    )
    inst._executor.run.assert_called_once_with(
        inst._failover_machine,
        observation,
    )


def test_initialize_failover_commits_first_phase():
    inst = _make_instance()
    inst._try_acquire_failover_coordinator = MagicMock(return_value=True)
    observation = MagicMock()
    inst._build_failover_observation = MagicMock(return_value=observation)
    inst._failover_machine = MagicMock()
    inst._failover_machine.can_start.return_value = True
    inst.zk.get_current_lock_holder.return_value = None
    inst.zk.write_failover_state.return_value = True
    db_state = {'role': 'replica', 'timeline': 1}
    zk_state = _zk_state(lock_holder=None)

    result = Pgconsul._initialize_failover(
        inst,
        db_state,
        zk_state,
        automatic=True,
    )

    assert result is True
    assert zk_state['failover_state'] == FailoverPhase.WALRECEIVER_DISABLING
    inst._build_failover_observation.assert_called_once_with(
        None,
        db_state,
        automatic=True,
    )
    inst.zk.write_failover_state.assert_called_once_with(FailoverPhase.WALRECEIVER_DISABLING)


@pytest.mark.parametrize(
    ('stream_from', 'single_node'),
    [('upstream', False), (None, True)],
)
def test_fallback_initialization_rejects_ineligible_host_before_coordinator_lock(
    stream_from,
    single_node,
):
    inst = _make_instance()
    inst.config.stream_from = stream_from
    inst._is_single_node = single_node
    inst._try_acquire_failover_coordinator = MagicMock()

    result = Pgconsul._initialize_failover(
        inst,
        {'role': 'replica', 'timeline': 1},
        _zk_state(lock_holder=None),
        automatic=False,
    )

    assert result is False
    inst._try_acquire_failover_coordinator.assert_not_called()


def test_initialize_failover_rechecks_primary_lock():
    inst = _make_instance()
    inst._try_acquire_failover_coordinator = MagicMock(return_value=True)
    inst.zk.get_current_lock_holder.return_value = 'primary'

    result = Pgconsul._initialize_failover(
        inst,
        {'role': 'replica', 'timeline': 1},
        _zk_state(lock_holder=None),
        automatic=True,
    )

    assert result is False
    inst.zk.release_lock.assert_called_once_with('epoch_manager')
    inst.zk.write_failover_state.assert_not_called()
