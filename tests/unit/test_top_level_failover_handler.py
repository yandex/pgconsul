from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from src.failover import FailoverPhase, FailoverRequest
from src.main import Pgconsul
from src.types import DurabilityConfig
from src.zk import ZookeeperException


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
    inst._return_state = MagicMock()
    inst._return_state.read.return_value = None
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
    inst.zk.ELECTION_VOTES_PATH = 'election_vote'
    inst.zk.ELECTION_WINNER_PATH = 'election_winner'
    inst.zk.FAILOVER_PARTICIPANTS_PATH = 'failover_participant'
    inst.zk.LAST_PRIMARY_PATH = 'last_leader'
    inst.zk.DESIRED_PRIMARY_PATH = 'desired_primary'
    inst.zk.FAILOVER_PROBE_PATH = 'failover_probe'
    inst.zk.FAILOVER_REQUEST_PATH = 'failover_request'
    inst.zk.LAST_FAILOVER_TIME_PATH = 'last_failover_time'
    inst.zk.ELECTION_ENTER_LOCK_PATH = 'epoch_enter'
    inst.zk.PRIMARY_LOCK_PATH = 'leader'
    inst.zk.get_desired_primary.return_value = (None, None)
    inst.zk.write_desired_primary.return_value = 0
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
        'desired_primary': None,
        'failover_probe': None,
        'failover_request': None,
        'last_failover_time': None,
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


def test_write_iteration_state_propagates_zk_write_failure():
    inst = _make_instance()
    inst.zk.write_ssn_on_changes.return_value = False

    with pytest.raises(ZookeeperException):
        inst.write_iteration_state(
            {'replication_state': ('sync', 'ANY 1(host1)')},
            'primary',
            '100',
        )

    inst.zk.get_members.assert_not_called()


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


def test_run_iteration_does_not_dispatch_role_logic_when_switchover_claims_it():
    """Keeps the operation-ownership invariant of the removed role tests."""
    inst = _make_instance()
    inst.notifier = MagicMock()
    inst.is_rewind_flag_set = MagicMock(return_value=False)
    inst.db.is_alive_and_in_terminal_state.return_value = (True, True)
    db_state = {'role': 'primary', 'replication_state': None}
    inst.db.get_state.return_value = db_state
    zk_state = _zk_state()
    inst.zk.get_state.return_value = zk_state
    inst.handle_failover = MagicMock(return_value=False)
    inst.handle_switchover = MagicMock(return_value=True)
    inst._start_failover = MagicMock()
    inst.write_iteration_state = MagicMock()
    inst._zk_alive_refresh = MagicMock()
    inst.primary_iter = MagicMock()
    inst.single_node_primary_iter = MagicMock()
    inst.re_init_db = MagicMock()
    inst.zk.get_members.return_value = []
    inst.finish_iteration = MagicMock()

    with patch('src.main.helpers.write_status_file'):
        inst.run_iteration('100')

    inst.handle_switchover.assert_called_once_with(db_state, zk_state)
    inst._start_failover.assert_not_called()
    inst.primary_iter.assert_not_called()
    inst.single_node_primary_iter.assert_not_called()


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


def test_failover_winner_blocks_generic_return_before_promotion():
    inst = _make_instance()
    observation = SimpleNamespace(
        election_winner='winner',
        failover_version='failover-7',
    )
    inst._build_failover_observation = MagicMock(return_value=observation)
    inst._executor = MagicMock()
    inst._failover_machine = MagicMock()
    inst.zk.get_current_lock_holder.return_value = 'coordinator'
    inst._return_state.read.return_value = None

    with patch('src.main.helpers.get_hostname', return_value='winner'):
        Pgconsul._run_failover_step(
            inst,
            FailoverPhase.WINNER_SELECTED,
            {'role': 'replica'},
            _zk_state(failover_state=FailoverPhase.WINNER_SELECTED),
            must_reset=False,
        )

    written = inst._return_state.write.call_args.args[0]
    assert written.operation_id == 'failover-7'
    assert written.phase.value == 'blocked'


def test_initialize_failover_commits_first_phase():
    inst = _make_instance()
    inst._try_acquire_failover_coordinator = MagicMock(return_value=True)
    observation = MagicMock()
    observation.durability = DurabilityConfig.build(['old-primary', 'host1', 'host2'])
    observation.durability_quorums = (observation.durability,)
    inst._build_failover_observation = MagicMock(return_value=observation)
    inst._failover_machine = MagicMock()
    inst._failover_machine.can_start.return_value = True
    inst.zk.get_current_lock_holder.return_value = None
    inst.zk.write_failover_state.return_value = True
    inst.zk.delete.return_value = True
    inst.zk.write_failover_members.return_value = True
    inst.zk.write_failover_version.return_value = True
    inst.zk.is_lock_holder.return_value = True
    db_state = {'role': 'replica', 'timeline': 1, 'primary_fqdn': 'old-primary'}
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
    inst.zk.write_failover_members.assert_called_once_with(['host1', 'host2'])
    version = inst.zk.write_failover_version.call_args.args[0]
    calls = inst.zk.method_calls
    assert calls.index(call.write_failover_members(['host1', 'host2'])) \
        < calls.index(call.write_failover_version(version)) \
        < calls.index(call.write_failover_state(FailoverPhase.WALRECEIVER_DISABLING))


def test_committed_handoff_starts_fence_failover_despite_old_local_timeline():
    inst = _make_instance()
    inst._try_acquire_failover_coordinator = MagicMock(return_value=True)
    observation = MagicMock()
    observation.durability = DurabilityConfig.build(['old-primary', 'host1', 'candidate'])
    observation.durability_quorums = (observation.durability,)
    observation.branch_source_durability = DurabilityConfig.build(
        ['old-primary', 'candidate'],
    )
    observation.branch_target_durability = observation.durability
    inst._build_failover_observation = MagicMock(return_value=observation)
    inst._failover_machine = MagicMock()
    inst.zk.get_current_lock_holder.return_value = None
    inst.zk.write_failover_state.return_value = True
    inst.zk.delete.return_value = True
    inst.zk.write_failover_members.return_value = True
    inst.zk.write_failover_version.return_value = True
    inst.zk.is_lock_holder.return_value = True
    db_state = {'role': 'replica', 'timeline': 1, 'primary_fqdn': 'old-primary'}
    zk_state = _zk_state(lock_holder=None)
    zk_state['timeline_info'] = 2
    zk_state['switchover_record'] = {
        'hostname': 'old-primary', 'candidate': 'candidate',
        'phase': 'handoff_committed', 'timeline': 1, 'expected_timeline': 2,
        'original_durability_members': ['old-primary', 'host1', 'candidate'],
        'operation_id': 'operation',
    }
    zk_state['switchover_version'] = 4

    assert Pgconsul._initialize_failover(inst, db_state, zk_state, automatic=True) is True

    inst._failover_machine.can_start.assert_not_called()
    call_kwargs = inst._build_failover_observation.call_args.kwargs
    assert call_kwargs['automatic'] is True
    assert call_kwargs['fence_mismatched_timelines'] is True
    assert call_kwargs['branch_record'].operation_id == 'operation'
    inst.zk.write_failover_state.assert_called_once_with(FailoverPhase.WALRECEIVER_DISABLING)


def test_active_failover_preempts_committed_handoff_candidate_promotion():
    inst = _make_instance()
    inst._run_switchover_candidate = MagicMock(return_value=True)
    inst.zk.TIMELINE_INFO_PATH = 'timeline_info'
    db_state = {'role': 'replica', 'timeline': 1}
    zk_state = _zk_state(failover_state=FailoverPhase.WALRECEIVER_DISABLING, lock_holder=None)
    zk_state['timeline_info'] = 2
    zk_state['switchover_record'] = {
        'hostname': 'old-primary', 'candidate': 'candidate',
        'phase': 'handoff_committed', 'expected_timeline': 2,
    }
    zk_state['switchover_version'] = 4

    with patch('src.main.helpers.get_hostname', return_value='candidate'):
        assert inst.handle_failover(db_state, zk_state) is True

    inst._run_switchover_candidate.assert_not_called()
    inst._run_failover_step.assert_called_once_with(
        FailoverPhase.WALRECEIVER_DISABLING,
        db_state,
        zk_state,
        must_reset=False,
    )


def test_old_primary_votes_in_active_handoff_failover():
    inst = _make_instance()
    inst._run_switchover_primary = MagicMock(return_value=True)
    db_state = {'role': None, 'timeline': 1}
    zk_state = _zk_state(failover_state=FailoverPhase.WALRECEIVER_DISABLING, lock_holder='candidate')
    zk_state['timeline_info'] = 2
    zk_state['switchover_record'] = {
        'hostname': 'old-primary', 'candidate': 'candidate',
        'phase': 'handoff_committed', 'expected_timeline': 2,
    }
    zk_state['switchover_version'] = 4

    with patch('src.main.helpers.get_hostname', return_value='old-primary'):
        assert inst.handle_failover(db_state, zk_state) is True

    inst._run_switchover_primary.assert_not_called()
    inst._run_failover_step.assert_called_once()


def test_fallback_initialization_rejects_cascading_replica_before_coordinator_lock():
    inst = _make_instance()
    inst.config.stream_from = 'upstream'
    inst._try_acquire_failover_coordinator = MagicMock()

    result = Pgconsul._initialize_failover(
        inst,
        {'role': 'replica', 'timeline': 1},
        _zk_state(lock_holder=None),
        automatic=False,
    )

    assert result is False
    inst._try_acquire_failover_coordinator.assert_not_called()


def test_switchover_fallback_does_not_trust_stale_single_node_marker():
    """dead_primary_switchover.feature:53 regression."""
    inst = _make_instance()
    inst._is_single_node = True
    inst._try_acquire_failover_coordinator = MagicMock(return_value=False)

    result = Pgconsul._initialize_failover(
        inst,
        {'role': 'replica', 'timeline': 1},
        _zk_state(lock_holder=None),
        automatic=False,
    )

    assert result is False
    inst._try_acquire_failover_coordinator.assert_called_once_with()


def test_active_failover_does_not_stop_on_stale_single_node_marker():
    inst = _make_instance()
    inst._is_single_node = True
    zk_state = _zk_state(
        failover_state=FailoverPhase.WALRECEIVER_DISABLING,
        lock_holder=None,
    )

    assert inst.handle_failover({'role': 'replica'}, zk_state) is True

    inst._run_failover_step.assert_called_once()


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


def test_operator_request_starts_failover_without_health_probe():
    inst = _make_instance()
    inst._start_requested_failover = MagicMock(return_value=True)
    request = FailoverRequest('primary', 'operation-1', True)
    zk_state = _zk_state(lock_holder='primary')
    zk_state['failover_request'] = request.to_dict()

    assert Pgconsul._start_failover(
        inst, {'role': 'replica'}, zk_state,
    ) is True

    inst._start_requested_failover.assert_called_once_with(
        request, {'role': 'replica'}, zk_state,
    )


def test_operator_request_initializes_while_old_primary_holds_lock():
    inst = _make_instance()
    inst._try_acquire_failover_coordinator = MagicMock(return_value=True)
    observation = MagicMock()
    observation.durability = DurabilityConfig.build(
        ['old-primary', 'host1', 'host2'],
    )
    observation.durability_quorums = (observation.durability,)
    inst._build_failover_observation = MagicMock(return_value=observation)
    inst._failover_machine = MagicMock()
    inst._failover_machine.can_start.return_value = True
    inst.zk.get_current_lock_holder.return_value = 'old-primary'
    inst.zk.write_failover_state.return_value = True
    inst.zk.delete.return_value = True
    inst.zk.write_failover_members.return_value = True
    inst.zk.write_failover_version.return_value = True
    inst.zk.is_lock_holder.return_value = True
    request = FailoverRequest('old-primary', 'operation-1', True)

    assert Pgconsul._initialize_failover(
        inst,
        {'role': 'replica', 'timeline': 1},
        _zk_state(lock_holder='old-primary'),
        automatic=False,
        failed_primary='old-primary',
        manual_request=request,
    ) is True

    inst.zk.write_failover_version.assert_called_once_with('operation-1')
