from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from src.failover import FailoverPhase, FailoverRequest
from src.main import Pgconsul
from src.types import DurabilityConfig, DurabilityState
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
    inst._failover_observation = MagicMock()
    inst._prepare_failover_observation = MagicMock(
        return_value=inst._failover_observation,
    )
    inst._run_failover_coordinator = MagicMock()
    inst._run_failover_participant = MagicMock()
    inst._start_failover = MagicMock()
    inst._run_durability_reconciliation = MagicMock()
    inst.zk.FAILOVER_STATE_PATH = 'failover_state'
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
    inst.zk.get_durability_state.return_value = (DurabilityState(None), None)
    inst.zk.write_desired_primary.return_value = 0
    return inst


def _zk_state(*, failover_state=None, lock_holder='primary'):
    return {
        'failover_state': failover_state,
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

    inst._prepare_failover_observation.assert_called_once_with(
        phase,
        db_state,
        zk_state,
    )
    inst._run_failover_coordinator.assert_called_once_with(
        inst._failover_observation,
    )
    inst._run_failover_participant.assert_called_once_with(
        inst._failover_observation, db_state,
    )


def test_no_failover_does_not_claim_healthy_iteration():
    inst = _make_instance()

    assert inst.handle_failover(
        {'role': 'replica', 'timeline': 1},
        _zk_state(),
    ) is False

    inst._prepare_failover_observation.assert_not_called()
    inst._start_failover.assert_not_called()


def test_orphaned_failover_coordinator_lock_is_released_after_cleanup():
    inst = _make_instance()
    inst.zk.get_failover_state.return_value = None
    inst.zk.is_lock_holder.return_value = True
    inst.zk.delete_unmaterialized_failover_desired_primary.return_value = True
    inst.zk.release_lock.return_value = True

    assert inst._release_orphaned_failover_coordinator_lock() is True

    inst.zk.delete_unmaterialized_failover_desired_primary.assert_called_once_with()
    inst.zk.release_lock.assert_called_once_with('epoch_manager')


def test_missing_primary_does_not_make_active_handler_claim_iteration():
    inst = _make_instance()
    db_state = {'role': 'replica', 'timeline': 1}
    zk_state = _zk_state(lock_holder=None)

    assert inst.handle_failover(db_state, zk_state) is False
    inst._prepare_failover_observation.assert_not_called()


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

    inst._maintenance.update_status.assert_called_once_with(
        db_state, inst.zk.get_state.return_value, inst._is_single_node,
    )
    inst._run_durability_reconciliation.assert_called_once_with(
        db_state, inst.zk.get_state.return_value,
    )
    inst.zk.write_host_maintenance_enabled.assert_called_once_with()
    inst.write_iteration_state.assert_not_called()
    inst._zk_alive_refresh.assert_not_called()
    inst.handle_failover.assert_not_called()
    inst.replica_iter.assert_not_called()
    inst.finish_iteration.assert_called_once()


def test_write_iteration_state_updates_ssn_and_priority():
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
    inst.zk.write_host_maintenance_enabled.assert_not_called()
    inst.zk.write_host_prio.assert_called_once_with('100')


def test_zk_write_failure_after_snapshot_only_reinitializes_zookeeper():
    inst = _make_instance()
    inst.notifier = MagicMock()
    inst.db.is_alive_and_in_terminal_state.return_value = (True, True)
    db_state = {'role': 'replica', 'replication_state': None}
    inst.db.get_state.return_value = db_state
    inst.zk.get_state.return_value = _zk_state()
    inst._zk_alive_refresh = MagicMock()
    inst.write_iteration_state = MagicMock(
        side_effect=ZookeeperException('write failed'),
    )
    inst.finish_iteration = MagicMock()

    with patch('src.main.helpers.write_status_file'):
        inst.run_iteration('100')

    inst.zk.re_init.assert_called_once_with()
    inst.finish_iteration.assert_called_once()


def test_initial_zk_snapshot_failure_fences_primary_from_local_state():
    inst = _make_instance()
    inst.notifier = MagicMock()
    inst.db.is_alive_and_in_terminal_state.return_value = (True, True)
    db_state = {'role': 'primary', 'replication_state': None}
    inst.db.get_state.return_value = db_state
    inst.zk.get_state.side_effect = ZookeeperException('snapshot failed')
    inst._zk_fail_timestamp = None
    inst._should_close_on_zk_loss = MagicMock(return_value=True)
    inst.resolve_zk_primary_lock = MagicMock()
    inst.finish_iteration = MagicMock()

    inst.run_iteration('100')

    inst.db.pgpooler.assert_called_once_with('stop')
    inst.db.stop_archiving_wal.assert_called_once_with()
    inst.resolve_zk_primary_lock.assert_not_called()
    inst.zk.re_init.assert_called_once_with()
    inst.finish_iteration.assert_called_once()


def test_initial_zk_snapshot_failure_keeps_replica_detach_policy():
    inst = _make_instance()
    inst.notifier = MagicMock()
    inst.db.is_alive_and_in_terminal_state.return_value = (True, True)
    db_state = {'role': 'replica', 'replication_state': None}
    inst.db.get_state.return_value = db_state
    inst.zk.get_state.side_effect = ZookeeperException('snapshot failed')
    inst.handle_detached_replica = MagicMock()
    inst.finish_iteration = MagicMock()

    inst.run_iteration('100')

    inst.handle_detached_replica.assert_called_once_with(db_state)
    inst.zk.re_init.assert_called_once_with()
    inst.finish_iteration.assert_called_once()


def test_maintenance_marker_failure_reinitializes_and_finishes_iteration():
    inst = _make_instance()
    inst.notifier = MagicMock()
    inst.db.is_alive_and_in_terminal_state.return_value = (True, True)
    db_state = {'role': 'primary', 'replication_state': None}
    inst.db.get_state.return_value = db_state
    inst.zk.get_state.return_value = _zk_state()
    inst._maintenance.is_in_maintenance = True
    inst.zk.write_host_maintenance_enabled.side_effect = ZookeeperException(
        'maintenance marker failed',
    )
    inst.resolve_zk_primary_lock = MagicMock()
    inst.finish_iteration = MagicMock()

    with patch('src.main.helpers.write_status_file'):
        inst.run_iteration('100')

    inst.resolve_zk_primary_lock.assert_not_called()
    inst.zk.re_init.assert_called_once_with()
    inst.finish_iteration.assert_called_once()


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


def test_non_ha_primary_is_removed_from_ha_members_before_health_checks():
    inst = _make_instance()
    inst.config.stream_from = 'upstream'
    inst.zk.get_members.return_value = []

    with patch('src.main.helpers.get_hostname', return_value='primary'):
        inst.write_iteration_state({'replication_state': None}, 'primary', '100')

    inst.zk.delete_host_ha.assert_called_once_with('primary')


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
    events = []
    inst.handle_failover = MagicMock(return_value=False)
    inst.handle_switchover = MagicMock(
        side_effect=lambda *_: events.append('switchover') or True,
    )
    inst._start_failover = MagicMock(
        side_effect=lambda *_: events.append('failover-probe') or False,
    )
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
    inst._start_failover.assert_called_once_with(db_state, zk_state)
    assert events == ['failover-probe', 'switchover']
    inst.primary_iter.assert_not_called()
    inst.single_node_primary_iter.assert_not_called()


def test_invalid_failover_phase_resolves_winner_without_mutating_snapshot():
    inst = _make_instance()
    db_state = {'role': 'replica', 'timeline': 1}
    zk_state = _zk_state(failover_state='broken')
    original_state = zk_state.copy()

    assert inst.handle_failover(db_state, zk_state) is True

    assert zk_state == original_state
    inst._prepare_failover_observation.assert_called_once_with(
        FailoverPhase.RESOLVING_WINNER,
        db_state,
        zk_state,
    )


def test_prepare_failover_observation_recovers_coordinator_for_invalid_phase():
    inst = _make_instance()
    inst._try_acquire_failover_coordinator = MagicMock(return_value=True)
    observation = SimpleNamespace(phase=FailoverPhase.RESOLVING_WINNER)
    inst._build_failover_observation = MagicMock(return_value=observation)
    inst.zk.get_current_lock_holder.return_value = None
    db_state = {'role': 'replica', 'timeline': 1}
    zk_state = _zk_state(failover_state='broken')

    result = Pgconsul._prepare_failover_observation(
        inst,
        FailoverPhase.RESOLVING_WINNER,
        db_state,
        zk_state,
    )

    inst._try_acquire_failover_coordinator.assert_called_once_with()
    inst._build_failover_observation.assert_called_once_with(
        FailoverPhase.RESOLVING_WINNER,
        db_state,
    )
    assert result is observation


def test_failover_winner_blocks_generic_return_before_promotion():
    inst = _make_instance()
    observation = SimpleNamespace(
        election_winner='winner',
        failover_version='failover-7',
    )
    inst._build_failover_observation = MagicMock(return_value=observation)
    inst._executor = MagicMock()
    inst._failover_participant = MagicMock()
    inst.zk.get_current_lock_holder.return_value = 'coordinator'
    inst._return_state.read.return_value = None

    with patch('src.main.helpers.get_hostname', return_value='winner'):
        Pgconsul._run_failover_participant(inst, observation, {'role': 'replica'})

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
    inst.zk.get_current_lock_holder.return_value = None
    inst.zk.write_failover_state.return_value = True
    inst.zk.delete.return_value = True
    inst.zk.write_failover_version.return_value = True
    inst.zk.fence_durability_state_for_failover.return_value = True
    inst.zk.get_durability_state.return_value = (
        DurabilityState(observation.durability), 7,
    )
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
    assert zk_state['failover_state'] == FailoverPhase.VOTING
    inst._build_failover_observation.assert_called_once_with(
        None,
        db_state,
        automatic=True,
    )
    inst.zk.write_failover_state.assert_called_once_with(FailoverPhase.VOTING)
    inst.zk.write_failover_members.assert_not_called()
    inst.zk.fence_durability_state_for_failover.assert_called_once_with(
        DurabilityState(observation.durability), 7,
    )
    version = inst.zk.write_failover_version.call_args.args[0]
    calls = inst.zk.method_calls
    assert calls.index(call.fence_durability_state_for_failover(
        DurabilityState(observation.durability), 7,
    )) < calls.index(call.write_failover_version(version)) \
        < calls.index(call.write_failover_state(FailoverPhase.VOTING))


def test_initialize_failover_aborts_when_durability_fence_cas_loses_race():
    inst = _make_instance()
    inst._try_acquire_failover_coordinator = MagicMock(return_value=True)
    observation = MagicMock()
    observation.durability = DurabilityConfig.build(['old-primary', 'host1'])
    observation.durability_quorums = (observation.durability,)
    inst._build_failover_observation = MagicMock(return_value=observation)
    inst.zk.get_current_lock_holder.return_value = None
    inst.zk.is_lock_holder.return_value = True
    inst.zk.get_durability_state.return_value = (
        DurabilityState(observation.durability), 7,
    )
    inst.zk.fence_durability_state_for_failover.return_value = False

    assert Pgconsul._initialize_failover(
        inst,
        {'role': 'replica', 'timeline': 1, 'primary_fqdn': 'old-primary'},
        _zk_state(lock_holder=None),
        automatic=True,
    ) is False

    inst.zk.write_failover_members.assert_not_called()
    inst.zk.write_failover_state.assert_not_called()
    inst.zk.release_lock.assert_called_once_with('epoch_manager')


def test_committed_handoff_starts_fence_failover_despite_old_local_timeline():
    inst = _make_instance()
    inst._try_acquire_failover_coordinator = MagicMock(return_value=True)
    observation = MagicMock()
    observation.durability = DurabilityConfig.build(['old-primary', 'host1', 'candidate'])
    observation.durability_quorums = (observation.durability,)
    observation.switchover_source_durability_quorums = (
        DurabilityConfig.build(['old-primary', 'candidate']),
    )
    observation.switchover_handoff_committed = True
    inst._build_failover_observation = MagicMock(return_value=observation)
    inst.zk.get_current_lock_holder.return_value = None
    inst.zk.write_failover_state.return_value = True
    inst.zk.delete.return_value = True
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

    call_kwargs = inst._build_failover_observation.call_args.kwargs
    assert call_kwargs['automatic'] is True
    assert call_kwargs['switchover_record'].operation_id == 'operation'
    inst.zk.write_failover_version.assert_called_once()
    inst.zk.write_failover_state.assert_called_once_with(FailoverPhase.VOTING)


def test_active_failover_preempts_committed_handoff_candidate_promotion():
    inst = _make_instance()
    inst._run_switchover_candidate = MagicMock(return_value=True)
    inst.zk.TIMELINE_INFO_PATH = 'timeline_info'
    db_state = {'role': 'replica', 'timeline': 1}
    zk_state = _zk_state(failover_state=FailoverPhase.VOTING, lock_holder=None)
    zk_state['timeline_info'] = 2
    zk_state['switchover_record'] = {
        'hostname': 'old-primary', 'candidate': 'candidate',
        'phase': 'handoff_committed', 'expected_timeline': 2,
    }
    zk_state['switchover_version'] = 4

    with patch('src.main.helpers.get_hostname', return_value='candidate'):
        assert inst.handle_failover(db_state, zk_state) is True

    inst._run_switchover_candidate.assert_not_called()
    inst._prepare_failover_observation.assert_called_once_with(
        FailoverPhase.VOTING,
        db_state,
        zk_state,
    )


def test_old_primary_votes_in_active_handoff_failover():
    inst = _make_instance()
    inst._run_switchover_primary = MagicMock(return_value=True)
    db_state = {'role': None, 'timeline': 1}
    zk_state = _zk_state(failover_state=FailoverPhase.VOTING, lock_holder='candidate')
    zk_state['timeline_info'] = 2
    zk_state['switchover_record'] = {
        'hostname': 'old-primary', 'candidate': 'candidate',
        'phase': 'handoff_committed', 'expected_timeline': 2,
    }
    zk_state['switchover_version'] = 4

    with patch('src.main.helpers.get_hostname', return_value='old-primary'):
        assert inst.handle_failover(db_state, zk_state) is True

    inst._run_switchover_primary.assert_not_called()
    inst._prepare_failover_observation.assert_called_once()


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


def test_non_ha_primary_runs_active_failover_to_fence_itself():
    inst = _make_instance()
    inst.config.stream_from = 'upstream'
    zk_state = _zk_state(
        failover_state=FailoverPhase.VOTING,
        lock_holder='primary',
    )

    with patch('src.main.helpers.get_hostname', return_value='primary'):
        assert inst.handle_failover({'role': 'primary'}, zk_state) is True

    inst._prepare_failover_observation.assert_called_once_with(
        FailoverPhase.VOTING,
        {'role': 'primary'},
        zk_state,
    )


def test_non_ha_replica_stays_out_of_active_failover():
    inst = _make_instance()
    inst.config.stream_from = 'upstream'
    zk_state = _zk_state(failover_state=FailoverPhase.VOTING)

    assert inst.handle_failover({'role': 'replica'}, zk_state) is True

    inst._prepare_failover_observation.assert_not_called()


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
        failover_state=FailoverPhase.VOTING,
        lock_holder=None,
    )

    assert inst.handle_failover({'role': 'replica'}, zk_state) is True

    inst._prepare_failover_observation.assert_called_once()


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


def test_committed_switchover_failed_candidate_lock_does_not_block_recovery_failover():
    """A rejected C still owns leader while the fenced recovery election begins."""
    inst = _make_instance()
    inst._try_acquire_failover_coordinator = MagicMock(return_value=True)
    inst.zk.get_current_lock_holder.return_value = 'candidate'
    durability = DurabilityConfig.build(['candidate', 'side'])
    inst.zk.get_durability_state.return_value = (DurabilityState(durability), 7)
    observation = MagicMock(
        durability=durability,
        durability_quorums=(durability,),
        switchover_handoff_committed=True,
        switchover_source_durability_quorums=(durability,),
    )
    inst._build_failover_observation = MagicMock(return_value=observation)
    inst.zk.fence_durability_state_for_failover.return_value = True
    inst.zk.delete.return_value = True
    inst.zk.write_failover_version.return_value = True
    inst.zk.is_lock_holder.return_value = True
    inst.zk.write_failover_state.return_value = True
    record = {
        'hostname': 'primary',
        'candidate': 'candidate',
        'timeline': 1,
        'phase': 'handoff_committed',
        'operation_id': 'operation',
        'expected_timeline': 2,
        'failure_reason': 'promote_failed',
    }

    zk_state = _zk_state(lock_holder='candidate')
    zk_state['switchover_record'] = record

    result = Pgconsul._initialize_failover(
        inst,
        {'role': 'replica', 'timeline': 1},
        zk_state,
        automatic=False,
        failed_primary='candidate',
    )

    assert result is True
    inst.zk.release_lock.assert_not_called()
    inst._build_failover_observation.assert_called_once()


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
    inst.zk.get_current_lock_holder.return_value = 'old-primary'
    inst.zk.write_failover_state.return_value = True
    inst.zk.delete.return_value = True
    inst.zk.write_failover_version.return_value = True
    inst.zk.is_lock_holder.return_value = True
    request = FailoverRequest('old-primary', 'operation-1', True)
    inst.zk.get_failover_request.return_value = (request, 0)
    inst.zk.write_failover_request.return_value = 1
    inst.zk.get_durability_state.return_value = (
        DurabilityState(observation.durability), 7,
    )
    inst.zk.fence_durability_state_for_failover.return_value = True

    assert Pgconsul._initialize_failover(
        inst,
        {'role': 'replica', 'timeline': 1},
        _zk_state(lock_holder='old-primary'),
        automatic=False,
        failed_primary='old-primary',
        manual_request=request,
    ) is True

    inst.zk.write_failover_version.assert_called_once_with('operation-1')
