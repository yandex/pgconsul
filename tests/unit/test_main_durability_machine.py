from unittest.mock import MagicMock, patch

from src.failover import FailoverPhase
from src.main import Pgconsul
from src.switchover import DurabilityPinMode, SwitchoverPhase, SwitchoverRecord
from src.types import DurabilityConfig, DurabilityState, DurabilityTransition


def _instance() -> Pgconsul:
    instance = Pgconsul.__new__(Pgconsul)
    instance.db = MagicMock()
    instance.zk = MagicMock()
    instance.zk.DURABILITY_MEMBERS_PATH = 'durability_members'
    instance.zk.DESIRED_PRIMARY_PATH = 'desired_primary'
    instance.zk.FAILOVER_STATE_PATH = 'failover_state'
    instance.zk.ELECTION_MANAGER_LOCK_PATH = 'election_manager'
    instance.zk.SWITCHOVER_RECORD_PATH = 'switchover/record'
    instance.zk.SWITCHOVER_VERSION_KEY = 'switchover_version'
    instance.zk.TIMELINE_INFO_PATH = 'timeline'
    instance._durability_manager = MagicMock()
    instance._maintenance = MagicMock(is_in_maintenance=False, wants_async_durability=False)
    instance._is_single_node = False
    instance.config = MagicMock(change_replication_type=True)
    return instance


def _zk_state(instance: Pgconsul, durability: DurabilityState) -> dict:
    return {
        instance.zk.DURABILITY_MEMBERS_PATH: durability,
        instance.zk.DESIRED_PRIMARY_PATH: {'hostname': 'primary'},
        instance.zk.FAILOVER_STATE_PATH: None,
        instance.zk.SWITCHOVER_RECORD_PATH: {},
        instance.zk.SWITCHOVER_VERSION_KEY: 1,
        instance.zk.TIMELINE_INFO_PATH: 2,
        'lock_holder': 'primary',
    }


def _prepare(instance: Pgconsul, state: DurabilityState) -> None:
    instance.zk.is_lock_holder.return_value = False
    instance.zk.try_acquire_lock.return_value = True
    instance.zk.get.return_value = None
    instance.zk.get_durability_state.return_value = (state, 4)


def _run(instance: Pgconsul, state: DurabilityState, db_state=None, zk_state=None) -> None:
    with patch('src.main.helpers.get_hostname', return_value='primary'):
        instance._run_durability_reconciliation(
            db_state or {'role': 'primary', 'timeline': 2},
            zk_state or _zk_state(instance, state),
        )


def test_only_materialized_primary_may_reconcile_durability():
    state = DurabilityState(DurabilityConfig.build(['primary', 'replica']))
    for db_state, changes in (
        ({'role': 'replica', 'timeline': 2}, {}),
        ({'role': 'primary', 'timeline': 1}, {}),
        ({'role': 'primary', 'timeline': 2}, {'lock_holder': 'other'}),
        ({'role': 'primary', 'timeline': 2}, {'desired_primary': {'hostname': 'other'}}),
    ):
        instance = _instance()
        zk_state = _zk_state(instance, state)
        zk_state.update(changes)
        _run(instance, state, db_state, zk_state)
        instance.zk.try_acquire_lock.assert_not_called()
        assert instance._durability_manager.mock_calls == []


def test_reconciliation_runs_one_persisted_transition_without_command_plan():
    source = DurabilityConfig.build(['primary', 'replica'])
    target = DurabilityConfig.build(['primary', 'replica', 'side'])
    state = DurabilityState(source, DurabilityTransition(source, target, 'op'))
    instance = _instance()
    _prepare(instance, state)

    _run(instance, state)

    instance._durability_manager.resume_durability_transition.assert_called_once_with()
    instance.zk.release_lock.assert_called_once_with('election_manager')


def test_reconciliation_skips_when_failover_appears_after_lock():
    state = DurabilityState(DurabilityConfig.build(['primary', 'replica']))
    instance = _instance()
    _prepare(instance, state)
    instance.zk.get.return_value = FailoverPhase.VOTING

    _run(instance, state)

    assert instance._durability_manager.mock_calls == []
    instance.zk.release_lock.assert_called_once_with('election_manager')


def test_switchover_pin_applies_mandatory_ssn_barrier_then_ack():
    stable = DurabilityConfig.build(['primary', 'candidate', 'side'])
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', phase=SwitchoverPhase.PREPARING_DURABILITY,
        operation_id='switch-1', durability_pin_mode=DurabilityPinMode.MANDATORY,
        durability_pin_owner='primary', original_durability_members=list(stable.members),
    )
    instance = _instance()
    state = DurabilityState(stable)
    _prepare(instance, state)
    zk_state = _zk_state(instance, state)
    zk_state[instance.zk.SWITCHOVER_RECORD_PATH] = record.to_dict()
    instance.db.advance_wal_barrier.return_value = True

    _run(instance, state, zk_state=zk_state)

    instance._durability_manager.set_mandatory_sync_replica.assert_called_once_with(stable, 'candidate')
    instance.db.advance_wal_barrier.assert_called_once_with('switchover:switch-1')
    instance.zk.write_switchover_ack.assert_called_once_with('primary', 'switch-1', {'durability_ready': True})


def test_ordinary_policy_starts_one_adjacent_transition():
    state = DurabilityState(DurabilityConfig.build(['primary', 'replica']))
    desired = DurabilityConfig.build(['primary', 'replica', 'side'])
    instance = _instance()
    _prepare(instance, state)
    instance._read_ordinary_durability_target = MagicMock(return_value=desired)

    _run(instance, state)

    instance._durability_manager.change_replication_to_durability_config.assert_called_once_with(desired)


def test_stable_ssn_is_reapplied_after_a_temporary_pin():
    stable = DurabilityConfig.build(['primary', 'replica'])
    state = DurabilityState(stable)
    instance = _instance()
    _prepare(instance, state)
    instance._read_ordinary_durability_target = MagicMock(return_value=stable)
    instance._durability_manager.ssn_for_durability.return_value = 'ANY 1(replica)'

    _run(instance, state, {'role': 'primary', 'timeline': 2, 'replication_state': ('sync', 'EVERY(replica), ANY 1(replica)')})

    instance._durability_manager.apply_stable_durability_config.assert_called_once_with(stable)


def test_maintenance_reconciles_to_single_primary_membership():
    state = DurabilityState(DurabilityConfig.build(['primary', 'replica']))
    instance = _instance()
    _prepare(instance, state)
    instance._maintenance.is_in_maintenance = True
    instance._maintenance.wants_async_durability = True

    _run(instance, state)

    instance._durability_manager.change_replication_to_durability_config.assert_called_once_with(
        DurabilityConfig.build(['primary']),
    )


def test_single_node_reconciles_to_single_primary_membership():
    state = DurabilityState(DurabilityConfig.build(['primary', 'replica']))
    instance = _instance()
    _prepare(instance, state)
    instance._is_single_node = True
    instance.config.change_replication_type = False

    _run(instance, state)

    instance._durability_manager.change_replication_to_durability_config.assert_called_once_with(
        DurabilityConfig.build(['primary']),
    )


def test_unpatched_switchover_contracts_one_member_at_a_time():
    stable = DurabilityConfig.build(['primary', 'candidate', 'side'])
    state = DurabilityState(stable)
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', phase=SwitchoverPhase.PREPARING_DURABILITY,
        operation_id='switch-1', durability_pin_mode=DurabilityPinMode.CONTRACTING,
        durability_pin_owner='primary', original_durability_members=list(stable.members),
    )
    instance = _instance()
    _prepare(instance, state)
    zk_state = _zk_state(instance, state)
    zk_state[instance.zk.SWITCHOVER_RECORD_PATH] = record.to_dict()

    _run(instance, state, zk_state=zk_state)

    instance._durability_manager.change_replication_to_durability_config.assert_called_once_with(
        DurabilityConfig.build(['primary', 'candidate']),
    )


def test_switchover_expansion_only_adds_turned_streaming_side():
    stable = DurabilityConfig.build(['primary', 'candidate'])
    state = DurabilityState(stable)
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate', side_replicas=['side'],
        phase=SwitchoverPhase.WAITING_ARCHIVE, operation_id='switch-1',
        durability_pin_mode=DurabilityPinMode.EXPANDING, durability_pin_owner='primary',
    )
    instance = _instance()
    _prepare(instance, state)
    instance.zk.get_switchover_ack.side_effect = lambda host, _: {
        'side': {'source': 'candidate', 'restore_disabled': True},
    }.get(host)
    zk_state = _zk_state(instance, state)
    zk_state[instance.zk.SWITCHOVER_RECORD_PATH] = record.to_dict()

    _run(instance, state, {'role': 'primary', 'timeline': 2, 'replics_info': [
        {'application_name': 'side', 'state': 'streaming'},
    ]}, zk_state)

    instance._durability_manager.change_replication_to_durability_config.assert_called_once_with(
        DurabilityConfig.build(['primary', 'candidate', 'side']),
    )


def test_ordinary_policy_does_not_run_during_active_switchover():
    state = DurabilityState(DurabilityConfig.build(['primary', 'replica']))
    record = SwitchoverRecord(phase=SwitchoverPhase.TURNING_SIDES, operation_id='switch-1')
    instance = _instance()
    _prepare(instance, state)
    instance._read_ordinary_durability_target = MagicMock()
    zk_state = _zk_state(instance, state)
    zk_state[instance.zk.SWITCHOVER_RECORD_PATH] = record.to_dict()

    _run(instance, state, zk_state=zk_state)

    instance._read_ordinary_durability_target.assert_not_called()
    assert instance._durability_manager.mock_calls == []
