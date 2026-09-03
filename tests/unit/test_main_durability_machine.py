from unittest.mock import MagicMock, patch

from src.durability import DurabilityAction, DurabilityMachine
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
    instance.zk.ELECTION_WINNER_PATH = 'election_winner'
    instance.zk.SWITCHOVER_RECORD_PATH = 'switchover/record'
    instance.zk.SWITCHOVER_VERSION_KEY = 'switchover_version'
    instance.zk.TIMELINE_INFO_PATH = 'timeline'
    instance._replication_manager = MagicMock()
    instance._maintenance = MagicMock()
    instance._maintenance.is_in_maintenance = False
    instance._maintenance.wants_async_durability = False
    instance._is_single_node = False
    instance.config = MagicMock(change_replication_type=True)
    instance._durability_machine = DurabilityMachine()
    return instance


def _zk_state(instance: Pgconsul, durability: DurabilityState) -> dict:
    return {
        instance.zk.DURABILITY_MEMBERS_PATH: durability,
        instance.zk.DESIRED_PRIMARY_PATH: {
            'hostname': 'primary',
            'operation_id': 'steady',
            'operation_type': 'steady',
        },
        instance.zk.FAILOVER_STATE_PATH: None,
        instance.zk.ELECTION_WINNER_PATH: None,
        instance.zk.SWITCHOVER_RECORD_PATH: {},
        instance.zk.SWITCHOVER_VERSION_KEY: 1,
        instance.zk.TIMELINE_INFO_PATH: 2,
        'lock_holder': 'primary',
    }


def test_switchover_sets_pin_policy_without_applying_it_itself():
    instance = _instance()
    stable = DurabilityConfig.build(['primary', 'candidate', 'side'])
    record = SwitchoverRecord(
        hostname='primary', candidate='candidate',
        phase=SwitchoverPhase.PREPARING_DURABILITY,
        operation_id='switch-1',
        durability_pin_mode=DurabilityPinMode.MANDATORY,
        durability_pin_owner='primary',
        original_durability_members=list(stable.members),
        version=4,
    )
    state = _zk_state(instance, DurabilityState(stable))
    state[instance.zk.SWITCHOVER_RECORD_PATH] = record.to_dict()
    state[instance.zk.SWITCHOVER_VERSION_KEY] = 4
    instance.zk.get_switchover_ack.return_value = None

    with patch('src.main.helpers.get_hostname', return_value='primary'):
        observation = instance._build_durability_observation(
            {'role': 'primary', 'timeline': 2}, state,
        )
        decision = instance._durability_machine.decide(observation)

    assert decision.owns_iteration is False
    assert len(decision.plan) == 1
    step = decision.plan[0]
    assert step.action == DurabilityAction.COMPLETE_SWITCHOVER_PIN
    assert step.desired == stable
    assert step.mandatory == 'candidate'


def test_active_failover_freezes_durability_transition():
    instance = _instance()
    source = DurabilityConfig.build(['primary', 'candidate'])
    target = DurabilityConfig.build(['primary', 'candidate', 'side'])
    state = _zk_state(instance, DurabilityState(
        source, DurabilityTransition(source, target, 'change'),
    ))
    state[instance.zk.FAILOVER_STATE_PATH] = FailoverPhase.VOTING

    with patch('src.main.helpers.get_hostname', return_value='primary'):
        observation = instance._build_durability_observation(
            {'role': 'primary', 'timeline': 2}, state,
        )

    assert instance._durability_machine.plan(observation) == []


def test_maintenance_requests_async_through_durability_machine():
    instance = _instance()
    stable = DurabilityConfig.build(['primary', 'replica'])
    instance._maintenance.is_in_maintenance = True
    instance._maintenance.wants_async_durability = True

    with patch('src.main.helpers.get_hostname', return_value='primary'):
        observation = instance._build_durability_observation(
            {'role': 'primary', 'timeline': 2},
            _zk_state(instance, DurabilityState(stable)),
        )
        step = instance._durability_machine.plan(observation)[0]

    assert step.action == DurabilityAction.RECONCILE
    assert step.desired == DurabilityConfig.build(['primary'])


def test_single_node_keeps_forcing_async_when_ordinary_changes_are_disabled():
    instance = _instance()
    instance._is_single_node = True
    instance.config.change_replication_type = False
    stable = DurabilityConfig.build(['primary', 'replica'])

    with patch('src.main.helpers.get_hostname', return_value='primary'):
        observation = instance._build_durability_observation(
            {'role': 'primary', 'timeline': 2},
            _zk_state(instance, DurabilityState(stable)),
        )
        step = instance._durability_machine.plan(observation)[0]

    assert step.action == DurabilityAction.RECONCILE
    assert step.desired == DurabilityConfig.build(['primary'])
