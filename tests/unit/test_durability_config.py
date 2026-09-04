from src.types import DurabilityConfig, DurabilityState, DurabilityTransition


def test_zk_value_contains_only_members():
    config = DurabilityConfig.build(['replica2', 'primary', 'replica1'])

    assert config.to_dict() == {'members': ['primary', 'replica1', 'replica2']}


def test_default_required_is_derived_from_standby_count():
    assert DurabilityConfig.build(['primary', 'r1', 'r2', 'r3']).required == 2


def test_required_changes_only_when_membership_size_crosses_majority_boundary():
    assert DurabilityConfig.build(['primary', 'r1', 'r2']).required == 1
    assert DurabilityConfig.build(['primary', 'r1', 'r2', 'r3']).required == 2


def test_state_round_trip_does_not_persist_required():
    source = DurabilityConfig.build(['primary', 'r1', 'r2'])
    target = DurabilityConfig.build(['primary', 'r1'])
    state = DurabilityState(
        stable=source,
        transition=DurabilityTransition(source, target, 'operation-1'),
    )

    value = state.to_dict()

    assert 'required' not in str(value)
    assert DurabilityState.from_dict(value) == state


def test_initialization_transition_round_trip_persists_operation_id():
    target = DurabilityConfig.build(['primary', 'r1', 'r2'])
    state = DurabilityState(
        stable=None,
        transition=DurabilityTransition(
            source=None,
            target=target,
            operation_id='operation-1',
        ),
    )

    value = state.to_dict()

    assert value['transition']['from_members'] == []
    assert value['transition']['operation_id'] == 'operation-1'
    assert DurabilityState.from_dict(value) == state


def test_transition_exposes_both_failover_quorums():
    source = DurabilityConfig.build(['primary', 'a', 'b', 'c'])
    target = DurabilityConfig.build(['primary', 'a', 'b', 'd'])
    state = DurabilityState(
        source,
        DurabilityTransition(source, target, 'operation-1'),
    )

    assert state.failover_configs() == (source, target)


def test_initialization_transition_has_no_safe_failover_quorum():
    target = DurabilityConfig.build(['primary', 'a'])

    assert DurabilityState(
        None,
        DurabilityTransition(None, target, 'operation-1'),
    ).failover_configs() == ()
