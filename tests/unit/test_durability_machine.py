from src.durability import (
    DurabilityAction,
    DurabilityMachine,
    DurabilityObservation,
)
from src.switchover import DurabilityPinMode, SwitchoverPhase, SwitchoverRecord
from src.types import DurabilityConfig, DurabilityState, DurabilityTransition


def _observation(**changes) -> DurabilityObservation:
    stable = DurabilityConfig.build(['primary', 'replica'])
    values = {
        'hostname': 'primary',
        'role': 'primary',
        'db_timeline': 2,
        'zk_timeline': 2,
        'lock_holder': 'primary',
        'desired_primary': 'primary',
        'state': DurabilityState(stable),
        'current_ssn_known': True,
        'current_ssn': 'ANY 1(replica)',
        'stable_ssn': 'ANY 1(replica)',
        'failover_active': False,
        'election_winner': None,
        'maintenance_wants_async': False,
        'single_node': False,
        'ordinary_changes_enabled': True,
        'switchover': SwitchoverRecord(),
        'switchover_acks': {},
        'streaming_applications': frozenset(),
        'ordinary_desired': stable,
    }
    values.update(changes)
    return DurabilityObservation(**values)


def test_only_materialized_primary_may_change_durability():
    machine = DurabilityMachine()
    desired = DurabilityConfig.build(['primary'])

    for changes in (
        {'role': 'replica'},
        {'lock_holder': 'other'},
        {'desired_primary': 'other'},
        {'db_timeline': 1},
    ):
        decision = machine.decide(_observation(
            ordinary_desired=desired,
            **changes,
        ))
        assert decision.plan == []
        assert decision.owns_iteration is False


def test_machine_requests_ordinary_target_only_for_ordinary_primary_work():
    machine = DurabilityMachine()

    assert machine.needs_ordinary_target(_observation())
    assert not machine.needs_ordinary_target(_observation(
        maintenance_wants_async=True,
    ))
    assert not machine.needs_ordinary_target(_observation(
        failover_active=True,
    ))
    assert not machine.needs_ordinary_target(_observation(
        switchover=SwitchoverRecord(
            phase=SwitchoverPhase.SCHEDULED,
        ),
    ))
    assert not machine.needs_ordinary_target(_observation(single_node=True))
    assert not machine.needs_ordinary_target(_observation(
        ordinary_changes_enabled=False,
    ))


def test_persisted_transition_is_finished_before_new_policy():
    machine = DurabilityMachine()
    source = DurabilityConfig.build(['primary', 'replica'])
    intermediate = DurabilityConfig.build(['primary', 'replica', 'side'])
    desired = DurabilityConfig.build(['primary'])
    transition = DurabilityTransition(source, intermediate, 'old-operation')

    decision = machine.decide(_observation(
        state=DurabilityState(source, transition),
        ordinary_desired=desired,
    ))

    assert len(decision.plan) == 1
    assert decision.plan[0].action == DurabilityAction.RESUME
    assert decision.owns_iteration is False


def test_failover_blocks_an_unfinished_transition():
    machine = DurabilityMachine()
    source = DurabilityConfig.build(['primary', 'replica'])
    target = DurabilityConfig.build(['primary', 'replica', 'side'])
    transition = DurabilityTransition(source, target, 'change')

    decision = machine.decide(_observation(
        state=DurabilityState(source, transition),
        failover_active=True,
    ))

    assert decision.plan == []
    assert decision.owns_iteration is False


def test_promoted_failover_winner_leaves_transition_frozen_until_cleanup():
    machine = DurabilityMachine()
    source = DurabilityConfig.build(['old-primary', 'winner'])
    target = DurabilityConfig.build(['old-primary', 'winner', 'side'])

    decision = machine.decide(_observation(
        hostname='winner',
        lock_holder='winner',
        desired_primary='winner',
        state=DurabilityState(
            source,
            DurabilityTransition(source, target, 'change'),
        ),
        failover_active=True,
        election_winner='winner',
    ))

    # A later election may still be necessary.  The winner must not publish a
    # new durability endpoint while failover metadata is still active.
    assert decision.plan == []


def test_ordinary_policy_reconciles_without_owning_iteration():
    machine = DurabilityMachine()
    desired = DurabilityConfig.build(['primary'])

    decision = machine.decide(_observation(ordinary_desired=desired))

    assert len(decision.plan) == 1
    assert decision.plan[0].action == DurabilityAction.RECONCILE
    assert decision.plan[0].desired == desired
    assert decision.owns_iteration is False


def test_satisfied_ordinary_policy_is_a_noop():
    machine = DurabilityMachine()

    assert machine.decide(_observation()).plan == []


def test_ordinary_policy_restores_stable_ssn_after_pin():
    machine = DurabilityMachine()
    stable = DurabilityConfig.build(['primary', 'replica'])

    decision = machine.decide(_observation(
        state=DurabilityState(stable),
        current_ssn='EVERY(replica), ANY 1(replica)',
        ordinary_desired=stable,
    ))

    assert len(decision.plan) == 1
    assert decision.plan[0].action == DurabilityAction.REAPPLY_STABLE
    assert decision.plan[0].desired == stable


def test_maintenance_requests_async_membership():
    machine = DurabilityMachine()

    decision = machine.decide(_observation(
        maintenance_wants_async=True,
    ))

    assert len(decision.plan) == 1
    assert decision.plan[0].action == DurabilityAction.RECONCILE
    assert decision.plan[0].desired == DurabilityConfig.build(['primary'])


def test_single_node_forces_async_when_ordinary_changes_are_disabled():
    machine = DurabilityMachine()

    decision = machine.decide(_observation(
        single_node=True,
        ordinary_changes_enabled=False,
    ))

    assert len(decision.plan) == 1
    assert decision.plan[0].action == DurabilityAction.RECONCILE
    assert decision.plan[0].desired == DurabilityConfig.build(['primary'])


def test_mandatory_switchover_pin_runs_barrier_until_acknowledged():
    machine = DurabilityMachine()
    stable = DurabilityConfig.build(['primary', 'candidate', 'side'])
    record = SwitchoverRecord(
        hostname='primary',
        candidate='candidate',
        phase=SwitchoverPhase.PREPARING_DURABILITY,
        operation_id='switch-1',
        durability_pin_mode=DurabilityPinMode.MANDATORY,
        durability_pin_owner='primary',
        original_durability_members=list(stable.members),
    )

    decision = machine.decide(_observation(
        state=DurabilityState(stable),
        switchover=record,
    ))

    assert len(decision.plan) == 1
    assert decision.plan[0].action == DurabilityAction.COMPLETE_SWITCHOVER_PIN
    assert decision.plan[0].mandatory == 'candidate'
    assert decision.plan[0].operation_id == 'switch-1'

    acknowledged = machine.decide(_observation(
        state=DurabilityState(stable),
        switchover=record,
        switchover_acks={'primary': {'durability_ready': True}},
    ))
    assert acknowledged.plan == []


def test_unpatched_switchover_contracts_before_operation_barrier():
    machine = DurabilityMachine()
    stable = DurabilityConfig.build(['primary', 'candidate', 'side'])
    pair = DurabilityConfig.build(['primary', 'candidate'])
    record = SwitchoverRecord(
        hostname='primary',
        candidate='candidate',
        phase=SwitchoverPhase.PREPARING_DURABILITY,
        operation_id='switch-1',
        durability_pin_mode=DurabilityPinMode.CONTRACTING,
        durability_pin_owner='primary',
        original_durability_members=list(stable.members),
    )

    decision = machine.decide(_observation(
        state=DurabilityState(stable),
        switchover=record,
    ))

    assert len(decision.plan) == 1
    assert decision.plan[0].action == DurabilityAction.RECONCILE
    assert decision.plan[0].desired == pair


def test_missing_switchover_pin_mode_fails_closed():
    machine = DurabilityMachine()
    record = SwitchoverRecord(
        hostname='primary',
        candidate='candidate',
        phase=SwitchoverPhase.PREPARING_DURABILITY,
        operation_id='switch-1',
        durability_pin_owner='primary',
        original_durability_members=['primary', 'candidate', 'side'],
    )

    assert machine.decide(_observation(switchover=record)).plan == []


def test_switchover_expansion_keeps_stable_and_adds_ready_sides():
    machine = DurabilityMachine()
    stable = DurabilityConfig.build(['primary', 'candidate', 'stable-side'])
    record = SwitchoverRecord(
        hostname='primary',
        candidate='candidate',
        side_replicas=['stable-side', 'new-side', 'not-streaming'],
        phase=SwitchoverPhase.WAITING_ARCHIVE,
        operation_id='switch-1',
        durability_pin_mode=DurabilityPinMode.EXPANDING,
        durability_pin_owner='candidate',
    )
    obs = _observation(
        hostname='candidate',
        lock_holder='candidate',
        desired_primary='candidate',
        state=DurabilityState(stable),
        switchover=record,
        switchover_acks={
            'new-side': {
                'source': 'candidate',
                'restore_disabled': True,
            },
            'not-streaming': {
                'source': 'candidate',
                'restore_disabled': True,
            },
        },
        streaming_applications=frozenset({'new_side'}),
    )

    decision = machine.decide(obs)

    assert len(decision.plan) == 1
    assert decision.plan[0].action == DurabilityAction.RECONCILE
    assert decision.plan[0].desired == DurabilityConfig.build([
        'primary', 'candidate', 'stable-side', 'new-side',
    ])


def test_satisfied_switchover_expansion_is_acknowledged():
    machine = DurabilityMachine()
    stable = DurabilityConfig.build(['primary', 'candidate'])
    record = SwitchoverRecord(
        hostname='primary',
        candidate='candidate',
        phase=SwitchoverPhase.WAITING_ARCHIVE,
        operation_id='switch-1',
        durability_pin_mode=DurabilityPinMode.EXPANDING,
        durability_pin_owner='candidate',
    )

    decision = machine.decide(_observation(
        hostname='candidate',
        lock_holder='candidate',
        desired_primary='candidate',
        state=DurabilityState(stable),
        switchover=record,
    ))

    assert len(decision.plan) == 1
    assert decision.plan[0].action == DurabilityAction.ACK_SWITCHOVER_EXPANSION
    assert decision.plan[0].operation_id == 'switch-1'
