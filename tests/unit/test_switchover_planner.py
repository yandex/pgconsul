"""Pure-plan tests for the manager-owned switchover state machine."""

from src.switchover import SwitchoverMachine, SwitchoverObservation, SwitchoverPhase, SwitchoverRecord


def _observation(
    phase=SwitchoverPhase.TURNING_SIDES,
    *,
    hostname='primary',
    role='replica',
    lock_holder='primary',
    expected_timeline=2,
    deadline_at=1000,
    current_time=100,
    failover_active=False,
    promotion_succeeded=False,
    record_valid=True,
):
    record = SwitchoverRecord(
        hostname='primary',
        candidate='candidate',
        phase=phase,
        operation_id='operation',
        expected_timeline=expected_timeline,
        deadline_at=deadline_at,
    )
    return SwitchoverObservation(
        record=record,
        my_hostname=hostname,
        role=role,
        lock_holder=lock_holder,
        zk_timeline=1,
        current_time=current_time,
        desired_hostname='candidate',
        desired_operation_id='operation',
        failover_active=failover_active,
        promotion_succeeded=promotion_succeeded,
        record_valid=record_valid,
        db_state={'role': role},
        zk_state={'lock_holder': lock_holder},
    )


def _actions(plan):
    return [command.action for command in plan]


def test_invalid_record_is_planned_for_cleanup():
    obs = _observation(record_valid=False)

    assert _actions(SwitchoverMachine().plan(obs)) == ['cleanup_invalid']


def test_terminal_record_is_cleaned_without_role_routing():
    obs = _observation(phase=SwitchoverPhase.CLEANUP, hostname='side')

    assert _actions(SwitchoverMachine().plan(obs)) == ['cleanup']


def test_old_primary_initializes_missing_operation_deadline_first():
    obs = _observation(hostname='primary', role='primary')
    obs.record.deadline_at = None

    assert _actions(SwitchoverMachine().plan(obs)) == ['initialize_deadline']


def test_old_primary_leaves_durability_to_its_own_machine():
    obs = _observation(hostname='primary', role='primary')

    assert _actions(SwitchoverMachine().plan(obs)) == ['run_primary']


def test_deadline_preempts_durability_reconciliation():
    obs = _observation(
        hostname='primary', role='primary', deadline_at=100, current_time=101,
    )

    assert _actions(SwitchoverMachine().plan(obs)) == ['handle_timeout']


def test_successful_promotion_is_not_failed_by_expired_deadline():
    obs = _observation(
        phase=SwitchoverPhase.WAITING_ARCHIVE,
        hostname='candidate',
        role='primary',
        lock_holder='candidate',
        deadline_at=100,
        current_time=101,
        promotion_succeeded=True,
    )

    assert _actions(SwitchoverMachine().plan(obs)) == ['run_candidate']


def test_missing_pre_handoff_leader_starts_recovery_instead_of_host_work():
    obs = _observation(
        phase=SwitchoverPhase.PREPARING_CANDIDATE,
        lock_holder=None,
        expected_timeline=None,
    )

    assert _actions(SwitchoverMachine().plan(obs)) == ['recover_pre_handoff']


def test_unexpected_pre_handoff_leader_schedules_cleanup():
    obs = _observation(lock_holder='other', expected_timeline=None)

    assert _actions(SwitchoverMachine().plan(obs)) == ['schedule_cleanup']


def test_failed_operation_is_scheduled_for_cleanup():
    obs = _observation(phase=SwitchoverPhase.FAILED, hostname='side')

    assert _actions(SwitchoverMachine().plan(obs)) == ['schedule_cleanup']


def test_old_primary_candidate_and_side_have_distinct_plans():
    primary = _observation(hostname='primary', role='primary')
    candidate = _observation(hostname='candidate')
    side = _observation(hostname='side')

    assert _actions(SwitchoverMachine().plan(primary)) == ['run_primary']
    assert _actions(SwitchoverMachine().plan(candidate)) == ['run_candidate']
    assert _actions(SwitchoverMachine().plan(side)) == ['run_side_replica']


def test_active_failover_preempts_committed_candidate_promotion():
    obs = _observation(
        phase=SwitchoverPhase.HANDOFF_COMMITTED,
        hostname='candidate',
        lock_holder=None,
        failover_active=True,
    )
    machine = SwitchoverMachine()

    decision = machine.decide(obs)

    assert decision.plan == []
    assert decision.owns_iteration is False


def test_committed_candidate_continues_without_waiting_for_manager():
    obs = _observation(
        phase=SwitchoverPhase.HANDOFF_COMMITTED,
        hostname='candidate',
        lock_holder=None,
    )
    machine = SwitchoverMachine()

    decision = machine.decide(obs)

    assert _actions(decision.plan) == ['run_candidate']
    assert decision.owns_iteration is True


def test_side_replica_releases_iteration_for_post_handoff_failover():
    obs = _observation(
        phase=SwitchoverPhase.HANDOFF_COMMITTED,
        hostname='side',
        lock_holder=None,
    )
    machine = SwitchoverMachine()

    decision = machine.decide(obs)

    assert decision.plan == []
    assert decision.owns_iteration is False
