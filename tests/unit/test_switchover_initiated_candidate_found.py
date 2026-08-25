from src.commands import (
    Checkpoint,
    StartTimer,
    StopPooler,
    StoreReplicsInfo,
    TransitionTo,
    WriteLocalState,
)
from src.switchover import (
    PrimarySwitchoverMachine,
    SwitchoverObservation,
    SwitchoverPhase,
    SwitchoverRecord,
)


def _observation(phase, *, candidate='host2', candidate_alive=True, downtime_started_ts=None):
    return SwitchoverObservation(
        record=SwitchoverRecord(
            hostname='host1',
            timeline=1,
            phase=phase,
            candidate=candidate,
        ),
        my_hostname='host1',
        role='primary',
        zk_timeline=1,
        last_role_transition_ts=None,
        ha_replics=frozenset({'host2'}),
        replics_info=[],
        streaming_replicas=(),
        candidate_alive=candidate_alive,
        lock_holder='host1',
        switchover_started_ts=None,
        downtime_started_ts=downtime_started_ts,
        all_side_replicas_turned=False,
        current_time=1.0,
    )


def test_initiated_waits_while_candidate_is_alive():
    plan = PrimarySwitchoverMachine().plan(
        _observation(SwitchoverPhase.INITIATED),
    )

    assert plan == []


def test_initiated_fails_when_candidate_is_dead():
    plan = PrimarySwitchoverMachine().plan(
        _observation(SwitchoverPhase.INITIATED, candidate_alive=False),
    )

    assert plan == [TransitionTo(SwitchoverPhase.FAILED)]


def test_candidate_found_prepares_primary_before_stopping_pooler():
    plan = PrimarySwitchoverMachine().plan(
        _observation(SwitchoverPhase.CANDIDATE_FOUND),
    )

    assert isinstance(plan[0], StoreReplicsInfo)
    assert isinstance(plan[1], Checkpoint)
    assert isinstance(plan[2], StartTimer)
    assert isinstance(plan[3], StopPooler)
    assert plan[-1] == WriteLocalState(
        'switchover_primary',
        SwitchoverPhase.POOLER_STOPPED,
    )


def test_candidate_found_does_not_restart_downtime_timer():
    plan = PrimarySwitchoverMachine().plan(
        _observation(
            SwitchoverPhase.CANDIDATE_FOUND,
            downtime_started_ts=10.0,
        ),
    )

    assert not any(isinstance(command, StartTimer) for command in plan)


def test_candidate_required_phase_without_candidate_fails():
    plan = PrimarySwitchoverMachine().plan(
        _observation(SwitchoverPhase.CANDIDATE_FOUND, candidate=None),
    )

    assert plan == [TransitionTo(SwitchoverPhase.FAILED)]
