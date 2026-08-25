# encoding: utf-8
"""Pure plan tests for the failover participant machine."""

from dataclasses import replace

from src.commands import (
    AcquireLock,
    ClearLocalState,
    DisableWalReceiver,
    FailoverTransitionTo,
    Log,
    Promote,
    ReleaseLock,
    StopTimer,
    WriteElectionVote,
    WriteLastFailoverTime,
)
from src.failover import (
    FailoverObservation,
    FailoverParticipantMachine,
    FailoverPhase,
)


def _obs(phase=FailoverPhase.REGISTRATION, **changes):
    obs = FailoverObservation(
        phase=phase,
        my_hostname='host1',
        role='replica',
        lock_holder=None,
        is_coordinator=False,
        election_winner=None,
        votes={},
        alive_hosts=['host1', 'host2'],
        replics_info=[],
        host_lsn=100,
        host_priority=1,
        last_failover_ts=None,
        last_primary_availability_ts=None,
        is_primary_unreachable=True,
        is_replaying_wal=False,
        failover_started_ts=None,
        downtime_started_ts=None,
        zk_timeline=5,
        local_timeline=5,
        allow_data_loss=False,
        quorum_size=2,
        current_time=100.0,
    )
    return replace(obs, **changes)


def test_unhandled_phase_returns_empty_plan():
    assert FailoverParticipantMachine().plan(_obs(FailoverPhase.GATES_PASSED)) == []


def test_registration_and_voting_write_vote():
    machine = FailoverParticipantMachine()
    expected = [WriteElectionVote(100, 1)]
    assert machine.plan(_obs(FailoverPhase.REGISTRATION)) == expected
    assert machine.plan(_obs(FailoverPhase.VOTING)) == expected


def test_vote_waits_when_lsn_is_unavailable():
    assert FailoverParticipantMachine().plan(_obs(host_lsn=None)) == []


def test_winner_clears_local_state_acquires_lock_and_advances():
    obs = _obs(FailoverPhase.WINNER_SELECTED, election_winner='host1')
    assert FailoverParticipantMachine().plan(obs) == [
        ClearLocalState('failover_participant'),
        AcquireLock(timeout=0),
        FailoverTransitionTo(FailoverPhase.PROMOTING),
    ]


def test_winner_waits_while_replaying_wal():
    obs = _obs(
        FailoverPhase.WINNER_SELECTED,
        election_winner='host1',
        is_replaying_wal=True,
    )
    assert FailoverParticipantMachine().plan(obs) == []


def test_loser_waits_for_cleanup():
    obs = _obs(FailoverPhase.WINNER_SELECTED, election_winner='host2')
    plan = FailoverParticipantMachine().plan(obs)
    assert len(plan) == 1
    assert isinstance(plan[0], Log)


def test_promoting_winner_resumes_promotion_pipeline():
    obs = _obs(FailoverPhase.PROMOTING, election_winner='host1')
    assert FailoverParticipantMachine().plan(obs) == [
        AcquireLock(timeout=0),
        Promote('failover_participant'),
        WriteLastFailoverTime(),
        StopTimer('failover'),
        FailoverTransitionTo(FailoverPhase.FINISHED),
        ClearLocalState('failover_participant'),
    ]


def test_debug_failure_before_promote_transitions_to_failed():
    machine = FailoverParticipantMachine(
        debug_failure=lambda name: name == 'participant_before_promote',
    )
    obs = _obs(FailoverPhase.PROMOTING, election_winner='host1')
    assert machine.plan(obs) == [FailoverTransitionTo(FailoverPhase.FAILED)]


def test_failed_winner_that_became_primary_finishes_promotion():
    obs = _obs(
        FailoverPhase.FAILED,
        election_winner='host1',
        lock_holder='host1',
        role='primary',
    )
    assert FailoverParticipantMachine().plan(obs) == [
        Promote('failover_participant'),
        WriteLastFailoverTime(),
        StopTimer('failover'),
        FailoverTransitionTo(FailoverPhase.FINISHED),
        ClearLocalState('failover_participant'),
    ]


def test_failed_winner_that_is_still_replica_releases_primary_lock():
    obs = _obs(
        FailoverPhase.FAILED,
        election_winner='host1',
        lock_holder='host1',
        role='replica',
    )
    assert FailoverParticipantMachine().plan(obs) == [
        ReleaseLock(),
        ClearLocalState('failover_participant'),
    ]


def test_failed_non_winner_waits_for_coordinator_cleanup():
    obs = _obs(FailoverPhase.FAILED, election_winner='host2', lock_holder='host2')
    plan = FailoverParticipantMachine().plan(obs)
    assert len(plan) == 1
    assert isinstance(plan[0], Log)


def test_finished_winner_has_nothing_to_do():
    obs = _obs(FailoverPhase.FINISHED, election_winner='host1')
    assert FailoverParticipantMachine().plan(obs) == []


def test_finished_loser_waits_for_cleanup():
    obs = _obs(FailoverPhase.FINISHED, election_winner='host2')
    assert isinstance(FailoverParticipantMachine().plan(obs)[0], Log)


def test_walreceiver_disabling_disables_walreceiver_without_transition():
    plan = FailoverParticipantMachine().plan(_obs(FailoverPhase.WALRECEIVER_DISABLING))
    assert plan == [DisableWalReceiver(timeout=30.0)]
