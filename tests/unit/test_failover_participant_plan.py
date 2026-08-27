# encoding: utf-8
"""Pure plan tests for the failover participant machine."""

from dataclasses import replace

from src.commands import (
    AcquireLock,
    ClearLocalState,
    FailoverTransitionTo,
    Log,
    PrepareFailoverVote,
    Promote,
    ReleaseLock,
    ReturnToCluster,
    WriteFailoverParticipantState,
)
from src.failover import (
    FailoverMachine,
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
        electorate=('host1', 'host2'),
        failover_version='version-1',
        current_time=100.0,
    )
    return replace(obs, **changes)


def test_gates_passed_keeps_vote_preparation_idempotent():
    assert FailoverParticipantMachine().plan(_obs(FailoverPhase.GATES_PASSED)) == [
        PrepareFailoverVote(1, 30.0, 'version-1', 5),
    ]


def test_registration_and_voting_write_vote():
    machine = FailoverParticipantMachine()
    expected = [PrepareFailoverVote(1, 30.0, 'version-1', 5)]
    assert machine.plan(_obs(FailoverPhase.REGISTRATION)) == expected
    assert machine.plan(_obs(FailoverPhase.VOTING)) == expected


def test_host_outside_electorate_does_not_vote():
    assert FailoverParticipantMachine().plan(_obs(electorate=('host2',))) == []


def test_winner_clears_local_state_acquires_lock_and_advances():
    obs = _obs(FailoverPhase.WINNER_SELECTED, election_winner='host1')
    assert FailoverParticipantMachine().plan(obs) == [
        ClearLocalState('failover_participant'),
        AcquireLock(timeout=0),
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


def test_loser_returns_to_cluster_once_winner_owns_primary_lock():
    """Regression for kill_primary.feature:101."""
    obs = _obs(
        FailoverPhase.PROMOTING,
        election_winner='host2',
        lock_holder='host2',
        winner_status='promoted',
    )

    plan = FailoverParticipantMachine().plan(obs)

    assert plan == [ReturnToCluster('host2', 'replica', False)]


def test_loser_does_not_repeat_return_when_already_following_winner():
    obs = _obs(
        FailoverPhase.PROMOTING,
        election_winner='host2',
        lock_holder='host2',
        winner_status='promoted',
        replication_source='host2',
    )

    plan = FailoverParticipantMachine().plan(obs)

    assert len(plan) == 1
    assert isinstance(plan[0], Log)


def test_losing_coordinator_returns_to_cluster_while_failover_is_promoting():
    """The manager-lock owner may lose the election too."""
    obs = _obs(
        FailoverPhase.PROMOTING,
        election_winner='host2',
        lock_holder='host2',
        is_coordinator=True,
        winner_status='promoted',
    )

    plan = FailoverMachine().plan(obs)

    assert plan[-1] == ReturnToCluster('host2', 'replica', False)
    assert any(isinstance(command, FailoverTransitionTo) for command in plan)


def test_losing_coordinator_returns_before_finished_cleanup():
    obs = _obs(
        FailoverPhase.FINISHED,
        election_winner='host2',
        lock_holder='host2',
        winner_status='promoted',
        is_coordinator=True,
    )

    assert FailoverMachine().plan(obs) == [
        ReturnToCluster('host2', 'replica', False),
    ]


def test_dead_loser_returns_using_previous_role():
    obs = _obs(
        FailoverPhase.PROMOTING,
        role=None,
        previous_role='replica',
        is_postgresql_dead=True,
        election_winner='host2',
        lock_holder='host2',
        winner_status='promoted',
    )

    assert FailoverParticipantMachine().plan(obs) == [
        ReturnToCluster('host2', 'replica', True),
    ]


def test_loser_waits_while_postgres_is_starting():
    obs = _obs(
        FailoverPhase.PROMOTING,
        role=None,
        is_postgresql_dead=False,
        election_winner='host2',
        lock_holder='host2',
    )

    assert isinstance(FailoverParticipantMachine().plan(obs)[0], Log)


def test_promoting_winner_resumes_promotion_pipeline():
    obs = _obs(FailoverPhase.PROMOTING, election_winner='host1')
    assert FailoverParticipantMachine().plan(obs) == [
        AcquireLock(timeout=0),
        Promote('failover_participant', failover_version='version-1'),
        WriteFailoverParticipantState('promoted', 'version-1'),
        ClearLocalState('failover_participant'),
    ]


def test_promoting_winner_starts_dead_postgres_before_resuming_pipeline():
    obs = _obs(
        FailoverPhase.PROMOTING,
        election_winner='host1',
        role=None,
        is_postgresql_dead=True,
    )

    plan = FailoverParticipantMachine().plan(obs)

    assert plan[1] == Promote(
        'failover_participant',
        start_postgresql=True,
        failover_version='version-1',
    )


def test_debug_failure_before_promote_transitions_to_failed():
    machine = FailoverParticipantMachine(
        debug_failure=lambda name: name == 'participant_before_promote',
    )
    obs = _obs(FailoverPhase.PROMOTING, election_winner='host1')
    assert machine.plan(obs) == [WriteFailoverParticipantState('failed', 'version-1')]


def test_failed_winner_that_became_primary_finishes_promotion():
    obs = _obs(
        FailoverPhase.FAILED,
        election_winner='host1',
        lock_holder='host1',
        role='primary',
    )
    assert FailoverParticipantMachine().plan(obs) == [
        Promote('failover_participant', failover_version='version-1'),
        WriteFailoverParticipantState('promoted', 'version-1'),
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
    assert plan == [PrepareFailoverVote(1, 30.0, 'version-1', 5)]
