# encoding: utf-8
"""Pure decision tests for the failover participant machine."""

from dataclasses import replace

from src.commands import (
    ClearFailoverDesiredPrimary,
    ClearLocalState,
    Log,
    PrepareFailoverVote,
    Promote,
    RequestReturnToCluster,
    StopPostgresql,
    WriteFailoverParticipantState,
)
from src.failover import (
    FailoverObservation,
    FailoverParticipantMachine,
    FailoverPhase,
)


def _plan(machine, observation):
    return machine.decide(observation).plan


def _obs(phase=FailoverPhase.REGISTRATION, **changes):
    obs = FailoverObservation(
        phase=phase,
        my_hostname='host1',
        role='replica',
        lock_holder='host1',
        is_coordinator=False,
        election_winner=None,
        votes={},
        replics_info=[],
        last_failover_ts=None,
        last_primary_availability_ts=None,
        is_primary_unreachable=True,
        failover_started_ts=None,
        downtime_started_ts=None,
        zk_timeline=5,
        local_timeline=5,
        quorum_size=2,
        electorate=('host1', 'host2'),
        failover_version='version-1',
        desired_hostname='host1',
        desired_operation_id='version-1',
        desired_operation_type='failover',
        current_time=100.0,
    )
    return replace(obs, **changes)


def test_registration_and_voting_write_vote():
    machine = FailoverParticipantMachine()
    expected = [PrepareFailoverVote(30.0, 'version-1')]
    assert _plan(machine, _obs(FailoverPhase.REGISTRATION)) == expected
    assert _plan(machine, _obs(FailoverPhase.VOTING)) == expected


def test_host_outside_electorate_does_not_vote():
    assert _plan(FailoverParticipantMachine(), _obs(electorate=('host2',))) == []


def test_old_primary_stops_even_outside_electorate():
    plan = _plan(FailoverParticipantMachine(), _obs(
        electorate=('host2',),
        failed_primary='host1',
        is_postgresql_dead=False,
    ))

    assert plan == [
        Log('Stopping old primary before failover'),
        StopPostgresql(wait=False),
    ]


def test_committed_handoff_fences_old_timeline_and_publishes_actual_branch():
    plan = _plan(FailoverParticipantMachine(),
        _obs(local_timeline=4, zk_timeline=5, allow_mismatched_timeline_votes=True)
    )
    assert plan == [PrepareFailoverVote(30.0, 'version-1')]


def test_stopped_old_primary_vote_does_not_depend_on_process_role_memory():
    plan = _plan(FailoverParticipantMachine(), _obs(
        local_timeline=4,
        zk_timeline=5,
        allow_mismatched_timeline_votes=True,
        branch_old_primary='host1',
        previous_role=None,
        is_postgresql_dead=True,
    ))

    assert plan == [
        PrepareFailoverVote(
            30.0, 'version-1', timeline_only=True,
        ),
    ]


def test_live_old_primary_stops_before_publishing_source_branch_vote():
    """A committed-handoff failover must fence P before its source vote."""
    plan = _plan(FailoverParticipantMachine(), _obs(
        local_timeline=4,
        zk_timeline=5,
        allow_mismatched_timeline_votes=True,
        branch_old_primary='host1',
        is_postgresql_dead=False,
    ))

    assert plan == [
        Log('Stopping old primary before publishing its branch vote'),
        StopPostgresql(wait=False),
    ]


def test_winner_clears_local_state_after_top_level_lock_acquisition():
    obs = _obs(FailoverPhase.WINNER_SELECTED, election_winner='host1')
    assert _plan(FailoverParticipantMachine(), obs) == [
        ClearLocalState('failover_participant'),
    ]


def test_loser_waits_for_cleanup():
    obs = _obs(FailoverPhase.WINNER_SELECTED, election_winner='host2')
    plan = _plan(FailoverParticipantMachine(), obs)
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

    plan = _plan(FailoverParticipantMachine(), obs)

    assert plan == [RequestReturnToCluster(
        'host2', 'replica', False, start_source='primary',
    )]


def test_loser_does_not_repeat_return_when_already_following_winner():
    obs = _obs(
        FailoverPhase.PROMOTING,
        election_winner='host2',
        lock_holder='host2',
        winner_status='promoted',
        replication_source='host2',
    )

    plan = _plan(FailoverParticipantMachine(), obs)

    assert len(plan) == 1
    assert isinstance(plan[0], Log)


def test_losing_coordinator_returns_to_cluster_while_failover_is_promoting():
    """The manager-lock owner runs the same participant path as every host."""
    obs = _obs(
        FailoverPhase.PROMOTING,
        election_winner='host2',
        lock_holder='host2',
        is_coordinator=True,
        winner_status='promoted',
    )

    plan = _plan(FailoverParticipantMachine(), obs)

    assert plan == [RequestReturnToCluster(
        'host2', 'replica', False, start_source='primary',
    )]


def test_failover_decision_owns_only_an_active_iteration():
    active = FailoverParticipantMachine().decide(_obs(FailoverPhase.PROMOTING))
    inactive = FailoverParticipantMachine().decide(_obs(None))

    assert active.owns_iteration is True
    assert inactive.owns_iteration is False


def test_losing_coordinator_returns_before_finished_cleanup():
    obs = _obs(
        FailoverPhase.FINISHED,
        election_winner='host2',
        lock_holder='host2',
        winner_status='promoted',
        is_coordinator=True,
    )

    assert _plan(FailoverParticipantMachine(), obs) == [
        RequestReturnToCluster('host2', 'replica', False, start_source='primary'),
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

    assert _plan(FailoverParticipantMachine(), obs) == [
        RequestReturnToCluster('host2', 'replica', True, start_source='primary'),
    ]


def test_loser_waits_while_postgres_is_starting():
    obs = _obs(
        FailoverPhase.PROMOTING,
        role=None,
        is_postgresql_dead=False,
        election_winner='host2',
        lock_holder='host2',
    )

    assert isinstance(_plan(FailoverParticipantMachine(), obs)[0], Log)


def test_promoting_winner_resumes_promotion_pipeline():
    obs = _obs(FailoverPhase.PROMOTING, election_winner='host1')
    assert _plan(FailoverParticipantMachine(), obs) == [
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

    plan = _plan(FailoverParticipantMachine(), obs)

    assert plan[0] == Promote(
        'failover_participant',
        start_postgresql=True,
        failover_version='version-1',
    )


def test_debug_failure_before_promote_transitions_to_failed():
    machine = FailoverParticipantMachine(
        debug_failure=lambda name: name == 'participant_before_promote',
    )
    obs = _obs(FailoverPhase.PROMOTING, election_winner='host1')
    assert _plan(machine, obs) == [WriteFailoverParticipantState('failed', 'version-1')]


def test_resolving_winner_that_became_primary_finishes_promotion():
    obs = _obs(
        FailoverPhase.RESOLVING_WINNER,
        election_winner='host1',
        lock_holder='host1',
        role='primary',
    )
    assert _plan(FailoverParticipantMachine(), obs) == [
        Promote('failover_participant', failover_version='version-1'),
        WriteFailoverParticipantState('promoted', 'version-1'),
        ClearLocalState('failover_participant'),
    ]


def test_resolving_winner_that_is_still_replica_only_clears_local_state():
    obs = _obs(
        FailoverPhase.RESOLVING_WINNER,
        election_winner='host1',
        lock_holder='host1',
        role='replica',
    )
    assert _plan(FailoverParticipantMachine(), obs) == [
        ClearFailoverDesiredPrimary('host1', 'version-1'),
        ClearLocalState('failover_participant'),
    ]


def test_resolving_non_winner_waits_for_coordinator_cleanup():
    obs = _obs(FailoverPhase.RESOLVING_WINNER, election_winner='host2', lock_holder='host2')
    plan = _plan(FailoverParticipantMachine(), obs)
    assert len(plan) == 1
    assert isinstance(plan[0], Log)


def test_finished_winner_has_nothing_to_do():
    obs = _obs(FailoverPhase.FINISHED, election_winner='host1')
    assert _plan(FailoverParticipantMachine(), obs) == []


def test_finished_loser_waits_for_cleanup():
    obs = _obs(FailoverPhase.FINISHED, election_winner='host2')
    assert isinstance(_plan(FailoverParticipantMachine(), obs)[0], Log)


def test_registration_disables_walreceiver_without_transition():
    plan = _plan(FailoverParticipantMachine(), _obs(FailoverPhase.REGISTRATION))
    assert plan == [PrepareFailoverVote(30.0, 'version-1')]


def test_manual_data_loss_vote_can_skip_wal_source_fencing():
    obs = _obs(
        FailoverPhase.REGISTRATION,
        manual_data_loss=True,
        manual_fence_wal_sources=False,
    )

    assert _plan(FailoverParticipantMachine(), obs) == [
        PrepareFailoverVote(
            30.0, 'version-1', fence_wal_sources=False,
        ),
    ]
