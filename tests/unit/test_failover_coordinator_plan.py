# encoding: utf-8
"""Pure decision tests for the failover coordinator machine."""

from dataclasses import replace

from src.commands import (
    ClearFailoverDesiredPrimary,
    CleanupFailover,
    FailoverTransitionTo,
    ForceReleasePrimaryLock,
    StartTimer,
    StopTimer,
    WriteElectionWinner,
    WriteLastFailoverTime,
    WriteLastFailedFailoverTime,
)
from src.failover import (
    FailoverCoordinatorMachine,
    FailoverMachineConfig,
    FailoverObservation,
    FailoverPhase,
)
from src.types import DurabilityConfig


def _plan(machine, observation):
    return machine.decide(observation).plan


def _obs(phase=FailoverPhase.VOTING, **changes):
    durability = DurabilityConfig.build(['old-primary', 'host1', 'host2'])
    obs = FailoverObservation(
        phase=phase,
        my_hostname='host1',
        role='replica',
        lock_holder=None,
        is_coordinator=True,
        election_winner=None,
        votes={},
        failover_started_ts=90.0,
        downtime_started_ts=91.0,
        promote_started_ts=92.0,
        zk_timeline=5,
        local_timeline=5,
        durability=durability,
        durability_quorums=(durability,),
        failed_primary='old-primary',
        electorate=('host1', 'host2'),
        failover_version='version-1',
        current_time=100.0,
    )
    if (
        'switchover_source_timeline' in changes
        and 'switchover_target_timeline' in changes
    ):
        changes.setdefault('switchover_handoff_committed', True)
    if 'votes' in changes:
        changes.setdefault(
            'vote_timelines',
            {
                host: changes.get('zk_timeline', obs.zk_timeline)
                for host in changes['votes']
            },
        )
    return replace(obs, **changes)


def _types(plan):
    return [type(command) for command in plan]


def test_unhandled_phase_returns_empty_plan():
    assert _plan(FailoverCoordinatorMachine(), _obs(phase=None)) == []


def test_coordinator_decision_does_not_own_iteration():
    assert FailoverCoordinatorMachine().decide(_obs()).owns_iteration is False


def test_voting_starts_timers():
    plan = _plan(FailoverCoordinatorMachine(), _obs(
        FailoverPhase.VOTING,
        failover_started_ts=None,
        downtime_started_ts=None,
    ))
    assert _types(plan) == [
        StartTimer,
        StartTimer,
    ]


def test_voting_waits_until_timeline_is_authorized():
    plan = _plan(FailoverCoordinatorMachine(), _obs(
        FailoverPhase.VOTING,
        zk_timeline=None,
        failover_started_ts=None,
        downtime_started_ts=None,
    ))

    assert _types(plan) == [StartTimer, StartTimer]


def test_manual_data_loss_waits_for_operator_winner():
    plan = _plan(FailoverCoordinatorMachine(), _obs(
        FailoverPhase.VOTING,
        votes={'host1': 100},
        manual_data_loss=True,
        vote_timelines={'host1': 6},
    ))

    assert not any(isinstance(command, FailoverTransitionTo) for command in plan)


def test_manual_data_loss_advances_with_only_selected_vote():
    plan = _plan(FailoverCoordinatorMachine(), _obs(
        FailoverPhase.VOTING,
        votes={'host1': 100},
        manual_data_loss=True,
        manual_winner='host1',
        vote_timelines={'host1': 6},
    ))

    assert plan[-1] == FailoverTransitionTo(FailoverPhase.PROMOTING)


def test_voting_keeps_started_timers():
    obs = _obs(
        FailoverPhase.VOTING,
        failover_started_ts=10.0,
        downtime_started_ts=11.0,
    )
    assert StartTimer not in _types(_plan(FailoverCoordinatorMachine(), obs))


def test_voting_waits_for_read_quorum():
    assert _plan(FailoverCoordinatorMachine(),
        _obs(
            FailoverPhase.VOTING,
            votes={'host1': 100},
            failover_started_ts=10.0,
            downtime_started_ts=10.0,
        ),
    ) == []

def test_voting_selects_highest_lsn_then_hostname():
    obs = _obs(
        FailoverPhase.VOTING,
        votes={'host1': 100, 'host2': 100},
    )
    plan = _plan(FailoverCoordinatorMachine(), obs)
    assert plan == [
        WriteElectionWinner('host1'),
        FailoverTransitionTo(FailoverPhase.PROMOTING),
    ]


def test_voting_selects_winner_only_from_stable_durability_members():
    durability = DurabilityConfig.build(['old-primary', 'host1'])
    obs = _obs(
        FailoverPhase.VOTING,
        durability=durability,
        durability_quorums=(durability,),
        electorate=('host1',),
        votes={'host1': 100, 'host2': 200},
    )

    assert _plan(FailoverCoordinatorMachine(), obs) == [
        WriteElectionWinner('host1'),
        FailoverTransitionTo(FailoverPhase.PROMOTING),
    ]


def test_voting_never_allows_winner_outside_frozen_electorate():
    durability = DurabilityConfig.build(['old-primary', 'host1'])
    obs = _obs(
        FailoverPhase.VOTING,
        durability=durability,
        durability_quorums=(durability,),
        electorate=('host1',),
        votes={'host1': 100, 'host2': 200},
    )

    assert _plan(FailoverCoordinatorMachine(), obs) == [
        WriteElectionWinner('host1'),
        FailoverTransitionTo(FailoverPhase.PROMOTING),
    ]


def test_voting_waits_without_eligible_durability_member():
    durability = DurabilityConfig.build(['old-primary'])
    obs = _obs(
        FailoverPhase.VOTING,
        durability=durability,
        durability_quorums=(durability,),
        electorate=(),
        votes={'host1': 100, 'host2': 200},
    )

    assert _plan(FailoverCoordinatorMachine(), obs) == []


def test_voting_waits_without_quorum():
    obs = _obs(FailoverPhase.VOTING, votes={'host1': 100})
    assert _plan(FailoverCoordinatorMachine(), obs) == []


def test_manual_data_loss_selects_operator_winner_without_quorum():
    obs = _obs(
        FailoverPhase.VOTING,
        votes={'host1': 90, 'host2': 100},
        vote_timelines={'host1': 6, 'host2': 5},
        manual_data_loss=True,
        manual_winner='host1',
    )

    assert _plan(FailoverCoordinatorMachine(), obs) == [
        WriteElectionWinner('host1'),
        FailoverTransitionTo(FailoverPhase.PROMOTING),
    ]


def test_voting_waits_for_old_primary_lock_before_timeout():
    machine = FailoverCoordinatorMachine(
        FailoverMachineConfig(primary_unavailability_timeout=5.0),
    )
    obs = _obs(
        FailoverPhase.VOTING,
        votes={'host1': 100, 'host2': 90},
        lock_holder='old-primary',
        failover_started_ts=98.0,
        current_time=100.0,
    )

    assert _plan(machine, obs) == []


def test_voting_force_releases_old_primary_lock_after_timeout():
    machine = FailoverCoordinatorMachine(
        FailoverMachineConfig(primary_unavailability_timeout=5.0),
    )
    obs = _obs(
        FailoverPhase.VOTING,
        votes={'host1': 100, 'host2': 90},
        lock_holder='old-primary',
        failover_started_ts=90.0,
        current_time=100.0,
    )

    assert _plan(machine, obs) == [
        ForceReleasePrimaryLock(expected_holder='old-primary'),
    ]


def test_voting_waits_for_old_primary_lock_when_force_release_is_disabled():
    machine = FailoverCoordinatorMachine(FailoverMachineConfig(
        primary_unavailability_timeout=5.0,
        force_release_primary_lock=False,
    ))
    obs = _obs(
        FailoverPhase.VOTING,
        votes={'host1': 100, 'host2': 90},
        lock_holder='old-primary',
        failover_started_ts=90.0,
        current_time=100.0,
    )

    assert _plan(machine, obs) == []


def test_voting_does_not_force_release_winner_lock():
    obs = _obs(
        FailoverPhase.VOTING,
        votes={'host1': 100, 'host2': 90},
        lock_holder='host1',
        failover_started_ts=90.0,
    )

    assert _plan(FailoverCoordinatorMachine(), obs) == [
        WriteElectionWinner('host1'),
        FailoverTransitionTo(FailoverPhase.PROMOTING),
    ]


def test_committed_handoff_keeps_target_while_its_commit_quorum_is_possible():
    target = DurabilityConfig.build(['old-primary', 'candidate', 'side1', 'side2'])
    source = DurabilityConfig.build(['old-primary', 'candidate'])
    obs = _obs(
        FailoverPhase.VOTING,
        failed_primary='candidate',
        electorate=('old-primary', 'side1', 'side2'),
        votes={
                'old-primary': 200,
                'side1': 100,
        },
        vote_timelines={'old-primary': 9, 'side1': 10},
        switchover_source_timeline=9,
        switchover_target_timeline=10,
        switchover_old_primary='old-primary',
        switchover_candidate='candidate',
        switchover_commit_members=('old-primary', 'side1', 'side2'),
        switchover_commit_required=2,
        switchover_source_durability_quorums=(source,),
        durability=target,
        durability_quorums=(target,),
    )

    assert FailoverCoordinatorMachine.authorized_timeline(obs) == 10
    assert _plan(FailoverCoordinatorMachine(), obs) == [
        WriteElectionWinner('side1'),
        FailoverTransitionTo(FailoverPhase.PROMOTING),
    ]


def test_committed_handoff_returns_to_source_when_target_commit_is_impossible():
    target = DurabilityConfig.build(['old-primary', 'candidate', 'side1', 'side2'])
    source = DurabilityConfig.build(['old-primary', 'candidate'])
    obs = _obs(
        FailoverPhase.VOTING,
        failed_primary='candidate',
        electorate=('old-primary', 'side1', 'side2'),
        votes={
            'old-primary': 200,
            'side1': 100,
            'side2': 90,
        },
        vote_timelines={
            'old-primary': 9,
            'side1': 9,
            'side2': 10,
        },
        switchover_source_timeline=9,
        switchover_target_timeline=10,
        switchover_old_primary='old-primary',
        switchover_candidate='candidate',
        switchover_commit_members=('old-primary', 'side1', 'side2'),
        switchover_commit_required=2,
        switchover_source_durability_quorums=(source,),
    )

    assert FailoverCoordinatorMachine.authorized_timeline(obs) == 9
    assert _plan(FailoverCoordinatorMachine(), obs) == [
        WriteElectionWinner('old-primary'),
        FailoverTransitionTo(FailoverPhase.PROMOTING),
    ]


def test_source_timeline_selects_fenced_old_primary_vote():
    source = DurabilityConfig.build(['old-primary', 'candidate', 'side1', 'side2'])
    target = DurabilityConfig.build(['old-primary', 'candidate', 'side1', 'side2'])
    obs = _obs(
        FailoverPhase.VOTING,
        failed_primary='candidate',
        electorate=('old-primary', 'side1', 'side2'),
        votes={
            'old-primary': 0,
            'side1': 100,
            'side2': 90,
        },
        vote_timelines={
            'old-primary': 9,
            'side1': 9,
            'side2': 9,
        },
        switchover_source_timeline=9,
        switchover_target_timeline=10,
        switchover_old_primary='old-primary',
        switchover_candidate='candidate',
        switchover_commit_members=('old-primary', 'side1', 'side2'),
        switchover_commit_required=2,
        switchover_source_durability_quorums=(source,),
    )

    assert FailoverCoordinatorMachine.authorized_timeline(obs) == 9
    assert _plan(FailoverCoordinatorMachine(), obs) == [
        WriteElectionWinner('old-primary'),
        FailoverTransitionTo(FailoverPhase.PROMOTING),
    ]


def test_source_timeline_elects_safe_side_when_old_primary_has_no_vote():
    source = DurabilityConfig.build(['old-primary', 'candidate', 'side1', 'side2'])
    target = DurabilityConfig.build(['old-primary', 'candidate', 'side1', 'side2'])
    obs = _obs(
        FailoverPhase.VOTING,
        failed_primary='candidate',
        electorate=('old-primary', 'side1', 'side2'),
        votes={'side1': 100, 'side2': 90},
        vote_timelines={'side1': 9, 'side2': 9},
        switchover_source_timeline=9,
        switchover_target_timeline=10,
        switchover_old_primary='old-primary',
        switchover_candidate='candidate',
        switchover_commit_members=('old-primary', 'side1', 'side2'),
        switchover_commit_required=2,
        switchover_source_durability_quorums=(source,),
    )

    assert FailoverCoordinatorMachine.authorized_timeline(obs) == 9
    assert _plan(FailoverCoordinatorMachine(), obs) == [
        WriteElectionWinner('side1'),
        FailoverTransitionTo(FailoverPhase.PROMOTING),
    ]


def test_source_timeline_waits_without_every_source_read_quorum():
    source = DurabilityConfig.build([
        'old-primary', 'candidate', 'side1', 'side2', 'side3',
    ])
    target = DurabilityConfig.build(['old-primary', 'candidate', 'side1', 'side2'])
    obs = _obs(
        FailoverPhase.VOTING,
        failed_primary='candidate',
        electorate=('old-primary', 'side1', 'side2', 'side3'),
        votes={'side1': 100, 'side2': 90},
        vote_timelines={'side1': 9, 'side2': 9},
        switchover_source_timeline=9,
        switchover_target_timeline=10,
        switchover_old_primary='old-primary',
        switchover_candidate='candidate',
        switchover_commit_members=('old-primary', 'side1', 'side2'),
        switchover_commit_required=2,
        switchover_source_durability_quorums=(source,),
    )

    assert FailoverCoordinatorMachine.authorized_timeline(obs) == 9
    assert _plan(FailoverCoordinatorMachine(), obs) == []


def test_source_timeline_selects_safe_candidate_from_config_union():
    source_a = DurabilityConfig.build(['old-primary', 'candidate', 'side1', 'side2'])
    source_b = DurabilityConfig.build(['old-primary', 'candidate', 'side1', 'side3'])
    target = DurabilityConfig.build(['old-primary', 'candidate', 'side1', 'side2'])
    obs = _obs(
        FailoverPhase.VOTING,
        failed_primary='candidate',
        electorate=('old-primary', 'side1', 'side2', 'side3'),
        votes={'side1': 100, 'side2': 90, 'side3': 110},
        vote_timelines={'side1': 9, 'side2': 9, 'side3': 9},
        switchover_source_timeline=9,
        switchover_target_timeline=10,
        switchover_old_primary='old-primary',
        switchover_candidate='candidate',
        switchover_commit_members=('old-primary', 'side1', 'side2'),
        switchover_commit_required=2,
        switchover_source_durability_quorums=(source_a, source_b),
    )

    assert FailoverCoordinatorMachine.authorized_timeline(obs) == 9
    assert _plan(FailoverCoordinatorMachine(), obs) == [
        WriteElectionWinner('side3'),
        FailoverTransitionTo(FailoverPhase.PROMOTING),
    ]


def test_promoting_starts_timer_while_waiting_for_result():
    plan = _plan(FailoverCoordinatorMachine(), _obs(
        FailoverPhase.PROMOTING,
        promote_started_ts=None,
    ))
    assert plan == [StartTimer('failover_promote')]


def test_promoting_waits_for_winner_result_even_after_lock_acquisition():
    obs = _obs(
        FailoverPhase.PROMOTING,
        election_winner='host2',
        lock_holder='host2',
        promote_started_ts=90.0,
    )
    assert _plan(FailoverCoordinatorMachine(), obs) == []


def test_promote_timeout_transitions_to_winner_resolution():
    machine = FailoverCoordinatorMachine(FailoverMachineConfig(promote_timeout=5.0))
    obs = _obs(FailoverPhase.PROMOTING, promote_started_ts=90.0, current_time=100.0)
    assert _plan(machine, obs) == [FailoverTransitionTo(FailoverPhase.RESOLVING_WINNER)]


def test_election_timeout_transitions_to_cleanup():
    machine = FailoverCoordinatorMachine(FailoverMachineConfig(failover_timeout=5.0))
    obs = _obs(
        FailoverPhase.VOTING,
        votes={'host1': 100},
        failover_started_ts=90.0,
        current_time=100.0,
    )
    assert _plan(machine, obs) == [
        WriteLastFailedFailoverTime(),
        FailoverTransitionTo(FailoverPhase.CLEANUP),
    ]


def test_coordinator_finishes_after_winner_publishes_promoted():
    obs = _obs(FailoverPhase.PROMOTING, winner_status='promoted')
    assert _plan(FailoverCoordinatorMachine(), obs) == [
        WriteLastFailoverTime(),
        FailoverTransitionTo(FailoverPhase.CLEANUP),
    ]


def test_coordinator_resolves_winner_after_winner_publishes_failed():
    obs = _obs(FailoverPhase.PROMOTING, winner_status='failed')
    assert _plan(FailoverCoordinatorMachine(), obs) == [
        FailoverTransitionTo(FailoverPhase.RESOLVING_WINNER),
    ]


def test_winner_resolution_waits_while_election_winner_holds_primary_lock():
    obs = _obs(
        FailoverPhase.RESOLVING_WINNER,
        election_winner='host2',
        lock_holder='host2',
    )
    assert _plan(FailoverCoordinatorMachine(), obs) == []


def test_winner_resolution_enters_cleanup_after_winner_releases_primary_lock():
    obs = _obs(
        FailoverPhase.RESOLVING_WINNER,
        election_winner='host2',
        failover_started_ts=10.0,
        downtime_started_ts=11.0,
        promote_started_ts=12.0,
    )
    assert _plan(FailoverCoordinatorMachine(), obs) == [
        ClearFailoverDesiredPrimary('host2', 'version-1'),
        WriteLastFailedFailoverTime(),
        FailoverTransitionTo(FailoverPhase.CLEANUP),
    ]


def test_cleanup_stops_timers_and_cleans_failover_metadata():
    plan = _plan(FailoverCoordinatorMachine(), _obs(
        FailoverPhase.CLEANUP,
        failover_started_ts=10.0,
        downtime_started_ts=11.0,
        promote_started_ts=12.0,
    ))
    assert [command.name for command in plan if isinstance(command, StopTimer)] == [
        'downtime', 'failover', 'failover_promote',
    ]
    assert isinstance(plan[-1], CleanupFailover)
