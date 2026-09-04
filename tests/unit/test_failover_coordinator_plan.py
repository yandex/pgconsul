# encoding: utf-8
"""Pure plan tests for the failover coordinator machine."""

from dataclasses import replace

from src.commands import (
    CleanupFailover,
    FailoverTransitionTo,
    ForceReleasePrimaryLock,
    PrepareFailoverVote,
    StartTimer,
    StopTimer,
    WriteElectionWinner,
    WriteLastFailoverTime,
)
from src.failover import (
    FailoverCoordinatorMachine,
    FailoverMachineConfig,
    FailoverObservation,
    FailoverPhase,
)
from src.types import DurabilityConfig


def _obs(phase=FailoverPhase.GATES_PASSED, **changes):
    durability = DurabilityConfig.build(['old-primary', 'host1', 'host2'])
    obs = FailoverObservation(
        phase=phase,
        my_hostname='host1',
        role='replica',
        lock_holder=None,
        is_coordinator=True,
        election_winner=None,
        votes={},
        replics_info=[
            {'application_name': 'host1', 'state': 'streaming'},
            {'application_name': 'host2', 'state': 'streaming'},
        ],
        last_failover_ts=None,
        last_primary_availability_ts=None,
        is_primary_unreachable=True,
        is_replaying_wal=False,
        failover_started_ts=None,
        downtime_started_ts=None,
        zk_timeline=5,
        local_timeline=5,
        quorum_size=2,
        durability=durability,
        durability_quorums=(durability,),
        failed_primary='old-primary',
        electorate=('host1', 'host2'),
        failover_version='version-1',
        current_time=100.0,
    )
    if (
        'branch_source_timeline' in changes
        and 'branch_target_timeline' in changes
    ):
        changes.setdefault('branch_target_is_active', True)
    return replace(obs, **changes)


def _types(plan):
    return [type(command) for command in plan]


def test_unhandled_phase_returns_empty_plan():
    assert FailoverCoordinatorMachine().plan(_obs(phase=None)) == []


def test_can_start_failover_when_gates_and_promote_safety_pass():
    obs = _obs()
    assert FailoverCoordinatorMachine().can_start_failover(obs)


def test_probe_verified_entry_does_not_repeat_primary_visibility_check():
    obs = _obs(is_primary_unreachable=False)
    assert FailoverCoordinatorMachine().can_start_failover(obs)


def test_probe_verified_entry_defers_timeline_to_votes():
    assert FailoverCoordinatorMachine().can_start_failover(_obs(local_timeline=None))


def test_probe_verified_entry_does_not_use_replics_info():
    obs = _obs(replics_info=None)
    assert FailoverCoordinatorMachine().can_start_failover(obs)


def test_probe_verified_entry_does_not_repeat_promote_safety_check():
    members = ['old-primary', 'host1', 'host2', 'host3', 'host4']
    obs = _obs(
        durability=DurabilityConfig.build(members),
        replics_info=[
            {'application_name': host, 'state': 'streaming'}
            for host in ('host1', 'host2')
        ],
    )

    assert FailoverCoordinatorMachine().can_start_failover(obs)


def test_walreceiver_disabling_starts_timers_and_prepares_vote():
    plan = FailoverCoordinatorMachine().plan(_obs(FailoverPhase.WALRECEIVER_DISABLING))
    assert _types(plan) == [
        StartTimer,
        StartTimer,
        PrepareFailoverVote,
    ]


def test_walreceiver_disabling_advances_after_read_quorum_voted():
    plan = FailoverCoordinatorMachine().plan(_obs(
        FailoverPhase.WALRECEIVER_DISABLING,
        votes={'host1': 100, 'host2': 90},
    ))
    assert isinstance(plan[-1], FailoverTransitionTo)
    assert plan[-1].phase == FailoverPhase.GATES_PASSED


def test_manual_data_loss_waits_for_operator_winner():
    plan = FailoverCoordinatorMachine().plan(_obs(
        FailoverPhase.WALRECEIVER_DISABLING,
        votes={'host1': 100},
        manual_data_loss=True,
        vote_timelines={'host1': 6},
    ))

    assert not any(isinstance(command, FailoverTransitionTo) for command in plan)


def test_manual_data_loss_advances_with_only_selected_vote():
    plan = FailoverCoordinatorMachine().plan(_obs(
        FailoverPhase.WALRECEIVER_DISABLING,
        votes={'host1': 100},
        manual_data_loss=True,
        manual_winner='host1',
        vote_timelines={'host1': 6},
    ))

    assert plan[-1] == FailoverTransitionTo(FailoverPhase.GATES_PASSED)


def test_walreceiver_disabling_keeps_started_timers():
    obs = _obs(
        FailoverPhase.WALRECEIVER_DISABLING,
        failover_started_ts=10.0,
        downtime_started_ts=11.0,
    )
    assert StartTimer not in _types(FailoverCoordinatorMachine().plan(obs))


def test_gates_passed_opens_registration():
    plan = FailoverCoordinatorMachine().plan(_obs())
    assert plan == [FailoverTransitionTo(FailoverPhase.REGISTRATION)]


def test_gates_passed_does_not_read_lsn_in_planner():
    plan = FailoverCoordinatorMachine().plan(_obs())
    assert plan == [FailoverTransitionTo(FailoverPhase.REGISTRATION)]


def test_registration_waits_for_all_alive_votes():
    assert FailoverCoordinatorMachine().plan(
        _obs(FailoverPhase.REGISTRATION, votes={'host1': 100}),
    ) == []


def test_registration_advances_when_frozen_electorate_voted():
    obs = _obs(
        FailoverPhase.REGISTRATION,
        votes={'host1': 100, 'host2': 90},
    )
    assert FailoverCoordinatorMachine().plan(obs) == [
        FailoverTransitionTo(FailoverPhase.VOTING),
    ]


def test_voting_selects_highest_lsn_then_hostname():
    obs = _obs(
        FailoverPhase.VOTING,
        votes={'host1': 100, 'host2': 100},
    )
    plan = FailoverCoordinatorMachine().plan(obs)
    assert plan == [
        WriteElectionWinner('host1'),
        FailoverTransitionTo(FailoverPhase.WINNER_SELECTED),
    ]


def test_voting_selects_winner_only_from_stable_durability_members():
    durability = DurabilityConfig.build(['old-primary', 'host1'])
    obs = _obs(
        FailoverPhase.VOTING,
        durability=durability,
        durability_quorums=(durability,),
        electorate=('host1',),
        quorum_size=1,
        votes={'host1': 100, 'host2': 200},
    )

    assert FailoverCoordinatorMachine().plan(obs) == [
        WriteElectionWinner('host1'),
        FailoverTransitionTo(FailoverPhase.WINNER_SELECTED),
    ]


def test_voting_never_allows_winner_outside_frozen_electorate():
    durability = DurabilityConfig.build(['old-primary', 'host1'])
    obs = _obs(
        FailoverPhase.VOTING,
        durability=durability,
        durability_quorums=(durability,),
        electorate=('host1',),
        quorum_size=1,
        votes={'host1': 100, 'host2': 200},
    )

    assert FailoverCoordinatorMachine().plan(obs) == [
        WriteElectionWinner('host1'),
        FailoverTransitionTo(FailoverPhase.WINNER_SELECTED),
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

    assert FailoverCoordinatorMachine().plan(obs) == []


def test_voting_waits_without_quorum():
    obs = _obs(FailoverPhase.VOTING, votes={'host1': 100})
    assert FailoverCoordinatorMachine().plan(obs) == []


def test_manual_data_loss_selects_operator_winner_without_quorum():
    obs = _obs(
        FailoverPhase.VOTING,
        votes={'host1': 90, 'host2': 100},
        vote_timelines={'host1': 6, 'host2': 5},
        manual_data_loss=True,
        manual_winner='host1',
    )

    assert FailoverCoordinatorMachine().plan(obs) == [
        WriteElectionWinner('host1'),
        FailoverTransitionTo(FailoverPhase.WINNER_SELECTED),
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

    assert machine.plan(obs) == []


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

    assert machine.plan(obs) == [
        ForceReleasePrimaryLock(expected_holder='old-primary'),
    ]


def test_voting_does_not_force_release_winner_lock():
    obs = _obs(
        FailoverPhase.VOTING,
        votes={'host1': 100, 'host2': 90},
        lock_holder='host1',
        failover_started_ts=90.0,
    )

    assert FailoverCoordinatorMachine().plan(obs) == [
        WriteElectionWinner('host1'),
        FailoverTransitionTo(FailoverPhase.WINNER_SELECTED),
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
        branch_source_timeline=9,
        branch_target_timeline=10,
        branch_old_primary='old-primary',
        branch_candidate='candidate',
        branch_commit_members=('old-primary', 'side1', 'side2'),
        branch_commit_required=2,
        branch_source_durability_quorums=(source,),
        durability=target,
        durability_quorums=(target,),
    )

    assert FailoverCoordinatorMachine.authorized_timeline(obs) == 10
    assert FailoverCoordinatorMachine().plan(obs) == [
        WriteElectionWinner('side1'),
        FailoverTransitionTo(FailoverPhase.WINNER_SELECTED),
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
        branch_source_timeline=9,
        branch_target_timeline=10,
        branch_old_primary='old-primary',
        branch_candidate='candidate',
        branch_commit_members=('old-primary', 'side1', 'side2'),
        branch_commit_required=2,
        branch_source_durability_quorums=(source,),
    )

    assert FailoverCoordinatorMachine.authorized_timeline(obs) == 9
    assert FailoverCoordinatorMachine().plan(obs) == [
        WriteElectionWinner('old-primary'),
        FailoverTransitionTo(FailoverPhase.WINNER_SELECTED),
    ]


def test_patched_source_branch_selects_fenced_old_primary_vote():
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
        branch_source_timeline=9,
        branch_target_timeline=10,
        branch_old_primary='old-primary',
        branch_candidate='candidate',
        branch_commit_members=('old-primary', 'side1', 'side2'),
        branch_commit_required=2,
        branch_source_durability_quorums=(source,),
        branch_use_pg_patches=True,
    )

    assert FailoverCoordinatorMachine.authorized_timeline(obs) == 9
    assert FailoverCoordinatorMachine().plan(obs) == [
        WriteElectionWinner('old-primary'),
        FailoverTransitionTo(FailoverPhase.WINNER_SELECTED),
    ]


def test_patched_source_branch_elects_safe_side_when_old_primary_has_no_vote():
    source = DurabilityConfig.build(['old-primary', 'candidate', 'side1', 'side2'])
    target = DurabilityConfig.build(['old-primary', 'candidate', 'side1', 'side2'])
    obs = _obs(
        FailoverPhase.VOTING,
        failed_primary='candidate',
        electorate=('old-primary', 'side1', 'side2'),
        votes={'side1': 100, 'side2': 90},
        vote_timelines={'side1': 9, 'side2': 9},
        branch_source_timeline=9,
        branch_target_timeline=10,
        branch_old_primary='old-primary',
        branch_candidate='candidate',
        branch_commit_members=('old-primary', 'side1', 'side2'),
        branch_commit_required=2,
        branch_source_durability_quorums=(source,),
        branch_use_pg_patches=True,
    )

    assert FailoverCoordinatorMachine.authorized_timeline(obs) == 9
    assert FailoverCoordinatorMachine().plan(obs) == [
        WriteElectionWinner('side1'),
        FailoverTransitionTo(FailoverPhase.WINNER_SELECTED),
    ]


def test_patched_source_branch_waits_without_every_source_read_quorum():
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
        branch_source_timeline=9,
        branch_target_timeline=10,
        branch_old_primary='old-primary',
        branch_candidate='candidate',
        branch_commit_members=('old-primary', 'side1', 'side2'),
        branch_commit_required=2,
        branch_source_durability_quorums=(source,),
        branch_use_pg_patches=True,
    )

    assert FailoverCoordinatorMachine.authorized_timeline(obs) == 9
    assert FailoverCoordinatorMachine().plan(obs) == []


def test_patched_source_branch_selects_safe_candidate_from_config_union():
    source_a = DurabilityConfig.build(['old-primary', 'candidate', 'side1', 'side2'])
    source_b = DurabilityConfig.build(['old-primary', 'candidate', 'side1', 'side3'])
    target = DurabilityConfig.build(['old-primary', 'candidate', 'side1', 'side2'])
    obs = _obs(
        FailoverPhase.VOTING,
        failed_primary='candidate',
        electorate=('old-primary', 'side1', 'side2', 'side3'),
        votes={'side1': 100, 'side2': 90, 'side3': 110},
        vote_timelines={'side1': 9, 'side2': 9, 'side3': 9},
        branch_source_timeline=9,
        branch_target_timeline=10,
        branch_old_primary='old-primary',
        branch_candidate='candidate',
        branch_commit_members=('old-primary', 'side1', 'side2'),
        branch_commit_required=2,
        branch_source_durability_quorums=(source_a, source_b),
        branch_use_pg_patches=True,
    )

    assert FailoverCoordinatorMachine.authorized_timeline(obs) == 9
    assert FailoverCoordinatorMachine().plan(obs) == [
        WriteElectionWinner('side3'),
        FailoverTransitionTo(FailoverPhase.WINNER_SELECTED),
    ]


def test_mixed_timeline_election_never_assigns_default_timeline_to_source_vote():
    source = DurabilityConfig.build(['old-primary', 'candidate'])
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
        vote_timelines={'side1': 9, 'side2': 9},
        branch_source_timeline=9,
        branch_target_timeline=10,
        branch_old_primary='old-primary',
        branch_candidate='candidate',
        branch_commit_members=('old-primary', 'side1', 'side2'),
        branch_commit_required=2,
        branch_source_durability_quorums=(source,),
        branch_use_pg_patches=True,
    )

    assert FailoverCoordinatorMachine.authorized_timeline(obs) == 9
    assert FailoverCoordinatorMachine().plan(obs) == []


def test_winner_selected_starts_timer_while_waiting_for_lock():
    plan = FailoverCoordinatorMachine().plan(_obs(FailoverPhase.WINNER_SELECTED))
    assert plan == [StartTimer('failover_promote')]


def test_winner_selected_advances_when_winner_has_lock():
    obs = _obs(
        FailoverPhase.WINNER_SELECTED,
        election_winner='host2',
        lock_holder='host2',
        promote_started_ts=90.0,
    )
    assert FailoverCoordinatorMachine().plan(obs) == [
        FailoverTransitionTo(FailoverPhase.PROMOTING),
    ]


def test_promote_timeout_transitions_to_failed():
    machine = FailoverCoordinatorMachine(FailoverMachineConfig(promote_timeout=5.0))
    obs = _obs(FailoverPhase.PROMOTING, promote_started_ts=90.0, current_time=100.0)
    assert machine.plan(obs) == [FailoverTransitionTo(FailoverPhase.FAILED)]


def test_coordinator_finishes_after_winner_publishes_promoted():
    obs = _obs(FailoverPhase.PROMOTING, winner_status='promoted')
    assert FailoverCoordinatorMachine().plan(obs) == [
        WriteLastFailoverTime(),
        FailoverTransitionTo(FailoverPhase.FINISHED),
    ]


def test_coordinator_fails_after_winner_publishes_failed():
    obs = _obs(FailoverPhase.PROMOTING, winner_status='failed')
    assert FailoverCoordinatorMachine().plan(obs) == [
        FailoverTransitionTo(FailoverPhase.FAILED),
    ]


def test_failed_waits_while_election_winner_holds_primary_lock():
    obs = _obs(
        FailoverPhase.FAILED,
        election_winner='host2',
        lock_holder='host2',
    )
    assert FailoverCoordinatorMachine().plan(obs) == []


def test_failed_cleans_up_after_winner_releases_primary_lock():
    obs = _obs(
        FailoverPhase.FAILED,
        election_winner='host2',
        failover_started_ts=10.0,
        downtime_started_ts=11.0,
        promote_started_ts=12.0,
    )
    plan = FailoverCoordinatorMachine().plan(obs)
    assert [command.name for command in plan if isinstance(command, StopTimer)] == [
        'downtime', 'failover', 'failover_promote',
    ]
    assert isinstance(plan[-1], CleanupFailover)


def test_interrupted_cleanup_waits_for_winner_lock():
    obs = _obs(
        FailoverPhase.FAILED,
        must_reset=True,
        election_winner='host2',
        lock_holder='host2',
    )
    assert FailoverCoordinatorMachine().plan(obs) == []


def test_finished_cleans_failover_metadata():
    plan = FailoverCoordinatorMachine().plan(_obs(FailoverPhase.FINISHED))
    assert isinstance(plan[-1], CleanupFailover)
