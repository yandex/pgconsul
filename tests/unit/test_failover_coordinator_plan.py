# encoding: utf-8
"""Pure plan tests for the failover coordinator machine."""

from dataclasses import replace

from src.commands import (
    CleanupFailover,
    CleanupVotes,
    DisableWalReceiver,
    FailoverTransitionTo,
    StartTimer,
    StopTimer,
    WriteElectionVote,
    WriteElectionWinner,
)
from src.failover import (
    FailoverCoordinatorMachine,
    FailoverMachineConfig,
    FailoverObservation,
    FailoverPhase,
)
from src.types import DurabilityConfig


def _obs(phase=FailoverPhase.GATES_PASSED, **changes):
    obs = FailoverObservation(
        phase=phase,
        my_hostname='host1',
        role='replica',
        lock_holder=None,
        is_coordinator=True,
        election_winner=None,
        votes={},
        alive_hosts=['host1', 'host2'],
        replics_info=[
            {'application_name': 'host1', 'state': 'streaming'},
            {'application_name': 'host2', 'state': 'streaming'},
        ],
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
        allow_data_loss=True,
        quorum_size=2,
        durability=DurabilityConfig.build(['old-primary', 'host1', 'host2']),
        current_time=100.0,
    )
    return replace(obs, **changes)


def _types(plan):
    return [type(command) for command in plan]


def test_unhandled_phase_returns_empty_plan():
    assert FailoverCoordinatorMachine().plan(_obs(phase=None)) == []


def test_can_start_failover_when_gates_and_promote_safety_pass():
    obs = _obs(allow_data_loss=False)
    assert FailoverCoordinatorMachine().can_start_failover(obs)


def test_cannot_start_failover_when_primary_is_reachable():
    obs = _obs(is_primary_unreachable=False)
    assert not FailoverCoordinatorMachine().can_start_failover(obs)


def test_cannot_start_failover_without_sync_durability_when_data_loss_disallowed():
    obs = _obs(allow_data_loss=False, durability=None)
    assert not FailoverCoordinatorMachine().can_start_failover(obs)


def test_promote_safety_uses_derived_any_required():
    members = ['old-primary', 'host1', 'host2', 'host3', 'host4']
    obs = _obs(
        allow_data_loss=False,
        durability=DurabilityConfig.build(members),
        alive_hosts=['host1', 'host2'],
        replics_info=[
            {'application_name': host, 'state': 'streaming'}
            for host in ('host1', 'host2')
        ],
    )

    assert not FailoverCoordinatorMachine().can_start_failover(obs)


def test_walreceiver_disabling_starts_timers_and_advances():
    plan = FailoverCoordinatorMachine().plan(_obs(FailoverPhase.WALRECEIVER_DISABLING))
    assert _types(plan) == [
        StartTimer,
        StartTimer,
        DisableWalReceiver,
        FailoverTransitionTo,
    ]
    assert plan[-1].phase == FailoverPhase.GATES_PASSED


def test_walreceiver_disabling_keeps_started_timers():
    obs = _obs(
        FailoverPhase.WALRECEIVER_DISABLING,
        failover_started_ts=10.0,
        downtime_started_ts=11.0,
    )
    assert StartTimer not in _types(FailoverCoordinatorMachine().plan(obs))


def test_gates_passed_cleans_votes_opens_registration_and_votes():
    plan = FailoverCoordinatorMachine().plan(_obs())
    assert _types(plan) == [CleanupVotes, FailoverTransitionTo, WriteElectionVote]
    assert plan[1].phase == FailoverPhase.REGISTRATION


def test_gates_passed_does_not_vote_without_lsn():
    plan = FailoverCoordinatorMachine().plan(_obs(host_lsn=None))
    assert _types(plan) == [CleanupVotes, FailoverTransitionTo]


def test_registration_waits_for_all_alive_votes():
    assert FailoverCoordinatorMachine().plan(
        _obs(FailoverPhase.REGISTRATION, votes={'host1': (100, 1)}),
    ) == []


def test_registration_advances_when_all_alive_hosts_voted():
    obs = _obs(
        FailoverPhase.REGISTRATION,
        votes={'host1': (100, 1), 'host2': (90, 2)},
    )
    assert FailoverCoordinatorMachine().plan(obs) == [
        FailoverTransitionTo(FailoverPhase.VOTING),
    ]


def test_voting_selects_highest_lsn_then_priority():
    obs = _obs(
        FailoverPhase.VOTING,
        votes={'host1': (100, 1), 'host2': (100, 2)},
    )
    plan = FailoverCoordinatorMachine().plan(obs)
    assert plan == [
        WriteElectionWinner('host2'),
        FailoverTransitionTo(FailoverPhase.WINNER_SELECTED),
    ]


def test_voting_selects_winner_only_from_stable_durability_members():
    obs = _obs(
        FailoverPhase.VOTING,
        allow_data_loss=False,
        durability=DurabilityConfig.build(['old-primary', 'host1']),
        votes={'host1': (100, 1), 'host2': (200, 1)},
    )

    assert FailoverCoordinatorMachine().plan(obs) == [
        WriteElectionWinner('host1'),
        FailoverTransitionTo(FailoverPhase.WINNER_SELECTED),
    ]


def test_voting_allows_winner_outside_durability_when_data_loss_is_allowed():
    obs = _obs(
        FailoverPhase.VOTING,
        allow_data_loss=True,
        durability=DurabilityConfig.build(['old-primary', 'host1']),
        votes={'host1': (100, 1), 'host2': (200, 1)},
    )

    assert FailoverCoordinatorMachine().plan(obs) == [
        WriteElectionWinner('host2'),
        FailoverTransitionTo(FailoverPhase.WINNER_SELECTED),
    ]


def test_voting_fails_without_eligible_durability_member():
    obs = _obs(
        FailoverPhase.VOTING,
        allow_data_loss=False,
        durability=DurabilityConfig.build(['old-primary']),
        votes={'host1': (100, 1), 'host2': (200, 1)},
    )

    assert FailoverCoordinatorMachine().plan(obs) == [
        FailoverTransitionTo(FailoverPhase.FAILED),
    ]


def test_voting_fails_without_quorum():
    obs = _obs(FailoverPhase.VOTING, votes={'host1': (100, 1)})
    assert FailoverCoordinatorMachine().plan(obs) == [
        FailoverTransitionTo(FailoverPhase.FAILED),
    ]


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
