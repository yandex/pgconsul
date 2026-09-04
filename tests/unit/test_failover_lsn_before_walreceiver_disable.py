# encoding: utf-8
"""The coordinator disables walreceiver before opening voting."""

from dataclasses import replace

from src.commands import (
    FailoverTransitionTo,
    PrepareFailoverVote,
)
from src.failover import (
    FailoverCoordinatorMachine,
    FailoverObservation,
    FailoverPhase,
)


def _obs(phase):
    return FailoverObservation(
        phase=phase,
        my_hostname='host1',
        role='replica',
        lock_holder=None,
        is_coordinator=True,
        election_winner=None,
        votes={},
        replics_info=[{'application_name': 'host1', 'state': 'streaming'}],
        last_failover_ts=None,
        last_primary_availability_ts=None,
        is_primary_unreachable=True,
        is_replaying_wal=False,
        failover_started_ts=1.0,
        downtime_started_ts=1.0,
        zk_timeline=5,
        local_timeline=5,
        quorum_size=1,
        electorate=('host1',),
        failover_version='version-1',
        current_time=2.0,
    )


def test_walreceiver_phase_prepares_fenced_vote_before_advancing():
    plan = FailoverCoordinatorMachine().plan(_obs(FailoverPhase.WALRECEIVER_DISABLING))
    assert isinstance(plan[0], PrepareFailoverVote)
    assert not any(isinstance(command, FailoverTransitionTo) for command in plan)


def test_gates_passed_only_opens_registration():
    plan = FailoverCoordinatorMachine().plan(_obs(FailoverPhase.GATES_PASSED))
    assert plan == [FailoverTransitionTo(FailoverPhase.REGISTRATION)]


def test_registration_uses_frozen_electorate():
    obs = replace(
        _obs(FailoverPhase.REGISTRATION),
        votes={'host1': 500},
    )
    assert FailoverCoordinatorMachine().plan(obs) == [
        FailoverTransitionTo(FailoverPhase.VOTING),
    ]
