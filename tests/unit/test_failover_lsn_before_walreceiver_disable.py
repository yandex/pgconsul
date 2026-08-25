# encoding: utf-8
"""The coordinator disables walreceiver before opening voting."""

from dataclasses import replace

from src.commands import (
    CleanupVotes,
    DisableWalReceiver,
    FailoverTransitionTo,
    WriteElectionVote,
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
        alive_hosts=['host1'],
        replics_info=[{'application_name': 'host1', 'state': 'streaming'}],
        host_lsn=500,
        host_priority=1,
        last_failover_ts=None,
        last_primary_availability_ts=None,
        is_primary_unreachable=True,
        is_replaying_wal=False,
        failover_started_ts=1.0,
        downtime_started_ts=1.0,
        zk_timeline=5,
        local_timeline=5,
        allow_data_loss=True,
        quorum_size=1,
        current_time=2.0,
    )


def test_walreceiver_phase_disables_before_advancing_to_gates_passed():
    plan = FailoverCoordinatorMachine().plan(_obs(FailoverPhase.WALRECEIVER_DISABLING))
    assert isinstance(plan[0], DisableWalReceiver)
    assert plan[1] == FailoverTransitionTo(FailoverPhase.GATES_PASSED)
    assert not any(isinstance(command, WriteElectionVote) for command in plan)


def test_gates_passed_opens_registration_and_votes_after_disable_phase():
    plan = FailoverCoordinatorMachine().plan(_obs(FailoverPhase.GATES_PASSED))
    assert plan == [
        CleanupVotes(),
        FailoverTransitionTo(FailoverPhase.REGISTRATION),
        WriteElectionVote(500, 1),
    ]


def test_registration_only_waits_for_votes():
    obs = replace(
        _obs(FailoverPhase.REGISTRATION),
        alive_hosts=['host1', 'host2'],
        votes={'host1': (500, 1)},
    )
    assert FailoverCoordinatorMachine().plan(obs) == []
