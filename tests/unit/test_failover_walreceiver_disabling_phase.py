# encoding: utf-8
"""WALRECEIVER_DISABLING is a committed, unconditional failover step."""

from dataclasses import replace

from src.commands import PrepareFailoverVote
from src.failover import (
    FailoverCoordinatorMachine,
    FailoverObservation,
    FailoverParticipantMachine,
    FailoverPhase,
)


def _obs(is_coordinator):
    return FailoverObservation(
        phase=FailoverPhase.WALRECEIVER_DISABLING,
        my_hostname='host1',
        role='replica',
        lock_holder=None,
        is_coordinator=is_coordinator,
        election_winner=None,
        votes={},
        alive_hosts=['host1'],
        replics_info=[],
        host_priority=1,
        last_failover_ts=None,
        last_primary_availability_ts=None,
        is_primary_unreachable=True,
        is_replaying_wal=False,
        failover_started_ts=1.0,
        downtime_started_ts=1.0,
        zk_timeline=1,
        local_timeline=1,
        quorum_size=1,
        electorate=('host1',),
        failover_version='version-1',
        current_time=2.0,
    )


def test_phase_has_persistent_value():
    assert FailoverPhase.WALRECEIVER_DISABLING == 'walreceiver_disabling'


def test_coordinator_prepares_fenced_vote():
    plan = FailoverCoordinatorMachine().plan(_obs(True))
    assert isinstance(plan[0], PrepareFailoverVote)


def test_participant_prepares_vote_without_advancing_global_phase():
    plan = FailoverParticipantMachine().plan(_obs(False))
    assert plan == [PrepareFailoverVote(1, 30.0, 'version-1', 1)]


def test_coordinator_does_not_recheck_primary_reachability_after_entry():
    obs = replace(_obs(True), is_primary_unreachable=False)
    plan = FailoverCoordinatorMachine().plan(obs)
    assert any(isinstance(command, PrepareFailoverVote) for command in plan)


def test_participant_does_not_recheck_primary_reachability_after_entry():
    obs = replace(_obs(False), is_primary_unreachable=False)
    plan = FailoverParticipantMachine().plan(obs)
    assert any(isinstance(command, PrepareFailoverVote) for command in plan)
