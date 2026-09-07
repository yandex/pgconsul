# encoding: utf-8
"""VOTING is a committed, unconditional failover step."""

from src.commands import PrepareFailoverVote
from src.failover import (
    FailoverObservation,
    FailoverParticipantMachine,
    FailoverPhase,
)


def _plan(machine, observation):
    return machine.decide(observation).plan


def _obs(is_coordinator):
    return FailoverObservation(
        phase=FailoverPhase.VOTING,
        my_hostname='host1',
        role='replica',
        lock_holder=None,
        is_coordinator=is_coordinator,
        election_winner=None,
        votes={},
        failover_started_ts=1.0,
        downtime_started_ts=1.0,
        zk_timeline=1,
        local_timeline=1,
        electorate=('host1',),
        failover_version='version-1',
        current_time=2.0,
    )


def test_phase_has_persistent_value():
    assert FailoverPhase.VOTING == 'voting'


def test_participant_prepares_vote_without_advancing_global_phase():
    plan = _plan(FailoverParticipantMachine(), _obs(False))
    assert plan == [PrepareFailoverVote(30.0, 'version-1')]
