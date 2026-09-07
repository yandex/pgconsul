# encoding: utf-8
"""Voting fences WAL sources before selecting a winner."""

from dataclasses import replace

from src.commands import FailoverTransitionTo, PrepareFailoverVote, WriteElectionWinner
from src.failover import (
    FailoverCoordinatorMachine,
    FailoverObservation,
    FailoverParticipantMachine,
    FailoverPhase,
)
from src.types import DurabilityConfig


def _plan(machine, observation):
    return machine.decide(observation).plan


def _obs(phase):
    return FailoverObservation(
        phase=phase,
        my_hostname='host1',
        role='replica',
        lock_holder=None,
        is_coordinator=True,
        election_winner=None,
        votes={},
        failover_started_ts=1.0,
        downtime_started_ts=1.0,
        zk_timeline=5,
        local_timeline=5,
        electorate=('host1',),
        failover_version='version-1',
        current_time=2.0,
    )


def test_participant_prepares_fenced_vote_without_advancing():
    plan = _plan(FailoverParticipantMachine(), _obs(FailoverPhase.VOTING))
    assert isinstance(plan[0], PrepareFailoverVote)
    assert not any(isinstance(command, FailoverTransitionTo) for command in plan)


def test_voting_uses_frozen_electorate():
    durability = DurabilityConfig.build(['old-primary', 'host1'])
    obs = replace(
        _obs(FailoverPhase.VOTING),
        votes={'host1': 500},
        vote_timelines={'host1': 5},
        durability=durability,
        durability_quorums=(durability,),
        failed_primary='old-primary',
    )
    assert _plan(FailoverCoordinatorMachine(), obs) == [
        WriteElectionWinner('host1'),
        FailoverTransitionTo(FailoverPhase.PROMOTING),
    ]
