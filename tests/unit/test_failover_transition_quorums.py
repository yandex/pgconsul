from dataclasses import replace

from src.commands import FailoverTransitionTo, WriteElectionWinner
from src.failover import FailoverCoordinatorMachine, FailoverObservation, FailoverPhase
from src.types import DurabilityConfig


def _plan(machine, observation):
    return machine.decide(observation).plan


def _observation(**changes):
    source = DurabilityConfig.build(['primary', 'a', 'b', 'c'])
    target = DurabilityConfig.build(['primary', 'a', 'b', 'd'])
    observation = FailoverObservation(
        phase=FailoverPhase.VOTING,
        my_hostname='a',
        role='replica',
        lock_holder=None,
        is_coordinator=True,
        election_winner=None,
        votes={},
        failover_started_ts=1,
        downtime_started_ts=1,
        zk_timeline=1,
        local_timeline=1,
        durability=source,
        durability_quorums=(source, target),
        failed_primary='primary',
        electorate=('a', 'b', 'c', 'd'),
        failover_version='operation',
    )
    if 'votes' in changes:
        changes.setdefault(
            'vote_timelines',
            {host: observation.zk_timeline for host in changes['votes']},
        )
    return replace(observation, **changes)


def test_transition_waits_when_source_quorum_passes_but_target_does_not():
    observation = _observation(votes={'a': 100, 'c': 100})

    assert _plan(FailoverCoordinatorMachine(), observation) == []


def test_transition_selects_source_member_safe_for_both_quorums():
    observation = _observation(votes={
            'a': 100,
            'b': 100,
            'c': 90,
            'd': 95,
    })

    assert _plan(FailoverCoordinatorMachine(), observation) == [
        WriteElectionWinner('a'),
        FailoverTransitionTo(FailoverPhase.PROMOTING),
    ]


def test_highest_safe_candidate_can_come_from_either_configuration():
    observation = _observation(votes={
            'a': 90,
            'c': 100,
            'd': 95,
    })

    assert _plan(FailoverCoordinatorMachine(), observation) == [
        WriteElectionWinner('c'),
        FailoverTransitionTo(FailoverPhase.PROMOTING),
    ]


def test_transition_can_select_target_only_member_safe_for_both_quorums():
    source = DurabilityConfig.build(['primary', 'a', 'b'])
    target = DurabilityConfig.build(['primary', 'a', 'b', 'd'])
    observation = _observation(
        durability=source,
        durability_quorums=(source, target),
        votes={
                'a': 100,
                'b': 100,
                'd': 110,
        },
    )

    assert _plan(FailoverCoordinatorMachine(), observation) == [
        WriteElectionWinner('d'),
        FailoverTransitionTo(FailoverPhase.PROMOTING),
    ]
