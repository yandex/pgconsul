from dataclasses import replace

from src.commands import FailoverTransitionTo, WriteElectionWinner
from src.failover import FailoverCoordinatorMachine, FailoverObservation, FailoverPhase
from src.types import DurabilityConfig


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
        alive_hosts=None,
        replics_info=None,
        host_priority=1,
        last_failover_ts=None,
        last_primary_availability_ts=None,
        is_primary_unreachable=True,
        is_replaying_wal=False,
        failover_started_ts=1,
        downtime_started_ts=1,
        zk_timeline=1,
        local_timeline=1,
        quorum_size=2,
        durability=source,
        durability_quorums=(source, target),
        failed_primary='primary',
        electorate=('a', 'b', 'c', 'd'),
        failover_version='operation',
    )
    return replace(observation, **changes)


def test_transition_waits_when_source_quorum_passes_but_target_does_not():
    observation = _observation(votes={'a': (100, 1), 'c': (100, 1)})

    assert FailoverCoordinatorMachine().plan(observation) == []


def test_transition_selects_source_member_safe_for_both_quorums():
    observation = _observation(votes={
        'a': (100, 1),
        'b': (100, 2),
        'c': (90, 1),
        'd': (95, 1),
    })

    assert FailoverCoordinatorMachine().plan(observation) == [
        WriteElectionWinner('b'),
        FailoverTransitionTo(FailoverPhase.WINNER_SELECTED),
    ]


def test_candidate_must_dominate_a_read_quorum_of_each_configuration():
    observation = _observation(votes={
        'a': (90, 1),
        'c': (100, 1),
        'd': (200, 1),
    })

    assert FailoverCoordinatorMachine().plan(observation) == []
