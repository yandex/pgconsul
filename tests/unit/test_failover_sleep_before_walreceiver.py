# encoding: utf-8
"""Tests for the debug sleep before disabling walreceiver."""

from src.commands import Log, PrepareFailoverVote, Sleep
from src.failover import (
    FailoverCoordinatorMachine,
    FailoverMachineConfig,
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
        allow_data_loss=True,
        quorum_size=1,
        electorate=('host1',),
        failover_version='version-1',
        current_time=2.0,
    )


def _assert_sleep_before_disable(plan):
    types = [type(command) for command in plan]
    assert Log in types
    assert Sleep in types
    assert types.index(Log) < types.index(PrepareFailoverVote)
    assert types.index(Sleep) < types.index(PrepareFailoverVote)
    sleep = next(command for command in plan if isinstance(command, Sleep))
    assert sleep.seconds == 5.0


def test_coordinator_sleeps_before_disabling_walreceiver():
    machine = FailoverCoordinatorMachine(
        FailoverMachineConfig(sleep_before_disable_walreceiver=5.0),
    )
    _assert_sleep_before_disable(machine.plan(_obs(True)))


def test_participant_sleeps_before_disabling_walreceiver():
    machine = FailoverParticipantMachine(
        FailoverMachineConfig(sleep_before_disable_walreceiver=5.0),
    )
    _assert_sleep_before_disable(machine.plan(_obs(False)))


def test_zero_sleep_adds_no_log_or_sleep():
    for machine, coordinator in (
        (FailoverCoordinatorMachine(), True),
        (FailoverParticipantMachine(), False),
    ):
        plan = machine.plan(_obs(coordinator))
        assert not any(isinstance(command, (Log, Sleep)) for command in plan)
