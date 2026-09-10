# encoding: utf-8
"""Test: walreceiver disabled before voting; get_wal_receive_lsn falls back (MDB-41951).

The phase order is:
    DETECTED → WALRECEIVER_DISABLING → GATES_PASSED → REGISTRATION → VOTING → WINNER_SELECTED

Walreceiver is disabled BEFORE voting. This ensures the old primary can no
longer get a synchronous write acknowledged once failover has started
(regression test: failover_with_network_inconsistency.feature:80).

When use_lwaldump=True, lwaldump() crashes the DB session after walreceiver
is disabled. get_wal_receive_lsn() catches PostgresConnectionError, reconnects,
and falls back to pg_last_wal_receive_lsn() which works without an active
walreceiver. See test_failover_lsn_fallback_after_walreceiver_disable.py for
the fallback unit tests.

This test asserts the phase ordering: plan_detected must transition to
WALRECEIVER_DISABLING (not REGISTRATION), and DisableWalReceiver must run
before any WriteElectionVote.
"""

import time

from src.commands import (
    DisableWalReceiver,
    FailoverTransitionTo,
    WriteElectionStatus,
    WriteElectionVote,
)
from src.failover import (
    FailoverCoordinatorMachine,
    FailoverMachineConfig,
    FailoverObservation,
    FailoverPhase,
    FailoverRecord,
)
from src.failover.coordinator import STATUS_REGISTRATION


def _make_obs(
    phase=FailoverPhase.DETECTED,
    host_lsn=100,
    host_priority=1,
    allow_data_loss=True,
    alive_hosts=None,
    votes=None,
    quorum_size=2,
):
    """Build a minimal FailoverObservation for testing."""
    record = FailoverRecord(phase=phase)
    return FailoverObservation(
        record=record,
        my_hostname='host1',
        role='replica',
        fallback_role=None,
        lock_holder=None,
        is_coordinator=True,
        election_status=None,
        election_winner=None,
        votes=votes or {},
        ha_replics=frozenset({'host2', 'host3'}),
        alive_hosts=alive_hosts if alive_hosts is not None else ['host2', 'host3'],
        replics_info=[
            {'application_name': 'host2', 'state': 'streaming'},
        ],
        host_lsn=host_lsn,
        host_priority=host_priority,
        last_failover_ts=None,
        last_primary_availability_ts=0.0,
        is_primary_unreachable=True,
        is_replaying_wal=False,
        switchover_in_progress=False,
        failover_timer_started=False,
        downtime_timer_started=False,
        zk_timeline=5,
        local_timeline=5,
        allow_data_loss=allow_data_loss,
        quorum_size=quorum_size,
        autofailover=True,
        current_time=time.time(),
    )


def _cmd_types(plan):
    """Return list of command type names in a Plan."""
    return [type(cmd).__name__ for cmd in plan]


class TestWalreceiverDisabledBeforeVoting:
    """Walreceiver must be disabled BEFORE voting (MDB-41951).

    Disabling walreceiver before voting ensures the old primary can no
    longer get a synchronous write acknowledged. The LSN for voting is
    read via get_wal_receive_lsn() which falls back to
    pg_last_wal_receive_lsn() when lwaldump() crashes after disable.
    """

    def test_detected_transitions_to_walreceiver_disabling(self):
        """plan_detected must go to WALRECEIVER_DISABLING, not REGISTRATION.

        Walreceiver is disabled first; voting happens later in GATES_PASSED.
        """
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(allow_data_loss=True)
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        assert 'FailoverTransitionTo' in types
        transition = next(c for c in plan if isinstance(c, FailoverTransitionTo))
        assert transition.phase == FailoverPhase.WALRECEIVER_DISABLING, (
            'detected must transition to WALRECEIVER_DISABLING (not REGISTRATION) '
            'so walreceiver is disabled before voting'
        )

    def test_no_vote_in_detected(self):
        """plan_detected must NOT contain WriteElectionVote.

        Voting happens in GATES_PASSED, after walreceiver is disabled.
        """
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(allow_data_loss=True, host_lsn=500)
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        assert 'WriteElectionVote' not in types, (
            'plan_detected must not vote — walreceiver must be disabled first'
        )

    def test_no_disable_walreceiver_in_detected(self):
        """plan_detected must NOT contain DisableWalReceiver.

        DisableWalReceiver runs in plan_walreceiver_disabling (next phase).
        """
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(allow_data_loss=True)
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        assert 'DisableWalReceiver' not in types, (
            'plan_detected must not disable walreceiver — that happens in '
            'plan_walreceiver_disabling'
        )

    def test_gates_passed_votes_after_disable(self):
        """GATES_PASSED phase opens registration and coordinator votes.

        By this point walreceiver has already been disabled (in
        WALRECEIVER_DISABLING). get_wal_receive_lsn falls back to
        pg_last_wal_receive_lsn.
        """
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(
            phase=FailoverPhase.GATES_PASSED,
            allow_data_loss=True,
            host_lsn=500,
        )
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        assert 'CleanupVotes' in types
        assert 'WriteElectionStatus' in types
        assert 'WriteElectionVote' in types
        assert 'FailoverTransitionTo' in types
        transition = next(c for c in plan if isinstance(c, FailoverTransitionTo))
        assert transition.phase == FailoverPhase.REGISTRATION

    def test_registration_waits_for_votes_not_disables_walreceiver(self):
        """REGISTRATION phase waits for all alive hosts to vote.

        It must NOT disable walreceiver — walreceiver is already disabled
        in WALRECEIVER_DISABLING (before GATES_PASSED).
        """
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(
            phase=FailoverPhase.REGISTRATION,
            allow_data_loss=True,
            host_lsn=500,
            votes={'host1': (500, 1)},  # only coordinator voted
        )
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        assert 'DisableWalReceiver' not in types, (
            'REGISTRATION must not disable walreceiver — already disabled'
        )

    def test_registration_transitions_to_voting_when_all_voted(self):
        """REGISTRATION → VOTING when all alive hosts have voted."""
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(
            phase=FailoverPhase.REGISTRATION,
            allow_data_loss=True,
            host_lsn=500,
            votes={
                'host1': (500, 1),
                'host2': (400, 2),
                'host3': (300, 3),
            },
        )
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        assert 'WriteElectionStatus' in types
        assert 'FailoverTransitionTo' in types
        transition = next(c for c in plan if isinstance(c, FailoverTransitionTo))
        assert transition.phase == FailoverPhase.VOTING
