# encoding: utf-8
"""Unit tests for FailoverCoordinatorMachine.plan() (ADR-0007, stage 4).

Pure plan() tests: assert on the Plan composition, not on interactions.
No mocks of infrastructure — only the machine and its observation.
"""

import time

from src.commands import (
    FailoverTransitionTo,
    Log,
    StartTimer,
    StopTimer,
    WriteElectionStatus,
    WriteElectionVote,
    WriteElectionWinner,
)
from src.failover import (
    FailoverCoordinatorMachine,
    FailoverMachineConfig,
    FailoverObservation,
    FailoverPhase,
    FailoverRecord,
)
from src.failover.coordinator import STATUS_DONE, STATUS_REGISTRATION, STATUS_SELECTION


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_obs(
    phase=FailoverPhase.DETECTED,
    my_hostname='host1',
    allow_data_loss=False,
    switchover_in_progress=False,
    zk_timeline=5,
    local_timeline=5,
    last_failover_ts=None,
    last_primary_availability_ts=0.0,
    is_primary_unreachable=True,
    is_replaying_wal=False,
    replics_info=None,
    alive_hosts=None,
    votes=None,
    quorum_size=2,
    host_lsn=100,
    host_priority=1,
    failover_timer_started=False,
    downtime_timer_started=False,
    lock_holder=None,
    autofailover=True,
    promote_started_ts=None,
):
    """Build a minimal FailoverObservation for testing."""
    record = FailoverRecord(phase=phase)
    return FailoverObservation(
        record=record,
        my_hostname=my_hostname,
        role='replica',
        fallback_role=None,
        lock_holder=lock_holder,
        is_coordinator=True,
        election_status=None,
        election_winner=None,
        votes=votes or {},
        ha_replics=frozenset({'host2', 'host3'}),
        alive_hosts=alive_hosts if alive_hosts is not None else ['host2', 'host3'],
        replics_info=replics_info if replics_info is not None else [
            {'application_name': 'host2', 'state': 'streaming'},
        ],
        host_lsn=host_lsn,
        host_priority=host_priority,
        last_failover_ts=last_failover_ts,
        last_primary_availability_ts=last_primary_availability_ts,
        is_primary_unreachable=is_primary_unreachable,
        is_replaying_wal=is_replaying_wal,
        switchover_in_progress=switchover_in_progress,
        failover_timer_started=failover_timer_started,
        downtime_timer_started=downtime_timer_started,
        zk_timeline=zk_timeline,
        local_timeline=local_timeline,
        allow_data_loss=allow_data_loss,
        quorum_size=quorum_size,
        autofailover=autofailover,
        promote_started_ts=promote_started_ts,
    )


def _cmd_types(plan):
    """Return list of command type names in a Plan."""
    return [type(cmd).__name__ for cmd in plan]


# ---------------------------------------------------------------------------
# plan() dispatch
# ---------------------------------------------------------------------------


class TestPlanDispatch:
    def test_empty_plan_for_unhandled_phase(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(phase=FailoverPhase.CREATING_SLOTS)
        assert machine.plan(obs) == []

    def test_empty_plan_for_none_phase(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(phase=None)
        assert machine.plan(obs) == []


# ---------------------------------------------------------------------------
# plan_detected — gates
# ---------------------------------------------------------------------------


class TestPlanDetected:
    def test_gates_pass_transitions_to_walreceiver_disabling(self):
        # DETECTED checks gates and transitions to WALRECEIVER_DISABLING.
        # Walreceiver is disabled before voting; get_wal_receive_lsn falls
        # back to pg_last_wal_receive_lsn when lwaldump crashes (MDB-41951).
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(allow_data_loss=True)
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        assert 'FailoverTransitionTo' in types
        transition = next(c for c in plan if isinstance(c, FailoverTransitionTo))
        assert transition.phase == FailoverPhase.WALRECEIVER_DISABLING

    def test_starts_timers_when_not_started(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(allow_data_loss=True)
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        assert types.count('StartTimer') == 2

    def test_skips_timers_when_already_started(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(
            allow_data_loss=True,
            failover_timer_started=True,
            downtime_timer_started=True,
        )
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        assert 'StartTimer' not in types

    def test_empty_plan_when_autofailover_disabled(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(autofailover=False, switchover_in_progress=False)
        assert machine.plan(obs) == []

    def test_gates_pass_with_autofailover_and_no_data_loss(self):
        """Reproduces MDB-41951: autofailover=yes + allow_potential_data_loss=no.

        The autofailover gate must check the ``autofailover`` config, not
        ``allow_potential_data_loss``.  When autofailover is enabled the
        coordinator must proceed to WALRECEIVER_DISABLING even if
        allow_data_loss is False — the data-loss flag only affects the
        promote-safe gate later.
        """
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(autofailover=True, allow_data_loss=False)
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        assert 'FailoverTransitionTo' in types
        transition = next(c for c in plan if isinstance(c, FailoverTransitionTo))
        assert transition.phase == FailoverPhase.WALRECEIVER_DISABLING

    def test_empty_plan_on_timeline_mismatch(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(allow_data_loss=True, zk_timeline=5, local_timeline=6)
        assert machine.plan(obs) == []

    def test_empty_plan_when_primary_still_reachable(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(allow_data_loss=True, is_primary_unreachable=False)
        assert machine.plan(obs) == []

    def test_empty_plan_when_replaying_wal(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(allow_data_loss=True, is_replaying_wal=True)
        assert machine.plan(obs) == []

    def test_empty_plan_when_last_failover_too_recent(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(
            allow_data_loss=True,
            last_failover_ts=999999999999.0,  # Far future
        )
        assert machine.plan(obs) == []

    def test_failed_when_no_alive_hosts(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(allow_data_loss=True, alive_hosts=[])
        plan = machine.plan(obs)
        assert len(plan) == 1
        assert isinstance(plan[0], FailoverTransitionTo)
        assert plan[0].phase == FailoverPhase.FAILED

    def test_switchover_in_progress_skips_unreachable_gate(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(
            autofailover=False,
            switchover_in_progress=True,
            is_primary_unreachable=False,
        )
        plan = machine.plan(obs)
        # Gates pass (unreachable check skipped)
        assert any(isinstance(c, FailoverTransitionTo) for c in plan)


# ---------------------------------------------------------------------------
# plan_gates_passed → registration (cleanup votes, open registration, vote)
# ---------------------------------------------------------------------------


class TestPlanGatesPassedVotes:
    def test_cleanup_votes_and_open_registration(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(phase=FailoverPhase.GATES_PASSED, allow_data_loss=True)
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        assert 'CleanupVotes' in types
        assert 'WriteElectionStatus' in types
        assert 'FailoverTransitionTo' in types
        assert 'WriteElectionVote' in types

    def test_status_set_to_registration(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(phase=FailoverPhase.GATES_PASSED, allow_data_loss=True)
        plan = machine.plan(obs)
        status_cmd = next(c for c in plan if isinstance(c, WriteElectionStatus))
        assert status_cmd.status == STATUS_REGISTRATION

    def test_transition_to_registration(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(phase=FailoverPhase.GATES_PASSED, allow_data_loss=True)
        plan = machine.plan(obs)
        transition = next(c for c in plan if isinstance(c, FailoverTransitionTo))
        assert transition.phase == FailoverPhase.REGISTRATION

    def test_coordinator_votes_with_lsn(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(
            phase=FailoverPhase.GATES_PASSED,
            allow_data_loss=True,
            host_lsn=42,
            host_priority=3,
        )
        plan = machine.plan(obs)
        vote_cmd = next(c for c in plan if isinstance(c, WriteElectionVote))
        assert vote_cmd.lsn == 42
        assert vote_cmd.priority == 3

    def test_no_vote_when_host_lsn_none(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(
            phase=FailoverPhase.GATES_PASSED,
            allow_data_loss=True,
            host_lsn=None,
        )
        plan = machine.plan(obs)
        assert not any(isinstance(c, WriteElectionVote) for c in plan)


# ---------------------------------------------------------------------------
# plan_registration → voting
# ---------------------------------------------------------------------------


class TestPlanRegistration:
    def test_transitions_to_voting_when_all_voted(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(
            phase=FailoverPhase.REGISTRATION,
            alive_hosts=['host2', 'host3'],
            votes={'host2': (100, 1), 'host3': (200, 2)},
        )
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        assert 'WriteElectionStatus' in types
        assert 'FailoverTransitionTo' in types
        status_cmd = next(c for c in plan if isinstance(c, WriteElectionStatus))
        assert status_cmd.status == STATUS_SELECTION
        transition = next(c for c in plan if isinstance(c, FailoverTransitionTo))
        assert transition.phase == FailoverPhase.VOTING

    def test_empty_plan_when_not_all_voted(self):
        """When not all voted, plan is empty (wait for votes)."""
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(
            phase=FailoverPhase.REGISTRATION,
            my_hostname='host1',
            alive_hosts=['host1', 'host2', 'host3'],
            votes={'host1': (500, 1), 'host2': (100, 1)},  # host3 missing
            host_lsn=500,
        )
        # Not all voted → empty plan.
        assert machine.plan(obs) == []

    def test_empty_plan_when_no_alive_hosts(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(
            phase=FailoverPhase.REGISTRATION,
            alive_hosts=[],
            votes={},
            host_lsn=None,
        )
        assert machine.plan(obs) == []


# ---------------------------------------------------------------------------
# plan_voting → winner_selected
# ---------------------------------------------------------------------------


class TestPlanVoting:
    def test_writes_winner_and_transitions_to_winner_selected(self):
        # VOTING → WINNER_SELECTED (walreceiver already disabled before voting).
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(
            phase=FailoverPhase.VOTING,
            allow_data_loss=True,
            alive_hosts=['host2', 'host3'],
            votes={'host2': (100, 1), 'host3': (200, 2)},
            quorum_size=2,
        )
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        assert 'WriteElectionWinner' in types
        assert 'WriteElectionStatus' in types
        assert 'FailoverTransitionTo' in types
        winner_cmd = next(c for c in plan if isinstance(c, WriteElectionWinner))
        assert winner_cmd.winner == 'host3'  # Higher vote
        status_cmd = next(c for c in plan if isinstance(c, WriteElectionStatus))
        assert status_cmd.status == STATUS_DONE
        transition = next(c for c in plan if isinstance(c, FailoverTransitionTo))
        assert transition.phase == FailoverPhase.WINNER_SELECTED

    def test_failed_when_quorum_not_met(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(
            phase=FailoverPhase.VOTING,
            allow_data_loss=False,
            alive_hosts=['host2', 'host3'],
            votes={'host2': (100, 1)},  # Only 1 vote, quorum=2
            quorum_size=2,
        )
        plan = machine.plan(obs)
        assert len(plan) == 1
        assert isinstance(plan[0], FailoverTransitionTo)
        assert plan[0].phase == FailoverPhase.FAILED

    def test_failed_when_no_winner(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(
            phase=FailoverPhase.VOTING,
            allow_data_loss=True,
            alive_hosts=['host2', 'host3'],
            votes={},  # No votes
            quorum_size=0,
        )
        plan = machine.plan(obs)
        assert len(plan) == 1
        assert isinstance(plan[0], FailoverTransitionTo)
        assert plan[0].phase == FailoverPhase.FAILED

    def test_winner_is_highest_vote(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(
            phase=FailoverPhase.VOTING,
            allow_data_loss=True,
            alive_hosts=['host2', 'host3', 'host4'],
            votes={
                'host2': (100, 1),
                'host3': (300, 2),
                'host4': (200, 3),
            },
            quorum_size=3,
        )
        plan = machine.plan(obs)
        winner_cmd = next(c for c in plan if isinstance(c, WriteElectionWinner))
        assert winner_cmd.winner == 'host3'  # (300, 2) > (200, 3) > (100, 1)


# ---------------------------------------------------------------------------
# plan_winner_selected
# ---------------------------------------------------------------------------


class TestPlanWinnerSelected:
    def test_empty_plan_when_no_lock_holder(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(phase=FailoverPhase.WINNER_SELECTED, lock_holder=None)
        assert machine.plan(obs) == []

    def test_transitions_to_promoting_when_lock_holder_present(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(phase=FailoverPhase.WINNER_SELECTED, lock_holder='host2')
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        # Coordinator transitions to PROMOTING + starts promote timer.
        assert 'FailoverTransitionTo' in types
        assert 'StartTimer' in types
        transition = next(c for c in plan if isinstance(c, FailoverTransitionTo))
        assert transition.phase == FailoverPhase.PROMOTING
        timer = next(c for c in plan if isinstance(c, StartTimer))
        assert timer.name == 'failover_promote'


# ---------------------------------------------------------------------------
# plan_promoting / plan_checkpointing / plan_creating_slots — timeout gate
# ---------------------------------------------------------------------------


class TestPromoteTimeoutGate:
    """Timeout gate in plan() short-circuits to FAILED after promote_timeout."""

    def test_promoting_empty_plan_when_not_timed_out(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(
            phase=FailoverPhase.PROMOTING,
            promote_started_ts=time.time() - 10,  # 10s ago, well within 300s
        )
        assert machine.plan(obs) == []

    def test_promoting_failed_when_timed_out(self):
        cfg = FailoverMachineConfig(promote_timeout=5.0)
        machine = FailoverCoordinatorMachine(config=cfg)
        obs = _make_obs(
            phase=FailoverPhase.PROMOTING,
            promote_started_ts=time.time() - 100,  # 100s > 5s timeout
        )
        plan = machine.plan(obs)
        assert len(plan) == 1
        assert isinstance(plan[0], FailoverTransitionTo)
        assert plan[0].phase == FailoverPhase.FAILED

    def test_checkpointing_failed_when_timed_out(self):
        cfg = FailoverMachineConfig(promote_timeout=5.0)
        machine = FailoverCoordinatorMachine(config=cfg)
        obs = _make_obs(
            phase=FailoverPhase.CHECKPOINTING,
            promote_started_ts=time.time() - 100,
        )
        plan = machine.plan(obs)
        assert len(plan) == 1
        assert plan[0].phase == FailoverPhase.FAILED

    def test_creating_slots_failed_when_timed_out(self):
        cfg = FailoverMachineConfig(promote_timeout=5.0)
        machine = FailoverCoordinatorMachine(config=cfg)
        obs = _make_obs(
            phase=FailoverPhase.CREATING_SLOTS,
            promote_started_ts=time.time() - 100,
        )
        plan = machine.plan(obs)
        assert len(plan) == 1
        assert plan[0].phase == FailoverPhase.FAILED

    def test_no_timeout_when_promote_started_ts_none(self):
        """If timer was never started, timeout gate does not fire."""
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(phase=FailoverPhase.PROMOTING, promote_started_ts=None)
        assert machine.plan(obs) == []


# ---------------------------------------------------------------------------
# plan_failed
# ---------------------------------------------------------------------------


class TestPlanFailed:
    def test_emits_event_log_and_releases_lock(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(phase=FailoverPhase.FAILED)
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        # Failed: log event + release election lock + reset failover node.
        assert 'Log' in types
        assert 'ReleaseLock' in types
        assert 'ResetFailoverNode' in types
        log_cmd = next(c for c in plan if isinstance(c, Log))
        assert log_cmd.event is True

    def test_stops_promote_timer_when_running(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(phase=FailoverPhase.FAILED, promote_started_ts=time.time())
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        assert 'StopTimer' in types
        timer = next(c for c in plan if isinstance(c, StopTimer))
        assert timer.name == 'failover_promote'

    def test_no_stop_timer_when_not_running(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(phase=FailoverPhase.FAILED, promote_started_ts=None)
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        assert 'StopTimer' not in types


# ---------------------------------------------------------------------------
# plan_finished — stop promote timer
# ---------------------------------------------------------------------------


class TestPlanFinished:
    def test_stops_promote_timer_when_running(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(phase=FailoverPhase.FINISHED, promote_started_ts=time.time())
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        assert 'StopTimer' in types
        assert 'ReleaseLock' in types
        assert 'ResetFailoverNode' in types

    def test_no_stop_timer_when_not_running(self):
        machine = FailoverCoordinatorMachine()
        obs = _make_obs(phase=FailoverPhase.FINISHED, promote_started_ts=None)
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        assert 'StopTimer' not in types


# ---------------------------------------------------------------------------
# _determine_winner (pure helper)
# ---------------------------------------------------------------------------


class TestDetermineWinner:
    def test_picks_highest_vote(self):
        votes = {'host2': (100, 1), 'host3': (200, 2)}
        assert FailoverCoordinatorMachine._determine_winner(votes) == 'host3'

    def test_returns_none_for_empty(self):
        assert FailoverCoordinatorMachine._determine_winner({}) is None

    def test_returns_none_for_all_none(self):
        votes = {'host2': None, 'host3': None}
        assert FailoverCoordinatorMachine._determine_winner(votes) is None

    def test_tie_breaks_by_order(self):
        # When votes are equal, the first encountered wins (dict order).
        votes = {'host2': (100, 1), 'host3': (100, 1)}
        winner = FailoverCoordinatorMachine._determine_winner(votes)
        assert winner in ('host2', 'host3')
