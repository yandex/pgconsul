# encoding: utf-8
"""Unit tests for FailoverParticipantMachine.plan() (ADR-0007, stage 3).

Pure plan() tests: assert on the Plan composition, not on interactions.
No mocks of infrastructure — only the machine and its observation.
"""

import time
from unittest.mock import MagicMock

from src.commands import (
    AcquireLock,
    ClearLocalState,
    DoFailover,
    FailoverTransitionTo,
    Log,
    StopTimer,
    WriteElectionVote,
    WriteLastFailoverTime,
)
from src.failover import (
    FailoverMachineConfig,
    FailoverObservation,
    FailoverParticipantMachine,
    FailoverPhase,
    FailoverRecord,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_obs(
    phase=FailoverPhase.REGISTRATION,
    my_hostname='host1',
    election_winner=None,
    host_lsn=100,
    host_priority=1,
    is_replaying_wal=False,
):
    """Build a minimal FailoverObservation for testing."""
    record = FailoverRecord(phase=phase, winner=election_winner)
    return FailoverObservation(
        record=record,
        my_hostname=my_hostname,
        role='replica',
        fallback_role=None,
        lock_holder=None,
        is_coordinator=False,
        election_status=None,
        election_winner=election_winner,
        votes={},
        ha_replics=frozenset({'host2', 'host3'}),
        alive_hosts=['host2', 'host3'],
        replics_info=[],
        host_lsn=host_lsn,
        host_priority=host_priority,
        last_failover_ts=None,
        last_primary_availability_ts=None,
        is_primary_unreachable=True,
        is_replaying_wal=is_replaying_wal,
        switchover_in_progress=False,
        failover_timer_started=False,
        downtime_timer_started=False,
        zk_timeline=5,
        local_timeline=5,
        allow_data_loss=False,
        quorum_size=2,
        current_time=time.time(),
    )


def _cmd_types(plan):
    """Return list of command type names in a Plan."""
    return [type(cmd).__name__ for cmd in plan]


# ---------------------------------------------------------------------------
# plan() dispatch
# ---------------------------------------------------------------------------


class TestPlanDispatch:
    def test_empty_plan_for_unhandled_phase(self):
        machine = FailoverParticipantMachine()
        obs = _make_obs(phase=FailoverPhase.GATES_PASSED)
        assert machine.plan(obs) == []

    def test_empty_plan_for_none_phase(self):
        machine = FailoverParticipantMachine()
        obs = _make_obs(phase=None)
        assert machine.plan(obs) == []


# ---------------------------------------------------------------------------
# plan_vote (registration / voting)
# ---------------------------------------------------------------------------


class TestPlanVote:
    def test_registration_writes_vote(self):
        machine = FailoverParticipantMachine()
        obs = _make_obs(phase=FailoverPhase.REGISTRATION, host_lsn=42, host_priority=3)
        plan = machine.plan(obs)
        assert _cmd_types(plan) == ['WriteElectionVote']
        assert isinstance(plan[0], WriteElectionVote)
        assert plan[0].lsn == 42
        assert plan[0].priority == 3

    def test_voting_writes_vote(self):
        machine = FailoverParticipantMachine()
        obs = _make_obs(phase=FailoverPhase.VOTING, host_lsn=99, host_priority=2)
        plan = machine.plan(obs)
        assert _cmd_types(plan) == ['WriteElectionVote']
        assert plan[0].lsn == 99

    def test_empty_plan_when_host_lsn_none(self):
        machine = FailoverParticipantMachine()
        obs = _make_obs(phase=FailoverPhase.REGISTRATION, host_lsn=None)
        assert machine.plan(obs) == []


# ---------------------------------------------------------------------------
# plan_winner_selected — winner branch
# ---------------------------------------------------------------------------


class TestPlanWinnerSelectedWinner:
    def test_winner_acquires_lock_and_transitions_to_promoting(self):
        machine = FailoverParticipantMachine()
        obs = _make_obs(
            phase=FailoverPhase.WINNER_SELECTED,
            my_hostname='host1',
            election_winner='host1',
        )
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        # winner_selected: clear local progress + AcquireLock + PROMOTING.
        # DoFailover runs in plan_promoting (next phase).
        assert types == ['ClearLocalState', 'AcquireLock', 'FailoverTransitionTo']
        assert plan[0] == ClearLocalState('failover_participant')
        assert isinstance(plan[1], AcquireLock)
        assert plan[1].timeout == 0
        assert isinstance(plan[2], FailoverTransitionTo)
        assert plan[2].phase == FailoverPhase.PROMOTING

    def test_winner_empty_plan_when_replaying_wal(self):
        machine = FailoverParticipantMachine()
        obs = _make_obs(
            phase=FailoverPhase.WINNER_SELECTED,
            my_hostname='host1',
            election_winner='host1',
            is_replaying_wal=True,
        )
        assert machine.plan(obs) == []

    def test_winner_empty_plan_when_no_winner(self):
        machine = FailoverParticipantMachine()
        obs = _make_obs(
            phase=FailoverPhase.WINNER_SELECTED,
            election_winner=None,
        )
        assert machine.plan(obs) == []

    def test_debug_failure_before_acquire_returns_empty(self):
        machine = FailoverParticipantMachine(
            debug_failure=lambda name: name == 'participant_before_acquire',
        )
        obs = _make_obs(
            phase=FailoverPhase.WINNER_SELECTED,
            my_hostname='host1',
            election_winner='host1',
        )
        assert machine.plan(obs) == []

    def test_debug_failure_before_promote_transitions_to_failed(self):
        """Debug failure in plan_promoting (not plan_winner_selected)."""
        machine = FailoverParticipantMachine(
            debug_failure=lambda name: name == 'participant_before_promote',
        )
        obs = _make_obs(
            phase=FailoverPhase.PROMOTING,
            my_hostname='host1',
            election_winner='host1',
        )
        plan = machine.plan(obs)
        types = _cmd_types(plan)
        assert types == ['FailoverTransitionTo']
        assert isinstance(plan[0], FailoverTransitionTo)
        assert plan[0].phase == FailoverPhase.FAILED


# ---------------------------------------------------------------------------
# plan_winner_selected — loser branch
# ---------------------------------------------------------------------------


class TestPlanWinnerSelectedLoser:
    def test_loser_emits_log(self):
        machine = FailoverParticipantMachine()
        obs = _make_obs(
            phase=FailoverPhase.WINNER_SELECTED,
            my_hostname='host1',
            election_winner='host2',
        )
        plan = machine.plan(obs)
        assert len(plan) == 1
        assert isinstance(plan[0], Log)
        assert plan[0].event is True
        assert 'host2' in plan[0].message


class TestPlanPromotingWinner:
    def test_reacquires_lock_runs_local_pipeline_and_finishes(self):
        machine = FailoverParticipantMachine()
        obs = _make_obs(
            phase=FailoverPhase.PROMOTING,
            my_hostname='host1',
            election_winner='host1',
        )

        plan = machine.plan(obs)

        assert _cmd_types(plan) == [
            'AcquireLock',
            'DoFailover',
            'WriteLastFailoverTime',
            'StopTimer',
            'FailoverTransitionTo',
            'ClearLocalState',
        ]
        assert plan[-2] == FailoverTransitionTo(FailoverPhase.FINISHED)
        assert plan[-1] == ClearLocalState('failover_participant')


# ---------------------------------------------------------------------------
# plan_finished
# ---------------------------------------------------------------------------


class TestPlanFinished:
    def test_loser_emits_log(self):
        machine = FailoverParticipantMachine()
        obs = _make_obs(
            phase=FailoverPhase.FINISHED,
            my_hostname='host1',
            election_winner='host2',
        )
        plan = machine.plan(obs)
        assert len(plan) == 1
        assert isinstance(plan[0], Log)

    def test_winner_empty_plan(self):
        machine = FailoverParticipantMachine()
        obs = _make_obs(
            phase=FailoverPhase.FINISHED,
            my_hostname='host1',
            election_winner='host1',
        )
        assert machine.plan(obs) == []

    def test_empty_plan_when_no_winner(self):
        machine = FailoverParticipantMachine()
        obs = _make_obs(phase=FailoverPhase.FINISHED, election_winner=None)
        assert machine.plan(obs) == []


# ---------------------------------------------------------------------------
# plan_failed
# ---------------------------------------------------------------------------


class TestPlanFailed:
    def test_emits_event_log(self):
        machine = FailoverParticipantMachine()
        obs = _make_obs(phase=FailoverPhase.FAILED)
        plan = machine.plan(obs)
        assert len(plan) == 1
        assert isinstance(plan[0], Log)
        assert plan[0].event is True
