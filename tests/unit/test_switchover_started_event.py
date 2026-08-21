"""Red test: candidate machine must emit 'SWITCHOVER STARTED' Log event.

Reproduces ssn_before_promote.feature:232 failure — the behave test asserts
that the candidate's pgconsul log contains "SWITCHOVER STARTED" as the first
message in an ordered sequence.  During the ADR-0006 migration the
``log_event('SWITCHOVER STARTED')`` call that lived in the old
``_accept_switchover()`` was lost; the new ``CandidateSwitchoverMachine``
never emits it.

This test must fail (red) against the unfixed code and pass (green) after
the fix adds a ``Log(message='SWITCHOVER STARTED', event=True)`` command to
``plan_initiated``.
"""

from src.commands import Log, TransitionTo
from src.switchover import (
    SwitchoverPhase,
)

from tests.unit.test_candidate_switchover_plan import _make_machine, _make_obs


class TestSwitchoverStartedEvent:
    """Candidate must emit 'SWITCHOVER STARTED' on first plan_initiated call."""

    def test_plan_initiated_emits_switchover_started_log(self):
        """plan_initiated must include a Log(event=True) with 'SWITCHOVER STARTED'.

        Behave test ssn_before_promote.feature:232 asserts the candidate log
        contains 'SWITCHOVER STARTED' before 'ACTION. Setting SSN before promote'.
        Without this event the ordered-message assertion times out (60s).
        """
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.INITIATED, all_side_replicas_turned=True)
        plan = m.plan_initiated(obs)

        log_cmds = [c for c in plan if isinstance(c, Log)]
        started_logs = [
            c for c in log_cmds
            if c.message == 'SWITCHOVER STARTED' and c.event
        ]
        assert len(started_logs) == 1, (
            f"Expected exactly one Log('SWITCHOVER STARTED', event=True), "
            f"got {started_logs} in plan {plan}"
        )

    def test_plan_initiated_emits_switchover_started_before_transition(self):
        """SWITCHOVER STARTED must precede TransitionTo(CANDIDATE_FOUND)."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.INITIATED, all_side_replicas_turned=True)
        plan = m.plan_initiated(obs)

        log_idx = next(
            (i for i, c in enumerate(plan)
             if isinstance(c, Log) and c.message == 'SWITCHOVER STARTED'),
            None,
        )
        transition_idx = next(
            (i for i, c in enumerate(plan)
             if c == TransitionTo(SwitchoverPhase.CANDIDATE_FOUND)),
            None,
        )
        assert log_idx is not None, "SWITCHOVER STARTED Log not found in plan"
        assert transition_idx is not None, "TransitionTo(CANDIDATE_FOUND) not found"
        assert log_idx < transition_idx, (
            "SWITCHOVER STARTED must come before TransitionTo(CANDIDATE_FOUND)"
        )

    def test_plan_initiated_emits_switchover_started_without_side_replicas(self):
        """Even with no side replicas, SWITCHOVER STARTED must be emitted."""
        m = _make_machine()
        obs = _make_obs(SwitchoverPhase.INITIATED, side_replicas=())
        plan = m.plan_initiated(obs)

        log_cmds = [c for c in plan if isinstance(c, Log)]
        started_logs = [
            c for c in log_cmds
            if c.message == 'SWITCHOVER STARTED' and c.event
        ]
        assert len(started_logs) == 1, (
            f"Expected SWITCHOVER STARTED even without side replicas, got {plan}"
        )
