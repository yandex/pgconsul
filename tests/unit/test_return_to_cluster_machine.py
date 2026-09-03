# encoding: utf-8
"""
Unit tests for decide_return_action (MDB-41951, ADR-0006).

Tests cover the pure decision: SIMPLE_SWITCH or REWIND, derived from the
observation. The function is stateless — action is re-derived each call.
"""


from src.return_to_cluster import (
    ReturnAction,
    ReturnIterationObservation,
    ReturnObservation,
    ReturnToClusterMachine,
    TimelineSwitch,
    decide_return_action,
)
from src.return_to_cluster.state import ReturnPhase, ReturnState


def _obs(**kwargs) -> ReturnObservation:
    """Build a ReturnObservation with sensible defaults for testing."""
    defaults = dict(
        new_primary='pgconsul_postgresql2_1.pgconsul_pgconsul_net',
        role='replica',
        local_timeline=1,
        zk_timeline=1,
        last_op=None,
        simple_switch_tried=False,
        archive_restore_disabled=False,
        recovery_timeout=60.0,
        is_dead=False,
    )
    defaults.update(kwargs)
    return ReturnObservation(**defaults)


class TestDecideReturnAction:
    """decide_return_action decides the action from the observation (pure)."""

    def test_simple_switch_when_no_blockers(self):
        """No blockers → SIMPLE_SWITCH."""
        assert decide_return_action(_obs()) == ReturnAction.SIMPLE_SWITCH

    def test_rewind_when_role_is_primary(self):
        """role='primary' → REWIND (easy way not possible)."""
        assert decide_return_action(_obs(role='primary')) == ReturnAction.REWIND

    def test_rewind_when_destructive_op(self):
        """Destructive last_op → REWIND."""
        assert decide_return_action(_obs(last_op='rewind')) == ReturnAction.REWIND

    def test_rewind_when_fallback_role_is_primary(self):
        """fallback_role='primary' (dead PG) → REWIND."""
        assert decide_return_action(
            _obs(role=None, fallback_role='primary')
        ) == ReturnAction.REWIND

    def test_simple_switch_retry_when_timelines_match(self):
        """simple_switch_tried=True, timelines match → SIMPLE_SWITCH (retry)."""
        assert decide_return_action(
            _obs(simple_switch_tried=True, local_timeline=1, zk_timeline=1)
        ) == ReturnAction.SIMPLE_SWITCH

    def test_wait_for_history_when_timelines_diverge(self):
        """A different target timeline is unsafe until its history is archived."""
        assert decide_return_action(
            _obs(simple_switch_tried=True, local_timeline=1, zk_timeline=2)
        ) == ReturnAction.WAIT_HISTORY

    def test_different_timelines_wait_for_history_before_simple_remaster(self):
        """History proves that a direct remaster cannot join a divergent branch."""
        assert decide_return_action(
            _obs(simple_switch_tried=False, local_timeline=1, zk_timeline=2)
        ) == ReturnAction.WAIT_HISTORY

    def test_before_fork_simple_remaster_does_not_wait_for_archive(self):
        history = (TimelineSwitch(1, 0x4732390),)
        assert decide_return_action(_obs(
            simple_switch_tried=False,
            local_timeline=1,
            zk_timeline=2,
            local_lsn=0x45AD3F8,
            timeline_history=history,
            required_wal_archived=False,
        )) == ReturnAction.SIMPLE_SWITCH

    def test_failed_turn_waits_for_required_wal_after_history(self):
        history = (TimelineSwitch(1, 0x4732390),)
        assert decide_return_action(_obs(
            simple_switch_tried=True,
            local_timeline=1,
            zk_timeline=2,
            local_lsn=0x5000000,
            timeline_history=history,
            required_wal_archived=False,
        )) == ReturnAction.WAIT_ARCHIVE

    def test_former_primary_rewinds_after_archive_barrier_even_before_fork(self):
        """failover_with_network_inconsistency.feature:4 regression."""
        history = (TimelineSwitch(1, 0x4732390),)
        assert decide_return_action(_obs(
            role='primary',
            local_timeline=1,
            zk_timeline=2,
            local_lsn=0,
            timeline_history=history,
            required_wal_archived=True,
        )) == ReturnAction.REWIND

    def test_rewind_when_timelines_unknown(self):
        """simple_switch_tried=True, timelines unknown (None) → REWIND (conservative)."""
        assert decide_return_action(
            _obs(simple_switch_tried=True, local_timeline=None, zk_timeline=None)
        ) == ReturnAction.REWIND


class TestTimelinesMatch:
    """timelines_match utility."""

    def test_both_equal(self):
        from src.return_to_cluster import timelines_match
        assert timelines_match(1, 1) is True

    def test_both_none(self):
        from src.return_to_cluster import timelines_match
        assert timelines_match(None, None) is False

    def test_one_none(self):
        from src.return_to_cluster import timelines_match
        assert timelines_match(1, None) is False
        assert timelines_match(None, 1) is False

    def test_different(self):
        from src.return_to_cluster import timelines_match
        assert timelines_match(1, 2) is False


class TestReturnIterationDecision:
    def _decision(self, state=None, **kwargs):
        defaults = dict(
            state=state,
            db_state={'alive': False, 'running': False},
            primary_switch_checks=3,
        )
        defaults.update(kwargs)
        return ReturnToClusterMachine().decide(
            ReturnIterationObservation(**defaults),
        )

    def test_absent_and_blocked_state_yield_the_iteration(self):
        absent = self._decision()
        blocked = self._decision(ReturnState('op', ReturnPhase.BLOCKED))

        assert absent.plan == []
        assert absent.owns_iteration is False
        assert blocked.plan == []
        assert blocked.owns_iteration is False

    def test_waiting_state_owns_iteration_even_with_no_effect(self):
        decision = self._decision(
            state_read_failed=True,
        )

        assert decision.plan == []
        assert decision.owns_iteration is True

    def test_requested_stopped_replica_with_changed_primary_rewinds(self):
        decision = self._decision(ReturnState(
            'op', ReturnPhase.REQUESTED, 'new-primary', role='replica',
        ))

        assert [command.action for command in decision.plan] == ['rewind']
        assert decision.owns_iteration is True

    def test_requested_stopped_replica_with_same_primary_restarts(self):
        decision = self._decision(
            ReturnState(
                'op', ReturnPhase.REQUESTED, 'primary', role='replica',
            ),
            previous_primary_unchanged=True,
        )

        assert [command.action for command in decision.plan] == ['start_unchanged']
        assert decision.owns_iteration is True

    def test_starting_replica_retries_only_within_existing_limit(self):
        retry = self._decision(ReturnState(
            'op', ReturnPhase.STARTING, 'primary', start_attempts=2,
        ))
        rewind = self._decision(ReturnState(
            'op', ReturnPhase.STARTING, 'primary', start_attempts=3,
        ))

        assert [command.action for command in retry.plan] == ['retry_start']
        assert [command.action for command in rewind.plan] == ['rewind']


class TestDecideRetryIncludesSimpleSwitch:
    """Regression test for cascade replication infinite loop (MDB-41951).

    When simple_switch_tried=True and timelines match, decide_return_action
    must return SIMPLE_SWITCH (not REWIND) so the shell retries the switch.
    Returning REWIND here would cause unnecessary pg_rewind; returning nothing
    would cause an infinite loop.
    """

    def test_retry_returns_simple_switch(self):
        """Timelines match → SIMPLE_SWITCH (retry, not rewind)."""
        action = decide_return_action(
            _obs(simple_switch_tried=True, local_timeline=1, zk_timeline=1)
        )
        assert action == ReturnAction.SIMPLE_SWITCH
