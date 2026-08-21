# encoding: utf-8
"""
Unit tests for decide_return_action (MDB-41951, ADR-0006).

Tests cover the pure decision: SIMPLE_SWITCH or REWIND, derived from the
observation. The function is stateless — action is re-derived each call.
"""


from src.return_to_cluster import (
    ReturnAction,
    ReturnObservation,
    decide_return_action,
)


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

    def test_rewind_when_timelines_diverge(self):
        """simple_switch_tried=True, timelines diverge → REWIND."""
        assert decide_return_action(
            _obs(simple_switch_tried=True, local_timeline=1, zk_timeline=2)
        ) == ReturnAction.REWIND

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
