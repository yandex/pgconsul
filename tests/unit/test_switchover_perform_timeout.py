# encoding: utf-8
"""
Unit tests for Switchover.perform() timeout behavior.

Reproduces pgconsul_util.feature:402 — the CLI's --block mode times out
because the state machine refactoring (commit 619062f) increased the number
of switchover iterations from ~3-4 to ~10+, each with a 1-second sleep.
With the default timeout=60, the switchover (~63 iterations) exceeds the
limit and perform() raises SwitchoverException before the switchover
completes.

The fix is to increase the CLI timeout (--timeout 120) in the feature
file. These tests document perform()'s timeout behavior and serve as
regression tests.

Root cause analysis:
  - Old switchover flow (pre-619062f): blocking waits inside 1-2 iterations
    per side, ~3-4 total iterations.
  - New state machine flow: non-blocking iterations with 1-second sleep
    between phases, ~5-6 iterations on primary, ~4-5 on candidate, ~10+
    total iterations.
  - Each iteration of perform()'s polling loop decrements `limit` by 1
    and sleeps 1 second. With timeout=60, the loop allows 60 iterations
    (~60 seconds of sleep + ZK overhead). The switchover needs ~63
    iterations, so the timeout fires at iteration 61 before completion.
"""

import logging
from unittest.mock import MagicMock, patch

import pytest

from src.exceptions import SwitchoverException


def _make_switchover(timeout=60):
    """Create a Switchover instance bypassing __init__ entirely.

    All dependencies are mocked so perform() can be tested in isolation.
    """
    # src.utils does `from . import read_config, helpers` — the conftest
    # stub for `src` lacks `read_config` (it's a function in src/__init__.py,
    # not a submodule). Patch it onto the stub before importing.
    import sys
    if 'src' in sys.modules and not hasattr(sys.modules['src'], 'read_config'):
        sys.modules['src'].read_config = MagicMock()  # type: ignore[attr-defined]
    from src.utils import Switchover

    inst = Switchover.__new__(Switchover)
    inst.timeout = timeout
    inst._log = logging.getLogger('switchover')
    inst._conf = MagicMock()
    inst._conf.getboolean.return_value = False  # quorum_commit = False
    inst._zk = MagicMock()
    inst._new_primary = 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'
    inst._plan = {
        'primary': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net',
        'timeline': 1,
    }
    return inst


def _make_in_progress_side_effect(complete_after):
    """Return a side_effect for in_progress() that returns 'initiated' for
    the first `complete_after` calls, then False (switchover complete).

    `in_progress()` is called once per polling-loop iteration. When it
    returns a truthy value, the loop continues; when it returns False,
    the loop breaks and perform() proceeds to post-switchover checks.
    """
    call_count = [0]

    def _side_effect(*args, **kwargs):
        call_count[0] += 1
        if call_count[0] <= complete_after:
            return 'initiated'
        return False

    return _side_effect


def _make_state_side_effect(complete_after):
    """Return a side_effect for state() that returns an in-progress state
    dict for the first `complete_after` calls, then a completed state dict.

    `state()` is called once per polling-loop iteration (via the debug log
    line) and once more after the loop breaks (to check the final result).
    """
    call_count = [0]
    in_progress_state: dict = {
        'progress': 'initiated',
        'failover': None,
        'info': {},
        'replicas': [],
    }
    completed_state: dict = {
        'progress': None,
        'failover': None,
        'info': {},
        'replicas': [],
    }

    def _side_effect(*args, **kwargs):
        call_count[0] += 1
        if call_count[0] <= complete_after:
            return in_progress_state
        return completed_state

    return _side_effect


def _setup_perform_mocks(inst, complete_after):
    """Wire up all mocks needed for perform() to run without real I/O.

    Args:
        inst: Switchover instance (from _make_switchover).
        complete_after: number of polling iterations before the
            switchover completes (in_progress returns False).
    """
    inst._initiate_switchover = MagicMock(return_value=True)
    inst.in_progress = MagicMock(
        side_effect=_make_in_progress_side_effect(complete_after)
    )
    inst.state = MagicMock(side_effect=_make_state_side_effect(complete_after))
    inst._wait_for_primary = MagicMock()
    inst._wait_for_replicas = MagicMock()
    inst._wait_for_sync_group = MagicMock()
    inst._zk.get_alive_hosts.return_value = [
        'pgconsul_postgresql1_1.pgconsul_pgconsul_net',
        'pgconsul_postgresql2_1.pgconsul_pgconsul_net',
        'pgconsul_postgresql3_1.pgconsul_pgconsul_net',
    ]


class TestSwitchoverPerformTimeout:
    """Tests for Switchover.perform() timeout behavior.

    Reproduces pgconsul_util.feature:402: the CLI's --block mode with the
    default timeout=60 times out because the state machine refactoring
    increased the number of switchover iterations beyond 60.
    """

    @patch('time.sleep')
    def test_perform_times_out_when_switchover_exceeds_timeout(self, mock_sleep):
        """perform() raises SwitchoverException when switchover takes more
        than `timeout` polling iterations.

        This is the bug: with timeout=60 (CLI default), the switchover needs
        ~63 iterations (due to the state machine refactoring adding more
        phases), so perform() raises SwitchoverException at iteration 61
        before the switchover completes.

        Reproduces: pgconsul_util.feature:402
        """
        inst = _make_switchover(timeout=60)
        # Switchover completes after 65 iterations (simulating ~63 seconds
        # of state machine phases + overhead).
        _setup_perform_mocks(inst, complete_after=65)

        with pytest.raises(SwitchoverException, match='timeout exceeded'):
            inst.perform(block=True, timeout=60)

        # Verify the loop ran 61 iterations (limit went from 60 to 0)
        # before the timeout check fired.
        assert inst.in_progress.call_count == 61
        # time.sleep called once per iteration (60 times, not 61 — the
        # 61st iteration hits the timeout check before sleeping).
        assert mock_sleep.call_count == 60

    @patch('time.sleep')
    def test_perform_succeeds_with_increased_timeout(self, mock_sleep):
        """perform() succeeds when timeout is increased to 120.

        This is the fix: increasing the CLI timeout to 120 seconds gives
        the state machine enough iterations to complete the switchover.
        With timeout=120 and the switchover completing after 65 iterations,
        perform() returns True (state['progress'] is None).
        """
        inst = _make_switchover(timeout=120)
        _setup_perform_mocks(inst, complete_after=65)

        result = inst.perform(block=True, timeout=120)

        assert result is True
        # in_progress called 66 times: 65 'initiated' + 1 False (break).
        assert inst.in_progress.call_count == 66
        # state called 66 times: 65 via debug log + 1 after loop.
        assert inst.state.call_count == 66
        # Post-switchover checks were called.
        inst._wait_for_primary.assert_called_once()
        inst._wait_for_replicas.assert_called_once()

    @patch('time.sleep')
    def test_perform_succeeds_when_switchover_completes_within_timeout(self, mock_sleep):
        """perform() succeeds when the switchover completes within the
        timeout.

        Normal case: timeout=60, switchover completes after 55 iterations
        (well within the limit). perform() returns True.
        """
        inst = _make_switchover(timeout=60)
        _setup_perform_mocks(inst, complete_after=55)

        result = inst.perform(block=True, timeout=60)

        assert result is True
        # in_progress called 56 times: 55 'initiated' + 1 False (break).
        assert inst.in_progress.call_count == 56
        # state called 56 times: 55 via debug log + 1 after loop.
        assert inst.state.call_count == 56

    @patch('time.sleep')
    def test_perform_non_block_returns_immediately(self, mock_sleep):
        """perform() returns True immediately in non-blocking mode without
        entering the polling loop.
        """
        inst = _make_switchover(timeout=60)
        _setup_perform_mocks(inst, complete_after=65)

        result = inst.perform(block=False, timeout=60)

        assert result is True
        # in_progress should not be called in non-blocking mode.
        assert inst.in_progress.call_count == 0

    @patch('time.sleep')
    def test_perform_returns_true_when_already_primary(self, mock_sleep):
        """perform() returns True when the target is already primary
        (_initiate_switchover returns False).
        """
        inst = _make_switchover(timeout=60)
        inst._initiate_switchover = MagicMock(return_value=False)
        inst.in_progress = MagicMock()
        inst.state = MagicMock()
        inst._zk.get_alive_hosts.return_value = ['host1', 'host2']

        result = inst.perform(block=True, timeout=60)

        assert result is True
        # The polling loop should not be entered.
        assert inst.in_progress.call_count == 0

    @patch('time.sleep')
    def test_perform_timeout_uses_self_timeout_when_none_passed(self, mock_sleep):
        """perform() uses self.timeout when timeout=None.

        Verifies that the timeout fallback works correctly: if timeout
        is not explicitly passed, perform() uses the instance's timeout
        attribute (set in __init__ from the CLI --timeout argument).
        """
        inst = _make_switchover(timeout=30)
        # Switchover completes after 35 iterations — exceeds timeout=30.
        _setup_perform_mocks(inst, complete_after=35)

        with pytest.raises(SwitchoverException, match='timeout exceeded'):
            inst.perform(block=True, timeout=None)

        # The loop should run 31 iterations (limit 30 → 0).
        assert inst.in_progress.call_count == 31
