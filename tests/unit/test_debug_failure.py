"""
Unit tests for DebugFailure fault-injection callable (src/debug.py).

Covers: counting semantics, name mismatch, reset(), frozen config.
"""
from src.debug import DebugFailure, DebugFailureConfig


class TestDebugFailureCall:
    """DebugFailure.__call__ returns True first N times for the configured name."""

    def test_returns_true_first_n_times(self):
        debug = DebugFailure(DebugFailureConfig('my_fault', 2))
        assert debug('my_fault') is True
        assert debug('my_fault') is True
        assert debug('my_fault') is False

    def test_returns_false_for_unconfigured_name(self):
        debug = DebugFailure(DebugFailureConfig('my_fault', 3))
        assert debug('other_fault') is False

    def test_returns_false_when_count_is_zero(self):
        debug = DebugFailure(DebugFailureConfig('my_fault', 0))
        assert debug('my_fault') is False

    def test_returns_false_when_name_is_none(self):
        debug = DebugFailure(DebugFailureConfig(None, 3))
        assert debug('my_fault') is False


class TestDebugFailureReset:
    """reset() clears counters so failures fire again."""

    def test_reset_allows_failures_again(self):
        debug = DebugFailure(DebugFailureConfig('my_fault', 1))
        assert debug('my_fault') is True
        assert debug('my_fault') is False
        debug.reset()
        assert debug('my_fault') is True
        assert debug('my_fault') is False

    def test_reset_on_unused_counter_is_noop(self):
        debug = DebugFailure(DebugFailureConfig('my_fault', 1))
        debug.reset()
        assert debug('my_fault') is True


class TestDebugFailureConfigFrozen:
    """DebugFailureConfig is frozen (immutable)."""

    def test_config_is_frozen(self):
        cfg = DebugFailureConfig('my_fault', 1)
        try:
            cfg.failure_name = 'other'
            raise AssertionError('Expected FrozenInstanceError')
        except AttributeError:
            pass  # frozen dataclass raises AttributeError on assignment
