# coding: utf8
"""
Tests for ADR-0005 §1: await_for / await_for_value must reject timeout=-1
(infinite wait is prohibited).
"""
import pytest

from src import helpers


class TestAwaitForRejectsInfiniteTimeout:
    def test_await_for_rejects_negative_timeout(self):
        with pytest.raises(ValueError, match='infinite timeout'):
            helpers.await_for(lambda: True, -1, 'test event')

    def test_await_for_value_rejects_negative_timeout(self):
        with pytest.raises(ValueError, match='infinite timeout'):
            helpers.await_for_value(lambda: True, -1, 'test event')

    def test_await_for_accepts_small_timeout(self):
        # timeout=0 is allowed (not infinite); small positive works
        result = helpers.await_for(lambda: True, 1, 'instant event')
        assert result is True

    def test_await_for_value_accepts_small_timeout(self):
        result = helpers.await_for_value(lambda: 42, 1, 'instant value')
        assert result == 42
