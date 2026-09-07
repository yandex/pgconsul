# encoding: utf-8
"""Tests for current failover phases."""

import pytest

from src.failover import FailoverPhase


@pytest.mark.parametrize(
    ('phase', 'value'),
    [
        (FailoverPhase.VOTING, 'voting'),
        (FailoverPhase.PROMOTING, 'promoting'),
        (FailoverPhase.RESOLVING_WINNER, 'resolving_winner'),
        (FailoverPhase.CLEANUP, 'cleanup'),
    ],
)
def test_phase_values_match_persistent_zk_values(phase, value):
    assert phase == value
    assert isinstance(phase, str)


@pytest.mark.parametrize('phase', list(FailoverPhase))
def test_from_str_parses_known_phase(phase):
    assert FailoverPhase.from_str(phase.value) == phase


def test_from_str_returns_none_for_absent_or_unknown_phase():
    assert FailoverPhase.from_str(None) is None
    assert FailoverPhase.from_str('unknown') is None
