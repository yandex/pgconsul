# encoding: utf-8
from unittest.mock import patch

from src.pg_conn_grace_period import PgConnGracePeriod


def test_negative_grace_period_falls_back_to_zero():
    grace = PgConnGracePeriod(-5)
    with patch('src.pg_conn_grace_period.time') as clock:
        clock.time.return_value = 0.0
        grace.record_failure()
        assert grace.should_act(pg_running=True) is True


def test_reset_clears_failure_state():
    grace = PgConnGracePeriod(60)
    with patch('src.pg_conn_grace_period.time') as clock:
        clock.time.return_value = 0.0
        grace.record_failure()
        grace.reset()
        clock.time.return_value = 1.0
        assert grace.should_act(pg_running=True) is True


def test_running_process_is_protected_within_grace_period():
    grace = PgConnGracePeriod(30)
    with patch('src.pg_conn_grace_period.time') as clock:
        clock.time.return_value = 100.0
        grace.record_failure()
        clock.time.return_value = 115.0
        assert grace.should_act(pg_running=True) is False


def test_stopped_process_is_not_protected():
    grace = PgConnGracePeriod(30)
    with patch('src.pg_conn_grace_period.time') as clock:
        clock.time.return_value = 100.0
        grace.record_failure()
        clock.time.return_value = 110.0
        assert grace.should_act(pg_running=False) is True


def test_running_process_is_not_protected_after_grace_period():
    grace = PgConnGracePeriod(30)
    with patch('src.pg_conn_grace_period.time') as clock:
        clock.time.return_value = 0.0
        grace.record_failure()
        clock.time.return_value = 31.0
        assert grace.should_act(pg_running=True) is True


def test_exact_grace_period_boundary_forces_action():
    grace = PgConnGracePeriod(30)
    with patch('src.pg_conn_grace_period.time') as clock:
        clock.time.return_value = 0.0
        grace.record_failure()
        clock.time.return_value = 30.0
        assert grace.should_act(pg_running=True) is True


def test_repeated_failure_keeps_first_timestamp():
    grace = PgConnGracePeriod(30)
    with patch('src.pg_conn_grace_period.time') as clock:
        clock.time.return_value = 0.0
        grace.record_failure()
        clock.time.return_value = 20.0
        grace.record_failure()
        clock.time.return_value = 31.0
        assert grace.should_act(pg_running=True) is True


def test_zero_grace_period_forces_action_immediately():
    grace = PgConnGracePeriod(0)
    with patch('src.pg_conn_grace_period.time') as clock:
        clock.time.return_value = 0.0
        grace.record_failure()
        assert grace.should_act(pg_running=True) is True
