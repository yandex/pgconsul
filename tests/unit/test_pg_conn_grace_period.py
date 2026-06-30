# encoding: utf-8
from unittest.mock import patch


from src.pg_conn_grace_period import PgConnGracePeriod


def make(grace: float = 30.0) -> PgConnGracePeriod:
    return PgConnGracePeriod(grace)


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------

def test_negative_grace_period_falls_back_to_zero():
    gp = PgConnGracePeriod(-5.0)
    # after record_failure, process running, should act immediately
    with patch('src.pg_conn_grace_period.time') as t:
        t.time.return_value = 0.0
        gp.record_failure()
        t.time.return_value = 0.0
        assert gp.should_act(pg_running=True) is True


# ---------------------------------------------------------------------------
# reset
# ---------------------------------------------------------------------------

def test_reset_causes_should_act_true_without_failure():
    gp = make()
    assert gp.should_act(pg_running=True) is True


def test_reset_clears_failure_state():
    gp = make(grace=60.0)
    with patch('src.pg_conn_grace_period.time') as t:
        t.time.return_value = 0.0
        gp.record_failure()
        gp.reset()
        t.time.return_value = 1.0
        # after reset, no failure context — acts normally
        assert gp.should_act(pg_running=True) is True


# ---------------------------------------------------------------------------
# within grace period
# ---------------------------------------------------------------------------

def test_within_grace_pg_running_returns_false():
    gp = make(grace=30.0)
    with patch('src.pg_conn_grace_period.time') as t:
        t.time.return_value = 100.0
        gp.record_failure()
        t.time.return_value = 115.0  # 15s elapsed < 30s grace
        assert gp.should_act(pg_running=True) is False


def test_within_grace_pg_not_running_returns_true():
    gp = make(grace=30.0)
    with patch('src.pg_conn_grace_period.time') as t:
        t.time.return_value = 100.0
        gp.record_failure()
        t.time.return_value = 110.0  # still within grace but process dead
        assert gp.should_act(pg_running=False) is True


# ---------------------------------------------------------------------------
# grace period expired
# ---------------------------------------------------------------------------

def test_expired_grace_pg_running_returns_true():
    gp = make(grace=30.0)
    with patch('src.pg_conn_grace_period.time') as t:
        t.time.return_value = 0.0
        gp.record_failure()
        t.time.return_value = 31.0  # 31s elapsed > 30s grace
        assert gp.should_act(pg_running=True) is True


def test_exactly_at_grace_boundary_returns_true():
    gp = make(grace=30.0)
    with patch('src.pg_conn_grace_period.time') as t:
        t.time.return_value = 0.0
        gp.record_failure()
        t.time.return_value = 30.0  # elapsed == grace: not strictly less
        assert gp.should_act(pg_running=True) is True


# ---------------------------------------------------------------------------
# record_failure idempotency
# ---------------------------------------------------------------------------

def test_record_failure_keeps_first_timestamp():
    gp = make(grace=30.0)
    with patch('src.pg_conn_grace_period.time') as t:
        t.time.return_value = 0.0
        gp.record_failure()
        t.time.return_value = 5.0
        gp.record_failure()   # second call must not update timestamp
        t.time.return_value = 20.0  # 20s from first failure, within grace
        assert gp.should_act(pg_running=True) is False


# ---------------------------------------------------------------------------
# zero grace period
# ---------------------------------------------------------------------------

def test_zero_grace_acts_immediately():
    gp = make(grace=0.0)
    with patch('src.pg_conn_grace_period.time') as t:
        t.time.return_value = 0.0
        gp.record_failure()
        t.time.return_value = 0.0
        assert gp.should_act(pg_running=True) is True
