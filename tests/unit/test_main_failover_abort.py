# coding: utf8
"""
Tests for ADR-0002 §2: _accept_failover must catch PostgresConnectionError and
abort cleanly (return None) instead of restarting the iteration mid-failover.
Other exceptions still propagate to run_iteration().
"""
from unittest.mock import MagicMock, patch

import pytest

from src.exceptions import PostgresConnectionError


def _make_instance():
    from src.main import pgconsul as PgConsul
    with patch('src.main.pgconsul.__init__', return_value=None):
        inst = PgConsul.__new__(PgConsul)
    inst.db = MagicMock()
    inst.zk = MagicMock()
    inst.config = MagicMock()
    inst._master_lost_ts = 0.0
    inst._replication_manager = MagicMock()
    inst._slot_manager = MagicMock()
    return inst


class TestAcceptFailoverAbort:
    """Critical section: _accept_failover aborts on DB loss (ADR-0002 §2)."""

    def test_connection_error_in_checks_aborts_with_none(self):
        """DB loss during pre-checks → abort failover (return None), no raise."""
        inst = _make_instance()
        with patch.object(inst, '_can_do_failover', side_effect=PostgresConnectionError('db down')):
            with patch.object(inst, '_start_timing'):
                result = inst._accept_failover()

        assert result is None
        # Failover must not have proceeded to lock acquisition.
        inst.zk.try_acquire_lock.assert_not_called()
        # Lock was never acquired, so it must not be released either.
        inst.zk.release_lock.assert_not_called()

    def test_connection_error_after_lock_aborts_with_none(self):
        """DB loss after acquiring the lock → abort and release the lock (CR-1)."""
        inst = _make_instance()
        inst.zk.try_acquire_lock.return_value = True
        inst.db.pg_wal_replay_resume.side_effect = PostgresConnectionError('db down')
        with patch.object(inst, '_can_do_failover', return_value=True):
            with patch.object(inst, '_start_timing'):
                result = inst._accept_failover()

        assert result is None
        # ADR-0002 §2: the acquired lock must be released on abort.
        inst.zk.release_lock.assert_called_once_with()

    def test_connection_error_in_do_failover_releases_lock(self):
        """DB loss deep inside _do_failover → _do_failover returns False,
        _accept_failover releases the lock as compensating action."""
        inst = _make_instance()
        inst.zk.try_acquire_lock.return_value = True
        with patch.object(inst, '_can_do_failover', return_value=True):
            with patch.object(inst, '_start_timing'):
                with patch.object(inst, '_do_failover', return_value=False):
                    result = inst._accept_failover()

        assert result is False
        inst.zk.release_lock.assert_called_once_with()

    def test_unexpected_error_propagates(self):
        """Non-DB errors are NOT swallowed by the critical-section handler."""
        inst = _make_instance()
        with patch.object(inst, '_can_do_failover', side_effect=RuntimeError('boom')):
            with patch.object(inst, '_start_timing'):
                with pytest.raises(RuntimeError):
                    inst._accept_failover()


class TestDoFailoverReturnsFalse:
    """_do_failover returns False on any failure; it does NOT release the lock.
    The callers (_accept_failover / _accept_switchover) own the lock and release
    it when _do_failover returns False."""

    def test_set_ssn_before_promote_failure_returns_false(self):
        """Failing set_ssn_before_promote returns False without releasing the lock."""
        inst = _make_instance()
        inst.zk.delete_failover_state.return_value = True
        inst._replication_manager.set_ssn_before_promote.return_value = False
        with patch.object(inst, '_promote_handle_slots', return_value=True):
            with patch.object(inst, '_debug_failure', return_value=False):
                result = inst._do_failover()

        assert result is False
        inst.zk.release_lock.assert_not_called()

    def test_db_error_in_promote_handle_slots_returns_false(self):
        """PostgresConnectionError from _promote_handle_slots (create_slots_for_hosts)
        is caught by _do_failover and converted to False; lock is NOT released here."""
        inst = _make_instance()
        inst.zk.delete_failover_state.return_value = True
        with patch.object(inst, '_promote_handle_slots', side_effect=PostgresConnectionError('db down')):
            result = inst._do_failover()

        assert result is False
        inst.zk.release_lock.assert_not_called()

    def test_accept_failover_releases_lock_on_do_failover_false(self):
        """_accept_failover releases the lock when _do_failover returns False."""
        inst = _make_instance()
        inst.zk.try_acquire_lock.return_value = True
        with patch.object(inst, '_can_do_failover', return_value=True):
            with patch.object(inst, '_start_timing'):
                with patch.object(inst, '_do_failover', return_value=False):
                    result = inst._accept_failover()

        assert result is False
        inst.zk.release_lock.assert_called_once_with()
