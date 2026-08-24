# coding: utf8
"""
Tests for ADR-0002 §2: _do_failover must catch PostgresConnectionError and
return False (so the executor releases the leader lock via fail-fast).

The failover promote logic (_do_failover/_promote/_promote_handle_slots) now
lives in CommandExecutor (ADR-0007 §2.3). These tests exercise it directly.
"""
from unittest.mock import MagicMock, patch

import pytest

from src.command_executor import CommandExecutor
from src.exceptions import PostgresConnectionError


def _make_executor():
    """Build a CommandExecutor with all infra objects mocked."""
    zk = MagicMock()
    db = MagicMock()
    replication_manager = MagicMock()
    timings = MagicMock()
    slot_manager = MagicMock()
    debug_failure = MagicMock(return_value=False)

    executor = CommandExecutor(
        zk=zk,
        db=db,
        replication_manager=replication_manager,
        timings=timings,
        slot_manager=slot_manager,
        rewind_from_source=MagicMock(return_value=True),
        debug_failure=debug_failure,
        promote_checkpoint_sql=None,
    )
    return executor


class TestDoFailoverReturnsFalse:
    """_do_failover returns False on any failure; it does NOT release the lock.
    The caller (CommandExecutor via DoFailover command) owns the lock and
    releases it when _do_failover returns False (fail-fast)."""

    def test_set_ssn_before_promote_failure_returns_false(self):
        """Failing set_ssn_before_promote returns False without releasing the lock."""
        executor = _make_executor()
        executor._zk.delete_failover_state.return_value = True
        executor._replication_manager.set_ssn_before_promote.return_value = False
        with patch.object(executor, '_promote_handle_slots', return_value=True):
            with patch.object(executor, '_debug_failure', return_value=False):
                result = executor._do_failover()

        assert result is False
        executor._zk.release_lock.assert_not_called()

    def test_db_error_in_promote_handle_slots_returns_false(self):
        """PostgresConnectionError from _promote_handle_slots (create_slots_for_hosts)
        is caught by _do_failover and converted to False; lock is NOT released here."""
        executor = _make_executor()
        executor._zk.delete_failover_state.return_value = True
        with patch.object(executor, '_promote_handle_slots', side_effect=PostgresConnectionError('db down')):
            result = executor._do_failover()

        assert result is False
        executor._zk.release_lock.assert_not_called()

    def test_db_error_in_pg_wal_replay_resume_returns_false(self):
        """PostgresConnectionError from pg_wal_replay_resume (moved from
        _accept_failover to _do_failover) is caught and returns False."""
        executor = _make_executor()
        executor._db.pg_wal_replay_resume.side_effect = PostgresConnectionError('db down')
        result = executor._do_failover()

        assert result is False
        executor._zk.release_lock.assert_not_called()

    def test_unexpected_error_propagates(self):
        """Non-DB errors are NOT swallowed by _do_failover's critical section."""
        executor = _make_executor()
        executor._db.pg_wal_replay_resume.side_effect = RuntimeError('boom')
        with pytest.raises(RuntimeError):
            executor._do_failover()
