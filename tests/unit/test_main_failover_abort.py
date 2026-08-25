# coding: utf8
"""
Tests for ADR-0002 §2: _run_promotion must catch PostgresConnectionError and
return False (so the executor releases the leader lock via fail-fast).
_accept_failover now delegates to the state machine (ADR-0007 §5); the
critical-section boundary lives in CommandExecutor._dispatch.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.exceptions import PostgresConnectionError


def _make_instance():
    from src.main import Pgconsul
    with patch('src.main.pgconsul.__init__', return_value=None):
        inst = Pgconsul.__new__(Pgconsul)
    inst.db = MagicMock()
    inst.zk = MagicMock()
    inst.config = SimpleNamespace(promote_checkpoint_sql=None)
    inst._master_lost_ts = 0.0
    inst._replication_manager = MagicMock()
    inst._slot_manager = MagicMock()
    inst._timings = MagicMock()
    # _debug_failure is now a callable DebugFailure instance (step 14e).
    inst._debug_failure = MagicMock(return_value=False)
    local_state = MagicMock()
    local_state.read.return_value = None
    inst._local_states = {'failover_participant': local_state}
    return inst


class TestRunPromotionReturnsFalse:
    """_run_promotion returns False on failure without releasing the lock."""

    def test_set_ssn_before_promote_failure_returns_false(self):
        """Failing set_ssn_before_promote returns False without releasing the lock."""
        inst = _make_instance()
        inst.zk.delete_failover_state.return_value = True
        inst._replication_manager.set_ssn_before_promote.return_value = False
        with patch.object(inst, '_promote_handle_slots', return_value=True):
            with patch.object(inst, '_debug_failure', return_value=False):
                result = inst._run_promotion('failover_participant')

        assert result is False
        inst.zk.release_lock.assert_not_called()

    def test_db_error_in_promote_handle_slots_returns_false(self):
        """PostgresConnectionError from _promote_handle_slots (create_slots_for_hosts)
        is caught by _run_promotion and converted to False; lock is not released here."""
        inst = _make_instance()
        inst.zk.delete_failover_state.return_value = True
        with patch.object(inst, '_promote_handle_slots', side_effect=PostgresConnectionError('db down')):
            result = inst._run_promotion('failover_participant')

        assert result is False
        inst.zk.release_lock.assert_not_called()

    def test_db_error_in_pg_wal_replay_resume_returns_false(self):
        """PostgresConnectionError from pg_wal_replay_resume (moved from
        failover entry to _run_promotion) is caught and returns False."""
        inst = _make_instance()
        inst.db.pg_wal_replay_resume.side_effect = PostgresConnectionError('db down')
        result = inst._run_promotion('failover_participant')

        assert result is False
        inst.zk.release_lock.assert_not_called()

    def test_unexpected_error_propagates(self):
        """Non-DB errors are not swallowed by _run_promotion's critical section."""
        inst = _make_instance()
        inst.db.pg_wal_replay_resume.side_effect = RuntimeError('boom')
        with pytest.raises(RuntimeError):
            inst._run_promotion('failover_participant')
