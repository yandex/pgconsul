# coding: utf8
"""
Tests for ADR-0002 §2: _do_failover must catch PostgresConnectionError and
return False (so the executor releases the leader lock via fail-fast).
_accept_failover now delegates to the state machine (ADR-0007 §5); the
critical-section boundary lives in CommandExecutor._dispatch.
"""
from unittest.mock import MagicMock, patch

import pytest

from src.exceptions import PostgresConnectionError


def _make_instance():
    from src.main import PgconsulConfig
    from src.main import Pgconsul
    with patch('src.main.pgconsul.__init__', return_value=None):
        inst = Pgconsul.__new__(Pgconsul)
    inst.db = MagicMock()
    inst.zk = MagicMock()
    inst.config = PgconsulConfig(
        welcome_message='',
        working_dir='/tmp',
        iteration_timeout=0.0,
        quorum_commit=False,
        use_lwaldump=False,
        update_prio_in_zk=False,
        use_replication_slots=False,
        replication_slots_polling=False,
        priority='100',
        stream_from=None,
        autofailover=False,


        max_rewind_retries=0,

        do_consecutive_primary_switch=False,
        max_allowed_switchover_lag_ms=0,
        allow_potential_data_loss=False,
        close_detached_after=0.0,
        start_pooler=False,
        recovery_timeout=0.0,
        can_delayed=False,
        primary_switch_disable_archive_restore=False,
        primary_switch_checks=0,
        primary_switch_restart=False,



        change_replication_type=False,
        sync_replication_in_maintenance=False,
        promote_checkpoint_sql=None,



    )
    inst._master_lost_ts = 0.0
    inst._replication_manager = MagicMock()
    inst._slot_manager = MagicMock()
    inst._timings = MagicMock()
    # _debug_failure is now a callable DebugFailure instance (step 14e).
    inst._debug_failure = MagicMock(return_value=False)
    return inst


class TestDoFailoverReturnsFalse:
    """_do_failover returns False on any failure; it does NOT release the lock.
    The caller (CommandExecutor via DoFailover command) owns the lock and
    releases it when _do_failover returns False (fail-fast)."""

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

    def test_db_error_in_pg_wal_replay_resume_returns_false(self):
        """PostgresConnectionError from pg_wal_replay_resume (moved from
        _accept_failover to _do_failover) is caught and returns False."""
        inst = _make_instance()
        inst.db.pg_wal_replay_resume.side_effect = PostgresConnectionError('db down')
        result = inst._do_failover()

        assert result is False
        inst.zk.release_lock.assert_not_called()

    def test_unexpected_error_propagates(self):
        """Non-DB errors are NOT swallowed by _do_failover's critical section."""
        inst = _make_instance()
        inst.db.pg_wal_replay_resume.side_effect = RuntimeError('boom')
        with pytest.raises(RuntimeError):
            inst._do_failover()
