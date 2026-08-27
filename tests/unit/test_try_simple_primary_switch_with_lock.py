# encoding: utf-8
"""
Red test: _try_simple_primary_switch_with_lock skips the switch when
do_consecutive_primary_switch is enabled and the lock is free.

Reproduces: tests/features/consecutive_switch.feature — "Change consecutively
on failover" scenario, where postgresql4 (a dead replica) enters return-to-
cluster, decide_return_action returns SIMPLE_SWITCH, but the
callback (_try_simple_primary_switch_with_lock) returns True without ever
calling _simple_primary_switch. PostgreSQL never starts, re_init_db crashes
with KeyError (cache file missing), pgconsul restarts — infinite loop.

Root cause: the guard condition is:
    if (lock_holder is None and not try_acquire_lock(...)) or lock_holder != hostname:
        return True
When lock_holder is None and try_acquire_lock succeeds (returns True), the
first clause is False, but the second clause (None != hostname) is True, so
the whole expression is True — the switch is skipped even though we just
acquired the lock.
"""

from unittest.mock import MagicMock, patch

import pytest


def _make_pgconsul(do_consecutive: bool = True):
    """Create a pgconsul instance bypassing __init__ entirely."""
    from src.main import PgconsulConfig
    with patch('src.main.pgconsul.__init__', return_value=None):
        from src.main import Pgconsul
        inst = Pgconsul.__new__(Pgconsul)

    inst.db = MagicMock()
    inst.config = PgconsulConfig(
        welcome_message='',
        working_dir='/tmp',
        iteration_timeout=0.0,
        quorum_commit=False,
        update_prio_in_zk=False,
        use_replication_slots=False,
        replication_slots_polling=False,
        priority='100',
        stream_from=None,
        autofailover=False,
        switchover_rollback_timeout=0.0,
        switchover_catchup_timeout=0.0,
        max_rewind_retries=3,
        do_consecutive_primary_switch=do_consecutive,
        max_allowed_switchover_lag_ms=0,
        allow_potential_data_loss=False,
        close_detached_after=0.0,
        start_pooler=False,
        recovery_timeout=60.0,
        can_delayed=False,
        primary_switch_disable_archive_restore=True,
        primary_switch_checks=3,
        primary_switch_restart=False,
        primary_unavailability_timeout=0.0,
        walreceiver_disable_timeout=0.0,
        min_failover_timeout=0.0,
        change_replication_type=False,
        sync_replication_in_maintenance=False,
        promote_checkpoint_sql=None,
        failure_name=None,
        failure_count=100000000,
        sleep_before_disable_walreceiver=0.0,
        election_lsn_read_sleep=0.0,
        election_loser_timeout=0,
    )
    inst.zk = MagicMock()
    inst.zk.PRIMARY_SWITCH_LOCK_PATH = 'remaster'
    inst.checks = {'primary_switch': 0, 'rewind': 0}
    inst._simple_primary_switch = MagicMock(return_value=True)
    return inst


class TestTrySimplePrimarySwitchWithLock:
    """
    _try_simple_primary_switch_with_lock must call _simple_primary_switch
    when do_consecutive_primary_switch is enabled and the lock is acquired.
    """

    def test_calls_switch_when_lock_acquired(self):
        """
        lock_holder is None, try_acquire_lock succeeds → must call
        _simple_primary_switch, not return True early.
        """
        inst = _make_pgconsul(do_consecutive=True)
        inst.zk.get_current_lock_holder.return_value = None
        inst.zk.try_acquire_lock.return_value = True

        with patch('src.main.helpers.get_hostname',
                   return_value='pgconsul_postgresql4_1.pgconsul_pgconsul_net'):
            result = inst._try_simple_primary_switch_with_lock(
                limit=60.0,
                new_primary='pgconsul_postgresql1_1.pgconsul_pgconsul_net',
                is_dead=True,
            )

        # Must have called the actual switch.
        inst._simple_primary_switch.assert_called_once_with(
            limit=60.0,
            new_primary='pgconsul_postgresql1_1.pgconsul_pgconsul_net',
            is_dead=True,
        )
        # Lock must be released after the switch.
        inst.zk.release_lock.assert_called_once_with('remaster')
        assert result is True

    def test_calls_switch_when_already_holds_lock(self):
        """
        lock_holder is us → must call _simple_primary_switch.
        """
        hostname = 'pgconsul_postgresql4_1.pgconsul_pgconsul_net'
        inst = _make_pgconsul(do_consecutive=True)
        inst.zk.get_current_lock_holder.return_value = hostname

        with patch('src.main.helpers.get_hostname', return_value=hostname):
            result = inst._try_simple_primary_switch_with_lock(
                limit=60.0,
                new_primary='pgconsul_postgresql1_1.pgconsul_pgconsul_net',
                is_dead=True,
            )

        inst._simple_primary_switch.assert_called_once()
        inst.zk.release_lock.assert_called_once_with('remaster')
        assert result is True

    def test_skips_when_lock_held_by_other(self):
        """
        lock_holder is another host → must return True without calling switch.
        """
        inst = _make_pgconsul(do_consecutive=True)
        inst.zk.get_current_lock_holder.return_value = 'other_host'

        with patch('src.main.helpers.get_hostname',
                   return_value='pgconsul_postgresql4_1.pgconsul_pgconsul_net'):
            result = inst._try_simple_primary_switch_with_lock(
                limit=60.0,
                new_primary='pgconsul_postgresql1_1.pgconsul_pgconsul_net',
                is_dead=True,
            )

        inst._simple_primary_switch.assert_not_called()
        inst.zk.release_lock.assert_not_called()
        assert result is True

    def test_skips_when_acquire_fails(self):
        """
        lock_holder is None, try_acquire_lock fails → return True, no switch.
        """
        inst = _make_pgconsul(do_consecutive=True)
        inst.zk.get_current_lock_holder.return_value = None
        inst.zk.try_acquire_lock.return_value = False

        with patch('src.main.helpers.get_hostname',
                   return_value='pgconsul_postgresql4_1.pgconsul_pgconsul_net'):
            result = inst._try_simple_primary_switch_with_lock(
                limit=60.0,
                new_primary='pgconsul_postgresql1_1.pgconsul_pgconsul_net',
                is_dead=True,
            )

        inst._simple_primary_switch.assert_not_called()
        inst.zk.release_lock.assert_not_called()
        assert result is True

    def test_no_lock_when_consecutive_disabled(self):
        """
        do_consecutive_primary_switch=False → direct call, no lock logic.
        """
        inst = _make_pgconsul(do_consecutive=False)

        with patch('src.main.helpers.get_hostname',
                   return_value='pgconsul_postgresql4_1.pgconsul_pgconsul_net'):
            result = inst._try_simple_primary_switch_with_lock(
                limit=60.0,
                new_primary='pgconsul_postgresql1_1.pgconsul_pgconsul_net',
                is_dead=True,
            )

        inst._simple_primary_switch.assert_called_once()
        inst.zk.get_current_lock_holder.assert_not_called()
        inst.zk.try_acquire_lock.assert_not_called()
        inst.zk.release_lock.assert_not_called()
        assert result is True
