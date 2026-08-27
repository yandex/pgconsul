# encoding: utf-8
"""
Integration test for the return-to-cluster decision function (MDB-41951).

Verifies that _return_to_cluster() calls decide_return_action() directly
(no CommandExecutor delegation), and that the decision function prevents
unnecessary pg_rewind when timelines match (transient failure) while still
invoking rewind when timelines diverge.

Reproduces: tests/features/targeted_switchover.feature:108
"""

from unittest.mock import MagicMock, patch

import pytest


def _make_pgconsul():
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
        do_consecutive_primary_switch=False,
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
    inst._timings = MagicMock()
    inst._maintenance = MagicMock()
    inst.zk = MagicMock()
    inst.checks = {'primary_switch': 0, 'rewind': 0}

    # Return-to-cluster callbacks (direct calls, no executor delegation).
    inst._simple_primary_switch = MagicMock(return_value=False)
    inst._ensure_restoring_wal = MagicMock()
    inst._rewind_from_source = MagicMock(return_value=None)
    inst._set_simple_primary_switch_try = MagicMock()
    inst._is_simple_primary_switch_tried = MagicMock(return_value=False)

    return inst


class TestReturnToClusterUnnecessaryRewind:
    """
    Reproduces targeted_switchover.feature:108 — postgresql3 must NOT invoke
    pg_rewind when timelines match and the simple switch failed for a transient
    reason (candidate unreachable, archive recovery blocked).
    """

    def test_no_rewind_when_timelines_match_and_simple_switch_fails(self):
        """
        Simple switch fails, but timelines match (local=1, zk=1).
        The machine must NOT call rewind_from_source.
        """
        inst = _make_pgconsul()
        new_primary = 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'

        # _get_db_state returns valid replica state.
        inst._get_db_state = MagicMock(return_value={
            'alive': True, 'running': True, 'role': 'replica', 'timeline': 1,
        })
        # db.get_state() for ReturnObservation.build.
        inst.db.get_state.return_value = {
            'alive': True, 'running': True, 'role': 'replica', 'timeline': 1,
        }

        inst.zk.noexcept_get.return_value = None
        inst.zk.get_failover_state.return_value = 'finished'
        inst.zk.get_timeline.return_value = 1  # matches local
        inst._is_simple_primary_switch_tried = MagicMock(return_value=False)
        inst._acquire_replication_source_slot_lock = MagicMock()

        # Simple switch fails (transient).
        inst._simple_primary_switch.return_value = False
        inst.db.is_host_unreachable.return_value = False
        inst.db._get_param_value.return_value = '/bin/false'

        with patch('src.main.helpers.get_hostname', return_value='pgconsul_postgresql3_1.pgconsul_pgconsul_net'), \
             patch('src.main.helpers.is_op_destructive', return_value=False):
            inst._return_to_cluster(new_primary, 'replica', is_dead=False)

        # rewind_from_source must NOT be called — timelines match.
        inst._rewind_from_source.assert_not_called()

    def test_rewind_when_timelines_diverge_and_simple_switch_fails(self):
        """
        Simple switch already tried, timelines diverge (local=1, zk=2).
        decide_return_action returns REWIND — rewind_from_source MUST be called.
        """
        inst = _make_pgconsul()
        new_primary = 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'

        inst._get_db_state = MagicMock(return_value={
            'alive': True, 'running': True, 'role': 'replica', 'timeline': 1,
        })
        inst.db.get_state.return_value = {
            'alive': True, 'running': True, 'role': 'replica', 'timeline': 1,
        }

        inst.zk.noexcept_get.return_value = None
        inst.zk.get_failover_state.return_value = 'finished'
        inst.zk.get_timeline.return_value = 2  # diverges from local (1)
        # simple_switch_tried=True → decide_return_action checks divergence → REWIND
        inst._is_simple_primary_switch_tried = MagicMock(return_value=True)
        inst._acquire_replication_source_slot_lock = MagicMock()

        inst._simple_primary_switch.return_value = False
        inst.db.is_host_unreachable.return_value = False
        inst.db._get_param_value.return_value = '/bin/false'
        inst.db.get_wal_flush_lsn.return_value = 0x5000000
        inst.db.fetch_timeline_history.return_value = '1\t0/4732390\tbranch\n'
        inst.db.get_wal_segment_size.return_value = 16 * 1024 * 1024
        inst.db.is_wal_archived.return_value = True

        with patch('src.main.helpers.get_hostname', return_value='pgconsul_postgresql3_1.pgconsul_pgconsul_net'), \
             patch('src.main.helpers.is_op_destructive', return_value=False):
            inst._return_to_cluster(new_primary, 'replica', is_dead=False)

        # rewind_from_source MUST be called — timelines diverge.
        inst._rewind_from_source.assert_called_once()

    def test_rewind_resets_restore_command_copied_from_winner_before_start(self):
        """ssn_before_promote.feature:11: pg_rewind copies the winner's vote fence."""
        inst = _make_pgconsul()
        new_primary = 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'
        inst.db.is_host_unreachable.return_value = False
        actions = []
        inst.db.resume_restoring_wal_stopped.side_effect = lambda: actions.append('restore') or True
        inst.db.do_rewind.side_effect = lambda _primary: actions.append('rewind') or 0
        inst.zk.write_host_op.return_value = True
        inst._attach_to_primary = MagicMock(return_value=None)

        with patch('src.main.helpers.await_for', return_value=True), \
             patch('src.main.helpers.get_hostname', return_value='former-primary'):
            type(inst)._rewind_from_source(
                inst,
                is_postgresql_dead=True,
                limit=60.0,
                new_primary=new_primary,
            )

        assert inst.db.resume_restoring_wal_stopped.call_count == 2
        assert actions == ['restore', 'rewind', 'restore']
        inst._attach_to_primary.assert_called_once_with(new_primary, 60.0)

    def test_different_timeline_waits_for_history_before_remaster(self):
        """A remaster must first prove that the local branch is an ancestor."""
        inst = _make_pgconsul()
        new_primary = 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'
        inst._get_db_state = MagicMock(return_value={
            'alive': True, 'running': True, 'role': 'replica', 'timeline': 1,
        })
        inst.db.get_state.return_value = {
            'alive': True, 'running': True, 'role': 'replica', 'timeline': 1,
        }
        inst.db.get_restore_command.return_value = '/bin/false'
        inst.db.get_wal_flush_lsn.return_value = 0x45AD3F8
        inst.db.fetch_timeline_history.return_value = None
        inst.zk.get_timeline.return_value = 2
        inst.zk.noexcept_get.return_value = None
        inst._acquire_replication_source_slot_lock = MagicMock()

        with patch('src.main.helpers.get_hostname', return_value='replica'):
            inst._return_to_cluster(new_primary, 'replica', is_dead=False)

        inst.db.fetch_timeline_history.assert_called_once_with(2)
        inst._simple_primary_switch.assert_not_called()
        inst._rewind_from_source.assert_not_called()

    @pytest.mark.parametrize(
        ('local_lsn', 'rewind_expected'),
        [(0x45AD3F8, False), (0x5000000, True)],
    )
    def test_history_fork_selects_simple_switch_or_rewind(
        self, local_lsn, rewind_expected,
    ):
        inst = _make_pgconsul()
        new_primary = 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'
        state = {
            'alive': True, 'running': True, 'role': 'replica', 'timeline': 1,
        }
        inst._get_db_state = MagicMock(return_value=state)
        inst.db.get_state.return_value = state
        inst.db.get_restore_command.return_value = '/bin/false'
        inst.db.get_wal_flush_lsn.return_value = local_lsn
        history_value = '1\t0/4732390\tno recovery target specified\n'
        inst.db.fetch_timeline_history.return_value = history_value
        inst.db.get_wal_segment_size.return_value = 16 * 1024 * 1024
        inst.db.is_wal_archived.return_value = True
        inst.db.install_timeline_history.return_value = True
        inst.zk.get_timeline.return_value = 2
        inst.zk.noexcept_get.return_value = None
        inst._acquire_replication_source_slot_lock = MagicMock()
        inst._is_simple_primary_switch_tried.return_value = True
        actions = []
        inst._simple_primary_switch.side_effect = lambda *_args: actions.append('switch') or True
        inst._ensure_restoring_wal.side_effect = lambda: actions.append('restore')

        with patch('src.main.helpers.get_hostname', return_value='replica'):
            inst._return_to_cluster(new_primary, 'replica', is_dead=False)

        if rewind_expected:
            inst._ensure_restoring_wal.assert_not_called()
            inst._simple_primary_switch.assert_not_called()
            inst._rewind_from_source.assert_called_once()
        else:
            inst.db.install_timeline_history.assert_called_once_with(2, history_value)
            assert actions == ['restore', 'switch']
            inst._ensure_restoring_wal.assert_called_once_with()
            inst._simple_primary_switch.assert_called_once()
            inst._rewind_from_source.assert_not_called()

    def test_does_not_enable_restore_when_history_cannot_be_installed(self):
        """PostgreSQL must see history before it can restore old-timeline WAL."""
        inst = _make_pgconsul()
        state = {
            'alive': True, 'running': True, 'role': 'replica', 'timeline': 1,
        }
        inst._get_db_state = MagicMock(return_value=state)
        inst.db.get_state.return_value = state
        inst.db.get_restore_command.return_value = '/bin/false'
        inst.db.get_wal_flush_lsn.return_value = 0x45AD3F8
        history_value = '1\t0/4732390\tbranch\n'
        inst.db.fetch_timeline_history.return_value = history_value
        inst.db.get_wal_segment_size.return_value = 16 * 1024 * 1024
        inst.db.is_wal_archived.return_value = True
        inst.db.install_timeline_history.return_value = False
        inst.zk.get_timeline.return_value = 2
        inst.zk.noexcept_get.return_value = None
        inst._acquire_replication_source_slot_lock = MagicMock()
        inst._is_simple_primary_switch_tried.return_value = True

        with patch('src.main.helpers.get_hostname', return_value='replica'):
            inst._return_to_cluster('new-primary', 'replica')

        inst.db.install_timeline_history.assert_called_once_with(2, history_value)
        inst._ensure_restoring_wal.assert_not_called()
        inst._simple_primary_switch.assert_not_called()

    def test_failed_switch_to_old_primary_does_not_rewind_from_new_primary(self):
        """kill_primary.feature:248: tried_remaster belongs to its original target."""
        inst = _make_pgconsul()
        new_primary = 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'
        inst._get_db_state = MagicMock(return_value={
            'alive': True, 'running': True, 'role': 'replica', 'timeline': 1,
        })
        inst.db.get_state.return_value = {
            'alive': True, 'running': True, 'role': 'replica', 'timeline': 1,
        }
        inst.zk.noexcept_get.return_value = None
        inst.zk.get_timeline.return_value = 2
        inst.db.get_wal_flush_lsn.return_value = 0x4000000
        inst.db.fetch_timeline_history.return_value = '1\t0/4732390\tbranch\n'
        inst._is_simple_primary_switch_tried.side_effect = lambda primary: primary == 'old-primary'
        inst._acquire_replication_source_slot_lock = MagicMock()

        with patch('src.main.helpers.get_hostname', return_value='replica'), \
             patch('src.main.helpers.is_op_destructive', return_value=False):
            inst._return_to_cluster(new_primary, 'replica', is_dead=False)

        inst._is_simple_primary_switch_tried.assert_called_once_with(new_primary)
        inst._rewind_from_source.assert_not_called()
        inst._set_simple_primary_switch_try.assert_called_once_with(new_primary)
