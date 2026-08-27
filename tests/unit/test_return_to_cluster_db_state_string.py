# encoding: utf-8
"""
Red test: _return_to_cluster crashes when _get_db_state returns a string.

Reproduces: tests/features/targeted_switchover.feature — "Host fail targeted
switchover" scenario, where postgresql2/postgresql3 are dead and cannot
return to cluster.

Root cause: _return_to_cluster() Pass 1 passes ``db_state=state`` where
``state`` is the return value of ``_get_db_state()`` — a *string* from
``pg_controldata`` (e.g. ``"shut down"``).  ReturnObservation.build() calls
``db_state.get('role')`` which raises ``AttributeError: 'str' object has no
attribute 'get'`` because strings have no ``.get()`` method.

Pass 2 correctly uses ``self.db.get_state() or {}`` (a dict), but the crash
in Pass 1 prevents the iteration from ever reaching Pass 2.
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
        use_lwaldump=False,
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


class TestReturnToClusterDbStateString:
    """
    _get_db_state() returns a string (pg_controldata output), not a dict.
    _return_to_cluster() must not crash with AttributeError when Pass 1
    feeds that string to ReturnObservation.build().
    """

    def test_no_crash_when_db_state_is_string(self):
        """
        Reproduces the production crash: _get_db_state() returns "shut down"
        (a string), db.get_state() returns a dict with role=None (DB is dead).
        _return_to_cluster() must complete without AttributeError.
        """
        inst = _make_pgconsul()
        new_primary = 'pgconsul_postgresql1_1.pgconsul_pgconsul_net'

        # _get_db_state returns a STRING — as it does in production
        # (pg_controldata "Database cluster state: shut down").
        inst._get_db_state = MagicMock(return_value='shut down')

        # db.get_state() returns a dict — DB is dead, role is None.
        inst.db.get_state.return_value = {
            'alive': False, 'running': False, 'role': None,
        }

        inst.zk.noexcept_get.return_value = None
        inst.zk.get_failover_state.return_value = 'finished'
        inst.zk.get_timeline.return_value = 1
        inst._is_simple_primary_switch_tried = MagicMock(return_value=False)
        inst._acquire_replication_source_slot_lock = MagicMock()

        inst._simple_primary_switch.return_value = False
        inst.db.is_host_unreachable.return_value = False
        inst.db._get_param_value.return_value = '/bin/false'

        with patch('src.main.helpers.get_hostname',
                   return_value='pgconsul_postgresql2_1.pgconsul_pgconsul_net'), \
             patch('src.main.helpers.is_op_destructive', return_value=False):
            # Must not raise AttributeError.
            inst._return_to_cluster(new_primary, None, is_dead=True)
