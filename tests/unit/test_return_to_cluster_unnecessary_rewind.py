# encoding: utf-8
"""
Integration test for the return-to-cluster state machine (MDB-41951).

Verifies that _return_to_cluster() delegates to ReturnToClusterMachine
via CommandExecutor, and that the machine prevents unnecessary pg_rewind
when timelines match (transient failure) while still invoking rewind
when timelines diverge.

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
        use_lwaldump=False,
        update_prio_in_zk=False,
        use_replication_slots=False,
        replication_slots_polling=False,
        priority='100',
        stream_from=None,
        autofailover=False,
        switchover_replica_turn_timeout=0.0,
        switchover_rollback_timeout=0.0,
        switchover_catchup_timeout=0.0,
        max_rewind_retries=3,
        election_timeout=0,
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

    # Return-to-cluster state machine.
    from src.return_to_cluster import ReturnToClusterMachine
    inst._return_machine = ReturnToClusterMachine()

    # Real CommandExecutor with mocked callbacks.
    from src.command_executor import CommandExecutor
    inst._executor = CommandExecutor(
        zk=inst.zk,
        db=inst.db,
        replication_manager=MagicMock(),
        timings=inst._timings,
        stop_postgresql=MagicMock(return_value=0),
        store_replics_info=MagicMock(return_value=True),
        rewind_from_source=MagicMock(return_value=None),
        do_failover=MagicMock(return_value=True),
        set_simple_primary_switch_try=MagicMock(),
        create_slots_for_hosts=MagicMock(return_value=True),
        simple_primary_switch=MagicMock(return_value=False),
        ensure_restoring_wal=MagicMock(),
    )

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
        inst._executor._simple_primary_switch.return_value = False
        inst.db.is_host_unreachable.return_value = False
        inst.db._get_param_value.return_value = '/bin/false'

        with patch('src.main.helpers.get_hostname', return_value='pgconsul_postgresql3_1.pgconsul_pgconsul_net'), \
             patch('src.main.helpers.is_op_destructive', return_value=False):
            inst._return_to_cluster(new_primary, 'replica', is_dead=False, skip_check=True)

        # rewind_from_source must NOT be called — timelines match.
        inst._executor._rewind_from_source.assert_not_called()

    def test_rewind_when_timelines_diverge_and_simple_switch_fails(self):
        """
        Simple switch fails, timelines diverge (local=1, zk=2).
        The machine MUST call rewind_from_source.
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
        inst._is_simple_primary_switch_tried = MagicMock(return_value=False)
        inst._acquire_replication_source_slot_lock = MagicMock()

        inst._executor._simple_primary_switch.return_value = False
        inst.db.is_host_unreachable.return_value = False
        inst.db._get_param_value.return_value = '/bin/false'

        with patch('src.main.helpers.get_hostname', return_value='pgconsul_postgresql3_1.pgconsul_pgconsul_net'), \
             patch('src.main.helpers.is_op_destructive', return_value=False):
            inst._return_to_cluster(new_primary, 'replica', is_dead=False, skip_check=True)

        # rewind_from_source MUST be called — timelines diverge.
        inst._executor._rewind_from_source.assert_called_once()
