# coding: utf8
"""
Red unit test for MDB-41951: replica_iter does not handle switchover phase FAILED.

Reproduces the bug from anywhere_switchover.feature:132 (@switchover_failed_promote):
  - Switchover advances: pg_stopped → primary_shut (lock released) → candidate
    takes the lock → promote fails (sleep 3 && false) → switchover state = FAILED.
  - The candidate (now replica again) and the old primary both enter replica_iter.
  - _check_replica_switchover() returns True (switchover record still in ZK).
  - sw_record.is_active() returns False (FAILED is not in the active set).
  - sw_record.phase = FAILED — not in the return-to-cluster phase list.
  - Falls through to "Switchover in progress (phase failed), waiting" →
    infinite loop, no one becomes primary.

The fix: in replica_iter, when switchover phase is FAILED and there is no
lock holder, the replica must fall back to failover (_accept_failover) so the
cluster can recover a primary instead of waiting forever.
"""
from unittest.mock import MagicMock, patch

import pytest


def _make_instance():
    from src.main import PgconsulConfig, Pgconsul
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
        switchover_replica_turn_timeout=0.0,
        switchover_rollback_timeout=0.0,
        switchover_catchup_timeout=0.0,
        max_rewind_retries=0,
        election_timeout=0,
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
    inst._master_lost_ts = None
    inst._is_single_node = False
    inst._slot_manager = MagicMock()
    inst._replication_manager = MagicMock()
    inst._timings = MagicMock()
    inst._maintenance = MagicMock()
    inst._maintenance.is_in_maintenance = False
    inst.last_zk_host_stat_write = 0.0
    inst.checks = {'primary_switch': 0, 'rewind': 0}
    inst._executor = MagicMock()
    inst._cand_machine = MagicMock()
    inst._sw_machine = MagicMock()
    # ZK path constants
    inst.zk.PRIMARY_LOCK_PATH = 'leader'
    inst.zk.SWITCHOVER_STATE_PATH = 'switchover_state'
    inst.zk.SWITCHOVER_ROOT_PATH = 'switchover_root'
    inst.zk.SWITCHOVER_SIDE_REPLICAS = 'switchover_side_replicas'
    inst.zk.SWITCHOVER_CANDIDATE = 'switchover_candidate'
    inst.zk.TIMELINE_INFO_PATH = 'timeline_info'
    inst.zk.FAILOVER_STATE_PATH = 'failover_state'
    inst.zk.CURRENT_PROMOTING_HOST = 'current_promoting_host'
    inst.zk.FAILOVER_MUST_BE_RESET = 'failover_must_be_reset'
    inst.zk.REPLICS_INFO_PATH = 'replics_info'
    return inst


def _failed_switchover_zk_state():
    """ZK state where switchover has FAILED and no one holds the leader lock."""
    return {
        'alive': True,
        'lock_holder': None,
        'switchover_state': 'failed',
        'switchover_root': {
            'hostname': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net',
            'timeline_info': 1,
            'destination': 'pgconsul_postgresql3_1.pgconsul_pgconsul_net',
        },
        'switchover_candidate': 'pgconsul_postgresql3_1.pgconsul_pgconsul_net',
        'timeline_info': 1,
        'failover_state': 'switchover_initiated',
        'current_promoting_host': None,
        'failover_must_be_reset': False,
        'replics_info': [],
    }


def _replica_db_state():
    """DB state for a live replica."""
    return {
        'alive': True,
        'running': True,
        'role': 'replica',
        'timeline': 1,
        'primary_fqdn': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net',
    }


class TestReplicaIterHandlesSwitchoverFailed:
    """replica_iter must fall back to failover when switchover phase is FAILED
    and no one holds the leader lock.

    Reproduces: anywhere_switchover.feature:132 (@switchover_failed_promote)
    Bug: replica_iter enters an infinite "waiting" loop when switchover phase
    is FAILED — the cluster is left without a primary.
    """

    def test_replica_iter_fails_over_when_switchover_failed_no_lock(self):
        """When switchover phase is FAILED and there is no lock holder,
        replica_iter must call _accept_failover(switchover_in_progress=True)
        instead of returning False (waiting forever).
        """
        inst = _make_instance()
        inst.db.role = 'replica'
        inst.db.get_timeline.return_value = 1
        # _check_replica_switchover reads switchover_info and timeline.
        inst.zk.get_timeline.return_value = 1
        # No one holds the leader lock — cluster has no primary.
        inst.zk.get_current_lock_holder.return_value = None
        inst.zk.get_host_op.return_value = None

        db_state = _replica_db_state()
        zk_state = _failed_switchover_zk_state()

        # Track whether _accept_failover is called.
        inst._accept_failover = MagicMock(return_value=None)

        with patch('src.main.helpers.get_hostname',
                   return_value='pgconsul_postgresql3_1.pgconsul_pgconsul_net'), \
             patch('src.main.helpers.app_name_from_fqdn',
                   return_value='pgconsul_postgresql3_1'), \
             patch('src.main.helpers.is_op_destructive', return_value=False):
            inst.replica_iter(db_state, zk_state)

        # The fix: _accept_failover must be called with switchover_in_progress=True.
        inst._accept_failover.assert_called_once_with(switchover_in_progress=True)
