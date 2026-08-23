# coding: utf8
"""Red unit test for MDB-41951: replica_iter stuck in failover 'promoting'.

Reproduces the bug from failover_with_network_inconsistency.feature:69
("Failover will happen"):
  - Failover winner (postgresql2) acquires the leader lock and ZK failover
    state transitions to 'promoting'.
  - The winner is still a replica (PG role=replica) — it must run
    _run_failover_step so the participant machine executes DoFailover (promote).
  - But replica_iter sees ``holder == my_hostname`` (the winner holds the
    lock) and skips the ``holder is None`` branch that calls
    _run_failover_step. It falls through to normal replica logic (WAL replay,
    slots) and never promotes — failover stalls forever in 'promoting'.

The fix: in replica_iter, when the failover state is active (promoting,
checkpointing, creating_slots) and the lock holder is this node, the node
must call _run_failover_step to drive the participant machine (DoFailover).
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
        priority='2',
        stream_from=None,
        autofailover=True,
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
    inst.zk.ELECTION_MANAGER_LOCK_PATH = 'epoch_manager'
    inst.zk.ELECTION_WINNER_PATH = 'election_winner'
    inst.zk.ELECTION_STATUS_PATH = 'election_status'
    return inst


_MY_HOST = 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'


def _promoting_zk_state():
    """ZK state: failover is 'promoting', this node holds the leader lock."""
    return {
        'alive': True,
        'lock_holder': _MY_HOST,
        'switchover_state': None,
        'switchover_root': None,
        'switchover_candidate': None,
        'timeline_info': 1,
        'failover_state': 'promoting',
        'current_promoting_host': _MY_HOST,
        'failover_must_be_reset': False,
        'replics_info': [],
    }


def _replica_db_state():
    """DB state for a live replica (the winner, not yet promoted)."""
    return {
        'alive': True,
        'running': True,
        'role': 'replica',
        'timeline': 1,
        'primary_fqdn': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net',
    }


class TestReplicaIterPromotingStuck:
    """replica_iter must drive failover when the winner holds the lock.

    When failover_state is active (promoting/checkpointing/creating_slots)
    and the lock holder is this node, replica_iter must call
    _run_failover_step so the participant machine runs DoFailover (promote).
    Otherwise the winner holds the lock but never promotes — failover stalls.
    """

    def test_replica_iter_calls_failover_step_when_promoting_and_self_holds_lock(self):
        """Winner holds the lock + failover_state=promoting → _run_failover_step."""
        inst = _make_instance()
        inst.db.role = 'replica'
        inst.db.get_timeline.return_value = 1
        inst.zk.get_timeline.return_value = 1
        # This node (the winner) holds the leader lock.
        inst.zk.get_current_lock_holder.return_value = _MY_HOST
        inst.zk.get_host_op.return_value = None

        db_state = _replica_db_state()
        zk_state = _promoting_zk_state()

        inst._run_failover_step = MagicMock(return_value=None)

        with patch('src.main.helpers.get_hostname', return_value=_MY_HOST), \
             patch('src.main.helpers.app_name_from_fqdn',
                   return_value='pgconsul_postgresql2_1'), \
             patch('src.main.helpers.is_op_destructive', return_value=False):
            inst.replica_iter(db_state, zk_state)

        # The fix: _run_failover_step must be called to drive the participant
        # machine (DoFailover → promote). Without it, the winner holds the
        # lock but never promotes — failover stalls in 'promoting' forever.
        inst._run_failover_step.assert_called_once()
