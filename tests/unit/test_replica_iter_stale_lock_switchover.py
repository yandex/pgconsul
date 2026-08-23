# coding: utf8
"""Red test for switchover_kill9_survives.feature:127 — stale ZK leader lock
causes spurious rewind of a side replica already streaming from the candidate.

kill -9 on primary during switchover leaves a stale ZK lock; replica_iter
falls through to change_primary(stale_holder) → spurious rewind.
Fix: guard skips change_primary when already streaming from the candidate.
"""

from unittest.mock import MagicMock, patch

from src.switchover.types import SwitchoverPhase


# Shared factory — mirrors test_replica_iter_fqdn_mismatch._make_instance.

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
    return inst


# ZK / DB state builders

def _stale_lock_switchover_zk_state():
    """Active switchover, stale lock holder = old primary (kill -9 scenario)."""
    return {
        'alive': True,
        'lock_holder': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net',  # stale
        'switchover_state': 'initiated',  # active, not failed
        'switchover_root': {
            'hostname': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net',  # old primary
            'timeline_info': 1,
            'destination': 'pgconsul_postgresql2_1.pgconsul_pgconsul_net',  # candidate
        },
        'switchover_candidate': 'pgconsul_postgresql2_1.pgconsul_pgconsul_net',
        'timeline_info': 1,
        'failover_state': None,
        'current_promoting_host': None,
        'failover_must_be_reset': False,
        'replics_info': [],
    }


def _side_replica_db_state():
    """Side replica already streaming from candidate (FQDN mismatch)."""
    return {
        'alive': True,
        'running': True,
        'role': 'replica',
        'timeline': 1,
        'primary_fqdn': 'pgconsul_postgresql2_1.pgconsul_pgconsul_net',  # candidate
    }


# Test

class TestReplicaIterStaleLockSwitchover:
    """replica_iter must NOT call change_primary when already streaming from
    the switchover candidate and the ZK leader lock is stale."""

    def test_no_change_primary_when_streaming_from_candidate(self):
        """Stale lock holder must not trigger change_primary (spurious rewind)."""
        inst = _make_instance()
        inst.db.role = 'replica'
        inst.db.get_timeline.return_value = 1
        inst.zk.get_timeline.return_value = 1
        inst.zk.get_current_lock_holder.return_value = (
            'pgconsul_postgresql1_1.pgconsul_pgconsul_net'  # stale holder
        )
        inst.zk.get_host_op.return_value = None

        db_state = _side_replica_db_state()       # primary_fqdn=postgresql2
        zk_state = _stale_lock_switchover_zk_state()  # holder=postgresql1 (stale)

        inst.change_primary = MagicMock(return_value=None)
        inst._return_to_cluster = MagicMock(return_value=None)

        with patch('src.main.helpers.get_hostname',
                   return_value='pgconsul_postgresql3_1.pgconsul_pgconsul_net'), \
             patch('src.main.helpers.app_name_from_fqdn',
                   return_value='pgconsul_postgresql3_1'), \
             patch('src.main.helpers.is_op_destructive', return_value=False):
            inst.replica_iter(db_state, zk_state)

        # change_primary must NOT be called — already streaming from candidate.
        inst.change_primary.assert_not_called()

    def test_no_change_primary_when_streaming_from_candidate_primary_shut(self):
        """Same scenario, switchover phase = primary_shut."""
        inst = _make_instance()
        inst.db.role = 'replica'
        inst.db.get_timeline.return_value = 1
        inst.zk.get_timeline.return_value = 1
        inst.zk.get_current_lock_holder.return_value = (
            'pgconsul_postgresql1_1.pgconsul_pgconsul_net'  # stale holder
        )
        inst.zk.get_host_op.return_value = None

        db_state = _side_replica_db_state()
        zk_state = _stale_lock_switchover_zk_state()
        zk_state['switchover_state'] = 'primary_shut'

        inst.change_primary = MagicMock(return_value=None)
        inst._return_to_cluster = MagicMock(return_value=None)

        with patch('src.main.helpers.get_hostname',
                   return_value='pgconsul_postgresql3_1.pgconsul_pgconsul_net'), \
             patch('src.main.helpers.app_name_from_fqdn',
                   return_value='pgconsul_postgresql3_1'), \
             patch('src.main.helpers.is_op_destructive', return_value=False):
            inst.replica_iter(db_state, zk_state)

        inst.change_primary.assert_not_called()
