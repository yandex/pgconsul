# coding: utf8
"""Red test for cascade.feature:305 — stale ZK leader lock causes spurious
rewind of a non-HA (cascade) replica already streaming from the candidate.

During switchover, _accept_switchover_non_ha switches the cascade replica to
the candidate.  On the next iteration the ZK lock is still stale (old primary),
streaming_from_primary is False, and the "not can_delayed" branch calls
_return_to_cluster(stale_holder) → spurious rewind.

Fix: stale-lock guard in non_ha_replica_iter (same pattern as replica_iter,
report 111).
"""

from unittest.mock import MagicMock, patch

from src.switchover.types import SwitchoverPhase


# Shared factory — mirrors test_replica_iter_stale_lock_switchover._make_instance.

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
        stream_from='pgconsul_postgresql1_1.pgconsul_pgconsul_net',
        autofailover=True,
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


# ZK / DB state builders

def _stale_lock_switchover_zk_state():
    """Active switchover, stale lock holder = old primary."""
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
        'switchover_side_replicas': ['pgconsul_postgresql3_1.pgconsul_pgconsul_net'],
        'timeline_info': 1,
        'failover_state': None,
        'current_promoting_host': None,
        'failover_must_be_reset': False,
        'replics_info': [],
    }


def _cascade_replica_db_state():
    """Cascade replica already streaming from candidate (postgresql2)."""
    return {
        'alive': True,
        'running': True,
        'role': 'replica',
        'timeline': 1,
        'primary_fqdn': 'pgconsul_postgresql2_1.pgconsul_pgconsul_net',  # candidate
        'wal_receiver': {
            'pid': 241,
            'status': 'streaming',
            'slot_name': None,
            'last_msg_receipt_time_msec': 1787255243308,
            'conninfo': 'host=pgconsul_postgresql2_1.pgconsul_pgconsul_net',
        },
        'replics_info': [],
    }


# Test

class TestNonHaReplicaIterStaleLockSwitchover:
    """non_ha_replica_iter must NOT call _return_to_cluster when already
    streaming from the switchover candidate and the ZK leader lock is stale."""

    def test_no_return_to_cluster_when_streaming_from_candidate(self):
        """Stale lock holder must not trigger _return_to_cluster (spurious rewind).

        Reproduces cascade.feature:305 — "Cascade replica accepts switchover
        with stale recovery.conf".  After _accept_switchover_non_ha switches
        the cascade replica to the candidate, the next iteration sees a stale
        lock holder and tries to switch back → spurious rewind.
        """
        inst = _make_instance()
        inst.db.role = 'replica'
        inst.db.get_timeline.return_value = 1
        inst.zk.get_timeline.return_value = 1
        inst.zk.get_current_lock_holder.return_value = (
            'pgconsul_postgresql1_1.pgconsul_pgconsul_net'  # stale holder
        )
        inst.zk.get_host_op.return_value = None
        # get_replics_info calls zk.get_stream_source_replics_info(stream_from)
        inst.zk.get_stream_source_replics_info.return_value = []
        # replication source (postgresql1) is alive but not streaming
        inst.db.is_host_unreachable.return_value = False
        inst.zk.get_host_wal_receiver.return_value = None

        db_state = _cascade_replica_db_state()       # primary_fqdn=postgresql2
        zk_state = _stale_lock_switchover_zk_state()  # holder=postgresql1 (stale)

        inst._return_to_cluster = MagicMock(return_value=None)

        with patch('src.main.helpers.get_hostname',
                   return_value='pgconsul_postgresql3_1.pgconsul_pgconsul_net'), \
             patch('src.main.helpers.app_name_from_fqdn',
                   return_value='pgconsul_postgresql3_1'), \
             patch('src.main.helpers.is_op_destructive', return_value=False):
            inst.non_ha_replica_iter(db_state, zk_state)

        # _return_to_cluster must NOT be called — already streaming from candidate.
        inst._return_to_cluster.assert_not_called()

    def test_no_return_to_cluster_when_streaming_from_candidate_primary_shut(self):
        """Same scenario, switchover phase = primary_shut."""
        inst = _make_instance()
        inst.db.role = 'replica'
        inst.db.get_timeline.return_value = 1
        inst.zk.get_timeline.return_value = 1
        inst.zk.get_current_lock_holder.return_value = (
            'pgconsul_postgresql1_1.pgconsul_pgconsul_net'  # stale holder
        )
        inst.zk.get_host_op.return_value = None
        inst.zk.get_stream_source_replics_info.return_value = []
        inst.db.is_host_unreachable.return_value = False
        inst.zk.get_host_wal_receiver.return_value = None

        db_state = _cascade_replica_db_state()
        zk_state = _stale_lock_switchover_zk_state()
        zk_state['switchover_state'] = 'primary_shut'

        inst._return_to_cluster = MagicMock(return_value=None)

        with patch('src.main.helpers.get_hostname',
                   return_value='pgconsul_postgresql3_1.pgconsul_pgconsul_net'), \
             patch('src.main.helpers.app_name_from_fqdn',
                   return_value='pgconsul_postgresql3_1'), \
             patch('src.main.helpers.is_op_destructive', return_value=False):
            inst.non_ha_replica_iter(db_state, zk_state)

        inst._return_to_cluster.assert_not_called()
