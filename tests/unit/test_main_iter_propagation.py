# coding: utf8
"""
PostgreSQL connection errors in the non-HA replica iteration reach the outer loop.
"""
from unittest.mock import MagicMock, patch

import pytest

from src.main import RecoveryChecks
from src.types import DbState, ReplicaInfo, ZkState
from src.exceptions import PostgresConnectionError


def _make_instance():
    from src.main import PgconsulConfig
    from src.main import Pgconsul
    with patch('src.main.pgconsul.__init__', return_value=None):
        inst = Pgconsul.__new__(Pgconsul)
    inst.db = MagicMock()
    inst.db.role = 'replica'
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
    inst.last_zk_host_stat_write = 0.0
    inst.checks = RecoveryChecks()
    inst.zk.REPLICS_INFO_PATH = 'replics_info'
    inst.zk.TIMELINE_INFO_PATH = 'timeline_info'
    inst.zk.SWITCHOVER_LOCK_PATH = 'switchover_lock'
    return inst


def _primary_zk_state():
    return ZkState(
        timeline=1,
        failover_must_be_reset=False,
        failover_state='finished',
        current_promoting_host=None,
        switchover=None,
    )


class TestNonHaReplicaIterPropagation:
    """non_ha_replica_iter propagates DB errors (ADR-0002 §1)."""

    def test_propagates_postgres_connection_error(self):
        inst = _make_instance()
        inst.zk.get_host_op.return_value = None
        inst.config.stream_from = 'upstream'
        # Force streaming=True so we reach start_pooler → pgpooler('status') (DB call).
        with patch.object(inst, '_get_streaming_replica_from_replics_info', return_value=ReplicaInfo.from_dict({'state': 'streaming'})):
            inst.db.pgpooler.side_effect = PostgresConnectionError('db down')

            zk_state = ZkState(
                alive=True,
                lock_holder='host1',
                replics_info=[],
                switchover=None,  # required by _check_replica_switchover
            )
            with pytest.raises(PostgresConnectionError):
                inst.non_ha_replica_iter(DbState.from_dict({'wal_receiver': {'status': 'streaming'}}), zk_state)
