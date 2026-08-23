# coding: utf8
"""
Tests for ADR-0002 §1: PostgresConnectionError / PostgresQueryError must
propagate from primary_iter / replica_iter / non_ha_replica_iter to
run_iteration() (the restart boundary). These methods must not swallow them.
"""
from unittest.mock import MagicMock, patch

import pytest

from src.exceptions import PostgresConnectionError, PostgresQueryError


def _make_instance():
    from src.main import PgconsulConfig
    from src.main import Pgconsul
    with patch('src.main.pgconsul.__init__', return_value=None):
        inst = Pgconsul.__new__(Pgconsul)
    inst.db = MagicMock()
    inst.db.role = 'primary'  # needed by _verify_timeline gate check
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
    inst._master_lost_ts = None
    inst._is_single_node = False
    inst._slot_manager = MagicMock()
    inst._replication_manager = MagicMock()
    inst.last_zk_host_stat_write = 0.0
    inst.checks = {'primary_switch': 0, 'rewind': 0}
    inst._timings = MagicMock()
    # Stable string constants so we can build matching zk_state dicts.
    inst.zk.REPLICS_INFO_PATH = 'replics_info'
    inst.zk.SWITCHOVER_STATE_PATH = 'switchover_state'
    inst.zk.TIMELINE_INFO_PATH = 'timeline_info'
    inst.zk.CURRENT_PROMOTING_HOST = 'current_promoting_host'
    inst.zk.FAILOVER_STATE_PATH = 'failover_state'
    inst.zk.FAILOVER_MUST_BE_RESET = 'failover_must_be_reset'
    inst.zk.SWITCHOVER_ROOT_PATH = 'switchover_root'
    inst.zk.SWITCHOVER_LOCK_PATH = 'switchover_lock'
    return inst


def _primary_zk_state():
    return {
        'timeline_info': 1,
        'failover_must_be_reset': False,
        'failover_state': 'finished',
        'current_promoting_host': None,
        'switchover_root': None,
    }


class TestPrimaryIterPropagation:
    """primary_iter propagates DB errors (ADR-0002 §1)."""

    def test_propagates_postgres_connection_error(self):
        inst = _make_instance()
        inst.zk.try_acquire_lock.return_value = True
        inst.zk.get_current_lock_holder.return_value = 'me'
        inst.zk.get_host_op.return_value = None
        inst.zk.get_switchover_primary_info.return_value = None
        inst.zk.try_acquire_lock.return_value = True
        inst.db.ensure_pooler_started.side_effect = PostgresConnectionError('db down')

        with pytest.raises(PostgresConnectionError):
            inst.primary_iter({'timeline': 1}, _primary_zk_state())

    def test_propagates_postgres_query_error(self):
        inst = _make_instance()
        inst.zk.try_acquire_lock.return_value = True
        inst.zk.get_current_lock_holder.return_value = 'me'
        inst.zk.get_host_op.return_value = None
        inst.zk.get_switchover_primary_info.return_value = None
        inst.db.ensure_pooler_started.side_effect = PostgresQueryError('bad result')

        with pytest.raises(PostgresQueryError):
            inst.primary_iter({'timeline': 1}, _primary_zk_state())


class TestReplicaIterPropagation:
    """replica_iter propagates DB errors (ADR-0002 §1)."""

    def test_propagates_postgres_connection_error(self):
        inst = _make_instance()
        inst.zk.get_host_op.return_value = None
        inst.zk.get_children.return_value = []
        # holder == primary_fqdn so we reach ensure_replaying_wal (direct DB call).
        inst.db.ensure_replaying_wal.side_effect = PostgresConnectionError('db down')

        zk_state = {
            'alive': True,
            'lock_holder': 'host1',
            'replics_info': [],
            'timeline_info': 1,
            'switchover_root': None,  # required by _check_replica_switchover
        }
        with pytest.raises(PostgresConnectionError):
            inst.replica_iter({'primary_fqdn': 'host1', 'wal_receiver': None}, zk_state)


class TestNonHaReplicaIterPropagation:
    """non_ha_replica_iter propagates DB errors (ADR-0002 §1)."""

    def test_propagates_postgres_connection_error(self):
        inst = _make_instance()
        inst.zk.get_host_op.return_value = None
        inst.config.stream_from = 'upstream'
        # Force streaming=True so we reach start_pooler → pgpooler('status') (DB call).
        with patch.object(inst, '_get_streaming_replica_from_replics_info', return_value={'state': 'streaming'}):
            inst.db.pgpooler.side_effect = PostgresConnectionError('db down')

            zk_state = {
                'alive': True,
                'lock_holder': 'host1',
                'replics_info': [],
                'switchover_root': None,  # required by _check_replica_switchover
            }
            with pytest.raises(PostgresConnectionError):
                inst.non_ha_replica_iter({'wal_receiver': {'status': 'streaming'}}, zk_state)
