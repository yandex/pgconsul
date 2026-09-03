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
        update_prio_in_zk=False,
        use_replication_slots=False,
        replication_slots_polling=False,
        priority='100',
        stream_from=None,
        autofailover=False,
        switchover_timeout=0.0,
        switchover_catchup_timeout=0.0,
        max_rewind_retries=0,
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
    inst._durability_manager = MagicMock()
    inst.last_zk_host_stat_write = 0.0
    inst.checks = {'primary_switch': 0, 'rewind': 0}
    inst._timings = MagicMock()
    # Stable string constants so we can build matching zk_state dicts.
    inst.zk.REPLICS_INFO_PATH = 'replics_info'
    inst.zk.SWITCHOVER_RECORD_PATH = 'switchover_record'
    inst.zk.SWITCHOVER_VERSION_KEY = 'switchover_version'
    inst.zk.TIMELINE_INFO_PATH = 'timeline_info'
    inst.zk.CURRENT_PROMOTING_HOST = 'current_promoting_host'
    inst.zk.FAILOVER_STATE_PATH = 'failover_state'
    inst.zk.FAILOVER_MUST_BE_RESET = 'failover_must_be_reset'
    inst.zk.SWITCHOVER_LOCK_PATH = 'switchover_lock'
    return inst


def _primary_zk_state():
    return {
        'timeline_info': 1,
        'failover_must_be_reset': False,
        'failover_state': 'finished',
        'current_promoting_host': None,
        'switchover_record': {},
        'switchover_version': 1,
    }


class TestPrimaryIterPropagation:
    """primary_iter propagates DB errors (ADR-0002 §1)."""

    def test_propagates_postgres_connection_error(self):
        inst = _make_instance()
        inst.zk.try_acquire_lock.return_value = True
        inst.zk.get_current_lock_holder.return_value = 'me'
        inst.zk.get_host_op.return_value = None
        inst.zk.try_acquire_lock.return_value = True
        inst.db.ensure_pooler_started.side_effect = PostgresConnectionError('db down')

        with pytest.raises(PostgresConnectionError):
            inst.primary_iter({'timeline': 1}, _primary_zk_state())

    def test_propagates_postgres_query_error(self):
        inst = _make_instance()
        inst.zk.try_acquire_lock.return_value = True
        inst.zk.get_current_lock_holder.return_value = 'me'
        inst.zk.get_host_op.return_value = None
        inst.db.ensure_pooler_started.side_effect = PostgresQueryError('bad result')

        with pytest.raises(PostgresQueryError):
            inst.primary_iter({'timeline': 1}, _primary_zk_state())

    @staticmethod
    def _prepare_primary_iteration(inst, timeline=2):
        inst.config.use_target_promote = True
        inst.zk.get_current_lock_holder.return_value = 'me'
        inst.zk.get_host_op.return_value = None
        inst.zk.try_acquire_lock.return_value = True
        inst.zk.get_ha_replics.return_value = []
        inst.zk.get_alive_hosts.return_value = []
        state = _primary_zk_state()
        state['timeline_info'] = timeline
        return {'timeline': timeline, 'replics_info': []}, state

    def test_initializes_missing_timeline_high_watermark(self):
        inst = _make_instance()
        db_state, zk_state = self._prepare_primary_iteration(inst)
        inst.zk.get_timeline_high_watermark.return_value = None
        inst.db.next_local_timeline.return_value = 5

        inst.primary_iter(db_state, zk_state)

        inst.db.next_local_timeline.assert_called_once_with(2)
        inst.zk.ensure_timeline_high_watermark.assert_called_once_with(4)

    def test_does_not_update_existing_timeline_high_watermark(self):
        inst = _make_instance()
        db_state, zk_state = self._prepare_primary_iteration(inst)
        inst.zk.get_timeline_high_watermark.return_value = 1

        inst.primary_iter(db_state, zk_state)

        inst.db.next_local_timeline.assert_not_called()
        inst.zk.ensure_timeline_high_watermark.assert_not_called()


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

    def test_non_streaming_replica_keeps_archive_restore_fenced(self):
        """failover_with_network_inconsistency.feature archive-barrier regression."""
        inst = _make_instance()
        inst.config.primary_switch_disable_archive_restore = True
        inst.write_host_stat = MagicMock()
        inst.replica_return = MagicMock()

        zk_state = {
            'alive': True,
            'lock_holder': 'host1',
            'replics_info': [],
        }
        inst.replica_iter(
            {'primary_fqdn': 'host1', 'wal_receiver': None},
            zk_state,
        )

        inst.replica_return.assert_called_once()
        inst.db.ensure_restoring_wal.assert_not_called()


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
