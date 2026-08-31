# coding: utf8
"""
Tests for timeline holdoff (ADR-0005 §1): replaces the former blocking
time.sleep(10 * iteration_timeout) in _verify_timeline with a ZK-based
holdoff marker.
"""
import time as time_module
from unittest.mock import MagicMock, patch

import pytest

from src.main import Pgconsul, PgconsulConfig


def _make_instance(iteration_timeout=1.0):
    with patch('src.main.pgconsul.__init__', return_value=None):
        inst = Pgconsul.__new__(Pgconsul)
    inst.db = MagicMock()
    inst.db.role = 'primary'
    inst.zk = MagicMock()
    inst.config = PgconsulConfig(
        welcome_message='',
        working_dir='/tmp',
        iteration_timeout=iteration_timeout,
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
    return inst


class TestStartTimelineHoldoff:
    def test_writes_timing_to_zk(self):
        inst = _make_instance()
        with patch('src.main.time.time', return_value=1000.0):
            inst._start_timeline_holdoff()
        inst.zk.write_timing.assert_called_once_with(inst.TIMELINE_HOLDOFF_NAME, 1000.0)


class TestIsTimelineHoldoffActive:
    def test_no_holdoff_returns_false(self):
        inst = _make_instance()
        inst.zk.get_timing.return_value = None
        assert inst._is_timeline_holdoff_active() is False
        inst.zk.delete_timing.assert_not_called()

    def test_active_holdoff_returns_true(self):
        inst = _make_instance(iteration_timeout=1.0)
        # Holdoff started 1 second ago, grace = 10 * 1.0 = 10 seconds
        inst.zk.get_timing.return_value = time_module.time() - 1.0
        assert inst._is_timeline_holdoff_active() is True
        inst.zk.delete_timing.assert_not_called()

    def test_expired_holdoff_cleared_and_returns_false(self):
        inst = _make_instance(iteration_timeout=1.0)
        # Holdoff started 100 seconds ago, grace = 10 seconds → expired
        inst.zk.get_timing.return_value = time_module.time() - 100.0
        assert inst._is_timeline_holdoff_active() is False
        inst.zk.delete_timing.assert_called_once_with(inst.TIMELINE_HOLDOFF_NAME)


class TestVerifyTimelineNoSleep:
    """_verify_timeline must not call time.sleep when ZK timeline is newer."""

    def test_no_sleep_on_newer_zk_timeline(self):
        inst = _make_instance(iteration_timeout=1.0)
        inst.zk.TIMELINE_INFO_PATH = 'timeline'
        inst.zk.REPLICS_INFO_PATH = 'replics_info'
        db_state = {'timeline': 1}
        zk_state = {'timeline': 2, 'replics_info_written': True}

        with patch('src.main.time.sleep') as mock_sleep:
            result = inst._verify_timeline(db_state, zk_state)
        assert result is None
        mock_sleep.assert_not_called()
        inst.zk.release_lock.assert_called_once()
        inst.zk.write_timing.assert_called_once_with(inst.TIMELINE_HOLDOFF_NAME, pytest.approx(time_module.time()))
