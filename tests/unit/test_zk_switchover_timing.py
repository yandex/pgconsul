# encoding: utf-8
"""
Unit tests for Zookeeper switchover and timing business methods.
"""

import json
from unittest.mock import MagicMock, patch


class TestZookeeperSwitchover:
    """Tests for switchover methods in Zookeeper class.

    The ``zk`` fixture is provided by ``tests/unit/conftest.py``.
    """

    def test_get_switchover_record_returns_json_and_version(self, zk):
        zk._zk_client.get_with_version = MagicMock()
        zk._zk_client.get_with_version.return_value = ('{"phase": "initiated"}', 7)
        assert zk.get_switchover_record() == ({'phase': 'initiated'}, 7)

    def test_write_switchover_record_uses_cas(self, zk):
        zk._zk_client.compare_and_set = MagicMock()
        zk._zk_client.compare_and_set.return_value = 8
        record = {'phase': 'initiated'}
        assert zk.write_switchover_record(record, 7) == 8
        zk._zk_client.compare_and_set.assert_called_once_with(
            'switchover/record', json.dumps(record), 7,
        )

    def test_write_switchover_record_reports_conflict(self, zk):
        zk._zk_client.compare_and_set = MagicMock()
        zk._zk_client.compare_and_set.return_value = None
        assert zk.write_switchover_record({'phase': 'failed'}, 7) is None

    # === write_last_switchover_time tests ===

    def test_write_last_switchover_time_calls_write(self, zk):
        """Test write_last_switchover_time writes current time."""
        zk.write = MagicMock(return_value=True)
        result = zk.write_last_switchover_time()
        assert result is True
        call_args = zk.write.call_args
        assert call_args[0][0] == 'last_switchover_time'
        assert isinstance(call_args[0][1], float)
        assert call_args[1]['need_lock'] is False

    def test_write_last_switchover_time_failure_returns_false(self, zk):
        """Test write_last_switchover_time returns False on exception."""
        zk.write = MagicMock(side_effect=Exception('ZK error'))
        result = zk.write_last_switchover_time()
        assert result is False

    # === cleanup_switchover tests ===

    def test_cleanup_switchover_clears_record_with_expected_version(self, zk):
        zk.write_switchover_record = MagicMock(return_value=12)
        assert zk.cleanup_switchover(11) is True
        zk.write_switchover_record.assert_called_once_with({}, 11)

    def test_cleanup_switchover_rejects_stale_version(self, zk):
        zk.write_switchover_record = MagicMock(return_value=None)
        assert zk.cleanup_switchover(11) is False

    def test_switchover_ack_is_scoped_to_operation(self, zk):
        zk.noexcept_write = MagicMock(return_value=True)

        assert zk.write_switchover_ack('host2', 'op-1', {'bridge_ready': True})

        zk.noexcept_write.assert_called_once_with(
            'switchover/acks/host2',
            {'operation_id': 'op-1', 'bridge_ready': True},
            preproc=json.dumps,
            need_lock=False,
        )

    def test_switchover_ack_ignores_previous_operation(self, zk):
        zk.noexcept_get = MagicMock(return_value={'operation_id': 'old-op', 'bridge_ready': True})

        assert zk.get_switchover_ack('host2', 'op-1') is None


class TestZookeeperTiming:
    """Tests for timing methods in Zookeeper class.

    The ``zk`` fixture is provided by ``tests/unit/conftest.py``.
    """

    # === get_timing tests ===

    def test_get_timing_returns_float(self, zk):
        """Test get_timing returns float timestamp."""
        zk.noexcept_get = MagicMock(return_value=1234.5)
        result = zk.get_timing('failover')
        assert result == 1234.5
        zk.noexcept_get.assert_called_once_with('timing/failover', preproc=float)

    def test_get_timing_returns_none_on_error(self, zk):
        """Test get_timing returns None on error."""
        zk.noexcept_get = MagicMock(return_value=None)
        result = zk.get_timing('failover')
        assert result is None

    # === write_timing tests ===

    def test_write_timing_calls_ensure_path_and_noexcept_write(self, zk):
        """Test write_timing calls ensure_path before noexcept_write."""
        zk.ensure_path = MagicMock(return_value=True)
        zk.noexcept_write = MagicMock(return_value=True)
        zk.write_timing('failover', 1234.5)
        zk.ensure_path.assert_called_once_with('timing/failover')
        zk.noexcept_write.assert_called_once_with('timing/failover', 1234.5, need_lock=False)

    def test_write_timing_does_not_raise_on_error(self, zk):
        """Test write_timing does not raise exception on error."""
        zk.ensure_path = MagicMock(side_effect=Exception('ZK error'))
        # Should not raise
        zk.write_timing('failover', 1234.5)

    # === delete_timing tests ===

    def test_delete_timing_calls_delete(self, zk):
        """Test delete_timing calls delete with recursive=True."""
        zk.delete = MagicMock(return_value=True)
        result = zk.delete_timing('failover')
        assert result is True
        zk.delete.assert_called_once_with('timing/failover', recursive=True)

    def test_delete_timing_failure_returns_false(self, zk):
        """Test delete_timing returns False when delete fails."""
        zk.delete = MagicMock(return_value=False)
        result = zk.delete_timing('failover')
        assert result is False
