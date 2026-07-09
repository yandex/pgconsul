# encoding: utf-8
"""
Unit tests for Zookeeper maintenance, timeline and replics_info business methods.
"""

import json
from unittest.mock import MagicMock, patch


class TestZookeeperMaintenance:
    """Tests for maintenance methods in Zookeeper class.

    The ``zk`` fixture is provided by ``tests/unit/conftest.py``.
    """

    # === get_maintenance_status tests ===

    def test_get_maintenance_status_returns_value(self, zk):
        """Test get_maintenance_status returns value from get."""
        zk.get = MagicMock(return_value='enabled')
        result = zk.get_maintenance_status()
        assert result == 'enabled'
        zk.get.assert_called_once_with('maintenance')

    def test_get_maintenance_status_returns_none(self, zk):
        """Test get_maintenance_status returns None when not set."""
        zk.get = MagicMock(return_value=None)
        result = zk.get_maintenance_status()
        assert result is None

    # === delete_maintenance tests ===

    def test_delete_maintenance_success(self, zk):
        """Test delete_maintenance calls delete with recursive=True."""
        zk.delete = MagicMock(return_value=True)
        result = zk.delete_maintenance()
        assert result is True
        zk.delete.assert_called_once_with('maintenance', recursive=True)

    def test_delete_maintenance_failure_returns_false(self, zk):
        """Test delete_maintenance returns False when delete fails."""
        zk.delete = MagicMock(return_value=False)
        result = zk.delete_maintenance()
        assert result is False

    # === get_maintenance_ts tests ===

    def test_get_maintenance_ts_returns_value(self, zk):
        """Test get_maintenance_ts returns timestamp string."""
        zk.get = MagicMock(return_value='1234567890.123')
        result = zk.get_maintenance_ts()
        assert result == '1234567890.123'
        zk.get.assert_called_once_with('maintenance/ts')

    # === write_maintenance_ts tests ===

    def test_write_maintenance_ts_calls_write(self, zk):
        """Test write_maintenance_ts writes current time."""
        zk.write = MagicMock(return_value=True)
        result = zk.write_maintenance_ts()
        assert result is True
        zk.write.assert_called_once()
        call_args = zk.write.call_args
        assert call_args[0][0] == 'maintenance/ts'
        assert isinstance(call_args[0][1], float)
        assert call_args[1]['need_lock'] is False

    def test_write_maintenance_ts_failure_returns_false(self, zk):
        """Test write_maintenance_ts returns False on exception."""
        zk.write = MagicMock(side_effect=Exception('ZK error'))
        result = zk.write_maintenance_ts()
        assert result is False

    # === get_maintenance_primary tests ===

    def test_get_maintenance_primary_returns_value(self, zk):
        """Test get_maintenance_primary returns hostname."""
        zk.get = MagicMock(return_value='primary-host.example.com')
        result = zk.get_maintenance_primary()
        assert result == 'primary-host.example.com'
        zk.get.assert_called_once_with('maintenance/master')

    # === write_maintenance_primary tests ===

    def test_write_maintenance_primary_calls_write(self, zk):
        """Test write_maintenance_primary writes hostname."""
        zk.write = MagicMock(return_value=True)
        result = zk.write_maintenance_primary('new-primary.example.com')
        assert result is True
        zk.write.assert_called_once_with('maintenance/master', 'new-primary.example.com', need_lock=False)

    def test_write_maintenance_primary_failure_returns_false(self, zk):
        """Test write_maintenance_primary returns False on exception."""
        zk.write = MagicMock(side_effect=Exception('ZK error'))
        result = zk.write_maintenance_primary('test-host')
        assert result is False

    # === write_host_maintenance_enabled tests ===

    def test_write_host_maintenance_enabled_calls_write(self, zk):
        """Test write_host_maintenance_enabled writes 'enable'."""
        zk.write = MagicMock(return_value=True)
        zk._get_host_maintenance_path = MagicMock(return_value='maintenance/test-host')
        result = zk.write_host_maintenance_enabled('test-host')
        assert result is True
        zk.write.assert_called_once_with('maintenance/test-host', 'enable', need_lock=False)

    def test_write_host_maintenance_enabled_uses_current_host(self, zk):
        """Test write_host_maintenance_enabled uses current hostname when None."""
        zk.write = MagicMock(return_value=True)
        zk._get_host_maintenance_path = MagicMock(return_value='maintenance/my-host')
        with patch('src.zk.helpers.get_hostname', return_value='my-host'):
            zk.write_host_maintenance_enabled()
            zk._get_host_maintenance_path.assert_called_once_with(None)

    def test_write_host_maintenance_enabled_failure_returns_false(self, zk):
        """Test write_host_maintenance_enabled returns False on exception."""
        zk.write = MagicMock(side_effect=Exception('ZK error'))
        zk._get_host_maintenance_path = MagicMock(return_value='maintenance/test-host')
        result = zk.write_host_maintenance_enabled('test-host')
        assert result is False


class TestZookeeperTimeline:
    """Tests for timeline methods in Zookeeper class.

    The ``zk`` fixture is provided by ``tests/unit/conftest.py``.
    """

    # === get_timeline tests ===

    def test_get_timeline_returns_int(self, zk):
        """Test get_timeline returns integer timeline."""
        zk.get = MagicMock(return_value=5)
        result = zk.get_timeline()
        assert result == 5
        zk.get.assert_called_once_with('timeline', preproc=int)

    def test_get_timeline_returns_none_when_not_set(self, zk):
        """Test get_timeline returns None when timeline not set."""
        zk.get = MagicMock(return_value=None)
        result = zk.get_timeline()
        assert result is None

    # === write_timeline tests ===

    def test_write_timeline_calls_write(self, zk):
        """Test write_timeline writes timeline value."""
        zk.write = MagicMock(return_value=True)
        result = zk.write_timeline(7)
        assert result is True
        zk.write.assert_called_once_with('timeline', 7)

    def test_write_timeline_failure_returns_false(self, zk):
        """Test write_timeline returns False on exception."""
        zk.write = MagicMock(side_effect=Exception('ZK error'))
        result = zk.write_timeline(7)
        assert result is False


class TestZookeeperReplicsInfo:
    """Tests for global replics_info methods in Zookeeper class.

    The ``zk`` fixture is provided by ``tests/unit/conftest.py``.
    """

    # === get_replics_info tests ===

    def test_get_replics_info_returns_list(self, zk):
        """Test get_replics_info returns parsed JSON list."""
        expected = [{'host': 'replica1', 'lag': 100}]
        zk.get = MagicMock(return_value=expected)
        result = zk.get_replics_info()
        assert result == expected
        zk.get.assert_called_once_with('replics_info', preproc=json.loads)

    def test_get_replics_info_returns_none(self, zk):
        """Test get_replics_info returns None when not set."""
        zk.get = MagicMock(return_value=None)
        result = zk.get_replics_info()
        assert result is None

    # === noexcept_get_replics_info tests ===

    def test_noexcept_get_replics_info_returns_list(self, zk):
        """Test noexcept_get_replics_info returns parsed JSON list."""
        expected = [{'host': 'replica1', 'lag': 100}]
        zk.noexcept_get = MagicMock(return_value=expected)
        result = zk.noexcept_get_replics_info()
        assert result == expected
        zk.noexcept_get.assert_called_once_with('replics_info', preproc=json.loads)

    def test_noexcept_get_replics_info_returns_none_on_error(self, zk):
        """Test noexcept_get_replics_info returns None on error."""
        zk.noexcept_get = MagicMock(return_value=None)
        result = zk.noexcept_get_replics_info()
        assert result is None

    # === write_replics_info tests ===

    def test_write_replics_info_serializes_json(self, zk):
        """Test write_replics_info serializes data as JSON."""
        zk.write = MagicMock(return_value=True)
        replics_info = [{'host': 'replica1', 'lag': 100}]
        result = zk.write_replics_info(replics_info)
        assert result is True
        zk.write.assert_called_once_with('replics_info', replics_info, preproc=json.dumps)

    def test_write_replics_info_failure_returns_false(self, zk):
        """Test write_replics_info returns False on exception."""
        zk.write = MagicMock(side_effect=Exception('ZK error'))
        result = zk.write_replics_info([])
        assert result is False
