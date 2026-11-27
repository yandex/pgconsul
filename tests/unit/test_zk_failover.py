# encoding: utf-8
"""
Unit tests for Zookeeper failover timestamps.
"""

from unittest.mock import MagicMock


class TestZookeeperFailoverState:
    """Tests for failover timestamp methods in Zookeeper class.

    The ``zk`` fixture is provided by ``tests/unit/conftest.py``.
    """


    def test_write_last_failover_time_calls_write(self, zk):
        """Test write_last_failover_time writes current time as float."""
        zk.write = MagicMock(return_value=True)
        result = zk.write_last_failover_time()
        assert result is True
        call_args = zk.write.call_args
        assert call_args[0][0] == 'last_failover_time'
        assert isinstance(call_args[0][1], float)
        assert call_args[1]['need_lock'] is False

    def test_write_last_failover_time_failure_returns_false(self, zk):
        """Test write_last_failover_time returns False on exception."""
        zk.write = MagicMock(side_effect=Exception('ZK error'))
        result = zk.write_last_failover_time()
        assert result is False


    def test_get_last_primary_availability_time_returns_float(self, zk):
        """Test get_last_primary_availability_time returns float timestamp."""
        expected = 1234567890.123
        zk.noexcept_get = MagicMock(return_value=expected)
        result = zk.get_last_primary_availability_time()
        assert result == expected
        zk.noexcept_get.assert_called_once_with('last_master_activity_time', preproc=float)

    def test_get_last_primary_availability_time_returns_none(self, zk):
        """Test get_last_primary_availability_time returns None on error."""
        zk.noexcept_get = MagicMock(return_value=None)
        result = zk.get_last_primary_availability_time()
        assert result is None


    def test_write_last_primary_availability_time_calls_write(self, zk):
        """Test write_last_primary_availability_time writes current time."""
        zk.write = MagicMock(return_value=True)
        result = zk.write_last_primary_availability_time()
        assert result is True
        call_args = zk.write.call_args
        assert call_args[0][0] == 'last_master_activity_time'
        assert isinstance(call_args[0][1], float)

    def test_write_last_primary_availability_time_failure_returns_false(self, zk):
        """Test write_last_primary_availability_time returns False on exception."""
        zk.write = MagicMock(side_effect=Exception('ZK error'))
        result = zk.write_last_primary_availability_time()
        assert result is False
