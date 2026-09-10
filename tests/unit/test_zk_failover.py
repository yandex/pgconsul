# encoding: utf-8
"""
Unit tests for Zookeeper failover state business methods.
"""

from unittest.mock import MagicMock, patch


class TestZookeeperFailoverState:
    """Tests for failover state methods in Zookeeper class.

    The ``zk`` fixture is provided by ``tests/unit/conftest.py``.
    """

    # === get_failover_state tests ===

    def test_get_failover_state_returns_value(self, zk):
        """Test get_failover_state returns value from noexcept_get."""
        zk.noexcept_get = MagicMock(return_value='promoting')
        result = zk.get_failover_state()
        assert result == 'promoting'
        zk.noexcept_get.assert_called_once_with('failover_state')

    def test_get_failover_state_returns_none(self, zk):
        """Test get_failover_state returns None when not set."""
        zk.noexcept_get = MagicMock(return_value=None)
        result = zk.get_failover_state()
        assert result is None

    # === write_failover_state tests ===

    def test_write_failover_state_calls_write(self, zk):
        """Test write_failover_state writes state string."""
        zk.write = MagicMock(return_value=True)
        result = zk.write_failover_state('promoting')
        assert result is True
        zk.write.assert_called_once_with('failover_state', 'promoting')

    def test_write_failover_state_with_finished(self, zk):
        """Test write_failover_state with 'finished' state."""
        zk.write = MagicMock(return_value=True)
        zk.write_failover_state('finished')
        zk.write.assert_called_once_with('failover_state', 'finished')

    def test_write_failover_state_failure_returns_false(self, zk):
        """Test write_failover_state returns False on exception."""
        zk.write = MagicMock(side_effect=Exception('ZK error'))
        result = zk.write_failover_state('promoting')
        assert result is False

    def test_write_failover_state_no_lock_returns_false(self, zk):
        """Test write_failover_state returns False when write() returns False (no lock holder)."""
        zk.write = MagicMock(return_value=False)
        result = zk.write_failover_state('promoting')
        assert result is False

    # === delete_failover_state tests ===

    def test_delete_failover_state_calls_delete(self, zk):
        """Test delete_failover_state calls delete."""
        zk.delete = MagicMock(return_value=True)
        result = zk.delete_failover_state()
        assert result is True
        zk.delete.assert_called_once_with('failover_state')

    def test_delete_failover_state_failure_returns_false(self, zk):
        """Test delete_failover_state returns False when delete fails."""
        zk.delete = MagicMock(return_value=False)
        result = zk.delete_failover_state()
        assert result is False

    # === write_current_promoting_host tests ===

    def test_write_current_promoting_host_calls_write(self, zk):
        """Test write_current_promoting_host writes hostname."""
        zk.write = MagicMock(return_value=True)
        result = zk.write_current_promoting_host('test-host')
        assert result is True
        zk.write.assert_called_once_with('current_promoting_host', 'test-host')

    def test_write_current_promoting_host_uses_current_hostname(self, zk):
        """Test write_current_promoting_host uses helpers.get_hostname() when None."""
        zk.write = MagicMock(return_value=True)
        with patch('src.zk.helpers.get_hostname', return_value='my-host'):
            zk.write_current_promoting_host()
            zk.write.assert_called_once_with('current_promoting_host', 'my-host')

    def test_write_current_promoting_host_failure_returns_false(self, zk):
        """Test write_current_promoting_host returns False on exception."""
        zk.write = MagicMock(side_effect=Exception('ZK error'))
        result = zk.write_current_promoting_host('test-host')
        assert result is False

    # === delete_current_promoting_host tests ===

    def test_delete_current_promoting_host_calls_delete(self, zk):
        """Test delete_current_promoting_host calls delete."""
        zk.delete = MagicMock(return_value=True)
        result = zk.delete_current_promoting_host()
        assert result is True
        zk.delete.assert_called_once_with('current_promoting_host')

    def test_delete_current_promoting_host_failure_returns_false(self, zk):
        """Test delete_current_promoting_host returns False when delete fails."""
        zk.delete = MagicMock(return_value=False)
        result = zk.delete_current_promoting_host()
        assert result is False

    # === ensure_failover_must_be_reset tests ===

    def test_ensure_failover_must_be_reset_success(self, zk):
        """Test ensure_failover_must_be_reset returns True on success."""
        zk.ensure_path = MagicMock(return_value='result')
        result = zk.ensure_failover_must_be_reset()
        assert result is True
        zk.ensure_path.assert_called_once_with('failover_must_be_reset')

    def test_ensure_failover_must_be_reset_failure_returns_false(self, zk):
        """Test ensure_failover_must_be_reset returns False when ensure_path returns None."""
        zk.ensure_path = MagicMock(return_value=None)
        result = zk.ensure_failover_must_be_reset()
        assert result is False

    # === delete_failover_must_be_reset tests ===

    def test_delete_failover_must_be_reset_calls_delete(self, zk):
        """Test delete_failover_must_be_reset calls delete."""
        zk.delete = MagicMock(return_value=True)
        result = zk.delete_failover_must_be_reset()
        assert result is True
        zk.delete.assert_called_once_with('failover_must_be_reset')

    def test_delete_failover_must_be_reset_failure_returns_false(self, zk):
        """Test delete_failover_must_be_reset returns False when delete fails."""
        zk.delete = MagicMock(return_value=False)
        result = zk.delete_failover_must_be_reset()
        assert result is False

    # === get_last_failover_time tests ===

    def test_get_last_failover_time_returns_float(self, zk):
        """Test get_last_failover_time returns float timestamp."""
        expected = 1234567890.123
        zk.noexcept_get = MagicMock(return_value=expected)
        result = zk.get_last_failover_time()
        assert result == expected
        zk.noexcept_get.assert_called_once_with('last_failover_time', preproc=float)

    def test_get_last_failover_time_returns_none(self, zk):
        """Test get_last_failover_time returns None when not set."""
        zk.noexcept_get = MagicMock(return_value=None)
        result = zk.get_last_failover_time()
        assert result is None

    # === write_last_failover_time tests ===

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

    # === get_last_primary_availability_time tests ===

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

    # === write_last_primary_availability_time tests ===

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
