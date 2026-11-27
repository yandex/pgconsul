# encoding: utf-8
"""
Unit tests for Zookeeper switchover timestamp.
"""

from unittest.mock import MagicMock


class TestZookeeperSwitchover:
    """Tests for switchover methods in Zookeeper class.

    The ``zk`` fixture is provided by ``tests/unit/conftest.py``.
    """


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
