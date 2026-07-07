# encoding: utf-8
"""
Unit tests for Zookeeper switchover and timing business methods.
"""

import json
from unittest.mock import MagicMock, patch

import pytest


class TestZookeeperSwitchover:
    """Tests for switchover methods in Zookeeper class."""

    @pytest.fixture
    def zk(self):
        """Create a Zookeeper instance with mocked dependencies."""
        with patch('src.zk.KazooClient'), \
             patch('src.zk.helpers.get_lockpath_prefix', return_value='/pgconsul/'):
            from src.zk import Zookeeper
            config = MagicMock()
            config.getint.return_value = 10
            config.getfloat.return_value = 5.0
            config.getboolean.return_value = False
            config.get.return_value = '/pgconsul/'
            zk = Zookeeper(config, plugins=MagicMock())
            return zk

    # === get_switchover_state tests ===

    def test_get_switchover_state_returns_value(self, zk):
        """Test get_switchover_state returns value from get."""
        zk.get = MagicMock(return_value='initiated')
        result = zk.get_switchover_state()
        assert result == 'initiated'
        zk.get.assert_called_once_with('switchover/state')

    def test_get_switchover_state_returns_none(self, zk):
        """Test get_switchover_state returns None when not set."""
        zk.get = MagicMock(return_value=None)
        result = zk.get_switchover_state()
        assert result is None

    # === write_switchover_state tests ===

    def test_write_switchover_state_calls_write(self, zk):
        """Test write_switchover_state writes state with need_lock=False."""
        zk.write = MagicMock(return_value=True)
        result = zk.write_switchover_state('initiated')
        assert result is True
        zk.write.assert_called_once_with('switchover/state', 'initiated', need_lock=False)

    def test_write_switchover_state_with_candidate_found(self, zk):
        """Test write_switchover_state with 'candidate_found' state."""
        zk.write = MagicMock(return_value=True)
        zk.write_switchover_state('candidate_found')
        zk.write.assert_called_once_with('switchover/state', 'candidate_found', need_lock=False)

    def test_write_switchover_state_failure_returns_false(self, zk):
        """Test write_switchover_state returns False on exception."""
        zk.write = MagicMock(side_effect=Exception('ZK error'))
        result = zk.write_switchover_state('initiated')
        assert result is False

    # === get_switchover_primary_info tests ===

    def test_get_switchover_primary_info_parses_json(self, zk):
        """Test get_switchover_primary_info returns parsed JSON."""
        expected = {'fqdn': 'primary.example.com', 'timeline': 5}
        zk.get = MagicMock(return_value=expected)
        result = zk.get_switchover_primary_info()
        assert result == expected
        zk.get.assert_called_once_with('switchover/master', preproc=json.loads)

    def test_get_switchover_primary_info_returns_none(self, zk):
        """Test get_switchover_primary_info returns None when not set."""
        zk.get = MagicMock(return_value=None)
        result = zk.get_switchover_primary_info()
        assert result is None

    # === write_switchover_candidate tests ===

    def test_write_switchover_candidate_calls_write(self, zk):
        """Test write_switchover_candidate writes candidate hostname."""
        zk.write = MagicMock(return_value=True)
        result = zk.write_switchover_candidate('candidate-host')
        assert result is True
        zk.write.assert_called_once_with('switchover/candidate', 'candidate-host')

    def test_write_switchover_candidate_failure_returns_false(self, zk):
        """Test write_switchover_candidate returns False on exception."""
        zk.write = MagicMock(side_effect=Exception('ZK error'))
        result = zk.write_switchover_candidate('candidate-host')
        assert result is False

    # === get_switchover_candidate_host tests ===

    def test_get_switchover_candidate_host_returns_value(self, zk):
        """Test get_switchover_candidate_host returns hostname."""
        zk.get = MagicMock(return_value='candidate-host')
        result = zk.get_switchover_candidate_host()
        assert result == 'candidate-host'
        zk.get.assert_called_once_with('switchover/candidate')

    # === write_switchover_side_replicas tests ===

    def test_write_switchover_side_replicas_serializes_json(self, zk):
        """Test write_switchover_side_replicas serializes list as JSON."""
        zk.write = MagicMock(return_value=True)
        replicas = ['replica1', 'replica2']
        result = zk.write_switchover_side_replicas(replicas)
        assert result is True
        zk.write.assert_called_once_with('switchover/side_replicas', replicas, preproc=json.dumps)

    def test_write_switchover_side_replicas_failure_returns_false(self, zk):
        """Test write_switchover_side_replicas returns False on exception."""
        zk.write = MagicMock(side_effect=Exception('ZK error'))
        result = zk.write_switchover_side_replicas([])
        assert result is False

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

    def test_cleanup_switchover_deletes_all_paths(self, zk):
        """Test cleanup_switchover deletes all 5 switchover paths."""
        zk.delete = MagicMock()
        zk.cleanup_switchover()
        assert zk.delete.call_count == 5
        deleted_paths = [call[0][0] for call in zk.delete.call_args_list]
        assert 'switchover/candidate' in deleted_paths
        assert 'switchover/side_replicas' in deleted_paths
        assert 'switchover/state' in deleted_paths
        assert 'switchover/master' in deleted_paths
        assert 'failover_state' in deleted_paths

    def test_cleanup_switchover_continues_on_error(self, zk):
        """Test cleanup_switchover continues even if one delete fails."""
        zk.delete = MagicMock(side_effect=[Exception('ZK error'), None, None, None, None])
        # Should not raise exception
        zk.cleanup_switchover()
        assert zk.delete.call_count == 5


class TestZookeeperTiming:
    """Tests for timing methods in Zookeeper class."""

    @pytest.fixture
    def zk(self):
        """Create a Zookeeper instance with mocked dependencies."""
        with patch('src.zk.KazooClient'), \
             patch('src.zk.helpers.get_lockpath_prefix', return_value='/pgconsul/'):
            from src.zk import Zookeeper
            config = MagicMock()
            config.getint.return_value = 10
            config.getfloat.return_value = 5.0
            config.getboolean.return_value = False
            config.get.return_value = '/pgconsul/'
            zk = Zookeeper(config, plugins=MagicMock())
            return zk

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
        """Test delete_timing returns False on exception."""
        zk.delete = MagicMock(side_effect=Exception('ZK error'))
        result = zk.delete_timing('failover')
        assert result is False
