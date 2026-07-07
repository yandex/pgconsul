# encoding: utf-8
"""
Unit tests for Zookeeper host-level business methods.
Tests methods: get_host_op_path, get_host_op, write_host_op, delete_host_op,
ensure_host_ha, delete_host_ha, write_host_replics_info, get_host_replics_info,
write_host_wal_receiver, get_host_wal_receiver
"""

import json
from unittest.mock import MagicMock, patch

import pytest


class TestZookeeperHostMethods:
    """Tests for host-level business methods in Zookeeper class."""

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

    # === get_host_op_path tests ===

    def test_get_host_op_path_with_hostname(self, zk):
        """Test get_host_op_path returns correct path for given hostname."""
        path = zk.get_host_op_path('test-host')
        assert path == 'all_hosts/test-host/op'

    def test_get_host_op_path_without_hostname_uses_current(self, zk):
        """Test get_host_op_path uses current hostname when None."""
        with patch('src.zk.helpers.get_hostname', return_value='current-host'):
            path = zk.get_host_op_path()
            assert path == 'all_hosts/current-host/op'

    # === get_host_op tests ===

    def test_get_host_op_returns_value(self, zk):
        """Test get_host_op returns value from noexcept_get."""
        zk.noexcept_get = MagicMock(return_value='promoting')
        result = zk.get_host_op('test-host')
        assert result == 'promoting'
        zk.noexcept_get.assert_called_once_with('all_hosts/test-host/op')

    def test_get_host_op_default_hostname(self, zk):
        """Test get_host_op uses current hostname when None."""
        zk.noexcept_get = MagicMock(return_value='rewind')
        with patch('src.zk.helpers.get_hostname', return_value='my-host'):
            result = zk.get_host_op()
            assert result == 'rewind'
            zk.noexcept_get.assert_called_once_with('all_hosts/my-host/op')

    def test_get_host_op_returns_none_when_not_found(self, zk):
        """Test get_host_op returns None when node doesn't exist."""
        zk.noexcept_get = MagicMock(return_value=None)
        result = zk.get_host_op('missing-host')
        assert result is None

    # === write_host_op tests ===

    def test_write_host_op_calls_noexcept_write(self, zk):
        """Test write_host_op calls noexcept_write with correct arguments."""
        zk.noexcept_write = MagicMock(return_value=True)
        result = zk.write_host_op('promoting', 'test-host')
        assert result is True
        zk.noexcept_write.assert_called_once_with('all_hosts/test-host/op', 'promoting', need_lock=False)

    def test_write_host_op_need_lock_false(self, zk):
        """Test write_host_op always uses need_lock=False."""
        zk.noexcept_write = MagicMock(return_value=True)
        zk.write_host_op('rewind', 'test-host')
        call_kwargs = zk.noexcept_write.call_args[1]
        assert call_kwargs['need_lock'] is False

    def test_write_host_op_default_hostname(self, zk):
        """Test write_host_op uses current hostname when None."""
        zk.noexcept_write = MagicMock(return_value=True)
        with patch('src.zk.helpers.get_hostname', return_value='my-host'):
            zk.write_host_op('promoting')
            zk.noexcept_write.assert_called_once_with('all_hosts/my-host/op', 'promoting', need_lock=False)

    # === delete_host_op tests ===

    def test_delete_host_op_calls_delete(self, zk):
        """Test delete_host_op calls delete with correct path."""
        zk.delete = MagicMock()
        result = zk.delete_host_op('test-host')
        assert result is True
        zk.delete.assert_called_once_with('all_hosts/test-host/op')

    def test_delete_host_op_returns_false_on_exception(self, zk):
        """Test delete_host_op returns False when delete fails."""
        zk.delete = MagicMock(side_effect=Exception('ZK error'))
        result = zk.delete_host_op('test-host')
        assert result is False

    # === ensure_host_ha tests ===

    def test_ensure_host_ha_success(self, zk):
        """Test ensure_host_ha returns True when ensure_path succeeds."""
        zk.ensure_path = MagicMock(return_value='some_result')
        result = zk.ensure_host_ha('test-host')
        assert result is True
        zk.ensure_path.assert_called_once_with('all_hosts/test-host/ha')

    def test_ensure_host_ha_failure(self, zk):
        """Test ensure_host_ha returns False when ensure_path returns None."""
        zk.ensure_path = MagicMock(return_value=None)
        result = zk.ensure_host_ha('test-host')
        assert result is False

    def test_ensure_host_ha_default_hostname(self, zk):
        """Test ensure_host_ha uses current hostname when None."""
        zk.ensure_path = MagicMock(return_value='result')
        with patch('src.zk.helpers.get_hostname', return_value='my-host'):
            zk.ensure_host_ha()
            zk.ensure_path.assert_called_once_with('all_hosts/my-host/ha')

    # === delete_host_ha tests ===

    def test_delete_host_ha_success(self, zk):
        """Test delete_host_ha returns True when delete succeeds."""
        zk.delete = MagicMock()
        result = zk.delete_host_ha('test-host')
        assert result is True
        zk.delete.assert_called_once_with('all_hosts/test-host/ha')

    def test_delete_host_ha_failure_returns_false(self, zk):
        """Test delete_host_ha returns False when delete fails."""
        zk.delete = MagicMock(side_effect=Exception('ZK error'))
        result = zk.delete_host_ha('test-host')
        assert result is False

    # === write_host_replics_info tests ===

    def test_write_host_replics_info_serializes_json(self, zk):
        """Test write_host_replics_info serializes data as JSON."""
        zk.noexcept_write = MagicMock(return_value=True)
        replics_info = [{'host': 'replica1', 'lag': 100}]
        result = zk.write_host_replics_info(replics_info, 'test-host')
        assert result is True
        zk.noexcept_write.assert_called_once_with(
            'all_hosts/test-host/replics_info',
            replics_info,
            preproc=json.dumps,
            need_lock=False
        )

    def test_write_host_replics_info_need_lock_false(self, zk):
        """Test write_host_replics_info always uses need_lock=False."""
        zk.noexcept_write = MagicMock(return_value=True)
        zk.write_host_replics_info([], 'test-host')
        call_kwargs = zk.noexcept_write.call_args[1]
        assert call_kwargs['need_lock'] is False

    # === get_host_replics_info tests ===

    def test_get_host_replics_info_parses_json(self, zk):
        """Test get_host_replics_info parses JSON response."""
        expected_data = [{'host': 'replica1', 'lag': 100}]
        zk.get = MagicMock(return_value=expected_data)
        result = zk.get_host_replics_info('test-host')
        assert result == expected_data
        zk.get.assert_called_once_with('all_hosts/test-host/replics_info', preproc=json.loads)

    def test_get_host_replics_info_returns_none_on_error(self, zk):
        """Test get_host_replics_info returns None when get fails."""
        zk.get = MagicMock(return_value=None)
        result = zk.get_host_replics_info('test-host')
        assert result is None

    # === write_host_wal_receiver tests ===

    def test_write_host_wal_receiver_serializes_json(self, zk):
        """Test write_host_wal_receiver serializes data as JSON."""
        zk.noexcept_write = MagicMock(return_value=True)
        wal_info = {'status': 'streaming', 'pid': 12345}
        result = zk.write_host_wal_receiver(wal_info, 'test-host')
        assert result is True
        zk.noexcept_write.assert_called_once_with(
            'all_hosts/test-host/wal_receiver',
            wal_info,
            preproc=json.dumps,
            need_lock=False
        )

    def test_write_host_wal_receiver_need_lock_false(self, zk):
        """Test write_host_wal_receiver always uses need_lock=False."""
        zk.noexcept_write = MagicMock(return_value=True)
        zk.write_host_wal_receiver({}, 'test-host')
        call_kwargs = zk.noexcept_write.call_args[1]
        assert call_kwargs['need_lock'] is False

    # === get_host_wal_receiver tests ===

    def test_get_host_wal_receiver_parses_json(self, zk):
        """Test get_host_wal_receiver parses JSON response."""
        expected_data = {'status': 'streaming', 'pid': 12345}
        zk.get = MagicMock(return_value=expected_data)
        result = zk.get_host_wal_receiver('test-host')
        assert result == expected_data
        zk.get.assert_called_once_with('all_hosts/test-host/wal_receiver', preproc=json.loads)

    def test_get_host_wal_receiver_returns_none_on_error(self, zk):
        """Test get_host_wal_receiver returns None when get fails."""
        zk.get = MagicMock(return_value=None)
        result = zk.get_host_wal_receiver('test-host')
        assert result is None
