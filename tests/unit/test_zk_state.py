# encoding: utf-8
"""Tests for Zookeeper.get_state and _get_ssn_info robustness."""

import pytest
from unittest.mock import MagicMock, patch


class TestGetSsnInfo:
    """_get_ssn_info must not crash when MEMBERS_PATH is absent."""

    def test_returns_empty_dict_when_all_hosts_none(self, zk):
        """get_children returns None (node absent) → empty dict, no TypeError."""
        zk.get_children = MagicMock(return_value=None)
        result = zk._get_ssn_info()
        assert result == {}

    def test_returns_empty_dict_when_all_hosts_empty(self, zk):
        zk.get_children = MagicMock(return_value=[])
        result = zk._get_ssn_info()
        assert result == {}

    def test_returns_ssn_info_for_hosts(self, zk):
        zk.get_children = MagicMock(return_value=['host1', 'host2'])
        zk.get = MagicMock(return_value='some_value')
        result = zk._get_ssn_info()
        assert 'host1' in result
        assert 'host2' in result


class TestGetState:
    """get_state must not crash when MEMBERS_PATH is absent."""

    def _make_alive_zk(self, zk):
        """Wire zk so that is_alive returns True and basic gets return None."""
        zk.is_alive = MagicMock(return_value=True)
        zk.get = MagicMock(return_value=None)
        zk.exists_path = MagicMock(return_value=False)
        zk.get_current_lock_version = MagicMock(return_value=None)
        zk.get_current_lock_holder = MagicMock(return_value=None)
        zk._get_ssn_info = MagicMock(return_value={})
        return zk

    def test_get_state_does_not_crash_when_members_path_absent(self, zk):
        """Ensure get_state survives when _get_ssn_info returns {} (MEMBERS_PATH absent)."""
        self._make_alive_zk(zk)
        # Should not raise TypeError
        data = zk.get_state()
        assert 'alive' in data
        assert data['synchronous_standby_names'] == {}

    def test_get_state_raises_when_not_alive(self, zk):
        from src.zk import ZookeeperException
        zk.is_alive = MagicMock(return_value=False)
        with pytest.raises(ZookeeperException):
            zk.get_state()
