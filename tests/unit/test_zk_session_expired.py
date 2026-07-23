# encoding: utf-8
"""
Zookeeper-level tests for SessionExpiredError handling.

Tests that Zookeeper.get/write/noexcept_get/noexcept_write wrap
ZkSessionExpiredError into ZookeeperException, and that is_alive()
delegates correctly when the ZkClient reports a dead session.

ZkClient-level session-expired behaviour is covered separately in
test_zk_client.py (TestZkClientState, TestListener, TestZkClientGet/Write).
"""

from unittest.mock import MagicMock

import pytest

from src.zk import ZookeeperException
from src.zk_client import ZkSessionExpiredError


class TestZookeeperGetSessionExpired:
    """Zookeeper.get() / noexcept_get() on ZkSessionExpiredError."""

    def test_get_raises_zookeeper_exception(self, zk):
        zk._zk_client.get = MagicMock(side_effect=ZkSessionExpiredError())
        with pytest.raises(ZookeeperException):
            zk.get('test_key')

    def test_noexcept_get_returns_none(self, zk):
        zk._zk_client.get = MagicMock(side_effect=ZkSessionExpiredError())
        assert zk.noexcept_get('test_key') is None


class TestZookeeperWriteSessionExpired:
    """Zookeeper.write() / noexcept_write() on ZkSessionExpiredError."""

    def test_write_raises_zookeeper_exception(self, zk):
        zk._zk_client.write = MagicMock(side_effect=ZkSessionExpiredError())
        with pytest.raises(ZookeeperException):
            zk.write('test_key', 'val', need_lock=False)

    def test_noexcept_write_returns_false(self, zk):
        zk._zk_client.write = MagicMock(side_effect=ZkSessionExpiredError())
        assert zk.noexcept_write('test_key', 'val', need_lock=False) is False


class TestZookeeperIsAliveSessionExpired:
    """Zookeeper.is_alive() delegates to ZkClient.is_alive()."""

    def test_is_alive_false_when_client_session_expired(self, zk):
        zk._zk_client.is_alive = MagicMock(return_value=False)
        assert zk.is_alive() is False

    def test_is_alive_true_when_client_connected(self, zk):
        zk._zk_client.is_alive = MagicMock(return_value=True)
        assert zk.is_alive() is True
