# encoding: utf-8
"""Tests for Zookeeper.reconnect lock lifecycle."""

from unittest.mock import MagicMock


class TestReconnectLocks:
    """After reconnect all locks are dropped and acquired lazily."""

    def test_reconnect_drops_all_locks(self, zk):
        from src.zk import Zookeeper

        zk._locks = {
            Zookeeper.PRIMARY_LOCK_PATH: MagicMock(),
            Zookeeper.PRIMARY_SWITCH_LOCK_PATH: MagicMock(),
            zk.get_host_alive_lock_path('host1'): MagicMock(),
        }
        zk._zk_client.reconnect = MagicMock(return_value=True)

        assert zk.reconnect() is True

        assert zk._locks == {}

    def test_reconnect_releases_old_locks(self, zk):
        from src.zk import Zookeeper

        primary = MagicMock()
        side = MagicMock()
        zk._locks = {
            Zookeeper.PRIMARY_LOCK_PATH: primary,
            Zookeeper.PRIMARY_SWITCH_LOCK_PATH: side,
        }
        zk._zk_client.reconnect = MagicMock(return_value=True)

        zk.reconnect()

        primary.release.assert_called_once()
        side.release.assert_called_once()

    def test_reconnect_drops_all_locks_on_failure(self, zk):
        from src.zk import Zookeeper

        zk._locks = {
            Zookeeper.PRIMARY_LOCK_PATH: MagicMock(),
            Zookeeper.PRIMARY_SWITCH_LOCK_PATH: MagicMock(),
        }
        zk._zk_client.reconnect = MagicMock(return_value=False)

        assert zk.reconnect() is False
        assert zk._locks == {}
