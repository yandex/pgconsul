# encoding: utf-8
"""Tests for Zookeeper.reconnect lock-restoration contract."""

from unittest.mock import MagicMock


class TestReconnectLocks:
    """After reconnect only PRIMARY_LOCK_PATH is restored; other locks are dropped."""

    def test_reconnect_restores_only_primary_lock(self, zk):
        from src.zk import Zookeeper

        # Seed several held locks besides the primary one.
        zk._locks = {
            Zookeeper.PRIMARY_LOCK_PATH: MagicMock(),
            Zookeeper.SWITCHOVER_LOCK_PATH: MagicMock(),
            zk.get_host_alive_lock_path('host1'): MagicMock(),
        }
        zk._zk_client.reconnect = MagicMock(return_value=True)

        assert zk.reconnect() is True

        # Only the primary lock is re-initialized; non-primary locks are gone.
        assert list(zk._locks.keys()) == [Zookeeper.PRIMARY_LOCK_PATH]

    def test_reconnect_releases_old_locks(self, zk):
        from src.zk import Zookeeper

        primary = MagicMock()
        side = MagicMock()
        zk._locks = {
            Zookeeper.PRIMARY_LOCK_PATH: primary,
            Zookeeper.SWITCHOVER_LOCK_PATH: side,
        }
        zk._zk_client.reconnect = MagicMock(return_value=True)

        zk.reconnect()

        primary.release.assert_called_once()
        side.release.assert_called_once()

    def test_reconnect_drops_all_locks_on_failure(self, zk):
        from src.zk import Zookeeper

        zk._locks = {
            Zookeeper.PRIMARY_LOCK_PATH: MagicMock(),
            Zookeeper.SWITCHOVER_LOCK_PATH: MagicMock(),
        }
        zk._zk_client.reconnect = MagicMock(return_value=False)

        assert zk.reconnect() is False
        assert zk._locks == {}
