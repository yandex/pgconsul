# encoding: utf-8
"""
Tests for _failed_inits_count reset on kazoo auto-reconnect (SUSPENDED → CONNECTED).

Problem: if kazoo self-heals the connection (SUSPENDED → CONNECTED),
Zookeeper.reconnect() is never called, so _failed_inits_count was not reset.
This caused the exponential backoff to start from an accumulated value on the
next disconnection event.

Coverage split:
- ZkClient._listener(CONNECTED) resets _failed_inits_count + _session_expired
  → tested here via zk_client fixture (ZkClient-level) and as a regression
    in test_zk_client.py::TestListener::test_listener_connected_clears_flags.
- Zookeeper._listener(LOST) clears all locks
  → tested here via zk fixture (Zookeeper-level).
"""

from unittest.mock import MagicMock

from kazoo.client import KazooState

from src.zk_client import ZkConnectionState


class TestZkClientListenerResetsBackoffCounter:
    """ZkClient._listener(CONNECTED) must reset _failed_inits_count."""

    def test_connected_resets_failed_inits_count(self, zk_client):
        zk_client._failed_inits_count = 5
        zk_client._listener(KazooState.CONNECTED)
        assert zk_client._failed_inits_count == 0

    def test_suspended_does_not_reset_failed_inits_count(self, zk_client):
        zk_client._failed_inits_count = 5
        zk_client._listener(KazooState.SUSPENDED)
        assert zk_client._failed_inits_count == 5

    def test_lost_does_not_reset_failed_inits_count(self, zk_client):
        zk_client._failed_inits_count = 5
        zk_client._listener(KazooState.LOST)
        assert zk_client._failed_inits_count == 5

    def test_connected_noop_when_count_is_zero(self, zk_client):
        zk_client._failed_inits_count = 0
        zk_client._listener(KazooState.CONNECTED)
        assert zk_client._failed_inits_count == 0

    def test_after_failures_listener_connected_resets_count(self, zk_client):
        """Full scenario: failures accumulate, then kazoo auto-reconnects via _listener."""
        # Simulate state after three failed reconnects (reconnect() itself
        # is tested in test_zk_client.py::TestZkClientReconnect).
        zk_client._failed_inits_count = 3

        # Kazoo self-heals; ZkClient._listener is called with CONNECTED
        zk_client._listener(KazooState.CONNECTED)
        assert zk_client._failed_inits_count == 0

        # Next backoff would start from base_delay (2^0), not 2^3
        max_sleep = min(zk_client._base_delay * 2 ** zk_client._failed_inits_count,
                        zk_client.config.max_delay_on_reinit)
        assert max_sleep == zk_client._base_delay

    def test_connected_also_clears_session_expired(self, zk_client):
        """_listener(CONNECTED) must clear _session_expired too."""
        zk_client._session_expired = True
        zk_client._failed_inits_count = 2
        zk_client._listener(KazooState.CONNECTED)
        assert zk_client._session_expired is False
        assert zk_client._failed_inits_count == 0


class TestZookeeperListenerLockCleanup:
    """Zookeeper._listener(LOST) must clear the lock registry."""

    def test_lost_clears_all_locks(self, zk):
        zk._locks = {'leader': MagicMock(), 'switchover/lock': MagicMock()}
        zk._listener(ZkConnectionState.LOST)
        assert zk._locks == {}

    def test_suspended_does_not_clear_locks(self, zk):
        sentinel = {'leader': MagicMock()}
        zk._locks = dict(sentinel)
        zk._listener(ZkConnectionState.SUSPENDED)
        assert list(zk._locks.keys()) == list(sentinel.keys())

    def test_connected_does_not_clear_locks(self, zk):
        sentinel = {'leader': MagicMock()}
        zk._locks = dict(sentinel)
        zk._listener(ZkConnectionState.CONNECTED)
        assert list(zk._locks.keys()) == list(sentinel.keys())
