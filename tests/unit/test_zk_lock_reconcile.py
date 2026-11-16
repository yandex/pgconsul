# encoding: utf-8
"""Leader lock reconciliation tests."""

from unittest.mock import MagicMock

from src.zk import Zookeeper, ZookeeperConfig


def make_zk(old_lock, new_lock=None):
    client = MagicMock()
    client.is_connected.return_value = True
    client.reconnect.return_value = True
    client.make_lock.side_effect = [old_lock] if new_lock is None else [old_lock, new_lock]
    zk = Zookeeper(client, ZookeeperConfig(False, 1.0, '/test/', 'host1'))
    return zk, client


def test_matching_local_lock_is_reused():
    lock = MagicMock()
    lock.acquired_node = 'local__lock__0000000001'
    zk, client = make_zk(lock)
    client.exists.return_value = True

    assert zk.try_acquire_lock() is True

    lock.acquire.assert_not_called()
    client.reconnect.assert_not_called()


def test_foreign_local_lock_reconnects_before_acquire():
    old_lock = MagicMock()
    old_lock.acquired_node = 'local__lock__0000000002'
    old_lock.contenders.return_value = ['host1']
    new_lock = MagicMock()
    new_lock.acquired_node = None
    new_lock.acquire.return_value = True
    zk, client = make_zk(old_lock, new_lock)
    client.exists.return_value = False

    assert zk.try_acquire_lock() is True

    client.reconnect.assert_called_once()
    new_lock.acquire.assert_called_once_with(blocking=True, timeout=1.0)


def test_non_primary_lock_uses_the_same_reconciliation():
    primary_lock = MagicMock()
    old_lock = MagicMock()
    old_lock.acquired_node = None
    old_lock.contenders.return_value = ['host1']
    reconnected_primary_lock = MagicMock()
    new_lock = MagicMock()
    new_lock.acquired_node = None
    new_lock.acquire.return_value = True
    client = MagicMock()
    client.is_connected.return_value = True
    client.reconnect.return_value = True
    client.exists.return_value = False
    client.make_lock.side_effect = [primary_lock, old_lock, reconnected_primary_lock, new_lock]
    zk = Zookeeper(client, ZookeeperConfig(False, 1.0, '/test/', 'host1'))

    assert zk.try_acquire_lock('failover_manager') is True

    client.reconnect.assert_called_once()
    new_lock.acquire.assert_called_once_with(blocking=True, timeout=1.0)


def test_mismatched_lock_reconnects_before_release():
    old_lock = MagicMock()
    old_lock.acquired_node = None
    old_lock.contenders.return_value = ['host1']
    new_lock = MagicMock()
    new_lock.contenders.return_value = ['host2']
    zk, client = make_zk(old_lock, new_lock)
    client.exists.return_value = False

    assert zk.release_lock(wait=1) is True

    client.reconnect.assert_called_once()


def test_failed_release_keeps_local_lock_for_retry():
    lock = MagicMock()
    lock.acquired_node = 'local__lock__0000000001'
    lock.contenders.return_value = ['host1']
    lock.release.return_value = False
    zk, client = make_zk(lock)
    client.exists.return_value = True

    assert zk.release_lock() is False
    assert zk.release_lock() is False
    assert lock.release.call_count == 2
