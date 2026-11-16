# encoding: utf-8
"""ZooKeeper lock release tests."""

from unittest.mock import MagicMock

from src.zk import Zookeeper, ZookeeperConfig


def test_failed_release_keeps_local_lock_for_retry():
    lock = MagicMock()
    lock.release.return_value = False
    client = MagicMock()
    client.make_lock.return_value = lock
    zk = Zookeeper(client, ZookeeperConfig(False, 1.0, '/test/', 'host1'))

    assert zk.release_lock() is False
    assert zk.release_lock() is False
    assert lock.release.call_count == 2
