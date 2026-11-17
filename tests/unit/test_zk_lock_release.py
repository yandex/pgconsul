# encoding: utf-8
"""ZooKeeper lock release tests."""

from unittest.mock import MagicMock, call

from src.zk import Zookeeper, ZookeeperConfig
from src.zk_client import ZkClientError, ZkNoNodeError


LOCK_NODE = 'a' * 32 + '__lock__0000000001'
READ_NODE = 'b' * 32 + '__rlock__0000000002'
OTHER_NODE = 'c' * 32 + '__lock__0000000003'


def make_zk():
    lock = MagicMock()
    lock.release.return_value = False
    client = MagicMock()
    client.make_lock.return_value = lock
    zk = Zookeeper(client, ZookeeperConfig(False, 1.0, '/test/', 'host1'))
    return zk, client, lock


def test_failed_release_removes_all_own_lock_nodes():
    zk, client, lock = make_zk()
    client.get_children.return_value = [LOCK_NODE, READ_NODE, OTHER_NODE, 'metadata']
    client.get.side_effect = lambda path: 'host2' if path.endswith(OTHER_NODE) else 'host1'

    assert zk.release_lock() is True
    lock.release.assert_called_once_with()
    assert client.delete.call_args_list == [
        call('/test/leader/' + LOCK_NODE),
        call('/test/leader/' + READ_NODE),
    ]
    assert client.get.call_count == 3


def test_failed_release_reports_failed_deletion_but_tries_remaining_nodes():
    zk, client, lock = make_zk()
    client.get_children.return_value = [LOCK_NODE, READ_NODE]
    client.get.return_value = 'host1'
    client.delete.side_effect = [ZkClientError('disconnected'), True]

    assert zk.release_lock() is False
    assert client.delete.call_args_list == [
        call('/test/leader/' + LOCK_NODE),
        call('/test/leader/' + READ_NODE),
    ]

    client.delete.side_effect = None
    assert zk.release_lock() is True
    assert lock.release.call_count == 2


def test_failed_release_reports_failed_contender_read():
    zk, client, _ = make_zk()
    client.get_children.return_value = [LOCK_NODE]
    client.get.side_effect = ZkClientError('disconnected')

    assert zk.release_lock() is False
    client.delete.assert_not_called()


def test_failed_release_ignores_disappeared_contender():
    zk, client, _ = make_zk()
    client.get_children.return_value = [LOCK_NODE]
    client.get.side_effect = ZkNoNodeError('gone')

    assert zk.release_lock() is True
    client.delete.assert_not_called()


def test_release_without_local_object_removes_own_node():
    zk, client, lock = make_zk()
    client.get_children.return_value = [LOCK_NODE]
    client.get.return_value = 'host1'
    assert zk.release_lock('other') is True
    lock.release.assert_not_called()
    client.delete.assert_called_once_with('/test/other/' + LOCK_NODE)


def test_successful_release_does_not_scan_contenders():
    zk, client, lock = make_zk()
    lock.release.return_value = True

    assert zk.release_lock() is True
    client.get_children.assert_not_called()
    client.delete.assert_not_called()


def test_released_lock_object_is_reused():
    zk, client, lock = make_zk()
    lock.contenders.return_value = []
    lock.acquire.return_value = True
    lock.release.return_value = True

    assert zk.try_acquire_lock('other') is True
    assert zk.release_lock('other') is True
    assert zk.try_acquire_lock('other') is True
    assert client.make_lock.call_count == 2


def test_repeated_release_checks_for_remaining_nodes():
    zk, client, lock = make_zk()
    lock.release.side_effect = [True, False]
    client.get_children.return_value = []

    assert zk.release_lock() is True
    assert zk.release_lock() is True
    assert lock.release.call_count == 2
    client.get_children.assert_called_once_with('/test/leader')


def test_close_cleans_up_unconfirmed_lock_node():
    zk, client, _ = make_zk()
    client.get_children.return_value = [LOCK_NODE]
    client.get.return_value = 'host1'

    zk.close()

    client.delete.assert_called_once_with('/test/leader/' + LOCK_NODE)
    client.close.assert_called_once_with()
