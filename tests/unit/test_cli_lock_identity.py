# encoding: utf-8
"""CLI lock contenders must not share the daemon's hostname-only identity."""

import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

with patch('src.read_config', create=True), patch('src.init_logging', create=True):
    from src import cli, helpers, utils
from src.zk import Zookeeper, ZookeeperConfig


def test_maintenance_show_does_not_release_daemon_leader_lock():
    node = 'a' * 32 + '__lock__0000000001'
    client = MagicMock()
    client.make_lock.return_value.release.return_value = False
    client.get_children.return_value = [node]
    client.get.return_value = 'host1'

    def create_zk(config, lock_contender_name=None):
        return Zookeeper(client, ZookeeperConfig(False, 1.0, '/test/', lock_contender_name))

    with patch.object(helpers, 'get_hostname', return_value='host1'), patch.object(cli, 'create_zk', side_effect=create_zk):
        cli.maintenance(SimpleNamespace(mode='show'), MagicMock())

    client.make_lock.assert_called_once_with('/test/leader', f'host1_{os.getpid()}')
    client.delete.assert_not_called()


def test_failover_cli_uses_pid_suffixed_contender_name():
    config = MagicMock()
    with patch.object(helpers, 'get_hostname', return_value='host1'), patch.object(utils, 'create_zk') as create_zk:
        cli.failover(SimpleNamespace(reset=False), config)

    create_zk.assert_called_once_with(config=config, lock_contender_name=f'host1_{os.getpid()}')


def test_switchover_cli_uses_pid_suffixed_contender_name():
    config = MagicMock()
    with patch.object(helpers, 'get_hostname', return_value='host1'), patch.object(utils, 'create_zk') as create_zk:
        utils.Switchover(conf=config, from_cli=True)

    create_zk.assert_called_once_with(config=config, lock_contender_name=f'host1_{os.getpid()}')
