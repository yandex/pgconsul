# encoding: utf-8
"""Unit tests for the Zookeeper factory."""

from configparser import RawConfigParser
from unittest.mock import MagicMock, patch

import pytest

from src.zk import create_zk


def _config() -> RawConfigParser:
    config = RawConfigParser()
    config['global'] = {
        'iteration_timeout': '1',
        'release_lock_after_acquire_failed': 'yes',
        'zk_lockpath_prefix': '/pgconsul',
    }
    return config


def test_daemon_connection_retries_until_zookeeper_recovers():
    """Daemon startup must survive a temporary ZooKeeper outage."""
    client = MagicMock()
    client.init.return_value = False
    client.reconnect.side_effect = [False, True]

    with patch('src.zk.create_zk_client', return_value=client), \
         patch('src.zk.Zookeeper') as zookeeper:
        result = create_zk(_config(), retry_connection=True)

    assert result is zookeeper.return_value
    client.init.assert_called_once_with()
    assert client.reconnect.call_count == 2


def test_cli_connection_failure_remains_fail_fast():
    """CLI callers must not retry indefinitely."""
    client = MagicMock()
    client.init.return_value = False

    with patch('src.zk.create_zk_client', return_value=client):
        with pytest.raises(Exception, match='Could not connect to ZK'):
            create_zk(_config())

    client.reconnect.assert_not_called()
