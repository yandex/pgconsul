# encoding: utf-8
"""Unit tests for the Zookeeper factory."""

from configparser import RawConfigParser
from unittest.mock import MagicMock, patch

import pytest

from src.zk import create_zk
from src.zk_client import ZkClient, ZkClientConfig
from src.zk_client import KazooState


def _config() -> RawConfigParser:
    config = RawConfigParser()
    config['global'] = {
        'iteration_timeout': '1',
        'release_lock_after_acquire_failed': 'yes',
        'zk_lockpath_prefix': '/pgconsul',
    }
    return config


def test_daemon_factory_returns_disconnected_client():
    """Daemon startup can enter its recovery loop after an initial ZK outage."""
    client = MagicMock()
    client.init.return_value = False

    with patch('src.zk.create_zk_client', return_value=client), \
         patch('src.zk.Zookeeper') as zookeeper:
        result = create_zk(_config(), allow_disconnected=True)

    assert result is zookeeper.return_value
    client.init.assert_called_once_with()
    client.reconnect.assert_not_called()


def test_cli_connection_failure_remains_fail_fast():
    """CLI callers must not retry indefinitely."""
    client = MagicMock()
    client.init.return_value = False

    with patch('src.zk.create_zk_client', return_value=client):
        with pytest.raises(Exception, match='Could not connect to ZK'):
            create_zk(_config())

    client.reconnect.assert_not_called()


@pytest.mark.parametrize('failed_attempts, expected_delays', [
    (1, []),
    (3, [6, 12]),
    (8, [6, 12, 24, 30, 30, 30, 30]),
])
def test_daemon_startup_recovers_with_transport_backoff(failed_attempts, expected_delays):
    """Keep retry backoff and close failed sessions across a startup outage."""
    client = ZkClient(ZkClientConfig(
        hosts='localhost:2181', timeout=1, connect_max_delay=10,
        max_delay_on_reinit=30, path_prefix='/pgconsul',
    ))
    sessions = [MagicMock() for _ in range(failed_attempts + 1)]
    for session in sessions[:-1]:
        session.connected = False
    sessions[-1].connected = True
    sessions[-1].state = KazooState.CONNECTED

    with patch('src.zk.create_zk_client', return_value=client) as factory, \
         patch('src.zk_client.KazooClient', side_effect=sessions), \
         patch('src.zk_client.uniform', side_effect=lambda low, high: high), \
         patch('src.zk_client.time.sleep') as sleep:
        result = create_zk(_config(), allow_disconnected=True)
        for _ in range(failed_attempts):
            result.re_init()

    assert result.is_alive()
    factory.assert_called_once()
    assert [call.args[0] for call in sleep.call_args_list] == expected_delays
    for session in sessions[:-1]:
        session.stop.assert_called_once_with()
        session.close.assert_called_once_with()
    sessions[-1].stop.assert_not_called()
