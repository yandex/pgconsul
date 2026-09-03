from unittest.mock import MagicMock, patch

from src.replication_manager import ReplicationManager, ReplicationManagerConfig
from src.types import DurabilityConfig


PRIMARY = 'primary'
PG2 = 'postgresql2.example'
PG3 = 'postgresql3.example'
PG4 = 'postgresql4.example'


def _manager():
    config = ReplicationManagerConfig(
        priority=100,
        primary_unavailability_timeout=0.0,
        quorum_removal_delay=0.0,
    )
    zk = MagicMock()
    with patch('src.replication_manager.SsnManager'):
        manager = ReplicationManager(config, MagicMock(), zk)
    return manager, zk


def _info(host: str, *, write_diff: int = 0) -> dict:
    return {
        'application_name': host.replace('.', '_'),
        'state': 'streaming',
        'write_location_diff': write_diff,
    }


def test_anywhere_switchover_prefers_priority_over_lsn():
    manager, zk = _manager()
    zk.get_durability_config.return_value = DurabilityConfig.build([PRIMARY, PG2, PG3])
    zk.is_host_alive.return_value = True
    zk.get_host_prio.side_effect = lambda host, **_: {PG2: '1', PG3: '10'}.get(host)

    assert manager.get_switchover_candidate([
        _info(PG2, write_diff=0),
        _info(PG3, write_diff=100),
    ]) == PG3


def test_anywhere_switchover_ignores_live_host_outside_durability():
    manager, zk = _manager()
    zk.get_durability_config.return_value = DurabilityConfig.build([PRIMARY, PG2])
    zk.is_host_alive.return_value = True
    zk.get_host_prio.side_effect = lambda host, **_: {PG2: '1', PG4: '100'}.get(host)

    assert manager.get_switchover_candidate([_info(PG2), _info(PG4)]) == PG2


def test_anywhere_switchover_ignores_dead_durability_host():
    manager, zk = _manager()
    zk.get_durability_config.return_value = DurabilityConfig.build([PRIMARY, PG2, PG3])
    zk.is_host_alive.side_effect = lambda host, **_: host == PG2
    zk.get_host_prio.side_effect = lambda host, **_: {PG2: '1', PG3: '100'}.get(host)

    assert manager.get_switchover_candidate([_info(PG2), _info(PG3)]) == PG2


def test_anywhere_switchover_requires_streaming_replica():
    manager, zk = _manager()
    zk.get_durability_config.return_value = DurabilityConfig.build([PRIMARY, PG2])
    zk.is_host_alive.return_value = True
    zk.get_host_prio.return_value = '1'
    info = _info(PG2)
    info['state'] = 'catchup'

    assert manager.get_switchover_candidate([info]) is None
