from unittest.mock import MagicMock

from src.main import Pgconsul
from src.switchover import SwitchoverExecutor
from src.types import DurabilityConfig


PRIMARY = 'primary'
PG2 = 'postgresql2.example'
PG3 = 'postgresql3.example'
PG4 = 'postgresql4.example'


def _pgconsul():
    instance = Pgconsul.__new__(Pgconsul)
    instance.zk = MagicMock()
    return instance


def _info(host: str, *, write_diff: int = 0) -> dict:
    return {'application_name': host.replace('.', '_'), 'state': 'streaming', 'write_location_diff': write_diff}


def test_anywhere_switchover_prefers_priority_over_lsn():
    instance = _pgconsul()
    instance.zk.get_durability_config.return_value = DurabilityConfig.build([PRIMARY, PG2, PG3])
    instance.zk.is_host_alive.return_value = True
    instance.zk.get_host_prio.side_effect = lambda host, **_: {PG2: '1', PG3: '10'}.get(host)

    assert SwitchoverExecutor.select_candidate(instance, [_info(PG2), _info(PG3, write_diff=100)]) == PG3


def test_anywhere_switchover_ignores_live_host_outside_durability():
    instance = _pgconsul()
    instance.zk.get_durability_config.return_value = DurabilityConfig.build([PRIMARY, PG2])
    instance.zk.is_host_alive.return_value = True
    instance.zk.get_host_prio.side_effect = lambda host, **_: {PG2: '1', PG4: '100'}.get(host)

    assert SwitchoverExecutor.select_candidate(instance, [_info(PG2), _info(PG4)]) == PG2


def test_anywhere_switchover_ignores_dead_durability_host():
    instance = _pgconsul()
    instance.zk.get_durability_config.return_value = DurabilityConfig.build([PRIMARY, PG2, PG3])
    instance.zk.is_host_alive.side_effect = lambda host, **_: host == PG2
    instance.zk.get_host_prio.side_effect = lambda host, **_: {PG2: '1', PG3: '100'}.get(host)

    assert SwitchoverExecutor.select_candidate(instance, [_info(PG2), _info(PG3)]) == PG2


def test_anywhere_switchover_requires_streaming_replica():
    instance = _pgconsul()
    instance.zk.get_durability_config.return_value = DurabilityConfig.build([PRIMARY, PG2])
    instance.zk.is_host_alive.return_value = True
    instance.zk.get_host_prio.return_value = '1'
    info = _info(PG2)
    info['state'] = 'catchup'

    assert SwitchoverExecutor.select_candidate(instance, [info]) is None
