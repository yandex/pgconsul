import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.zk import ZookeeperException

with patch('src.read_config', create=True), patch('src.init_logging', create=True):
    from src import cli
    from src.utils import Switchover


def wire_zk(zk):
    zk.is_alive = MagicMock(return_value=True)
    zk.exists_path = MagicMock(return_value=False)
    zk.get_current_lock_holder = MagicMock(return_value='primary.example')
    zk._zk_client.lock_version = MagicMock(return_value=7)
    zk._get_ssn_info = MagicMock(return_value={'primary.example': ('ANY 1 (replica)', '123')})
    values = {'switchover/master': '{}', 'replics_info': '[]', 'timeline': '3'}
    zk.get = MagicMock(side_effect=lambda key, preproc=None: preproc(values[key]) if key in values and preproc else values.get(key))
    return zk


def expected_zk():
    return {
        'alive': True, 'replics_info': [], 'last_failover_time': None,
        'last_switchover_time': None, 'failover_state': None, 'failover_must_be_reset': False,
        'current_promoting_host': None, 'lock_version': 7, 'lock_holder': 'primary.example',
        'single_node': False, 'timeline': 3, 'switchover': {}, 'switchover/candidate': None,
        'switchover/side_replicas': None, 'switchover/state': None,
        'maintenance': {'status': None, 'ts': None}, 'last_leader': None,
        'synchronous_standby_names': {'primary.example': ('ANY 1 (replica)', '123')},
    }


def test_zk_state_wire_format(zk):
    assert wire_zk(zk).get_state().to_dict() == expected_zk()


def test_zk_disconnect_after_reads_raises(zk):
    wire_zk(zk).is_alive.side_effect = [True, False]
    with pytest.raises(ZookeeperException, match='unavailable'):
        zk.get_state()


@pytest.mark.parametrize('short', [False, True])
def test_cli_info_wire_format(zk, short):
    wire_zk(zk)
    context = MagicMock()
    context.__enter__.return_value = zk
    with patch.object(cli, 'create_zk', return_value=context), patch.object(cli, '_get_db_state', return_value={'alive': False, 'role': 'replica'}):
        actual = cli._show_info(SimpleNamespace(short=short), MagicMock())
    if short:
        expected = {'alive': True, 'primary': 'primary.example', 'last_failover_time': None, 'maintenance': None, 'replics_info': {}}
    else:
        expected = {**{'alive': False, 'role': 'replica'}, **expected_zk()}
        expected['primary'] = expected.pop('lock_holder')
        expected['maintenance'] = None
    assert actual == expected


def test_switchover_empty_state_wire_format(zk):
    switch = Switchover.__new__(Switchover)
    switch._zk = zk
    zk.noexcept_get = MagicMock(return_value=None)
    assert switch.state().to_dict() == {'progress': None, 'info': {}, 'failover': None, 'replicas': {}}


@pytest.mark.parametrize('use_json', [False, True])
def test_cli_prints_compatible_json_and_yaml(zk, use_json, monkeypatch, capsys):
    import sys

    # Unit bootstrap stubs yaml; use its real serializer for this wire-format check.
    monkeypatch.delitem(sys.modules, 'yaml')
    yaml = pytest.importorskip('yaml')
    monkeypatch.setattr(cli, 'yaml', yaml)
    wire_zk(zk)
    context = MagicMock()
    context.__enter__.return_value = zk
    with patch.object(cli, 'create_zk', return_value=context), patch.object(cli, '_get_db_state', return_value={'alive': False, 'role': 'replica'}):
        cli.show_info(SimpleNamespace(short=False, json=use_json), MagicMock())
    output = capsys.readouterr().out
    expected = {**{'alive': False, 'role': 'replica'}, **expected_zk()}
    expected['primary'] = expected.pop('lock_holder')
    expected['maintenance'] = None
    if use_json:
        assert json.loads(output) == json.loads(json.dumps(expected))
    else:
        assert yaml.full_load(output) == expected


def test_switchover_scheduling_preserves_coordinates_and_null(zk):
    import logging

    switch = Switchover.__new__(Switchover)
    switch._zk = zk
    switch._log = logging.getLogger('test')
    switch._lock = MagicMock()
    switch.state = MagicMock(return_value=None)
    writes = []
    zk.write = MagicMock(side_effect=lambda key, value, preproc=str, **kwargs: writes.append((key, preproc(value), kwargs)) or True)
    assert switch._initiate_switchover('primary.example', '3', None)
    assert writes == [
        ('switchover/master', '{"hostname": "primary.example", "timeline": "3", "destination": null}', {'need_lock': False}),
        ('switchover/state', 'scheduled', {'need_lock': False}),
    ]


@pytest.mark.parametrize('wire', [None, {}, {'pid': 123, 'status': 'streaming', 'conninfo': None}])
def test_wal_receiver_roundtrip_preserves_absent_empty_and_null(zk, wire):
    from src.types import WalReceiverInfo

    zk.get = MagicMock(return_value=wire)
    receiver = zk.get_host_wal_receiver('replica.example')
    assert receiver is None if wire is None else isinstance(receiver, WalReceiverInfo)
    zk.noexcept_write = MagicMock(return_value=True)
    assert zk.write_host_wal_receiver(receiver, 'replica.example')
    assert zk.noexcept_write.call_args.args[1] == wire


def test_nullable_replica_fields_and_priority_survive_zk_roundtrip(zk):
    wire = [{'application_name': 'replica', 'client_hostname': None, 'replay_lag_msec': None, 'priority': None}]
    zk.get = MagicMock(return_value=wire)
    replicas = zk.get_replics_info()
    zk.write = MagicMock(return_value=True)
    assert zk.write_replics_info(replicas)
    assert zk.write.call_args.args[1] == wire
