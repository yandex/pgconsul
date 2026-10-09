"""Receiver normalization at model, persistence and decision boundaries."""
import importlib.machinery
import importlib.util
import json
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src import helpers
from src.main import Pgconsul
from src.types import DbState, WalReceiverInfo, ZkState
from tests.unit.test_pg import _make_postgres
from tests.unit.state_samples import DB_DEFAULTS
from tests.unit.state_fixtures import state_cluster, state_consul  # noqa: F401


DEFAULTS = {'pid': 0, 'status': '', 'slot_name': '', 'last_msg_receipt_time_msec': 0, 'conninfo': ''}


@pytest.mark.parametrize('payload', [
    {}, dict.fromkeys(DEFAULTS), {'status': 'streaming'},
    {'pid': -1, 'last_msg_receipt_time_msec': -1000},
    {'pid': 123, 'status': 'streaming', 'slot_name': 'slot',
     'last_msg_receipt_time_msec': 123456, 'conninfo': 'host=primary'},
])
def test_receiver_normalizes_legacy_record_and_writes_all_fields(payload):
    receiver = WalReceiverInfo.from_dict(payload)
    expected = {**DEFAULTS, **{key: value for key, value in payload.items() if value is not None}}
    assert receiver
    assert receiver.to_dict() == expected
    assert WalReceiverInfo.from_dict(json.loads(json.dumps(receiver.to_dict()))) == receiver


@pytest.mark.parametrize('slot_name', [None, '', 'slot'])
@pytest.mark.parametrize('conninfo', [None, '', 'host=primary'])
def test_receiver_normalizes_optional_strings_independently(slot_name, conninfo):
    receiver = WalReceiverInfo.from_dict({'slot_name': slot_name, 'conninfo': conninfo})
    assert receiver.slot_name == ('' if slot_name is None else slot_name)
    assert receiver.conninfo == ('' if conninfo is None else conninfo)


def test_direct_receiver_and_later_updates_are_serialized():
    receiver = WalReceiverInfo()
    assert receiver
    assert receiver.to_dict() == DEFAULTS
    receiver.pid = 123
    receiver.status = 'streaming'
    assert receiver.to_dict() == {**DEFAULTS, 'pid': 123, 'status': 'streaming'}
    assert WalReceiverInfo(pid=123, status='streaming').to_dict() == receiver.to_dict()


@pytest.mark.parametrize('payload, expected', [
    ({}, {}), ({'wal_receiver': None}, {'wal_receiver': None}),
    ({'wal_receiver': {}}, {'wal_receiver': None}),
    ({'wal_receiver': {'status': None}}, {'wal_receiver': DEFAULTS}),
])
def test_db_state_normalizes_receiver_and_ignores_previous_state(payload, expected):
    state = DbState.from_dict(payload)
    assert state.to_dict() == {**DB_DEFAULTS, **expected}
    assert (state.wal_receiver is None) == (expected.get('wal_receiver') is None)
    assert DbState.from_dict({**payload, 'prev_state': payload}).to_dict() == {**DB_DEFAULTS, **expected}


@pytest.mark.parametrize('payload, expected', [
    ({}, None), (None, None), ({'status': None}, DEFAULTS),
    ({'status': 'streaming'}, {**DEFAULTS, 'status': 'streaming'}),
])
def test_zk_receiver_read_and_explicit_write(state_cluster, payload, expected):
    path = 'all_hosts/upstream/wal_receiver'
    state_cluster.records[path] = json.dumps(payload)
    receiver = state_cluster.zk.get_host_wal_receiver('upstream')
    assert (receiver is None) == (expected is None)
    assert state_cluster.zk.write_host_wal_receiver(receiver, 'upstream')
    assert json.loads(state_cluster.records[path]) == expected


@pytest.mark.parametrize('payload, expected', [
    ({}, None), (None, None), ({'slot_name': None}, DEFAULTS),
    ({'status': 'streaming'}, {**DEFAULTS, 'status': 'streaming'}),
])
def test_receiver_cache_status_and_full_cli(tmp_path, capsys, monkeypatch, payload, expected):
    with patch('src.read_config', create=True), patch('src.init_logging', create=True):
        from src import cli

    pg = _make_postgres()
    pg.config.working_dir = str(tmp_path)
    cache = tmp_path / '.pgconsul_db_state.cache'
    cache.write_text(json.dumps({'wal_receiver': payload}))
    state = pg.get_prev_state()
    pg.save_state(state)
    assert json.loads(cache.read_text()) == {**DB_DEFAULTS, 'wal_receiver': expected}
    helpers.write_status_file(state, ZkState(), str(tmp_path))
    assert json.loads((tmp_path / 'pgconsul.status').read_text())['db_state'] == {**DB_DEFAULTS, 'wal_receiver': expected}
    conf = MagicMock()
    conf.get.return_value = str(tmp_path)
    with patch('src.cli.create_zk') as create_zk:
        create_zk.return_value.__enter__.return_value.get_state.return_value = ZkState()
        create_zk.return_value.__enter__.return_value.MAINTENANCE_PATH = 'maintenance'
        cli.show_info(SimpleNamespace(short=False, json=True), conf)
        assert json.loads(capsys.readouterr().out)['wal_receiver'] == expected
        spec = importlib.machinery.PathFinder.find_spec('yaml')
        yaml = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, 'yaml', yaml)
        spec.loader.exec_module(yaml)
        monkeypatch.setattr(cli, 'yaml', yaml)
        cli.show_info(SimpleNamespace(short=False, json=False), conf)
        assert yaml.safe_load(capsys.readouterr().out)['wal_receiver'] == expected


@pytest.mark.parametrize('payload', [None, {}, {'status': None}])
def test_host_stat_skips_absent_receiver_but_publishes_partial_record(state_consul, state_cluster, payload):
    path = 'all_hosts/me/wal_receiver'
    previous = {'status': 'streaming', 'pid': 123}
    state_cluster.records[path] = json.dumps(previous)
    state_consul.write_host_stat('me', DbState.from_dict({'wal_receiver': payload}))
    assert json.loads(state_cluster.records[path]) == (DEFAULTS if payload else previous)


@pytest.mark.parametrize('last_write', [0, 95])
@pytest.mark.parametrize('payload, stale', [
    (None, True), ({}, True), ({'last_msg_receipt_time_msec': None}, True),
    ({'last_msg_receipt_time_msec': 0}, True), ({'last_msg_receipt_time_msec': 99000}, False),
])
def test_detached_replica_uses_normalized_receiver_time(payload, stale, last_write):
    instance = Pgconsul.__new__(Pgconsul)
    instance.config = SimpleNamespace(close_detached_after=10)
    instance.last_zk_host_stat_write = last_write
    instance.db = MagicMock()
    with patch('src.main.time.time', return_value=100):
        instance.handle_detached_replica(DbState.from_dict({'wal_receiver': payload}))
    if stale and last_write == 0:
        instance.db.pgpooler.assert_called_once_with('stop')
    else:
        instance.db.pgpooler.assert_not_called()


@pytest.mark.parametrize('local_payload', [None, {}, {'status': None}])
@pytest.mark.parametrize('source_payload', [None, {}, {'status': None}, {'status': 'streaming'}])
def test_cascade_distinguishes_receiver_presence_from_source_streaming(state_consul, state_cluster, local_payload, source_payload):
    cluster = state_cluster
    state_consul.config.stream_from = 'upstream'
    state_consul.config.replication_slots_polling = True
    cluster.holders.update({'quorum/members/me': ['me'], 'replication_sources/old': ['me']})
    cluster.records.update({
        'replication_sources/old': '', 'replication_sources/upstream': '',
        'all_hosts/upstream/replics_info': '[{"application_name":"me","state":"streaming"}]',
        'all_hosts/upstream/wal_receiver': json.dumps(source_payload),
    })
    # The external DB remains in recovery after its upstream is reconfigured.
    state_consul.db.get_role.return_value = 'replica'
    state_consul.non_ha_replica_iter(
        DbState.from_dict({'wal_receiver': local_payload}), ZkState(alive=True, lock_holder='primary', replics_info=[]),
    )
    switches_upstream = not local_payload and source_payload == {'status': 'streaming'}
    assert cluster.holders.get('replication_sources/upstream', []) == (['me'] if local_payload or switches_upstream else [])
    assert cluster.holders['quorum/members/me'] == (['me'] if local_payload else [])
    if local_payload or switches_upstream:
        assert cluster.holders['replication_sources/old'] == []
    if switches_upstream:
        state_consul.db.recovery_conf.assert_called_with('create', 'upstream')
        state_consul.db.reload.assert_called()
        state_consul.db.ensure_replaying_wal.assert_called()
    else:
        state_consul.db.recovery_conf.assert_not_called()
