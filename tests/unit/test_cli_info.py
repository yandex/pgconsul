import json
from configparser import ConfigParser
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

with patch('src.read_config', create=True), \
     patch('src.init_logging', create=True):
    from src.cli import _info_timestamp, show_info


@pytest.mark.parametrize('value, divisor, expected', [
    (None, 1, None),
    (None, 1000, None),
    (0, 1, [{'ts': 0.0}, {'time': '1970-01-01T00:00:00+00:00'}]),
    (1704067200, 1, [{'ts': 1704067200.0}, {'time': '2024-01-01T00:00:00+00:00'}]),
    ('1704067200.123456', 1, [{'ts': 1704067200.123456}, {'time': '2024-01-01T00:00:00.123456+00:00'}]),
    (1704067200123, 1000, [{'ts': 1704067200.123}, {'time': '2024-01-01T00:00:00.123000+00:00'}]),
])
def test_info_timestamp(value, divisor, expected):
    assert _info_timestamp(value, divisor) == expected


@pytest.fixture
def info_source(zk, tmp_path):
    state = {
        'alive': True,
        'lock_holder': 'primary',
        'timeline': 3,
        'last_failover_time': 1704067200,
        'last_switchover_time': 1704067200.123,
        'maintenance': {'status': 'enable', 'ts': '1704067200.123'},
        'synchronous_standby_names': {'primary': ('ANY 1 (replica)', '1704067200.123')},
        'replics_info': [{
            'client_hostname': 'replica',
            'state': 'streaming',
            'sync_state': 'quorum',
            'replay_lag_msec': 42,
            'backend_start_ts': 1704067200,
            'reply_time_ms': 1704067200123,
        }],
    }
    db_state = {
        'role': 'replica',
        'timeline': 2,
        'wal_receiver': {
            'pid': 1234,
            'status': 'streaming',
            'slot_name': 'replica_slot',
            'last_msg_receipt_time_msec': 1704067200123,
            'conninfo': 'host=upstream port=5432 options=-c wal_sender_timeout=30000 application_name=replica',
        },
    }
    cache = tmp_path / '.pgconsul_db_state.cache'
    cache.write_text(json.dumps(db_state))
    conf = ConfigParser()
    conf['global'] = {'working_dir': str(tmp_path)}
    zk.get_state = MagicMock(return_value=state)
    context = MagicMock()
    context.__enter__.return_value = zk
    with patch('src.cli.create_zk', return_value=context):
        yield conf, state, cache


@pytest.mark.parametrize('short', [False, True])
@pytest.mark.parametrize('as_json', [False, True])
def test_info_formats_all_timestamps(info_source, capsys, short, as_json):
    conf, _, cache = info_source
    cached_state = cache.read_text()
    with patch('src.cli.yaml.dump', return_value='yaml output') as dump:
        show_info(SimpleNamespace(short=short, json=as_json), conf)
    info = json.loads(capsys.readouterr().out) if as_json else dump.call_args.args[0]
    assert cache.read_text() == cached_state
    seconds = [{'ts': 1704067200.0}, {'time': '2024-01-01T00:00:00+00:00'}]
    fractional = [{'ts': 1704067200.123}, {'time': '2024-01-01T00:00:00.123000+00:00'}]
    assert info['last_failover_time'] == seconds
    assert info['maintenance'] == {'status': 'enable', 'since': fractional}
    assert info['primary'] == 'primary'
    if short:
        assert info['replics_info'] == {'replica': 'streaming, sync_state quorum, replay_lag_msec 42'}
        assert 'last_switchover_time' not in info
    else:
        assert info['timeline'] == 3
        assert info['timeline_local'] == 2
        assert info['last_switchover_time'] == fractional
        assert info['replics_info'] == [{
            'client_hostname': 'replica',
            'state': 'streaming',
            'sync_state': 'quorum',
            'replay_lag_msec': 42,
            'backend_start': seconds,
            'reply_time': fractional,
        }]
        assert info['wal_receiver'] == {
            'pid': 1234,
            'status': 'streaming',
            'slot_name': 'replica_slot',
            'last_msg_receipt_time': fractional,
            'conninfo': {'host': 'upstream', 'port': '5432', 'options': '-c', 'wal_sender_timeout': '30000', 'application_name': 'replica'},
        }
        assert info['synchronous_standby_names'] == {'primary': {'value': 'ANY 1 (replica)', 'last_update': fractional}}


@pytest.mark.parametrize('short', [False, True])
def test_info_without_timestamps_or_cache(info_source, capsys, short):
    conf, state, cache = info_source
    state.update({
        'last_failover_time': None,
        'last_switchover_time': None,
        'maintenance': {'status': None, 'ts': None},
        'synchronous_standby_names': {'primary': (None, None)},
        'replics_info': None,
    })
    cache.unlink()
    show_info(SimpleNamespace(short=short, json=True), conf)
    info = json.loads(capsys.readouterr().out)
    assert info['last_failover_time'] is None
    assert info['maintenance'] is None
    if not short:
        assert info['timeline'] == 3
        assert info['timeline_local'] is None
        assert info['last_switchover_time'] is None
        assert info['synchronous_standby_names'] == {'primary': {'value': None, 'last_update': None}}


@pytest.mark.parametrize('receiver, expected_receiver', [
    (None, None),
    ({'status': 'starting'}, {'status': 'starting'}),
    ({'last_msg_receipt_time_msec': None}, {'last_msg_receipt_time': None}),
    ({'last_msg_receipt_time_msec': 0}, {'last_msg_receipt_time': [{'ts': 0.0}, {'time': '1970-01-01T00:00:00+00:00'}]}),
])
def test_info_with_missing_replication_timestamps(info_source, capsys, receiver, expected_receiver):
    conf, state, cache = info_source
    state['maintenance']['ts'] = None
    state['replics_info'] = [{'backend_start_ts': None, 'reply_time_ms': None}, {'state': 'streaming'}]
    cache.write_text(json.dumps({'wal_receiver': receiver}))
    show_info(SimpleNamespace(short=False, json=True), conf)
    info = json.loads(capsys.readouterr().out)
    assert info['maintenance'] == {'status': 'enable', 'since': None}
    assert info['replics_info'] == [{'backend_start': None, 'reply_time': None}, {'state': 'streaming'}]
    assert info['wal_receiver'] == expected_receiver
    assert info['timeline_local'] is None


@pytest.mark.parametrize('timeline_local, zk_timeline', [(2, 3), (3, 2), (None, 3), (2, None)])
def test_info_preserves_local_and_zk_timelines(info_source, capsys, timeline_local, zk_timeline):
    conf, state, cache = info_source
    state['timeline'] = zk_timeline
    cache.write_text(json.dumps({'role': 'primary', 'timeline': timeline_local}))
    show_info(SimpleNamespace(short=False, json=True), conf)
    info = json.loads(capsys.readouterr().out)
    assert info['timeline'] == zk_timeline
    assert info['timeline_local'] == timeline_local


@pytest.mark.parametrize('as_json', [False, True])
@pytest.mark.parametrize('receiver, parsed', [
    (None, None),
    ({}, None),
    ({'conninfo': None}, {}),
    ({'conninfo': ''}, {}),
    ({'conninfo': 'host=upstream port=5432'}, {'host': 'upstream', 'port': '5432'}),
    ({'conninfo': 'application_name=worker fallback_application_name=other'}, {'application_name': 'worker', 'fallback_application_name': 'other'}),
    ({'conninfo': 'options=-c application_name=worker sslmode=prefer'}, {'options': '-c', 'application_name': 'worker', 'sslmode': 'prefer'}),
    ({'conninfo': 'host=upstream new_libpq_option=value'}, {'host': 'upstream', 'new_libpq_option': 'value'}),
    ({'conninfo': 'host=upstream application_name=worker host=other'}, {'host': 'other', 'application_name': 'worker'}),
    ({'conninfo': "passfile=/path with spaces/.pgpass application_name=replica's name"}, {'passfile': '/path', 'application_name': "replica's"}),
    ({'conninfo': '  host=upstream\tport=5432\noptions=--search_path=public  '}, {'host': 'upstream', 'port': '5432', 'options': '--search_path=public'}),
    ({'conninfo': 'host= ignored'}, {'host': ''}),
    ({'conninfo': 'not a connection string'}, {}),
])
def test_info_parses_conninfo(info_source, capsys, receiver, parsed, as_json):
    conf, _, cache = info_source
    cache.write_text(json.dumps({'wal_receiver': receiver}))
    with patch('src.cli.yaml.dump', return_value='yaml output') as dump:
        show_info(SimpleNamespace(short=False, json=as_json), conf)
    info = json.loads(capsys.readouterr().out) if as_json else dump.call_args.args[0]
    expected = {**receiver, 'conninfo': parsed} if receiver is not None and 'conninfo' in receiver else receiver
    assert info['wal_receiver'] == expected
