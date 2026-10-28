"""DbState compatibility at persistence, recovery and publication boundaries."""
import importlib.machinery
import importlib.util
import json
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import psycopg2

from src import helpers
from src.main import Pgconsul
from src.types import DbState, ZkState
from src.pg import Postgres, PostgresConfig
from src.zk import Zookeeper, ZookeeperConfig
from tests.unit.state_samples import DB_DEFAULTS, LEGACY_REPLICA_ROW


@pytest.fixture
def postgres(tmp_path, monkeypatch):
    """Real PostgreSQL adapter with responses supplied at the SQL/command boundary."""
    server = SimpleNamespace(available=True, role='primary', pgdata='/data', checksums=1, wal_log_hints='off', disconnect_after=None, connection_states=[])
    commands = MagicMock()
    commands.list_clusters.return_value = []
    commands.get_pooler_status.return_value = 0

    def control_parameter(path, name, *args):
        return {
            'Latest checkpoint.s TimeLineID': 7, 'Data page checksum version': server.checksums,
            'wal_log_hints setting': server.wal_log_hints,
        }[name]

    commands.get_control_parameter.side_effect = control_parameter

    def connect(*args, **kwargs):
        if hasattr(server, 'db'):
            server.connection_states.append((server.db.role, server.db.pgdata))
        if not server.available:
            raise psycopg2.OperationalError('database unavailable')
        connection = MagicMock()

        def cursor():
            result = MagicMock()

            def execute(query, *args, **kwargs):
                if not server.available:
                    raise psycopg2.OperationalError('database unavailable')
                sql = ' '.join(query.lower().rstrip(';').split())
                rows = {
                    'select 1': [(1,)], 'select 42': [(42,)],
                    'select pg_is_in_recovery()': [(server.role == 'replica',)],
                    'show data_directory': [(server.pgdata,)],
                    'show synchronous_standby_names': [('',)],
                    "select count(*) from pg_stat_activity where state!='idle'": [(0,)],
                    'show max_connections': [(100,)],
                    'show primary_conninfo': [('host=leader',)],
                    'show archive_mode': [('on',)],
                    "select * from pg_extension where extname = 'lwaldump'": [],
                }
                if 'from pg_stat_replication' in sql or 'from pg_stat_wal_receiver' in sql:
                    response = []
                else:
                    assert sql in rows, f'Unexpected SQL: {query}'
                    response = rows[sql]
                result.description = []
                result.fetchone.return_value = response[0] if response else None
                result.fetchall.return_value = response
                if server.disconnect_after and server.disconnect_after in sql:
                    server.available = False

            result.execute.side_effect = execute
            return result

        connection.cursor.side_effect = cursor
        return connection

    monkeypatch.setattr('src.pg.psycopg2.connect', connect)
    config = PostgresConfig(
        conn_string='host=localhost port=5432 dbname=postgres', use_lwaldump=False,
        working_dir=str(tmp_path), recovery_filepath='recovery.conf', use_replication_slots=False,
        standalone_pooler=False, pooler_conn_timeout=1.0, pooler_addr='localhost',
        pooler_port=6432, postgres_timeout=5.0, iteration_timeout=0.0,
    )
    server.db = Postgres(config, commands)
    return server


@pytest.fixture
def zk_transport():
    return MagicMock()


@pytest.fixture
def zookeeper(zk_transport):
    """Real domain adapter over an in-memory transport store."""
    data = {}
    client = zk_transport
    client.lock_version.return_value = None
    client.get.side_effect = data.get
    client.write.side_effect = lambda path, value, **kwargs: data.__setitem__(path, value)
    client.ensure_path.return_value = True
    client.exists.side_effect = lambda path: path in data
    client.get_children.return_value = []
    client.is_alive.return_value = True
    client.make_lock.return_value.contenders.return_value = []
    return Zookeeper(client, ZookeeperConfig(False, 1.0, '/test/', 'host1')), data


@pytest.fixture
def consul(postgres, zookeeper, tmp_path, monkeypatch):
    monkeypatch.delenv('NOTIFY_SOCKET', raising=False)
    monkeypatch.setattr('signal.signal', lambda *args: None)
    config = SimpleNamespace(
        welcome_message='', working_dir=str(tmp_path), pg_conn_failure_grace_period=10,
        quorum_commit=False, use_lwaldump=False, stream_from=None, iteration_timeout=0,
    )
    return Pgconsul(config, postgres.db, zookeeper[0], MagicMock(), MagicMock(), MagicMock(), MagicMock())


@pytest.mark.parametrize('replacement', [None, 'null', '{}', '{"timeline":"bad"}'])
def test_disappearing_switchover_keeps_baseline_lock_and_actions(replacement, consul, zookeeper, zk_transport):
    zk, data = zookeeper
    hostname = helpers.get_hostname()
    data.update({'timeline': '7', 'switchover/master': '{"hostname":"leader","timeline":7}',
                 'switchover/state': 'candidate_found', 'switchover/candidate': hostname})
    snapshot = zk.get_state()
    holders = []
    lock = zk_transport.make_lock.return_value
    lock.contenders.side_effect = lambda: list(holders)
    def acquire(**kwargs):
        holders.append('host1')
        data['switchover/master'] = replacement
        return True
    lock.acquire.side_effect = acquire
    lock.release.side_effect = lambda: holders.clear() or True
    consul.config.failure_name = None
    consul.config.switchover_rollback_timeout = 0
    consul._do_failover = MagicMock(return_value=False)
    state = DbState(alive=True, role='replica', timeline=7, primary_fqdn='leader')
    assert consul.replica_iter(state, snapshot) is False
    if replacement in (None, 'null'):
        assert holders == ['host1']
        consul._do_failover.assert_not_called()
    else:
        assert holders == []
        consul._do_failover.assert_called_once_with(old_primary=None)
    assert data['switchover/state'] == 'candidate_found'


@pytest.mark.parametrize('priority', [{}, {'priority': None}])
def test_historical_replica_row_survives_cache_and_zk_readers(postgres, zookeeper, tmp_path, priority):
    zk, data = zookeeper
    row = {**LEGACY_REPLICA_ROW, **priority}
    cache = tmp_path / '.pgconsul_db_state.cache'
    cache.write_text(json.dumps({'role': 'primary', 'pgdata': '/old', 'replics_info': [row]}))
    state = postgres.db.get_prev_state()
    assert state['replics_info'][0] == row
    postgres.db.save_state(DbState.from_dict(state))
    assert json.loads(cache.read_text())['replics_info'] == [row]
    data['replics_info'] = json.dumps([row])
    data['all_hosts/leader/replics_info'] = json.dumps([row])
    for replicas in (zk.get_replics_info(), zk.get_host_replics_info('leader'), zk.get_stream_source_replics_info('leader')):
        assert replicas[0].to_dict() == row


def test_unvalidated_timeline_survives_daemon_snapshot(consul, zookeeper):
    zk, data = zookeeper
    data.update({'switchover/master': '{"hostname":"leader","timeline":"bad"}', 'switchover/state': 'scheduled'})
    before = dict(data)
    assert zk.get_state().switchover.timeline == 'bad'
    assert data == before


@pytest.mark.parametrize('legacy', [{}, dict.fromkeys(DB_DEFAULTS), {'alive': None, 'pgdata': None}])
def test_legacy_missing_and_null_fields_stay_distinct(legacy):
    state = DbState.from_dict(legacy)
    assert state.to_dict() == legacy


@pytest.mark.parametrize('field', ['alive', 'running', 'opened', 'connection_timed_out'])
@pytest.mark.parametrize('value', [None, False, True])
def test_legacy_boolean_fields_preserve_null_independently(field, value):
    state = DbState.from_dict({field: value})
    expected = value
    assert getattr(state, field) is expected
    assert state.to_dict() == {field: expected}


@pytest.mark.parametrize('field, value', [
    ('pgdata', ''), ('pgdata', '/old'), ('role', ''), ('role', 'replica'),
    ('timeline', 0), ('timeline', -1),
    ('primary_fqdn', ''), ('primary_fqdn', 'leader'), ('replics_info', []),
    ('replication_state', ['async', None]), ('replication_state', ['sync', 'replica']),
])
def test_legacy_values_keep_their_meaning(field, value):
    state = DbState.from_dict({field: value})
    expected = value
    assert json.loads(json.dumps(state.to_dict())) == {field: expected}


def test_direct_state_and_later_assignments_are_serialized():
    state = DbState()
    assert state.to_dict() == DB_DEFAULTS
    state.role = 'primary'
    state.pgdata = '/data'
    state.replics_info = []
    assert state.to_dict() == {**DB_DEFAULTS, 'role': 'primary', 'pgdata': '/data', 'replics_info': []}
    assert DbState(role='primary', pgdata='/data', replics_info=[]).to_dict() == state.to_dict()


@pytest.mark.parametrize('legacy, rejected', [
    ({'role': 'primary'}, 'pgdata'),
    ({'pgdata': '/old'}, 'role'),
    ({'role': None, 'pgdata': '/old'}, None),
    ({'role': '', 'pgdata': '/old'}, None),
    ({'role': 'primary', 'pgdata': None}, None),
    ({'role': 'primary', 'pgdata': ''}, None),
    ({'role': 'primary', 'pgdata': '/old'}, None),
    ({'role': 'replica', 'pgdata': '/old'}, None),
    ({'role': 'legacy-role', 'pgdata': '/old'}, None),
    ({}, None), (None, None),
])
def test_re_init_uses_raw_cache_and_rejects_missing_required_keys(tmp_path, legacy, rejected, postgres, consul):
    pg = postgres.db
    postgres.available = False
    pg.role, pg.pgdata = 'original-role', '/original'
    (tmp_path / '.pgconsul_db_state.cache').write_text(json.dumps(legacy))
    if rejected:
        with pytest.raises(SystemExit) as error:
            consul.re_init_db()
        assert error.value.code == 1
        assert pg.pgdata == '/original'
        # No connection attempt may use a missing/empty cache value.
        assert all(role and path for role, path in postgres.connection_states)
    else:
        consul.re_init_db()
        expected = (legacy['role'], legacy['pgdata']) if legacy else ('original-role', '/original')
        assert (pg.role, pg.pgdata) == expected
        assert postgres.connection_states[-1] == expected


@pytest.mark.parametrize('alive', [False, True])
@pytest.mark.parametrize('legacy, invalid_path', [
    ({'role': 'primary'}, True),
    ({'role': 'primary', 'pgdata': None}, False),
    ({'role': 'primary', 'pgdata': ''}, False),
    ({'pgdata': '/old'}, False),
    ({'role': None, 'pgdata': '/old'}, False),
    ({'role': 'replica', 'pgdata': '/old'}, False),
    ({}, False), (None, False),
])
def test_startup_checks_cache_path_only_when_database_is_dead(tmp_path, alive, legacy, invalid_path, postgres, consul):
    postgres.available = alive
    postgres.pgdata = '/original'
    postgres.db.pgdata = '/original'
    (tmp_path / '.pgconsul_db_state.cache').write_text(json.dumps(legacy))
    if invalid_path and not alive:
        with pytest.raises(KeyError, match='pgdata'):
            consul.startup_checks()
        assert postgres.db.pgdata == '/original'
    else:
        consul.startup_checks()
        assert postgres.db.pgdata == (legacy['pgdata'] if legacy and not alive else '/original')


@pytest.mark.parametrize('role', ['primary', 'replica'])
@pytest.mark.parametrize('still_alive', [False, True])
def test_collected_state_keeps_fields_but_only_live_state_replaces_cache(tmp_path, role, still_alive, postgres):
    postgres.role = role
    if not still_alive:
        postgres.disconnect_after = 'show max_connections' if role == 'primary' else 'from pg_stat_replication'
    cache = tmp_path / '.pgconsul_db_state.cache'
    previous = '{"role":"replica","pgdata":"/previous"}'
    cache.write_text(previous)
    state = postgres.db.get_state()
    expected = {
        'alive': still_alive, 'running': True,
        'role': role if still_alive else None, 'pgdata': '/data',
        'opened': True, 'timeline': 7, 'replics_info': [], 'wal_receiver': None,
    }
    expected.update({'replication_state': ['async', None], 'sessions_ratio': 0}
                    if role == 'primary' else {'primary_fqdn': 'leader'})
    assert json.loads(json.dumps(state.to_dict())) == expected
    if still_alive:
        assert json.loads(cache.read_text()) == expected
    else:
        assert cache.read_text() == previous


@pytest.mark.parametrize('legacy, publish', [({}, False), ({'replics_info': None}, False), ({'replics_info': []}, True)])
def test_host_stat_does_not_turn_unknown_replicas_into_empty_publication(legacy, publish, consul, zookeeper):
    _, data = zookeeper
    replicas_path = 'all_hosts/host1/replics_info'
    receiver_path = 'all_hosts/host1/wal_receiver'
    data[replicas_path] = '[{"application_name":"existing"}]'
    data[receiver_path] = '{"status":"streaming"}'
    consul.write_host_stat('host1', DbState.from_dict({**legacy, 'wal_receiver': None}))
    assert json.loads(data[replicas_path]) == ([] if publish else [{'application_name': 'existing'}])
    assert json.loads(data[receiver_path]) == {'status': 'streaming'}


@pytest.mark.parametrize('legacy, publish, ssn', [
    ({}, False, None), ({'replication_state': None}, False, None),
    ({'replication_state': ['async', None]}, True, None),
    ({'replication_state': ['sync', 'replica']}, True, 'replica'),
])
def test_iteration_publishes_ssn_only_for_measured_replication_state(legacy, publish, ssn, consul, zookeeper, tmp_path):
    _, data = zookeeper
    path = f'all_hosts/{helpers.get_hostname()}/synchronous_standby_names/value'
    data.update({path: 'previous', 'maintenance': 'enable'})
    # This orchestrator test supplies a snapshot through the public DB interface.
    consul.db = MagicMock()
    consul.db.is_alive_and_in_terminal_state.return_value = (False, True)
    consul.db.get_state.return_value = DbState.from_dict(legacy)
    consul.run_iteration('100')
    assert data[path] == (str(ssn) if publish else 'previous')
    status = json.loads((tmp_path / 'pgconsul.status').read_text())
    expected = dict(legacy)
    if publish:
        expected['replication_state'] = legacy['replication_state']
    assert status['db_state'] == expected


@pytest.mark.parametrize('as_json', [False, True])
def test_legacy_cache_status_and_real_cli_serializers(tmp_path, capsys, monkeypatch, as_json, postgres):
    spec = importlib.machinery.PathFinder.find_spec('yaml')
    yaml = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, 'yaml', yaml)
    spec.loader.exec_module(yaml)
    with patch('src.read_config', create=True), patch('src.init_logging', create=True):
        from src import cli
    monkeypatch.setattr(cli, 'yaml', yaml)
    payload = {'role': 'primary', 'wal_receiver': {'slot_name': None}, 'sessions_ratio': 23.5,
               'replication_state': ['async', None], 'replics_info': [{'application_name': 'cached'}]}
    (tmp_path / '.pgconsul_db_state.cache').write_text(json.dumps(payload))
    zk_state = ZkState(alive=True, timeline=9, lock_version='42', replics_info=None)
    conf = MagicMock()
    conf.get.return_value = str(tmp_path)
    with patch('src.cli.create_zk') as create_zk:
        zk = create_zk.return_value.__enter__.return_value
        zk.get_state.return_value = zk_state
        zk.MAINTENANCE_PATH = 'maintenance'
        zk.LAST_FAILOVER_TIME_PATH = 'last_failover_time'
        cli.show_info(SimpleNamespace(short=False, json=as_json), conf)
        output = capsys.readouterr().out
        result = json.loads(output) if as_json else yaml.unsafe_load(output)
        assert result['wal_receiver'] == {'slot_name': None}
        assert result['sessions_ratio'] == 23.5
        assert result['replication_state'] == ['async', None]
        assert result['replics_info'] is None
        assert result['lock_version'] == '42'
        assert result['timeline'] == 9
        (tmp_path / '.pgconsul_db_state.cache').write_text('invalid cache')
        cli.show_info(SimpleNamespace(short=True, json=as_json), conf)
        output = capsys.readouterr().out
        result = json.loads(output) if as_json else yaml.unsafe_load(output)
        assert result == {'alive': True, 'primary': None, 'last_failover_time': None,
                          'maintenance': None, 'replics_info': {}}


def test_re_init_leaves_live_database_and_cache_untouched(tmp_path, postgres, consul):
    cache = tmp_path / '.pgconsul_db_state.cache'
    cache.write_text('{"role":null,"pgdata":null}')
    postgres.pgdata = '/live'
    postgres.db.pgdata = '/live'
    postgres.connection_states.clear()
    consul.re_init_db()
    assert (postgres.db.role, postgres.db.pgdata) == ('primary', '/live')
    assert cache.read_text() == '{"role":null,"pgdata":null}'
    assert all(role == 'primary' and path == '/live' for role, path in postgres.connection_states)


@pytest.mark.parametrize('leader', [None, '', 'old-primary'])
@pytest.mark.parametrize('as_json', [False, True])
def test_diagnostic_strings_from_zk_reach_status_and_cli_without_rewriting_records(
    zookeeper, zk_transport, tmp_path, capsys, monkeypatch, leader, as_json,
):
    spec = importlib.machinery.PathFinder.find_spec('yaml')
    yaml = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, 'yaml', yaml)
    spec.loader.exec_module(yaml)
    with patch('src.read_config', create=True), patch('src.init_logging', create=True):
        from src import cli
    monkeypatch.setattr(cli, 'yaml', yaml)
    zk, records = zookeeper
    rows = [
        {'state': 'startup'},
        {'client_hostname': None, 'state': 'catchup'},
        {'client_hostname': '', 'state': 'streaming'},
        {'client_hostname': 'replica.example', 'state': 'streaming'},
    ]
    records['replics_info'] = json.dumps(rows)
    if leader is not None:
        records['last_leader'] = leader
    records.update({
        'all_hosts/value-only/synchronous_standby_names/value': 'ANY 1 (replica)',
        'all_hosts/date-only/synchronous_standby_names/last_update': '123.5',
        'all_hosts/empty/synchronous_standby_names/value': '',
        'all_hosts/empty/synchronous_standby_names/last_update': '',
        'all_hosts/full/synchronous_standby_names/value': 'None',
        'all_hosts/full/synchronous_standby_names/last_update': '456.5',
    })
    expected_ssn = {
        'missing': [None, None],
        'value-only': ['ANY 1 (replica)', None],
        'date-only': [None, '123.5'],
        'empty': ['', ''],
        'full': ['None', '456.5'],
    }
    zk_transport.get_children.return_value = list(expected_ssn)
    before = dict(records)
    snapshot = zk.get_state()
    helpers.write_status_file(DbState(), snapshot, str(tmp_path))
    status = json.loads((tmp_path / 'pgconsul.status').read_text())['zk_state']
    assert status['last_leader'] == leader
    assert status['synchronous_standby_names'] == expected_ssn
    assert status['replics_info'] == rows

    conf = MagicMock()
    conf.get.return_value = str(tmp_path)
    monkeypatch.setattr(cli, 'create_zk', lambda **kwargs: zk)
    for short in (False, True):
        if short:
            with pytest.raises((KeyError, TypeError)):
                cli.show_info(SimpleNamespace(short=True, json=as_json), conf)
        else:
            cli.show_info(SimpleNamespace(short=False, json=as_json), conf)
            output = capsys.readouterr().out
            result = json.loads(output) if as_json else yaml.unsafe_load(output)
            expected = {k: tuple(v) for k, v in expected_ssn.items()} if not as_json else expected_ssn
            assert result['last_leader'] == leader
            assert result['synchronous_standby_names'] == expected
            assert result['replics_info'] == rows
    assert records == before


@pytest.mark.parametrize('alive', [False, True])
@pytest.mark.parametrize('legacy', [{'role': 'primary', 'pgdata': '/cached'}, {}, None])
def test_startup_refuses_existing_cluster_without_rewind_prerequisites(tmp_path, alive, legacy, postgres, consul):
    (tmp_path / '.pgconsul_db_state.cache').write_text(json.dumps(legacy))
    postgres.available = alive
    postgres.checksums = 0
    postgres.wal_log_hints = 'off'
    if legacy:
        with pytest.raises(SystemExit) as error:
            consul.startup_checks()
        assert error.value.code == 1
    else:
        consul.startup_checks()
