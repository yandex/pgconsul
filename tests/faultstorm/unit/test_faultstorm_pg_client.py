import importlib.util
from pathlib import Path
import sys
from types import ModuleType
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setitem(sys.modules, 'psycopg2', ModuleType('psycopg2'))
    db_client = ModuleType('faultstorm.db_client')
    db_client.DatabaseClient = object
    monkeypatch.setitem(sys.modules, 'faultstorm.db_client', db_client)
    path = Path(__file__).resolve().parents[3] / 'docker/faultstorm/faultstorm_pg_client.py'
    spec = importlib.util.spec_from_file_location('faultstorm_pg_client', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.PgConsulClient(['primary', 'replica'])


@pytest.fixture
def connection(client, monkeypatch):
    conn = MagicMock()
    monkeypatch.setattr(client, '_connect', lambda node: conn)
    return conn


def test_final_read_rejects_stale_replica_snapshot(client, connection):
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [(1,)]

    def execute(query):
        if query == 'SET TRANSACTION READ WRITE':
            raise RuntimeError('cannot set transaction read-write mode during recovery')

    cursor.execute.side_effect = execute

    with pytest.raises(RuntimeError, match='during recovery'):
        client.read('replica')

    cursor.fetchall.assert_not_called()
    connection.close.assert_called_once_with()


@pytest.mark.parametrize('values', [set(), {1, 2}])
def test_final_read_returns_primary_snapshot_in_one_transaction(client, connection, values):
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [(value,) for value in values]

    assert client.read('primary') == values

    assert connection.autocommit is False
    assert cursor.execute.call_args_list == [
        (('SET TRANSACTION READ WRITE',),),
        (('SELECT value FROM set',),),
    ]
    connection.close.assert_called_once_with()


def test_final_read_propagates_query_failure_and_closes_connection(client, connection):
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.side_effect = RuntimeError('connection lost')

    with pytest.raises(RuntimeError, match='connection lost'):
        client.read('primary')

    connection.close.assert_called_once_with()
