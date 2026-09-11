# encoding: utf-8
from unittest.mock import MagicMock, call, patch

import psycopg2
import pytest

from src.exceptions import PostgresConnectionError, PostgresConnectionTimeout
from src.pg import Postgres, PostgresConfig


def _make_postgres() -> Postgres:
    postgres = Postgres.__new__(Postgres)
    postgres.config = PostgresConfig(
        conn_string='host=localhost connect_timeout=99 dbname=postgres',
        use_lwaldump=False,
        working_dir='/tmp',
        recovery_filepath='recovery.conf',
        use_replication_slots=False,
        standalone_pooler=False,
        pooler_conn_timeout=1.0,
        pooler_addr='localhost',
        pooler_port=6432,
        postgres_timeout=5.0,
        iteration_timeout=1.0,
    )
    postgres._cmd_manager = MagicMock()
    postgres.conn_local = None
    postgres.role = 'primary'
    postgres.pgdata = '/data/pg'
    postgres.terminal_state = True
    postgres._conn_timeout_count = 0
    postgres._base_conn_string = postgres._strip_connect_timeout(postgres.config.conn_string)
    return postgres


def test_connection_timeout_is_a_typed_postgres_connection_error():
    assert issubclass(PostgresConnectionTimeout, PostgresConnectionError)


def test_configured_connect_timeout_is_removed_case_insensitively():
    assert Postgres._strip_connect_timeout(
        'host=localhost CONNECT_TIMEOUT=99 dbname=postgres'
    ) == 'host=localhost dbname=postgres'


def test_timeout_uses_exponential_backoff_and_resets_after_success():
    postgres = _make_postgres()
    connection = MagicMock()
    timeout = psycopg2.OperationalError('connection timeout expired')

    with patch('src.pg.psycopg2.connect', side_effect=[timeout, timeout, connection]) as connect, \
         patch.object(postgres, 'get_role', return_value='primary'), \
         patch.object(postgres, '_get_pgdata_path', return_value='/data/pg'), \
         patch.object(postgres, '_log_connection_timeout_diagnostics'):
        with pytest.raises(PostgresConnectionTimeout) as first:
            postgres.reconnect()
        with pytest.raises(PostgresConnectionTimeout) as second:
            postgres.reconnect()
        postgres.reconnect()

    assert first.value.timeout_count == 1
    assert second.value.timeout_count == 2
    assert connect.call_args_list == [
        call('host=localhost dbname=postgres connect_timeout=1'),
        call('host=localhost dbname=postgres connect_timeout=2'),
        call('host=localhost dbname=postgres connect_timeout=4'),
    ]
    assert postgres._conn_timeout_count == 0


def test_connect_timeout_backoff_is_capped_at_ten_seconds():
    postgres = _make_postgres()
    postgres._conn_timeout_count = 10
    timeout = psycopg2.OperationalError('TIMEOUT expired')

    with patch('src.pg.psycopg2.connect', side_effect=timeout) as connect, \
         patch.object(postgres, '_log_connection_timeout_diagnostics'):
        with pytest.raises(PostgresConnectionTimeout):
            postgres.reconnect()

    connect.assert_called_once_with('host=localhost dbname=postgres connect_timeout=10')


def test_non_timeout_operational_error_keeps_existing_reconnect_contract():
    postgres = _make_postgres()

    with patch(
        'src.pg.psycopg2.connect',
        side_effect=psycopg2.OperationalError('connection refused'),
    ):
        postgres.reconnect()

    assert postgres.conn_local is None
    assert postgres._conn_timeout_count == 0


def test_startup_state_is_not_classified_as_an_overload_timeout():
    postgres = _make_postgres()
    message = 'FATAL:  the database system is starting up\ntimeout while waiting'

    with patch('src.pg.psycopg2.connect', side_effect=psycopg2.OperationalError(message)):
        postgres.reconnect()

    assert postgres.terminal_state is False
    assert postgres._conn_timeout_count == 0


def test_timeout_propagates_through_liveness_probe():
    postgres = _make_postgres()
    error = PostgresConnectionTimeout(1)

    with patch.object(postgres, 'reconnect', side_effect=error):
        with pytest.raises(PostgresConnectionTimeout) as raised:
            postgres.is_alive_and_in_terminal_state()

    assert raised.value is error


def test_timeout_propagates_through_get_state():
    postgres = _make_postgres()
    error = PostgresConnectionTimeout(1)

    with patch.object(postgres, 'is_alive_and_in_terminal_state', side_effect=error):
        with pytest.raises(PostgresConnectionTimeout) as raised:
            postgres.get_state()

    assert raised.value is error
