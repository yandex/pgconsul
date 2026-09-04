from unittest.mock import MagicMock, patch

import psycopg2

from src.pg import Postgres


def _postgres():
    postgres = Postgres.__new__(Postgres)
    postgres.config = MagicMock(conn_string='dbname=postgres')
    postgres._wal_barrier_conn = None
    postgres._wal_barrier_cursor = None
    postgres._wal_barrier_operation_id = None
    postgres._wal_barrier_query_started = False
    postgres._wal_barrier_started_at = None
    postgres.config.wal_barrier_timeout = 30
    return postgres


def test_barrier_is_nonblocking_and_completes_only_after_commit_poll():
    postgres = _postgres()
    connection = MagicMock()
    cursor = MagicMock()
    connection.cursor.return_value = cursor
    connection.poll.side_effect = [psycopg2.extensions.POLL_OK, psycopg2.extensions.POLL_OK]

    with patch('src.pg.psycopg2.connect', return_value=connection) as connect, \
         patch('src.pg.SQL') as sql, patch('src.pg.Literal') as literal:
        assert not postgres.advance_wal_barrier('operation-1')
        assert postgres.advance_wal_barrier('operation-1')

    connect.assert_called_once_with(
        'dbname=postgres',
        async_=True,
        options='-c statement_timeout=30000 -c lock_timeout=30000',
    )
    query = sql.call_args.args[0]
    assert 'CREATE TABLE IF NOT EXISTS public.pgconsul_durability_barrier' in query
    assert 'TRUNCATE TABLE public.pgconsul_durability_barrier' in query
    assert 'INSERT INTO public.pgconsul_durability_barrier' in query
    assert 'ON CONFLICT' not in query
    assert 'pg_logical_emit_message' not in query
    assert 'synchronous_commit' in query
    literal.assert_called_once_with('operation-1')
    connection.close.assert_called_once_with()


def test_new_operation_cancels_local_tracking_and_starts_a_new_connection():
    postgres = _postgres()
    old_connection = MagicMock()
    postgres._wal_barrier_conn = old_connection
    postgres._wal_barrier_cursor = MagicMock()
    postgres._wal_barrier_operation_id = 'old-operation'
    postgres._wal_barrier_query_started = True
    new_connection = MagicMock()
    new_connection.poll.return_value = psycopg2.extensions.POLL_READ

    with patch('src.pg.psycopg2.connect', return_value=new_connection):
        assert not postgres.advance_wal_barrier('new-operation')

    old_connection.close.assert_called_once_with()


def test_barrier_deadline_closes_unknown_attempt_and_retries_later():
    postgres = _postgres()
    connection = MagicMock()
    connection.poll.return_value = psycopg2.extensions.POLL_READ

    with patch('src.pg.psycopg2.connect', return_value=connection), \
         patch('src.pg.time.monotonic', side_effect=[100, 131]):
        assert not postgres.advance_wal_barrier('operation')
        assert not postgres.advance_wal_barrier('operation')

    connection.close.assert_called_once_with()
    assert postgres._wal_barrier_conn is None
