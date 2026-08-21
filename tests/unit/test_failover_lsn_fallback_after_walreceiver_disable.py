# encoding: utf-8
"""Red test: get_wal_receive_lsn must fall back to pg_last_wal_receive_lsn (MDB-41951).

Reproduces the behave failure in failover_with_network_inconsistency.feature:80
("Old primary can not get a write acknowledged after voting for a new primary
has started").

Root cause: the phase reorder in report 61 moved voting BEFORE walreceiver
disable so that lwaldump() does not crash. But this leaves the walreceiver
active after voting, so the old primary can still get a synchronous write
acknowledged — the regression test fails.

The correct fix is to keep the original phase order (disable walreceiver
BEFORE voting) and add a fallback in get_wal_receive_lsn(): when use_lwaldump
is True and lwaldump() crashes the session (PostgresConnectionError), reconnect
and retry with pg_last_wal_receive_lsn() which works even after walreceiver
is disabled.

This test asserts the fallback behaviour.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.exceptions import PostgresConnectionError
from src.pg import Postgres, PostgresConfig


def _make_config(**overrides) -> PostgresConfig:
    defaults = dict(
        conn_string='host=localhost port=5432 dbname=postgres user=postgres',
        use_lwaldump=True,
        working_dir='/tmp',
        recovery_filepath='/tmp/recovery.conf',
        use_replication_slots=False,
        standalone_pooler=False,
        pooler_conn_timeout=1.0,
        pooler_addr='localhost',
        pooler_port=6432,
        postgres_timeout=5.0,
        iteration_timeout=5.0,
    )
    defaults.update(overrides)
    return PostgresConfig(**defaults)


def _make_postgres_lwaldump() -> Postgres:
    """Create a Postgres with use_lwaldump=True."""
    config = _make_config(use_lwaldump=True)
    mock_cmd = MagicMock()
    mock_cmd.list_clusters.return_value = []
    with patch('src.pg.psycopg2.connect') as mock_connect:
        fake_conn = MagicMock()
        fake_conn.cursor.return_value = MagicMock()
        mock_connect.return_value = fake_conn
        with patch.object(Postgres, 'get_role', return_value='replica'), \
             patch.object(Postgres, '_get_pgdata_path', return_value='/data/pg'):
            return Postgres(config, mock_cmd)


class TestGetWalReceiveLsnFallback:
    """get_wal_receive_lsn must fall back to pg_last_wal_receive_lsn when lwaldump crashes.

    lwaldump() is a C extension that kills the DB session once the walreceiver
    has been disabled (primary_conninfo cleared). After the crash the connection
    is gone (PostgresConnectionError). The method must reconnect and retry with
    pg_last_wal_receive_lsn() which works without an active walreceiver.
    """

    def test_falls_back_to_pg_last_wal_receive_lsn_on_lwaldump_crash(self):
        """When lwaldump() crashes, reconnect and use pg_last_wal_receive_lsn.

        RED against current code: lwaldump() raises PostgresConnectionError
        and get_wal_receive_lsn propagates it (no fallback).
        """
        pg = _make_postgres_lwaldump()

        fallback_cur = MagicMock()
        fallback_cur.fetchone.return_value = (78678488,)

        with patch.object(pg, 'lwaldump', side_effect=PostgresConnectionError("server closed")):
            with patch.object(pg, 'reconnect') as mock_reconnect:
                with patch.object(pg, '_exec_query', return_value=fallback_cur) as mock_exec:
                    result = pg.get_wal_receive_lsn()

        assert result == 78678488
        # Must have reconnected after the crash
        mock_reconnect.assert_called_once()
        # Must have executed the fallback query (pg_last_wal_receive_lsn)
        assert mock_exec.call_count == 1
        query = mock_exec.call_args[0][0]
        assert 'pg_last_wal_receive_lsn' in query

    def test_fallback_query_works_after_walreceiver_disabled(self):
        """The fallback query must not contain lwaldump().

        pg_last_wal_receive_lsn() returns the last LSN received by the
        walreceiver even after it has been stopped — it reads from shared
        memory / WAL receiver state, not from a live connection.
        """
        pg = _make_postgres_lwaldump()

        fallback_cur = MagicMock()
        fallback_cur.fetchone.return_value = (42,)

        with patch.object(pg, 'lwaldump', side_effect=PostgresConnectionError("crash")):
            with patch.object(pg, 'reconnect'):
                with patch.object(pg, '_exec_query', return_value=fallback_cur) as mock_exec:
                    pg.get_wal_receive_lsn()

        query = mock_exec.call_args[0][0]
        assert 'lwaldump' not in query, (
            "Fallback query must not call lwaldump() — it crashes after "
            "walreceiver is disabled"
        )
        assert 'pg_last_wal_receive_lsn' in query

    def test_propagates_error_if_fallback_also_fails(self):
        """If the fallback query also fails, PostgresConnectionError propagates."""
        pg = _make_postgres_lwaldump()

        with patch.object(pg, 'lwaldump', side_effect=PostgresConnectionError("crash")):
            with patch.object(pg, 'reconnect'):
                with patch.object(pg, '_exec_query', side_effect=PostgresConnectionError("still down")):
                    with pytest.raises(PostgresConnectionError):
                        pg.get_wal_receive_lsn()

    def test_no_fallback_when_use_lwaldump_false(self):
        """When use_lwaldump=False, pg_last_wal_receive_lsn is used directly (no fallback needed)."""
        config = _make_config(use_lwaldump=False)
        mock_cmd = MagicMock()
        mock_cmd.list_clusters.return_value = []
        with patch('src.pg.psycopg2.connect') as mock_connect:
            fake_conn = MagicMock()
            fake_conn.cursor.return_value = MagicMock()
            mock_connect.return_value = fake_conn
            with patch.object(Postgres, 'get_role', return_value='replica'), \
                 patch.object(Postgres, '_get_pgdata_path', return_value='/data/pg'):
                pg = Postgres(config, mock_cmd)

        cur = MagicMock()
        cur.fetchone.return_value = (999,)
        with patch.object(pg, '_exec_query', return_value=cur) as mock_exec:
            result = pg.get_wal_receive_lsn()

        assert result == 999
        query = mock_exec.call_args[0][0]
        assert 'pg_last_wal_receive_lsn' in query
        assert 'lwaldump' not in query
