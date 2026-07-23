# encoding: utf-8
# noqa: E501
"""
Unit tests for src/pg.py.

Uses mocked psycopg2 so no real PostgreSQL instance is needed.
The conftest.py at this directory level stubs out psycopg2 with real
exception classes before any import from src occurs.
"""

import psycopg2
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from src.exceptions import (
    PostgresException,
    PostgresConnectionError,
    PostgresQueryError,
    pgconsulException,
)
from src.pg import Postgres, PostgresConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(**overrides) -> PostgresConfig:
    """Return a minimal PostgresConfig suitable for unit tests."""
    defaults = dict(
        conn_string='host=localhost port=5432 dbname=postgres user=postgres',
        use_lwaldump=False,
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


def _make_postgres(conn=None, mock_cmd=None) -> Postgres:
    """
    Create a Postgres instance without touching a real DB.

    Both psycopg2.connect and CommandManager are mocked so __init__ succeeds.
    """
    if mock_cmd is None:
        mock_cmd = MagicMock()
        mock_cmd.list_clusters.return_value = []

    config = _make_config()

    with patch('src.pg.psycopg2.connect') as mock_connect:
        if conn is not None:
            mock_connect.return_value = conn
        else:
            fake_conn = MagicMock()
            fake_conn.cursor.return_value = MagicMock()
            mock_connect.return_value = fake_conn

        with patch.object(Postgres, 'get_role', return_value='primary'), \
             patch.object(Postgres, '_get_pgdata_path', return_value='/data/pg'):
            pg = Postgres(config, mock_cmd)

    return pg


# ---------------------------------------------------------------------------
# Tests: exception hierarchy
# ---------------------------------------------------------------------------

class TestExceptionHierarchy:
    """PostgresException hierarchy is properly derived from pgconsulException."""

    def test_postgres_exception_is_pgconsul_exception(self):
        assert issubclass(PostgresException, pgconsulException)

    def test_postgres_connection_error_is_postgres_exception(self):
        assert issubclass(PostgresConnectionError, PostgresException)

    def test_postgres_query_error_is_postgres_exception(self):
        assert issubclass(PostgresQueryError, PostgresException)

    def test_postgres_connection_error_is_exception(self):
        assert issubclass(PostgresConnectionError, Exception)

    def test_raise_and_catch_connection_error(self):
        with pytest.raises(PostgresConnectionError):
            raise PostgresConnectionError("connection refused")

    def test_catch_as_postgres_exception(self):
        with pytest.raises(PostgresException):
            raise PostgresConnectionError("connection refused")

    def test_catch_as_pgconsul_exception(self):
        with pytest.raises(pgconsulException):
            raise PostgresConnectionError("connection refused")

    def test_connection_error_preserves_cause(self):
        cause = psycopg2.OperationalError("FATAL: connection refused")
        try:
            raise PostgresConnectionError(str(cause)) from cause
        except PostgresConnectionError as exc:
            assert exc.__cause__ is cause


# ---------------------------------------------------------------------------
# Tests: _exec_query translates psycopg2.OperationalError
# ---------------------------------------------------------------------------

class TestExecQueryTranslation:
    """_exec_query must translate psycopg2.OperationalError → PostgresConnectionError."""

    def _make_pg_with_failing_execute(self, exc):
        """
        Return a Postgres instance where:
          - _create_cursor health-check (SELECT 1;) succeeds,
          - the actual query execute() raises *exc*.
        """
        pg = _make_postgres()

        # _create_cursor calls cursor.execute('SELECT 1;') as a health-check,
        # then _exec_query calls cursor.execute(real_query, {}).
        # We distinguish them by call count.
        call_count = {'n': 0}

        def execute_side_effect(query, *args):
            call_count['n'] += 1
            if call_count['n'] > 1:
                # Second call is the real query
                raise exc

        cur = MagicMock()
        cur.execute.side_effect = execute_side_effect
        fake_conn = MagicMock()
        fake_conn.cursor.return_value = cur
        pg.conn_local = fake_conn
        return pg

    def test_operational_error_raises_postgres_connection_error(self):
        """psycopg2.OperationalError during execute → PostgresConnectionError."""
        pg = self._make_pg_with_failing_execute(
            psycopg2.OperationalError("server closed the connection")
        )
        with pytest.raises(PostgresConnectionError):
            pg._exec_query("SELECT something")

    def test_operational_error_cause_preserved(self):
        """The original psycopg2.OperationalError is chained as __cause__."""
        original = psycopg2.OperationalError("broken pipe")
        pg = self._make_pg_with_failing_execute(original)

        with pytest.raises(PostgresConnectionError) as exc_info:
            pg._exec_query("SELECT something")
        assert exc_info.value.__cause__ is original

    def test_other_exception_not_translated(self):
        """Non-OperationalError exceptions are NOT translated."""
        pg = self._make_pg_with_failing_execute(ValueError("unexpected"))
        with pytest.raises(ValueError):
            pg._exec_query("SELECT something")

    def test_connection_closed_after_operational_error(self):
        """After psycopg2.OperationalError, self.close() is called."""
        pg = self._make_pg_with_failing_execute(
            psycopg2.OperationalError("broken")
        )
        with patch.object(pg, 'close') as mock_close:
            with pytest.raises(PostgresConnectionError):
                pg._exec_query("SELECT something")
            mock_close.assert_called_once()


# ---------------------------------------------------------------------------
# Tests: PR 1 — low-risk functions (no @return_none_on_error decorator)
# ---------------------------------------------------------------------------

class TestGetWalReceiverInfo:
    """_get_wal_receiver_info raises PostgresConnectionError on DB error."""

    def test_returns_first_row_on_success(self):
        """Returns first row dict when pg_stat_wal_receiver has data."""
        pg = _make_postgres()
        row = {'pid': 1234, 'status': 'streaming', 'slot_name': None,
               'last_msg_receipt_time_msec': 0, 'conninfo': 'host=primary'}
        with patch.object(pg, '_get', return_value=[row]):
            result = pg._get_wal_receiver_info()
        assert result == row

    def test_returns_none_when_empty(self):
        """Returns None (no walreceiver running) when query returns empty list."""
        pg = _make_postgres()
        with patch.object(pg, '_get', return_value=[]):
            result = pg._get_wal_receiver_info()
        assert result is None

    def test_raises_on_connection_error(self):
        """PostgresConnectionError propagates — no safe default returned."""
        pg = _make_postgres()
        with patch.object(pg, '_get', side_effect=PostgresConnectionError("db down")):
            with pytest.raises(PostgresConnectionError):
                pg._get_wal_receiver_info()


class TestGetSessionsRatio:
    """get_sessions_ratio raises PostgresConnectionError on DB error."""

    def test_returns_ratio(self):
        """Returns float ratio of active / max_connections."""
        pg = _make_postgres()
        # First call: active sessions count; second call: max_connections
        active_cur = MagicMock()
        active_cur.fetchone.return_value = (5,)
        max_cur = MagicMock()
        max_cur.fetchone.return_value = ('100',)

        with patch.object(pg, '_exec_query', side_effect=[active_cur, max_cur]):
            result = pg.get_sessions_ratio()
        assert result == pytest.approx(5.0)

    def test_raises_on_connection_error(self):
        """PostgresConnectionError propagates — no 0.0 safe default returned."""
        pg = _make_postgres()
        with patch.object(pg, '_exec_query', side_effect=PostgresConnectionError("db down")):
            with pytest.raises(PostgresConnectionError):
                pg.get_sessions_ratio()


class TestGetWalReceiveLsn:
    """get_wal_receive_lsn raises PostgresConnectionError on DB error."""

    def test_returns_lsn_value(self):
        """Returns LSN integer from pg_last_wal_receive_lsn diff."""
        pg = _make_postgres()
        cur = MagicMock()
        cur.fetchone.return_value = (12345678,)
        with patch.object(pg, '_exec_query', return_value=cur):
            result = pg.get_wal_receive_lsn()
        assert result == 12345678

    def test_raises_on_connection_error(self):
        """PostgresConnectionError propagates — no None returned."""
        pg = _make_postgres()
        with patch.object(pg, '_exec_query', side_effect=PostgresConnectionError("db down")):
            with pytest.raises(PostgresConnectionError):
                pg.get_wal_receive_lsn()

    def test_raises_via_lwaldump_on_connection_error(self):
        """When use_lwaldump=True, PostgresConnectionError from lwaldump propagates."""
        config = _make_config(use_lwaldump=True)
        mock_cmd = MagicMock()
        mock_cmd.list_clusters.return_value = []
        with patch('src.pg.psycopg2.connect') as mock_connect:
            fake_conn = MagicMock()
            fake_conn.cursor.return_value = MagicMock()
            mock_connect.return_value = fake_conn
            with patch.object(Postgres, 'get_role', return_value='replica'), \
                 patch.object(Postgres, '_get_pgdata_path', return_value='/data/pg'):
                pg = Postgres(config, mock_cmd)

        with patch.object(pg, 'lwaldump', side_effect=PostgresConnectionError("db down")):
            with pytest.raises(PostgresConnectionError):
                pg.get_wal_receive_lsn()


# ---------------------------------------------------------------------------
# Tests: PR 2 — get_replication_slots + lwaldump
# ---------------------------------------------------------------------------

class TestGetReplicationSlots:
    """get_replication_slots raises PostgresConnectionError on DB error."""

    def test_returns_slot_list(self):
        """Returns list of slot names on success."""
        pg = _make_postgres()
        with patch.object(pg, '_get', return_value=[{'slot_name': 'slot_a'}, {'slot_name': 'slot_b'}]):
            result = pg.get_replication_slots()
        assert result == ['slot_a', 'slot_b']

    def test_returns_empty_list_when_no_slots(self):
        """Returns empty list when pg_replication_slots has no rows."""
        pg = _make_postgres()
        with patch.object(pg, '_get', return_value=[]):
            result = pg.get_replication_slots()
        assert result == []

    def test_raises_on_connection_error(self):
        """PostgresConnectionError propagates — no None returned."""
        pg = _make_postgres()
        with patch.object(pg, '_get', side_effect=PostgresConnectionError("db down")):
            with pytest.raises(PostgresConnectionError):
                pg.get_replication_slots()

    def test_other_exception_propagates(self):
        """Non-connection errors also propagate to the caller."""
        pg = _make_postgres()
        with patch.object(pg, '_get', side_effect=RuntimeError("unexpected")):
            with pytest.raises(RuntimeError):
                pg.get_replication_slots()


# ---------------------------------------------------------------------------
# Tests: get_replics_info raises PostgresConnectionError on DB error
# ---------------------------------------------------------------------------

class TestGetReplicsInfo:
    """get_replics_info raises PostgresConnectionError on DB error."""

    def test_returns_replica_list(self):
        """Returns list of replica dicts on success."""
        pg = _make_postgres()
        row = {'pid': 1, 'application_name': 'replica1', 'state': 'streaming',
               'sync_state': 'async'}
        with patch.object(pg, '_get', return_value=[row]):
            result = pg.get_replics_info('primary')
        assert result == [row]

    def test_returns_empty_list_when_no_replicas(self):
        """Returns empty list when no replicas connected."""
        pg = _make_postgres()
        with patch.object(pg, '_get', return_value=[]):
            result = pg.get_replics_info('primary')
        assert result == []

    def test_raises_on_connection_error(self):
        """PostgresConnectionError propagates — no [] safe default returned."""
        pg = _make_postgres()
        with patch.object(pg, '_get', side_effect=PostgresConnectionError("db down")):
            with pytest.raises(PostgresConnectionError):
                pg.get_replics_info('primary')


# ---------------------------------------------------------------------------
# Tests: PR 4 — get_replication_state (safe default on DB error)
# ---------------------------------------------------------------------------

class TestGetReplicationState:
    """get_replication_state raises PostgresConnectionError on DB error."""

    def test_returns_async_when_ssn_empty(self):
        """Returns ('async', None) when synchronous_standby_names is empty."""
        pg = _make_postgres()
        cur = MagicMock()
        cur.fetchone.return_value = ('',)
        with patch.object(pg, '_exec_query', return_value=cur):
            result = pg.get_replication_state()
        assert result == ('async', None)

    def test_returns_sync_with_value(self):
        """Returns ('sync', value) when synchronous_standby_names is set."""
        pg = _make_postgres()
        cur = MagicMock()
        cur.fetchone.return_value = ('ANY 1 (replica1)',)
        with patch.object(pg, '_exec_query', return_value=cur):
            result = pg.get_replication_state()
        assert result == ('sync', 'ANY 1 (replica1)')

    def test_raises_on_connection_error(self):
        """PostgresConnectionError propagates — no ('async', None) safe default."""
        pg = _make_postgres()
        with patch.object(pg, '_exec_query', side_effect=PostgresConnectionError("db down")):
            with pytest.raises(PostgresConnectionError):
                pg.get_replication_state()


class TestLwaldump:
    """lwaldump raises PostgresConnectionError on DB error (no decorator)."""

    def test_returns_lsn_integer(self):
        """Returns integer LSN value on success."""
        pg = _make_postgres()
        cur = MagicMock()
        cur.fetchone.return_value = (9876543,)
        with patch.object(pg, '_exec_query', return_value=cur):
            result = pg.lwaldump()
        assert result == 9876543

    def test_raises_on_connection_error(self):
        """PostgresConnectionError propagates when DB is unavailable."""
        pg = _make_postgres()
        with patch.object(pg, '_exec_query', side_effect=PostgresConnectionError("db down")):
            with pytest.raises(PostgresConnectionError):
                pg.lwaldump()


# ---------------------------------------------------------------------------
# Tests: PR 3 — _get_pgdata_path (no @return_none_on_error)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Tests: PR 5 — get_replay_diff + is_replaying_wal
# ---------------------------------------------------------------------------

class TestGetReplayDiff:
    """get_replay_diff raises PostgresConnectionError on DB error."""

    def test_returns_diff_value(self):
        """Returns integer LSN diff on success."""
        pg = _make_postgres()
        cur = MagicMock()
        cur.fetchone.return_value = (42,)
        with patch.object(pg, '_exec_query', return_value=cur):
            result = pg.get_replay_diff()
        assert result == 42

    def test_raises_on_connection_error(self):
        """PostgresConnectionError propagates — no None returned."""
        pg = _make_postgres()
        with patch.object(pg, '_exec_query', side_effect=PostgresConnectionError("db down")):
            with pytest.raises(PostgresConnectionError):
                pg.get_replay_diff()


class TestIsReplayingWal:
    """is_replaying_wal raises PostgresConnectionError on DB error."""

    def test_returns_true_when_replaying(self):
        """Returns True when replay LSN increases between checks."""
        pg = _make_postgres()
        with patch.object(pg, 'get_replay_diff', side_effect=[100, 200]), \
             patch('src.pg.time.sleep'):
            result = pg.is_replaying_wal(1)
        assert result is True

    def test_returns_false_when_not_replaying(self):
        """Returns False when replay LSN does not change."""
        pg = _make_postgres()
        with patch.object(pg, 'get_replay_diff', side_effect=[100, 100]), \
             patch('src.pg.time.sleep'):
            result = pg.is_replaying_wal(1)
        assert result is False

    def test_raises_on_connection_error(self):
        """PostgresConnectionError propagates from get_replay_diff."""
        pg = _make_postgres()
        with patch.object(pg, 'get_replay_diff', side_effect=PostgresConnectionError("db down")), \
             patch('src.pg.time.sleep'):
            with pytest.raises(PostgresConnectionError):
                pg.is_replaying_wal(1)


class TestGetPgdataPath:
    """_get_pgdata_path raises PostgresConnectionError instead of returning None."""

    def test_returns_path_on_success(self):
        """Returns data directory path string on success."""
        pg = _make_postgres()
        cur = MagicMock()
        cur.fetchone.return_value = ('/var/lib/postgresql/data',)
        with patch.object(pg, '_exec_query', return_value=cur):
            result = pg._get_pgdata_path()
        assert result == '/var/lib/postgresql/data'

    def test_raises_on_connection_error(self):
        """PostgresConnectionError propagates when DB is unavailable."""
        pg = _make_postgres()
        with patch.object(pg, '_exec_query', side_effect=PostgresConnectionError("db down")):
            with pytest.raises(PostgresConnectionError):
                pg._get_pgdata_path()

    def test_other_exception_propagates(self):
        """Non-connection exceptions also propagate (no suppression)."""
        pg = _make_postgres()
        with patch.object(pg, '_exec_query', side_effect=RuntimeError("unexpected")):
            with pytest.raises(RuntimeError):
                pg._get_pgdata_path()


class TestReconnect:
    """reconnect() handles PostgresConnectionError from _get_pgdata_path."""

    def test_reconnect_success(self):
        """On success, conn_local is set and pgdata is populated."""
        pg = _make_postgres()
        fake_conn = MagicMock()
        fake_conn.cursor.return_value = MagicMock()

        with patch('src.pg.psycopg2.connect', return_value=fake_conn), \
             patch.object(pg, 'get_role', return_value='primary'), \
             patch.object(pg, '_get_pgdata_path', return_value='/data/pg'):
            pg.reconnect()

        assert pg.conn_local is fake_conn
        assert pg.pgdata == '/data/pg'
        assert pg.terminal_state is True

    def test_reconnect_psycopg2_operational_error(self):
        """psycopg2.OperationalError during connect → conn_local is None."""
        pg = _make_postgres()
        with patch('src.pg.psycopg2.connect', side_effect=psycopg2.OperationalError("refused")):
            pg.reconnect()

        assert pg.conn_local is None

    def test_reconnect_postgres_connection_error_from_pgdata(self):
        """PostgresConnectionError from _get_pgdata_path → conn_local is set to None."""
        pg = _make_postgres()
        fake_conn = MagicMock()
        fake_conn.cursor.return_value = MagicMock()

        with patch('src.pg.psycopg2.connect', return_value=fake_conn), \
             patch.object(pg, 'get_role', return_value='primary'), \
             patch.object(pg, '_get_pgdata_path', side_effect=PostgresConnectionError("show data_directory failed")):
            pg.reconnect()

        assert pg.conn_local is None

    def test_reconnect_postgres_connection_error_does_not_raise(self):
        """PostgresConnectionError from _get_pgdata_path is caught, reconnect() returns normally."""
        pg = _make_postgres()
        fake_conn = MagicMock()
        fake_conn.cursor.return_value = MagicMock()

        with patch('src.pg.psycopg2.connect', return_value=fake_conn), \
             patch.object(pg, 'get_role', return_value='primary'), \
             patch.object(pg, '_get_pgdata_path', side_effect=PostgresConnectionError("show data_directory failed")):
            # Should not raise
            pg.reconnect()


class TestGetState:

    def test_alive_false_when_db_not_running(self):
        pg = _make_postgres()
        with patch.object(pg, 'is_alive_and_in_terminal_state', return_value=(False, True)):
            result = pg.get_state()
        assert result['alive'] is False

    def test_get_state_does_not_raise_on_connection_error_in_wal_receiver(self):
        # get_state() swallows exceptions via except Exception — must not raise
        pg = _make_postgres()
        with patch.object(pg, 'is_alive_and_in_terminal_state', return_value=(True, True)), \
             patch.object(pg, 'get_role', return_value='replica'), \
             patch.object(pg, '_get_pgdata_path', return_value='/data'), \
             patch.object(pg, 'pgpooler', return_value=(True, True)), \
             patch.object(pg, 'get_timeline', return_value=1), \
             patch.object(pg, '_get_wal_receiver_info', side_effect=PostgresConnectionError("db down")):
            result = pg.get_state()  # must not raise
        assert result['alive'] is False

    def test_get_state_does_not_raise_on_connection_error_in_replics_info(self):
        pg = _make_postgres()
        with patch.object(pg, 'is_alive_and_in_terminal_state', return_value=(True, True)), \
             patch.object(pg, 'get_role', return_value='primary'), \
             patch.object(pg, '_get_pgdata_path', return_value='/data'), \
             patch.object(pg, 'pgpooler', return_value=(True, True)), \
             patch.object(pg, 'get_timeline', return_value=1), \
             patch.object(pg, '_get_wal_receiver_info', return_value=None), \
             patch.object(pg, 'get_replics_info', side_effect=PostgresConnectionError("db down")):
            result = pg.get_state()  # must not raise
        assert result['alive'] is False


class TestCheckpoint:

    def test_checkpoint_succeeds(self):
        pg = _make_postgres()
        with patch.object(pg, '_exec_without_result', return_value=True):
            assert pg.checkpoint() is True

    def test_checkpoint_raises_on_connection_error(self):
        pg = _make_postgres()
        with patch.object(pg, '_exec_without_result', side_effect=PostgresConnectionError("db down")):
            with pytest.raises(PostgresConnectionError):
                pg.checkpoint()

    def test_checkpoint_with_custom_query(self):
        pg = _make_postgres()
        with patch.object(pg, '_exec_without_result', return_value=True) as mock_exec:
            pg.checkpoint(query='CHECKPOINT;')
        mock_exec.assert_called_once_with('CHECKPOINT;')


class TestCheckWalreceiver:

    def test_returns_true_when_streaming(self):
        pg = _make_postgres()
        cur = MagicMock()
        cur.fetchall.return_value = [(1234,)]
        with patch.object(pg, '_exec_query', return_value=cur):
            assert pg.check_walreceiver() is True

    def test_returns_false_when_not_streaming(self):
        pg = _make_postgres()
        cur = MagicMock()
        cur.fetchall.return_value = []
        with patch.object(pg, '_exec_query', return_value=cur):
            assert pg.check_walreceiver() is False

    def test_raises_on_connection_error(self):
        pg = _make_postgres()
        with patch.object(pg, '_exec_query', side_effect=PostgresConnectionError("db down")):
            with pytest.raises(PostgresConnectionError):
                pg.check_walreceiver()


class TestTerminateBackend:

    def test_terminate_backend_succeeds(self):
        pg = _make_postgres()
        with patch.object(pg, '_exec_without_result', return_value=True):
            assert pg.terminate_backend(1234) is None

    def test_terminate_backend_catches_connection_error(self):
        pg = _make_postgres()
        with patch.object(pg, '_exec_without_result', side_effect=PostgresConnectionError("db down")):
            pg.terminate_backend(1234)  # must not raise

    def test_terminate_backend_logs_warning_on_connection_error(self):
        pg = _make_postgres()
        with patch.object(pg, '_exec_without_result', side_effect=PostgresConnectionError("db down")), \
             patch('src.pg.logging.warning') as mock_warning:
            pg.terminate_backend(1234)
        mock_warning.assert_called_once()
        assert '1234' in str(mock_warning.call_args[0])
