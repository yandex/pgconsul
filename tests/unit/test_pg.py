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
import selectors
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


class TestGetWalFlushLsn:
    """The failover vote LSN comes directly from PostgreSQL."""

    def test_safe_mode_reads_end_of_local_wal_with_lwaldump(self):
        pg = _make_postgres()
        pg.config.use_lwaldump = True
        cur = MagicMock()
        cur.fetchone.return_value = (5, 12345678)

        with patch.object(pg, '_exec_query', return_value=cur) as execute:
            assert pg.get_wal_flush_lsn() == 12345678

        query = execute.call_args.args[0]
        assert 'FROM lwaldump_with_timeline()' in query
        assert 'pg_last_wal_receive_lsn()' not in query
        assert 'pg_last_wal_replay_lsn()' not in query

    def test_safe_mode_does_not_fallback_when_lwaldump_fails(self):
        pg = _make_postgres()
        pg.config.use_lwaldump = True

        with patch.object(
            pg,
            '_exec_query',
            side_effect=PostgresConnectionError('lwaldump failed'),
        ) as execute:
            with pytest.raises(PostgresConnectionError):
                pg.get_wal_flush_lsn()

        execute.assert_called_once_with(
            "SELECT timeline, pg_wal_lsn_diff(flush_lsn, '0/00000000')::bigint "
            "FROM lwaldump_with_timeline()"
        )

    def test_returns_lsn_value(self):
        """Returns LSN integer from pg_last_wal_receive_lsn diff."""
        pg = _make_postgres()
        cur = MagicMock()
        cur.fetchone.return_value = (12345678,)
        with patch.object(pg, '_exec_query', return_value=cur):
            result = pg.get_wal_flush_lsn()
        assert result == 12345678

    def test_reads_both_received_and_replayed_positions(self):
        pg = _make_postgres()
        pg.config.use_lwaldump = False
        cur = MagicMock()
        cur.fetchone.return_value = (12345678,)

        with patch.object(pg, '_exec_query', return_value=cur) as execute:
            pg.get_wal_flush_lsn()

        query = execute.call_args.args[0]
        assert 'pg_last_wal_receive_lsn()' in query
        assert 'pg_last_wal_replay_lsn()' in query

    def test_raises_on_connection_error(self):
        """PostgresConnectionError propagates — no None returned."""
        pg = _make_postgres()
        with patch.object(pg, '_exec_query', side_effect=PostgresConnectionError("db down")):
            with pytest.raises(PostgresConnectionError):
                pg.get_wal_flush_lsn()


class TestTimeline:
    """Timeline comes from PostgreSQL while it is available."""

    def test_reads_current_wal_timeline_from_wal_filename(self):
        pg = _make_postgres()
        cur = MagicMock()
        cur.fetchone.return_value = ('000000020000000000000003',)

        with patch.object(pg, '_exec_query', return_value=cur) as execute:
            assert pg.get_current_wal_timeline() == 2

        assert 'pg_walfile_name(pg_current_wal_lsn())' in execute.call_args.args[0]

    def test_rejects_invalid_current_wal_filename(self):
        pg = _make_postgres()
        cur = MagicMock()
        cur.fetchone.return_value = ('not-a-wal-file',)

        with patch.object(pg, '_exec_query', return_value=cur):
            with pytest.raises(PostgresQueryError):
                pg.get_current_wal_timeline()

    def test_reads_live_timeline_with_identify_system(self):
        pg = _make_postgres()
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchone.return_value = ('system-id', 2, '0/3000000', None)

        with patch('src.pg.psycopg2.connect', return_value=conn) as connect:
            assert pg.get_timeline() == 2

        connect.assert_called_once_with(
            pg.config.conn_string,
            connection_factory=pg._REPLICATION_CONNECTION_FACTORY,
        )
        cur.execute.assert_called_once_with('IDENTIFY_SYSTEM')
        conn.close.assert_called_once_with()

    def test_uses_control_data_when_postgres_is_down(self):
        pg = _make_postgres()
        with patch.object(pg, 'get_live_timeline', side_effect=PostgresConnectionError('down')), \
             patch.object(pg, '_get_data_from_control_file', return_value=1) as control:
            assert pg.get_timeline() == 1
        control.assert_called_once_with(
            'Latest checkpoint.s TimeLineID', preproc=int, log=False,
        )

    def test_live_timeline_connection_error_does_not_fallback_itself(self):
        pg = _make_postgres()

        with patch('src.pg.psycopg2.connect', side_effect=psycopg2.OperationalError('down')), \
             patch.object(pg, '_get_data_from_control_file') as control:
            with pytest.raises(PostgresConnectionError):
                pg.get_live_timeline()

        control.assert_not_called()

    def test_rejects_missing_timeline(self):
        pg = _make_postgres()
        cur = MagicMock()
        cur.fetchone.return_value = None

        with patch('src.pg.psycopg2.connect', return_value=MagicMock(cursor=MagicMock(return_value=cur))):
            with pytest.raises(PostgresQueryError):
                pg.get_live_timeline()


class TestDisableWalReceiver:
    def test_connection_error_propagates(self):
        pg = _make_postgres()

        with patch.object(
            pg,
            '_exec_query',
            side_effect=PostgresConnectionError('db down'),
        ):
            with pytest.raises(PostgresConnectionError):
                pg.disable_wal_receiver(5.0)

    def test_does_not_vote_ready_when_primary_conninfo_cannot_be_cleared(self):
        pg = _make_postgres()
        show = MagicMock()
        show.fetchone.return_value = ('host=old-primary',)

        with patch.object(pg, '_exec_query', return_value=show), \
             patch.object(pg, '_alter_system_set_param', return_value=False), \
             patch.object(pg, 'reload', return_value=True), \
             patch('src.pg.helpers.await_for') as await_for:
            assert pg.disable_wal_receiver(5.0) is False

        await_for.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: PR 2 — get_replication_slots
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

    def test_collects_flush_position_and_flush_lag(self):
        """Switchover side eligibility must not use replay lag."""
        pg = _make_postgres()
        with patch.object(pg, '_get', return_value=[]) as get:
            pg.get_replics_info('primary')
        query = get.call_args.args[0]
        assert 'flush_location_diff' in query
        assert 'flush_lag_msec' in query
        assert 'flush_lsn' in query

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


class TestDurabilityBarrierLsn:

    def test_returns_current_wal_flush_lsn_as_integer(self):
        pg = _make_postgres()
        cur = MagicMock()
        cur.fetchone.return_value = (123456,)

        with patch.object(pg, '_exec_query', return_value=cur):
            assert pg.get_current_wal_flush_lsn() == 123456

    def test_rejects_missing_current_wal_flush_lsn(self):
        pg = _make_postgres()
        cur = MagicMock()
        cur.fetchone.return_value = (None,)

        with patch.object(pg, '_exec_query', return_value=cur):
            with pytest.raises(PostgresQueryError):
                pg.get_current_wal_flush_lsn()

    def test_returns_streaming_replica_flush_lsns(self):
        pg = _make_postgres()
        rows = [
            {'application_name': 'host1', 'flush_lsn': 100},
            {'application_name': 'host2', 'flush_lsn': None},
        ]

        with patch.object(pg, '_get', return_value=rows):
            assert pg.get_replica_flush_lsns() == {'host1': 100}


# ---------------------------------------------------------------------------
# Tests: PR 3 — _get_pgdata_path (no @return_none_on_error)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Tests: PR 5 — get_replay_diff + is_replaying_wal
# ---------------------------------------------------------------------------


def test_startup_progress_combines_controldata_and_process_progress():
    pg = _make_postgres()
    pg._get_data_from_control_file = MagicMock(
        side_effect=['in archive recovery', '0/100', '0/80', '0/200'],
    )
    pg._get_startup_process_progress = MagicMock(
        return_value=('startup', (('000000010000000000000001', 42),), (10, 2, 8)),
    )

    assert pg.get_startup_progress_signature() == (
        'in archive recovery', '0/100', '0/80', '0/200',
        'startup', (('000000010000000000000001', 42),), (10, 2, 8),
    )


def test_start_postgresql_async_delegates_to_command_manager():
    command_manager = MagicMock()
    pg = _make_postgres(mock_cmd=command_manager)
    pg.pgdata = '/data/pg'

    assert pg.start_postgresql_async(300) is command_manager.start_postgresql_async.return_value
    command_manager.start_postgresql_async.assert_called_once_with(300, '/data/pg')

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


class TestGetReceiveDiff:
    def test_returns_receive_lsn_diff(self):
        pg = _make_postgres()
        cur = MagicMock()
        cur.fetchone.return_value = (42,)

        with patch.object(pg, '_exec_query', return_value=cur) as query:
            assert pg.get_receive_diff() == 42

        assert 'pg_last_wal_receive_lsn()' in query.call_args.args[0]
        assert 'pg_last_wal_replay_lsn()' not in query.call_args.args[0]


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


class TestHostHealthCheck:
    def test_async_query_reports_reachable_host(self):
        pg = _make_postgres()
        conn = MagicMock()
        conn.poll.side_effect = [
            psycopg2.extensions.POLL_WRITE,
            psycopg2.extensions.POLL_OK,
            psycopg2.extensions.POLL_READ,
            psycopg2.extensions.POLL_OK,
        ]
        conn.cursor.return_value.fetchone.return_value = (42,)
        selector = MagicMock()
        selector.__enter__.return_value = selector
        selector.select.return_value = [(MagicMock(), selectors.EVENT_READ)]

        with patch('src.pg.psycopg2.connect', return_value=conn), \
             patch('src.pg.selectors.DefaultSelector', return_value=selector):
            assert pg.is_host_unreachable('primary') is False

        conn.cursor.return_value.execute.assert_called_once_with('SELECT 42')
        conn.close.assert_called_once_with()

    def test_async_query_timeout_marks_host_unreachable(self):
        pg = _make_postgres()
        conn = MagicMock()
        conn.poll.return_value = psycopg2.extensions.POLL_READ
        selector = MagicMock()
        selector.__enter__.return_value = selector
        selector.select.return_value = []

        with patch('src.pg.psycopg2.connect', return_value=conn) as connect, \
             patch('src.pg.selectors.DefaultSelector', return_value=selector):
            assert pg.is_host_unreachable('primary') is True

        connect.assert_called_once_with(
            'host=primary  target_session_attrs=primary',
            async_=True,
        )
        selector.register.assert_called_once_with(conn.fileno(), selectors.EVENT_READ)
        selector.select.assert_called_once()
        conn.cursor.assert_not_called()
        conn.close.assert_called_once_with()


class TestGetState:

    def test_alive_false_when_db_not_running(self):
        pg = _make_postgres()
        with patch.object(pg, 'is_alive_and_in_terminal_state', return_value=(False, True)):
            result = pg.get_state()
        assert result['alive'] is False
        assert result['role'] is None

    def test_alive_false_when_db_in_nonterminal_state(self):
        # DB is starting/stopping — running=True, alive=False, _collect_db_state not called
        pg = _make_postgres()
        with patch.object(pg, 'is_alive_and_in_terminal_state', return_value=(False, False)):
            result = pg.get_state()
        assert result['alive'] is False
        assert result['running'] is True

    def test_get_state_returns_full_state_when_alive(self):
        pg = _make_postgres()
        with patch.object(pg, 'is_alive_and_in_terminal_state', return_value=(True, True)), \
             patch.object(pg, '_collect_db_state') as mock_collect, \
             patch.object(pg, 'save_state'):
            # _collect_db_state sets alive=True to simulate a healthy DB
            def _fill(data):
                data['alive'] = True
                data['role'] = 'primary'
            mock_collect.side_effect = _fill
            result = pg.get_state()
        assert result['alive'] is True
        mock_collect.assert_called_once()

    def test_get_state_raises_on_connection_error_in_collect(self):
        # ADR-0001: PostgresConnectionError from _collect_db_state() propagates;
        # get_state() must not swallow it.
        pg = _make_postgres()
        with patch.object(pg, 'is_alive_and_in_terminal_state', return_value=(True, True)), \
             patch.object(pg, '_collect_db_state', side_effect=PostgresConnectionError("db down")):
            with pytest.raises(PostgresConnectionError):
                pg.get_state()

    def test_collect_db_state_raises_on_wal_receiver_error(self):
        # _collect_db_state() propagates PostgresConnectionError (ADR-0001).
        pg = _make_postgres()
        data: dict = {'alive': True}
        with patch.object(pg, 'get_role', return_value='replica'), \
             patch.object(pg, '_get_pgdata_path', return_value='/data'), \
             patch.object(pg, 'pgpooler', return_value=(True, True)), \
             patch.object(pg, 'get_timeline', return_value=1), \
             patch.object(pg, '_get_wal_receiver_info', side_effect=PostgresConnectionError("db down")):
            with pytest.raises(PostgresConnectionError):
                pg._collect_db_state(data)

    def test_collect_db_state_raises_on_replics_info_error(self):
        # _collect_db_state() propagates PostgresConnectionError (ADR-0001).
        pg = _make_postgres()
        data: dict = {'alive': True}
        with patch.object(pg, 'get_role', return_value='primary'), \
             patch.object(pg, '_get_pgdata_path', return_value='/data'), \
             patch.object(pg, 'pgpooler', return_value=(True, True)), \
             patch.object(pg, 'get_timeline', return_value=1), \
             patch.object(pg, '_get_wal_receiver_info', return_value=None), \
             patch.object(pg, 'get_replics_info', side_effect=PostgresConnectionError("db down")):
            with pytest.raises(PostgresConnectionError):
                pg._collect_db_state(data)


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

    def test_checkpoint_translates_postgres_query_error(self):
        pg = _make_postgres()
        with patch.object(
            pg,
            '_exec_without_result',
            side_effect=psycopg2.DatabaseError('recovery'),
        ):
            with pytest.raises(PostgresQueryError):
                pg.checkpoint()

    def test_checkpoint_with_custom_query(self):
        pg = _make_postgres()
        with patch.object(pg, '_exec_without_result', return_value=True) as mock_exec:
            pg.checkpoint(query='CHECKPOINT;')
        mock_exec.assert_called_once_with('CHECKPOINT;')

    def test_switch_wal_succeeds(self):
        pg = _make_postgres()
        with patch.object(pg, '_exec_without_result', return_value=True) as mock_exec:
            assert pg.switch_wal() is True
        mock_exec.assert_called_once_with('SELECT pg_switch_wal()')

    def test_switch_wal_translates_query_error(self):
        pg = _make_postgres()
        with patch.object(
            pg,
            '_exec_without_result',
            side_effect=psycopg2.DatabaseError('read-only'),
        ):
            with pytest.raises(PostgresQueryError):
                pg.switch_wal()


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


class TestReInit:
    """re_init() — re_init_db logic moved to pg.py (step 12b)."""

    def test_returns_true_when_db_alive(self):
        pg = _make_postgres()
        with patch.object(pg, 'is_alive', return_value=True):
            assert pg.re_init() is True

    def test_restores_role_pgdata_from_cache(self):
        pg = _make_postgres()
        prev_state = {'role': 'replica', 'pgdata': '/data/pg'}
        with patch.object(pg, 'is_alive', return_value=False), \
             patch.object(pg, 'get_prev_state', return_value=prev_state), \
             patch.object(pg, 'reconnect') as mock_reconnect:
            assert pg.re_init() is False
        assert pg.role == 'replica'
        assert pg.pgdata == '/data/pg'
        mock_reconnect.assert_called_once()

    def test_empty_cache_does_not_crash_calls_reconnect(self):
        """Empty cache + dead DB → reconnect, no KeyError (MDB-41951)."""
        pg = _make_postgres()
        with patch.object(pg, 'is_alive', return_value=False), \
             patch.object(pg, 'get_prev_state', return_value={}), \
             patch.object(pg, 'reconnect') as mock_reconnect:
            assert pg.re_init() is False
        mock_reconnect.assert_called_once()

    def test_raises_key_error_when_cache_incomplete(self):
        """Cache present but missing 'pgdata' → KeyError (genuinely corrupt)."""
        pg = _make_postgres()
        with patch.object(pg, 'is_alive', return_value=False), \
             patch.object(pg, 'get_prev_state', return_value={'role': 'replica'}), \
             patch.object(pg, 'reconnect') as mock_reconnect:
            with pytest.raises(KeyError):
                pg.re_init()
        mock_reconnect.assert_not_called()

    def test_propagates_connection_error_from_reconnect(self):
        pg = _make_postgres()
        prev_state = {'role': 'primary', 'pgdata': '/data/pg'}
        with patch.object(pg, 'is_alive', return_value=False), \
             patch.object(pg, 'get_prev_state', return_value=prev_state), \
             patch.object(pg, 'reconnect', side_effect=PostgresConnectionError('no db')):
            with pytest.raises(PostgresConnectionError):
                pg.re_init()


class TestAlterSystemStopped:
    def test_resume_restoring_removes_vote_fence(self, tmp_path):
        pg = _make_postgres()
        pg.pgdata = str(tmp_path)
        auto_conf = tmp_path / 'postgresql.auto.conf'
        auto_conf.write_text(
            "restore_command = '/bin/false'\nprimary_conninfo = 'host=primary'\n"
        )

        assert pg.resume_restoring_wal_stopped() is True
        assert auto_conf.read_text() == (
            '# Do not edit this file manually!\n'
            '# It will be overwritten by the ALTER SYSTEM command.\n'
            "primary_conninfo = 'host=primary'\n"
        )

    def test_enable_wal_receiver_removes_persistent_vote_fence(self, tmp_path):
        pg = _make_postgres()
        pg.pgdata = str(tmp_path)
        auto_conf = tmp_path / 'postgresql.auto.conf'
        auto_conf.write_text(
            "primary_conninfo = ''\nrestore_command = 'cp /archive/%f %p'\n"
        )

        assert pg.enable_wal_receiver_stopped() is True
        assert auto_conf.read_text() == (
            '# Do not edit this file manually!\n'
            '# It will be overwritten by the ALTER SYSTEM command.\n'
            "restore_command = 'cp /archive/%f %p'\n"
        )
