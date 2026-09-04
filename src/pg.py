"""
Pg wrapper module. Postgres class defined here.
"""
# encoding: utf-8

import contextlib
from dataclasses import dataclass
import json
import logging
from functools import partial
import os
import selectors
import socket
import struct
import time
from typing import Callable

import psycopg2
from psycopg2.extras import PhysicalReplicationConnection
from psycopg2.sql import SQL, Identifier, Literal

from . import helpers
from .command_manager import CommandManager
from .exceptions import PostgresConnectionError, PostgresQueryError
from .types import ReplicaInfos
from configparser import RawConfigParser

DEC2INT_TYPE = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values, 'DEC2INT', lambda value, curs: int(value) if value is not None else None
)

TRANSIENT_ERRORS = {
    'FATAL:  the database system is starting up',
    'FATAL:  the database system is shutting down',
    'FATAL:  the database system is not yet accepting connections',
    'DETAIL:  Consistent recovery state has not been yet reached',
}

psycopg2.extensions.register_type(DEC2INT_TYPE)


def _get_names(cur):
    return [r[0].lower() for r in cur.description]


def _plain_format(cur):
    names = _get_names(cur)
    for row in cur.fetchall():
        yield dict(zip(names, tuple(row)))


@dataclass
class PostgresConfig:
    conn_string: str
    working_dir: str
    recovery_filepath: str
    use_replication_slots: bool
    standalone_pooler: bool
    pooler_conn_timeout: float
    pooler_addr: str
    pooler_port: int
    postgres_timeout: float
    iteration_timeout: float
    append_primary_conn_string: str = ''
    wals_to_upload: int = 20
    use_lwaldump: bool = False
    wal_barrier_timeout: float = 60.0

    @property
    def db_state_path(self):
        return '%s/.pgconsul_db_state.cache' % self.working_dir


class Postgres(object):
    """
    Postgres class
    """

    DISABLED_ARCHIVE_COMMAND = '/bin/false'
    DISABLED_RESTORE_COMMAND = '/bin/false'
    _REPLICATION_CONNECTION_FACTORY = PhysicalReplicationConnection

    def __init__(self, config: PostgresConfig, cmd_manager: CommandManager):
        self.config = config
        self._cmd_manager = cmd_manager
        self.conn_local: psycopg2.extensions.connection | None = None
        self._wals_to_upload = self.config.wals_to_upload
        self.role: str | None = None
        self.pgdata = ''
        self._wal_barrier_conn = None
        self._wal_barrier_cursor = None
        self._wal_barrier_operation_id: str | None = None
        self._wal_barrier_query_started = False
        self._wal_barrier_started_at: float | None = None
        # pg is either running or stopped, not starting or stopping
        self.terminal_state: bool = True
        self._offline_detect_pgdata()
        self.reconnect()

    def _create_cursor(self):
        if self.conn_local:
            try:
                cursor = self.conn_local.cursor()
                cursor.execute('SELECT 1;')
                return cursor
            except psycopg2.Error:
                logging.debug('Error creating cursor, reconnecting', exc_info=True)
                self.reconnect()
        else:
            # No active connection — reconnect; reconnect() raises
            # PostgresConnectionError if it cannot restore the connection.
            self.reconnect()
        if self.conn_local is None:
            raise PostgresConnectionError('Local conn is dead')
        return self.conn_local.cursor()

    def _exec_query(self, query, **kwargs):
        cur = self._create_cursor()
        try:
            cur.execute(query, kwargs)
        except psycopg2.OperationalError as exc:
            self.close()
            raise PostgresConnectionError(str(exc)) from exc
        return cur

    def _get(self, query, **kwargs):
        with contextlib.closing(self._exec_query(query, **kwargs)) as cur:
            records = list(_plain_format(cur))
            return records

    def _exec_without_result(self, query):
        """Execute a query, ignoring the result.

        Raises:
            PostgresConnectionError: if the DB connection is lost.
        """
        self._exec_query(query)
        return True

    def _get_data_from_control_file(self, parameter, preproc=None, log=True):
        """
        Run pg_controldata and grep it's output
        """
        return self._cmd_manager.get_control_parameter(self.pgdata, parameter, preproc, log)

    def get_live_timeline(self) -> int:
        """Read the current timeline through PostgreSQL's replication protocol."""
        conn = None
        try:
            conn = psycopg2.connect(
                self.config.conn_string,
                connection_factory=self._REPLICATION_CONNECTION_FACTORY,
            )
            cur = conn.cursor()
            cur.execute('IDENTIFY_SYSTEM')
            row = cur.fetchone()
        except psycopg2.OperationalError as exc:
            raise PostgresConnectionError(str(exc)) from exc
        except psycopg2.Error as exc:
            raise PostgresQueryError('Could not identify current timeline') from exc
        finally:
            if conn is not None:
                conn.close()
        if row is None or row[1] is None:
            raise PostgresQueryError('Could not identify current timeline')
        return int(row[1])

    def get_timeline(self) -> int:
        """Read the live timeline, falling back to checkpoint control data."""
        try:
            return self.get_live_timeline()
        except (PostgresConnectionError, PostgresQueryError):
            return self._get_data_from_control_file(
                'Latest checkpoint.s TimeLineID', preproc=int, log=False,
            )

    def get_current_wal_timeline(self):
        """Read the current insertion timeline from a running primary."""
        try:
            row = self._exec_query(
                'SELECT pg_walfile_name(pg_current_wal_lsn())'
            ).fetchone()
        except psycopg2.Error as exc:
            raise PostgresQueryError('Could not read current WAL timeline') from exc
        if row is None or row[0] is None or not isinstance(row[0], str) or len(row[0]) != 24:
            raise PostgresQueryError('Could not read current WAL timeline')
        try:
            int(row[0], 16)
            return int(row[0][:8], 16)
        except ValueError:
            raise PostgresQueryError('Could not parse current WAL timeline') from None

    def get_data_safety_settings(self) -> dict[str, str]:
        """Read PostgreSQL settings that underpin durable commit semantics."""
        row = self._exec_query(
            "SELECT current_setting('fsync'), "
            "current_setting('synchronous_commit')"
        ).fetchone()
        if row is None or row[0] is None or row[1] is None:
            raise PostgresQueryError('Could not read data-safety settings')
        return {'fsync': str(row[0]), 'synchronous_commit': str(row[1])}

    def get_database_cluster_state(self):
        return self._get_data_from_control_file('Database cluster state')

    def get_startup_progress_signature(self) -> tuple:
        """Return offline recovery progress from control data and startup /proc."""
        control = tuple(
            self._get_data_from_control_file(parameter, log=False)
            for parameter in (
                'Database cluster state',
                'Latest checkpoint location',
                "Latest checkpoint's REDO location",
                'Minimum recovery ending location',
            )
        )
        return control + self._get_startup_process_progress()

    def _get_startup_process_progress(self) -> tuple:
        """Read the startup process WAL descriptors and I/O counters on Linux."""
        try:
            with open(os.path.join(self.pgdata, 'postmaster.pid')) as pid_file:
                postmaster_pid = int(pid_file.readline().strip())
            children_path = f'/proc/{postmaster_pid}/task/{postmaster_pid}/children'
            with open(children_path) as children_file:
                child_pids = children_file.read().split()
            for child_pid in child_pids:
                with open(f'/proc/{child_pid}/cmdline', 'rb') as cmdline_file:
                    cmdline = cmdline_file.read().replace(b'\x00', b' ').decode(
                        'utf-8', errors='replace',
                    )
                if 'startup' not in cmdline:
                    continue
                descriptors = []
                fd_directory = f'/proc/{child_pid}/fd'
                for fd_name in os.listdir(fd_directory):
                    try:
                        target = os.readlink(os.path.join(fd_directory, fd_name))
                        if '/pg_wal/' not in target and '/pg_xlog/' not in target:
                            continue
                        position = None
                        with open(f'/proc/{child_pid}/fdinfo/{fd_name}') as fdinfo:
                            for line in fdinfo:
                                if line.startswith('pos:'):
                                    position = int(line.split(':', 1)[1].strip())
                                    break
                        descriptors.append((os.path.basename(target), position))
                    except (FileNotFoundError, OSError, ValueError):
                        continue
                io_values = []
                try:
                    with open(f'/proc/{child_pid}/io') as io_file:
                        io = dict(
                            line.rstrip().split(': ', 1)
                            for line in io_file
                            if ': ' in line
                        )
                    io_values = [
                        int(io.get(name, 0))
                        for name in ('rchar', 'syscr', 'read_bytes')
                    ]
                except (FileNotFoundError, OSError, ValueError):
                    pass
                return ('startup', tuple(sorted(descriptors)), tuple(io_values))
        except (FileNotFoundError, OSError, ValueError):
            pass
        return ('startup', None, None)

    def get_data_page_checksum_version(self):
        return self._get_data_from_control_file('Data page checksum version', preproc=int)

    def get_wal_log_hints_settings(self):
        return self._get_data_from_control_file('wal_log_hints setting')

    def get_wal_segment_size(self):
        return self._get_data_from_control_file('Bytes per WAL segment', preproc=int)

    def _local_conn_string_get_port(self):
        for param in self.config.conn_string.split():
            key, value = param.strip().split('=')
            if key == 'port':
                port = value
                break
        else:
            port = '5432'
        return port

    def _offline_detect_pgdata(self):
        """
        Try to find pgdata and version parameter from list_clusters command by port
        """
        try:
            state: dict[str, object] = {}
            need_port = self._local_conn_string_get_port()
            rows = self._cmd_manager.list_clusters()
            logging.debug(rows)
            for row in rows:
                if not row:
                    continue
                version, _, port, pgstate, _, pgdata, _ = row.split()
                if port != need_port:
                    continue
                if state:  # not empty
                    logging.error('Found more than one cluster on %s port', need_port)
                    return
                self.role = state['role'] = 'replica' if 'recovery' in pgstate else 'primary'
                self.pgdata = state['pgdata'] = pgdata
        except Exception:
            logging.exception('Error getting database state')

    def get_replication_slots(self) -> list[str]:
        """Get names of all replication slots.

        Raises:
            PostgresConnectionError: if the DB connection is lost.
        """
        rows = self._get('SELECT slot_name FROM pg_replication_slots')
        return [r['slot_name'] for r in rows]

    def _create_replication_slot(self, slot_name):
        logging.debug('ACTION. Creating slot %s.', slot_name)
        query = f"SELECT pg_create_physical_replication_slot('{slot_name}', true)"
        return self._exec_without_result(query)

    def _drop_replication_slot(self, slot_name):
        logging.debug('ACTION. Dropping slot %s.', slot_name)
        query = f"SELECT pg_drop_replication_slot('{slot_name}')"
        return self._exec_without_result(query)

    def reconnect(self):
        """
        Reestablish connection with local postgresql
        """
        self.close()
        logging.debug('Trying to reconnect to postgres')
        try:
            self.conn_local = psycopg2.connect(self.config.conn_string)
            self.conn_local.autocommit = True
            self.role = self.get_role()
            self.pgdata = self._get_pgdata_path()
            self.terminal_state = True
        except psycopg2.OperationalError as err:
            logging.exception('Could not connect to "%s".', self.config.conn_string)
            self.conn_local = None
            if any(e in str(err) for e in TRANSIENT_ERRORS):
                self.terminal_state = False
            else:
                self.terminal_state = True
        except PostgresConnectionError:
            # _get_pgdata_path failed after connection was established
            logging.exception('Could not get pgdata path after reconnect to "%s".', self.config.conn_string)
            self.conn_local = None

    def close(self):
        """
        Closes current connection in any state
        """
        logging.debug('Closing connection to PG')
        if self.conn_local:
            try:
                self.conn_local.close()
            except psycopg2.OperationalError as err:
                logging.warning('failed to close old connection: %s', err)
        self.conn_local = None

    def _collect_db_state(self, data: dict[str, object]) -> None:
        """Collect detailed DB state fields into data dict.

        Called only when the liveness probe confirms DB is alive.
        Raises PostgresConnectionError on connection loss — propagates to
        run_iteration() (ADR-0001 / ADR-0002 §1).
        """
        data['role'] = self.role = self.get_role()
        data['pgdata'] = self.pgdata = self._get_pgdata_path()
        data['opened'] = self.pgpooler('status')[1]
        data['timeline'] = self.get_timeline()
        data['wal_receiver'] = self._get_wal_receiver_info()

        if data['role'] == 'primary':
            data['replics_info'] = self.get_replics_info('primary')
            data['replication_state'] = self.get_replication_state()
            data['sessions_ratio'] = self.get_sessions_ratio()
        elif data['role'] == 'replica':
            data['primary_fqdn'] = self.get_primary_fqdn()
            data['replics_info'] = self.get_replics_info('replica')

        #
        # Re-check liveness: DB may die while we were collecting state.
        # It can lead to unpredictable results if we proceed with stale data.
        #
        data['alive'] = self.is_alive()

    def get_state(self):
        """Get current database state.

        Uses is_alive_and_in_terminal_state() as a liveness probe (allowed to
        swallow exceptions per ADR-0001, like reconnect()). If the DB is alive,
        delegates to _collect_db_state() which raises PostgresConnectionError
        on connection loss — propagates to run_iteration() (ADR-0002 §1).
        """
        data: dict[str, object] = {'alive': False}
        is_db_alive, terminal_state = self.is_alive_and_in_terminal_state()
        if terminal_state:
            data['running'] = is_db_alive
            data['alive'] = is_db_alive
        else:
            data['running'] = True
            data['alive'] = False

        if data['alive']:
            self._collect_db_state(data)

        if not data['alive']:
            logging.error('PostgreSQL is dead')
            data['role'] = None

        if data['alive']:
            self.save_state(data)

        return data

    def save_state(self, data: dict):
        try:
            with open(self.config.db_state_path, 'w') as fh:
                fh.write(json.dumps(data))
        except IOError:
            logging.warning('Could not write db state cache file. Skipping it.')

    def get_prev_state(self):
        try:
            with open(self.config.db_state_path, 'r') as fh:
                return json.loads(fh.read())
        except IOError:
            logging.warning('Could not read db state cache file. Returning stub.')
            return {}
        except json.JSONDecodeError:
            logging.warning('Invalid db state cache file content. Returning stub.')
            return {}

    def re_init(self) -> bool:
        """Reinit DB connection: restore role/pgdata from cache, reconnect.

        Returns True if DB is already alive.
        Empty cache → skip restoration, reconnect (MDB-41951: KeyError here
        caused infinite restart loop). Incomplete cache → KeyError (corrupt).
        Raises PostgresConnectionError if reconnect fails (ADR-0001).
        """
        if self.is_alive():
            return True
        logging.error(
            'Could not get data from PostgreSQL. Seems, '
            'that it is dead. Getting last role from cached '
            'file. And trying to reconnect.'
        )
        prev_state = self.get_prev_state()
        if prev_state:
            self.role = prev_state['role']
            self.pgdata = prev_state['pgdata']
        else:
            logging.warning('DB state cache empty. Skipping role/pgdata restore.')
        self.reconnect()
        return False

    def is_alive(self):
        return self.is_alive_and_in_terminal_state()[0]

    def is_alive_and_in_terminal_state(self):
        """
        Check that postgresql is alive.
        Returns (is_alive, is_terminal_state) where is_terminal_state=False means
        PostgreSQL is starting up or shutting down (non-terminal / transient state).
        """
        try:
            # In order to check that postgresql is really alive
            # we need to drop current connection and establish a new one
            self.reconnect()
            res = self._exec_query('SELECT 42;').fetchone()
            return len(res) > 0, True
        except (PostgresConnectionError, psycopg2.Error):
            # Liveness probe (ADR-0001): catch only DB errors, not code bugs.
            logging.debug('Error checking alive/running state', exc_info=True)
            return False, self.terminal_state

    def get_role(self) -> str:
        """
        Get role of local postgresql (replica or primary).
        Raises PostgresConnectionError if the database is unavailable.
        """
        res = self._exec_query('SELECT pg_is_in_recovery();')
        if res.fetchone()[0]:
            return 'replica'
        else:
            return 'primary'

    def _get_pgdata_path(self):
        """
        Get local pg_data
        """
        res = self._exec_query('SHOW data_directory;').fetchone()
        return res[0]

    def get_replics_info(self, role) -> ReplicaInfos:
        """Get replicas from pg_stat_replication.

        Raises:
            PostgresConnectionError: if the DB connection is lost.
        """
        current_lsn = {'primary': 'pg_current_wal_lsn()', 'replica': 'pg_last_wal_replay_lsn()'}
        wal_func = {
            'current_lsn': current_lsn[role],
            'diff_lsn': 'pg_wal_lsn_diff',
            'app_name': 'pg_receivewal',
            'sent_lsn': 'sent_lsn',
            'write_lsn': 'write_lsn',
            'flush_lsn': 'flush_lsn',
            'replay_lsn': 'replay_lsn',
        }
        replay_lag = 'COALESCE(1000*EXTRACT(epoch from replay_lag), 0)::bigint AS replay_lag_msec,'
        flush_lag = '(1000*EXTRACT(epoch from flush_lag))::bigint AS flush_lag_msec,'
        query = """SELECT pid, application_name,
                    client_hostname, client_addr, state,
                {current_lsn}
                    AS primary_location,
                {diff_lsn}({current_lsn}, {sent_lsn})
                    AS sent_location_diff,
                {diff_lsn}({current_lsn}, {write_lsn})
                    AS write_location_diff,
                {diff_lsn}({current_lsn}, {flush_lsn})
                    AS flush_location_diff,
                {diff_lsn}({current_lsn},
                    {replay_lsn})
                    AS replay_location_diff,
                {flush_lag}
                {replay_lag}
                extract(epoch from backend_start)::bigint AS backend_start_ts,
                (1000*extract(epoch from reply_time))::bigint AS reply_time_ms,
                sync_state FROM pg_stat_replication
                WHERE application_name != 'pg_basebackup'
                AND application_name != '{app_name}'
                AND state = 'streaming'""".format(
            current_lsn=wal_func['current_lsn'],
            diff_lsn=wal_func['diff_lsn'],
            app_name=wal_func['app_name'],
            sent_lsn=wal_func['sent_lsn'],
            write_lsn=wal_func['write_lsn'],
            flush_lsn=wal_func['flush_lsn'],
            flush_lag=flush_lag,
            replay_lag=replay_lag,
            replay_lsn=wal_func['replay_lsn'],
        )
        return self._get(query)

    def _get_wal_receiver_info(self):
        """Get wal_receiver info from pg_stat_wal_receiver.

        Raises:
            PostgresConnectionError: if the DB connection is lost.
        """
        query = """SELECT pid, status, slot_name,
                   COALESCE(1000*EXTRACT(epoch FROM last_msg_receipt_time), 0)::bigint AS last_msg_receipt_time_msec,
                   conninfo FROM pg_stat_wal_receiver"""
        result = self._get(query)
        if result:
            return result[0]
        return None

    def get_replication_state(self):
        """Get replication type (sync/async).

        Raises:
            PostgresConnectionError: if the DB connection is lost.
        """
        res = self._exec_query('SHOW synchronous_standby_names;').fetchone()
        res = ('async', None) if res[0] == '' else ('sync', res[0])
        return res

    def get_current_wal_flush_lsn(self) -> int:
        """Return the primary's durable WAL position as an integer."""
        row = self._exec_query(
            "SELECT pg_wal_lsn_diff(pg_current_wal_flush_lsn(), '0/0')::bigint"
        ).fetchone()
        if row is None or row[0] is None:
            raise PostgresQueryError('Could not read current WAL flush LSN')
        return int(row[0])

    def get_replica_flush_lsns(self) -> dict[str, int]:
        """Return durable WAL positions of currently streaming replicas."""
        rows = self._get(
            """SELECT application_name,
                      pg_wal_lsn_diff(flush_lsn, '0/0')::bigint AS flush_lsn
               FROM pg_stat_replication
               WHERE application_name != 'pg_basebackup'
               AND application_name != 'pg_receivewal'
               AND state = 'streaming'"""
        )
        return {
            str(row['application_name']): int(row['flush_lsn'])
            for row in rows
            if row.get('flush_lsn') is not None
        }

    def get_sessions_ratio(self):
        """Get ratio of active sessions/max sessions (in percents).

        Raises:
            PostgresConnectionError: if the DB connection is lost.
        """
        cur = self._exec_query("SELECT count(*) FROM pg_stat_activity WHERE state!='idle';")
        cur = cur.fetchone()[0]
        max_sessions = self._exec_query('SHOW max_connections;').fetchone()[0]
        return (cur / int(max_sessions)) * 100

    def lwaldump(self) -> int | None:
        """Return the end of valid WAL stored on the replica's local disk."""
        value = self._exec_query(
            "SELECT pg_wal_lsn_diff(lwaldump(), '0/00000000')::bigint"
        ).fetchone()[0]
        return int(value) if value is not None else None

    def get_wal_flush_lsn(self):
        """Return the local WAL position used by failover election."""
        if self.config.use_lwaldump:
            return self.lwaldump()
        query = """SELECT pg_wal_lsn_diff(
                GREATEST(
                    COALESCE(pg_last_wal_receive_lsn(), '0/0'),
                    COALESCE(pg_last_wal_replay_lsn(), '0/0')
                ),
                '0/00000000')::bigint"""
        value = self._exec_query(query).fetchone()[0]
        return int(value) if value is not None else None

    def get_wal_receive_lsn(self):
        """Compatibility alias for callers outside the failover protocol."""
        return self.get_wal_flush_lsn()

    def _fetch_archive_file(self, filename: str, *, read: bool):
        filepath = os.path.join(
            self.config.working_dir,
            f'.pgconsul_{filename}.fetch',
        )
        try:
            if os.path.exists(filepath):
                os.unlink(filepath)
            if self._cmd_manager.fetch_timeline_history(filename, filepath) != 0:
                logging.info('Archive file %s is not available yet', filename)
                return None
            if not read:
                return True
            with open(filepath, 'r') as archive_file:
                return archive_file.read()
        except (OSError, UnicodeError):
            logging.warning('Could not fetch archive file %s', filename, exc_info=True)
            return None
        finally:
            try:
                if os.path.exists(filepath):
                    os.unlink(filepath)
            except OSError:
                logging.warning('Could not remove fetched archive file %s', filepath)

    def fetch_timeline_history(self, timeline: int) -> str | None:
        """Fetch ``<timeline>.history`` from the configured WAL archive."""
        value = self._fetch_archive_file(f'{timeline:08X}.history', read=True)
        return value if isinstance(value, str) else None

    def is_wal_archived(self, filename: str) -> bool:
        """Check archive availability by fetching and discarding a WAL file."""
        return self._fetch_archive_file(filename, read=False) is True

    def install_timeline_history(self, timeline: int, value: str) -> bool:
        """Atomically install validated history where PostgreSQL can see it."""
        filename = f'{timeline:08X}.history'
        filepath = os.path.join(self.pgdata, 'pg_wal', filename)
        temporary = f'{filepath}.pgconsul-new'
        try:
            with open(temporary, 'w') as history_file:
                history_file.write(value)
                history_file.flush()
                os.fsync(history_file.fileno())
            os.replace(temporary, filepath)
            return True
        except OSError:
            logging.warning('Could not install timeline history %s', filename, exc_info=True)
            try:
                if os.path.exists(temporary):
                    os.unlink(temporary)
            except OSError:
                logging.warning('Could not remove temporary timeline history %s', temporary)
            return False

    def check_walreceiver(self) -> bool:
        """Check if walreceiver is running via pg_stat_wal_receiver.

        Raises:
            PostgresConnectionError: if the DB connection is lost.
        """
        cur = self._exec_query('SELECT pid FROM pg_stat_wal_receiver WHERE status = \'streaming\'')
        return bool(cur.fetchall())

    def is_ready_for_pg_rewind(self):
        """
        Check if pg_rewind could be used on local postgresql
        """
        res = self.get_data_page_checksum_version()
        if res:
            logging.info("Checksums are enabled, host is ready for pg_rewind.")
            return True

        res = self.get_wal_log_hints_settings()
        if res == 'on':
            logging.info("Checksums are disabled but wal_log_hints = on, host is ready for pg_rewind.")
            return True

        logging.error("Checksums or wal_log_hints should be enabled for pg_rewind to work properly.")
        return False

    def get_replay_diff(self, diff_from='0/00000000'):
        """Get WAL replay LSN diff from the given base LSN.

        Raises:
            PostgresConnectionError: if the DB connection is lost.
        """
        query = f"""SELECT pg_wal_lsn_diff(
                pg_last_wal_replay_lsn(),
                '{diff_from}')::bigint"""
        return self._exec_query(query).fetchone()[0]

    def get_receive_diff(self, diff_from='0/00000000'):
        """Get WAL receive LSN diff from the given base LSN.

        Unlike replay LSN, this measures traffic from the current primary and
        is therefore suitable for failover health monitoring.
        """
        query = f"""SELECT pg_wal_lsn_diff(
                pg_last_wal_receive_lsn(),
                '{diff_from}')::bigint"""
        return self._exec_query(query).fetchone()[0]

    def get_primary_fqdn(self) -> str | None:
        # Single source for primary FQDN: runtime primary_conninfo takes priority
        # (more reliable than stale recovery.conf), recovery.conf is used as a fallback.
        # PostgresConnectionError from _get_param_value propagates to run_iteration().
        primary_fqdn = helpers.extract_host(self._get_param_value('primary_conninfo'))
        logging.debug('Primary FQDN: %s', primary_fqdn)
        if primary_fqdn is not None:
            return primary_fqdn
        configured_primary = self.recovery_conf('get_primary')
        return configured_primary if isinstance(configured_primary, str) else None

    def recovery_conf(self, action, primary_host=None) -> str | int | None:
        """
        Perform recovery conf action (create, remove, get_primary)
        """
        recovery_filepath = os.path.join(self.pgdata, self.config.recovery_filepath)

        if action == 'create':
            res = self._cmd_manager.generate_recovery_conf(recovery_filepath, primary_host)
            return res
        elif action == 'remove':
            try:
                os.unlink(recovery_filepath)
            except FileNotFoundError:
                pass
            return 0
        else:
            if os.path.exists(recovery_filepath):
                with open(recovery_filepath, 'r') as recovery_file:
                    for i in recovery_file.read().split('\n'):
                        if 'primary_conninfo' in i:
                            return helpers.extract_host(i)
            return None

    def promote(self, timeline: int | None = None) -> bool:
        """
        Make local postgresql primary
        """
        # We need to stop archiving WAL and resume after promote
        # to prevent wrong history file in archive in case of failure
        if not self.stop_archiving_wal():
            logging.error('ACTION-FAILED. Could not stop archiving WAL')
            return False

        # We need to resume replaying WAL before promote
        self.pg_wal_replay_resume()

        logging.info('ACTION. Starting promote')
        if timeline is None:
            promoted = self._cmd_manager.promote(self.pgdata) == 0
        else:
            promoted = self._cmd_manager.promote(
                self.pgdata, timeline=timeline,
            ) == 0
        try:
            is_primary = self.get_role() == 'primary'
        except PostgresConnectionError:
            is_primary = False
        if is_primary:
            if not self.resume_archiving_wal():
                logging.error('ACTION-FAILED. Could not resume archiving WAL')
            self._upload_wals()
            return True
        if promoted:
            logging.error('Promote command completed but PostgreSQL is not primary')
        return False

    def _upload_wals(self):
        """
        Upload WAL files that were not archived during promote.
        """
        if self.conn_local is None:
            logging.error("No database connection for WAL upload")
            return

        logging.info("Starting WAL upload after promote")
        # Promote is already done; WAL upload failure is non-fatal.
        try:
            wals_to_upload = self._wals_to_upload
            logging.debug(f"Will upload up to {wals_to_upload} WAL files")

            with self.conn_local.cursor() as cur:
                cur.execute("SELECT pg_walfile_name(pg_current_wal_lsn())")
                current_wal = cur.fetchone()[0]
                logging.info(f"Current WAL file: {current_wal}")

                cur.execute("SHOW archive_command")
                archive_command = cur.fetchone()[0]
                logging.debug(f"Original archive_command: {archive_command}")
                cur.execute("SHOW data_directory")
                pgdata = cur.fetchone()[0]
                logging.info(f"PostgreSQL data_directory: {pgdata}")

            wals = os.listdir('{pgdata}/pg_wal/'.format(pgdata=pgdata))
            logging.debug(f"Found {len(wals)} files in WAL directory")
            wals.sort()
            wals_to_upload_list = []
            skipped_non_wal = []
            for wal in wals:
                if wal < current_wal:
                    try:
                        struct.unpack('>3I', bytearray.fromhex(wal))
                        wals_to_upload_list.append(wal)
                        logging.debug(f"WAL file eligible for upload: {wal}")
                    except (struct.error, ValueError) as e:
                        skipped_non_wal.append(wal)
                        logging.debug(f"Skipping non-WAL file (invalid format): {wal} - {e}")
                        continue

            logging.info(f"Found {len(wals_to_upload_list)} WAL files to upload (skipped {len(skipped_non_wal)} non-WAL files)")

            wals_to_upload_list = wals_to_upload_list[-wals_to_upload:]
            logging.info(f"Selected last {len(wals_to_upload_list)} WAL files for upload")

            for i, wal in enumerate(wals_to_upload_list, 1):
                path = '{pgdata}/pg_wal/{wal}'.format(pgdata=pgdata, wal=wal)
                cmd = archive_command.replace('%p', path).replace('%f', wal)
                logging.info(f"[{i}/{len(wals_to_upload_list)}] Uploading WAL: {wal}")
                self._cmd_manager.run_external(cmd)

            logging.info("WAL upload completed successfully")
        except Exception as error_message:
            # Broad catch is intentional: promote() already succeeded at this point.
            # Any exception here must not propagate — callers check promote()'s return
            # value; an unhandled exception would mask a successful promote.
            logging.error(f"WAL upload failed with error: {error_message}", exc_info=True)

    def pgpooler(self, action):
        """
        Start/stop/status pooler wrapper
        """
        if action == 'stop':
            if self._get_pooler_status():
                return True
            res = self._cmd_manager.stop_pooler()
        elif action == 'status':
            if self.config.standalone_pooler:
                try:
                    sock = socket.create_connection((self.config.pooler_addr, self.config.pooler_port), self.config.pooler_conn_timeout)
                    sock.close()
                    return True, True
                except socket.error:
                    return False, not self._get_pooler_status()
            else:
                res = not self._get_pooler_status()
                return res, res
        elif action == 'start':
            if not self._get_pooler_status():
                return True
            res = self._cmd_manager.start_pooler()
        else:
            raise RuntimeError('Unknown pooler action: %s' % action)
        if res == 0:
            return True
        return False

    def stop_pooler_async(self) -> bool:
        """Request pooler shutdown without delaying a fencing handoff."""
        return self._cmd_manager.stop_pooler_async()

    def _get_pooler_status(self) -> bool:
        result = self._cmd_manager.get_pooler_status()
        return bool(result)

    def do_rewind(self, primary_host):
        """
        Run pg_rewind on localhost against primary_host
        """
        if self.config.use_replication_slots:
            #
            # We should move pg_replslot directory somewhere before rewind
            # and move it back after it since pg_rewind doesn't do it.
            #
            try:
                helpers.backup_dir('%s/pg_replslot' % self.pgdata, '/tmp/pgconsul_replslots_backup')
            except Exception:
                logging.warning('Could not backup replication slots before rewinding. Skipping it.')

        # pg_rewind runs target crash recovery through a single-user backend.
        # A stopped standby retains standby.signal, which that backend rejects.
        standby_signal = os.path.join(self.pgdata, 'standby.signal')
        saved_standby_signal = f'{standby_signal}.pgconsul-rewind'
        moved_standby_signal = False
        try:
            if os.path.exists(standby_signal):
                os.replace(standby_signal, saved_standby_signal)
                moved_standby_signal = True
        except OSError:
            logging.exception('Could not prepare standby.signal for pg_rewind')
            return 1

        logging.info('ACTION. Starting pg_rewind')
        res = self._cmd_manager.rewind(self.pgdata, primary_host)

        if moved_standby_signal and res != 0:
            try:
                os.replace(saved_standby_signal, standby_signal)
            except OSError:
                logging.exception('Could not restore standby.signal after pg_rewind failure')
        elif moved_standby_signal:
            try:
                os.unlink(saved_standby_signal)
            except FileNotFoundError:
                pass
            except OSError:
                logging.warning('Could not remove saved standby.signal after pg_rewind', exc_info=True)

        if self.config.use_replication_slots and res == 0:
            if os.path.exists('/tmp/pgconsul_replslots_backup'):
                try:
                    helpers.backup_dir('/tmp/pgconsul_replslots_backup', '%s/pg_replslot' % self.pgdata)
                except Exception:
                    logging.warning('Could not restore replication slots after rewinding. Skipping it.')

        # Validate postgresql.auto.conf after rewind: pg_rewind chunked copy can
        # cause torn read if primary replaces file via ALTER SYSTEM. Detect/repair
        # corruption, signal failure so caller retries.
        if res == 0 and not self._is_postgresql_auto_conf_valid():
            logging.warning('postgresql.auto.conf is corrupted after pg_rewind (possible torn read)')
            self._repair_postgresql_auto_conf()
            return 1
        return res

    def _get_param_value(self, param):
        cursor = self._exec_query(f'SHOW {param}')
        (value,) = cursor.fetchone()
        return value

    def next_local_timeline(self, source_timeline: int) -> int:
        """Return the timeline PostgreSQL will choose with archive restore off."""
        newest = source_timeline
        while os.path.exists(os.path.join(
            self.pgdata,
            'pg_wal',
            f'{newest + 1:08X}.history',
        )):
            newest += 1
        return newest + 1

    def get_restore_command(self) -> str | None:
        """Public accessor for the ``restore_command`` GUC.

        Raises PostgresConnectionError on connection loss (like _get_param_value).
        """
        return self._get_param_value('restore_command')

    def _alter_system_set_param(self, param: str, value=None, reset=False) -> bool:
        """Set or reset a PostgreSQL parameter via ALTER SYSTEM.

        Raises:
            PostgresConnectionError: if the DB connection is lost.
        """
        def equal() -> bool:
            return self._get_param_value(param) == value

        def unequal(prev_value) -> bool:
            return self._get_param_value(param) != prev_value

        if reset:
            prev_value = self._get_param_value(param)
            logging.info(f'ACTION. Resetting {param} with ALTER SYSTEM')
            query = SQL("ALTER SYSTEM RESET {param}").format(param=Identifier(param))
            self._exec_query(query)
            await_func: Callable[[], bool] = partial(unequal, prev_value)
            await_message = f'{param} is reset after reload'
        else:
            logging.info(f'ACTION. Setting {param} to {value} with ALTER SYSTEM')
            query = SQL("ALTER SYSTEM SET {param} TO %(value)s").format(param=Identifier(param))
            self._exec_query(query, value=value)
            await_func = equal
            await_message = f'{param} is set to {value} after reload'

        reload_result = self._cmd_manager.reload_postgresql(self.pgdata)
        if reload_result:
            logging.debug(f'Reload has failed, not waiting for param {param} change')
            return False

        return helpers.await_for(await_func, self.config.postgres_timeout, await_message)

    def change_replication_type(self, synchronous_standby_names):
        return self._alter_system_set_param('synchronous_standby_names', synchronous_standby_names)

    def _reset_wal_barrier(self) -> None:
        if self._wal_barrier_cursor is not None:
            try:
                self._wal_barrier_cursor.close()
            except psycopg2.Error:
                pass
        if self._wal_barrier_conn is not None:
            try:
                self._wal_barrier_conn.close()
            except psycopg2.Error:
                pass
        self._wal_barrier_conn = None
        self._wal_barrier_cursor = None
        self._wal_barrier_operation_id = None
        self._wal_barrier_query_started = False
        self._wal_barrier_started_at = None

    def advance_wal_barrier(self, operation_id: str) -> bool:
        """Advance a non-blocking synchronous-commit WAL barrier.

        A truncate-and-insert into a singleton service table guarantees a real
        WAL record without accumulating old rows.  The asynchronous connection
        keeps the main iteration responsive while COMMIT waits for target SSN.
        """
        if self._wal_barrier_operation_id not in (None, operation_id):
            self._reset_wal_barrier()
        if (
            self._wal_barrier_started_at is not None
            and time.monotonic() - self._wal_barrier_started_at
            >= self.config.wal_barrier_timeout
        ):
            logging.warning(
                'WAL barrier result is unknown after %.1fs; retrying operation %s',
                self.config.wal_barrier_timeout, operation_id,
            )
            self._reset_wal_barrier()
            return False
        try:
            if self._wal_barrier_conn is None:
                timeout_ms = max(1, int(self.config.wal_barrier_timeout * 1000))
                self._wal_barrier_conn = psycopg2.connect(
                    self.config.conn_string,
                    async_=True,
                    options=(
                        f'-c statement_timeout={timeout_ms} '
                        f'-c lock_timeout={timeout_ms}'
                    ),
                )
                self._wal_barrier_operation_id = operation_id
                self._wal_barrier_started_at = time.monotonic()

            barrier_conn = self._wal_barrier_conn
            assert barrier_conn is not None
            poll_state = barrier_conn.poll()
            if poll_state != psycopg2.extensions.POLL_OK:
                return False

            if not self._wal_barrier_query_started:
                self._wal_barrier_cursor = barrier_conn.cursor()
                query = SQL(
                    "BEGIN; "
                    "SET LOCAL synchronous_commit = on; "
                    "CREATE TABLE IF NOT EXISTS public.pgconsul_durability_barrier ("
                    "singleton boolean PRIMARY KEY CHECK (singleton), "
                    "operation_id text NOT NULL"
                    "); "
                    "TRUNCATE TABLE public.pgconsul_durability_barrier; "
                    "INSERT INTO public.pgconsul_durability_barrier "
                    "(singleton, operation_id) VALUES (true, {}); "
                    "COMMIT;"
                ).format(Literal(operation_id))
                barrier_cursor = self._wal_barrier_cursor
                assert barrier_cursor is not None
                barrier_cursor.execute(query)
                self._wal_barrier_query_started = True
                return False

            logging.info('WAL barrier committed for operation %s', operation_id)
            self._reset_wal_barrier()
            return True
        except psycopg2.OperationalError as exc:
            self._reset_wal_barrier()
            raise PostgresConnectionError(str(exc)) from exc
        except psycopg2.Error as exc:
            self._reset_wal_barrier()
            raise PostgresQueryError('Could not commit WAL barrier') from exc

    def ensure_pooler_started(self):
        pooler_port_available, pooler_service_running = self.pgpooler('status')
        if pooler_service_running and not pooler_port_available:
            logging.warning('Service alive, but pooler not accepting connections, restarting.')
            self.pgpooler('stop')
            self.pgpooler('start')
            logging.info('Pooler restarted successfully')
        elif not pooler_service_running:
            logging.info('Pooler not running, starting it')
            self.pgpooler('start')
            logging.info('Pooler started successfully')

    def ensure_archive_mode(self):
        archive_mode = self._get_param_value('archive_mode')
        if archive_mode == 'off':
            return False
        return True

    def ensure_archiving_wal(self):
        archive_command = self._get_param_value('archive_command')
        if archive_command == self.DISABLED_ARCHIVE_COMMAND:
            logging.info('ACTION. Archive command was disabled, enabling it')
            self.resume_archiving_wal()
            logging.info('WAL archiving enabled successfully')
        config = self._get_postgresql_auto_conf()
        if config.get('archive_command') == self.DISABLED_ARCHIVE_COMMAND:
            logging.info('ACTION. Archive command was disabled in postgresql.auto.conf, resetting it')
            self.resume_archiving_wal()
            logging.info('WAL archiving enabled successfully (from auto.conf)')

    def stop_archiving_wal(self):
        return self._alter_system_set_param('archive_command', self.DISABLED_ARCHIVE_COMMAND)

    def resume_archiving_wal(self):
        return self._alter_system_set_param('archive_command', reset=True)

    def stop_archiving_wal_stopped(self):
        return self._alter_system_stopped('archive_command', self.DISABLED_ARCHIVE_COMMAND)

    def stop_restoring_wal(self):
        return self._alter_system_set_param('restore_command', self.DISABLED_RESTORE_COMMAND)

    def stop_restoring_wal_stopped(self):
        return self._alter_system_stopped('restore_command', self.DISABLED_RESTORE_COMMAND)

    def resume_restoring_wal(self):
        return self._alter_system_set_param('restore_command', reset=True)

    def resume_restoring_wal_stopped(self):
        return self._alter_system_stopped('restore_command', reset=True)

    def ensure_restoring_wal(self):
        restore_command = self._get_param_value('restore_command')
        if restore_command == self.DISABLED_RESTORE_COMMAND:
            logging.info('ACTION. Restore command was disabled, enabling it')
            self.resume_restoring_wal()

    def _get_postgresql_auto_conf(self):
        config = {}
        current_file = os.path.join(self.pgdata, 'postgresql.auto.conf')
        with open(current_file, 'r') as fobj:
            for line in fobj:
                if line.lstrip().startswith('#'):
                    continue
                key, value = line.rstrip('\n').split('=', maxsplit=1)
                config[key.strip()] = value.lstrip().lstrip('\'').rstrip('\'')
        return config

    def _is_postgresql_auto_conf_valid(self) -> bool:
        """Check postgresql.auto.conf for corruption (e.g. torn read from pg_rewind)."""
        current_file = os.path.join(self.pgdata, 'postgresql.auto.conf')
        if not os.path.exists(current_file):
            return True
        try:
            with open(current_file, 'r') as fobj:
                for line in fobj:
                    stripped = line.strip()
                    if not stripped or stripped.startswith('#'):
                        continue
                    if '=' not in stripped:
                        return False
                    _, _, value = stripped.partition('=')
                    if value.count("'") % 2 != 0:
                        return False
        except Exception:
            logging.exception('Error validating postgresql.auto.conf')
            return False
        return True

    def _repair_postgresql_auto_conf(self) -> bool:
        """Remove corrupted lines from postgresql.auto.conf, atomically replace file."""
        current_file = os.path.join(self.pgdata, 'postgresql.auto.conf')
        new_file = os.path.join(self.pgdata, 'postgresql.auto.conf.repair')
        try:
            with open(current_file, 'r') as fobj:
                lines = fobj.readlines()
            valid_lines = []
            for line in lines:
                stripped = line.strip()
                if not stripped or stripped.startswith('#'):
                    valid_lines.append(line)
                    continue
                if '=' not in stripped:
                    logging.warning('Dropping corrupted line from postgresql.auto.conf: %s', stripped)
                    continue
                _, _, value = stripped.partition('=')
                if value.count("'") % 2 != 0:
                    logging.warning('Dropping corrupted line from postgresql.auto.conf (unbalanced quotes): %s', stripped)
                    continue
                valid_lines.append(line)
            with open(new_file, 'w') as fobj:
                fobj.writelines(valid_lines)
            os.replace(new_file, current_file)
            logging.info('postgresql.auto.conf repaired: corrupted lines removed')
            return True
        except Exception:
            logging.exception('Error repairing postgresql.auto.conf')
            return False

    #
    # We do it with writing to file and not with ALTER SYSTEM command since
    # PostgreSQL is stopped when this method is called.
    # We are not afraid of future rewriting postgresql.auto.conf with ALTER
    # SYSTEM command since this change is temporary.
    #
    def _alter_system_stopped(self, param, set_value=None, reset=False):
        """
        Set param to value while PostgreSQL is stopped.
        Method should be called only with stopped PostgreSQL.
        """
        try:
            action = 'Resetting' if reset else 'Setting'
            logging.info(f'ACTION. {action} {param} in postgresql.auto.conf')
            config = self._get_postgresql_auto_conf()
            current_file = os.path.join(self.pgdata, 'postgresql.auto.conf')
            new_file = os.path.join(self.pgdata, 'postgresql.auto.conf.new')
            old_value = config.get(param)
            if reset and old_value is None:
                logging.debug(f'Param {param} is already absent from postgresql.auto.conf')
                return True
            if not reset and old_value == set_value:
                logging.debug(f'Param {param} already has value {set_value} in postgresql.auto.conf')
                return True
            if reset:
                logging.debug(f'Removing {param} from postgresql.auto.conf')
                del config[param]
            else:
                logging.debug(f'Changing {param} from {old_value} to {set_value} in postgresql.auto.conf')
                config[param] = set_value
            with open(new_file, 'w') as fobj:
                fobj.write('# Do not edit this file manually!\n')
                fobj.write('# It will be overwritten by the ALTER SYSTEM command.\n')
                for key, value in config.items():
                    fobj.write(f'{key} = \'{value}\'\n')
            os.replace(new_file, current_file)
            return True
        except Exception:
            logging.exception('Error writing PostgreSQL config file')
            return False

    def checkpoint(self, query=None):
        """Perform checkpoint.

        Raises:
            PostgresConnectionError: if the DB connection is lost (propagates
                from _exec_without_result to the caller).
            PostgresQueryError: if PostgreSQL rejects the checkpoint query.
        """
        logging.info('ACTION. Initiating checkpoint')
        if not query:
            query = 'CHECKPOINT'
        try:
            return self._exec_without_result(query)
        except PostgresConnectionError:
            raise
        except psycopg2.Error as exc:
            raise PostgresQueryError('Could not perform checkpoint') from exc

    def switch_wal(self) -> bool:
        """Close the current WAL segment so archive recovery can fetch it."""
        logging.info('ACTION. Switching WAL segment')
        try:
            return self._exec_without_result('SELECT pg_switch_wal()')
        except PostgresConnectionError:
            raise
        except psycopg2.Error as exc:
            raise PostgresQueryError('Could not switch WAL segment') from exc

    def start_postgresql(self, timeout=60):
        """
        Start PG server on current host
        """
        return self._cmd_manager.start_postgresql(timeout, self.pgdata)

    def start_postgresql_async(self, timeout=60):
        """Launch pg_start without waiting for recovery to finish."""
        return self._cmd_manager.start_postgresql_async(timeout, self.pgdata)

    def get_postgresql_status(self):
        """
        Returns PG status on current host
        """
        return self._cmd_manager.get_postgresql_status(self.pgdata)

    def stop_postgresql(self, timeout=60, wait=True):
        """Stop PostgreSQL on the current host without changing replication."""
        return self._cmd_manager.stop_postgresql(timeout, self.pgdata, wait=wait)

    def is_replaying_wal(self, check_time):
        prev_replay_diff = self.get_replay_diff()
        time.sleep(check_time)
        replay_diff = self.get_replay_diff()
        return prev_replay_diff < replay_diff

    def pg_wal_replay_resume(self):
        if self.is_wal_replay_paused():
            logging.debug('WAL replay is paused. So we resume it')
            self._pg_wal_replay("resume")

    def is_wal_replay_paused(self):
        return self._exec_query('SELECT pg_is_wal_replay_paused();').fetchone()[0]

    def ensure_replaying_wal(self):
        self.enable_wal_receiver_if_disabled()
        self.pg_wal_replay_resume()

    def _is_wal_receiver_stopped(self) -> bool:
        """
        True if pg_stat_wal_receiver has no rows (receiver process is gone).
        Raises on query errors so callers do not treat DB failures as "stopped".
        """
        cur = self._exec_query('SELECT pid FROM pg_stat_wal_receiver')
        return not cur.fetchall()

    def disable_wal_receiver(self, timeout: float) -> bool:
        """
        Disable walreceiver by clearing primary_conninfo, reloading, and waiting
        until the receiver process actually disappears.

        Startup applies the reload asynchronously, so emptying primary_conninfo
        alone does not guarantee that WAL is no longer being received/acked.
        """
        if self._exec_query('SHOW primary_conninfo;').fetchone()[0] != '':
            logging.info('ACTION. Disabling walreceiver.')
            if not self._alter_system_set_param('primary_conninfo', ''):
                logging.error('Could not clear primary_conninfo.')
                return False
        else:
            logging.debug('primary_conninfo is already empty')

        if not helpers.await_for(
            self._is_wal_receiver_stopped,
            timeout,
            'walreceiver to stop',
        ):
            logging.error('Walreceiver did not stop within %.1fs after disable.', timeout)
            return False
        logging.info('Walreceiver stopped.')
        return True

    def enable_wal_receiver_if_disabled(self):
        """
        Enable walreceiver.
        Applicable only for replicas.
        """
        if not self.is_wal_receiver_disabled():
            logging.debug('walreceiver is not disabled, we do nothing here')
            return

        if 'primary' == self.role:
            logging.warning('PostgreSQL is not in recovery. So we can not enable walreceiver.')
            return

        logging.info('ACTION. Enabling walreceiver')
        self._alter_system_set_param('primary_conninfo', reset=True)
        self.reload()

    def enable_wal_receiver_stopped(self) -> bool:
        """Remove the persistent vote fence before starting a replica."""
        return self._alter_system_stopped('primary_conninfo', reset=True)

    def is_wal_receiver_disabled(self) -> bool:
        return self._get_param_value('primary_conninfo') == ''

    def _pg_wal_replay(self, pause_or_resume):
        logging.info('ACTION. WAL replay: %s', pause_or_resume)
        self._exec_query(f'SELECT pg_wal_replay_{pause_or_resume}();')

    def check_extension_installed(self, name):
        cur = self._exec_query(f"SELECT * FROM pg_extension WHERE extname = '{name}';")
        result = cur.fetchall()
        return len(result) == 1

    @staticmethod
    def _wait_async_connection(conn, deadline: float) -> None:
        """Drive an asynchronous libpq operation until completion or deadline."""
        with selectors.DefaultSelector() as selector:
            registered_events = None
            while True:
                state = conn.poll()
                if state == psycopg2.extensions.POLL_OK:
                    return
                if state == psycopg2.extensions.POLL_READ:
                    events = selectors.EVENT_READ
                elif state == psycopg2.extensions.POLL_WRITE:
                    events = selectors.EVENT_WRITE
                else:
                    raise psycopg2.OperationalError('Unexpected asynchronous libpq state')

                if events != registered_events:
                    if registered_events is not None:
                        selector.unregister(conn.fileno())
                    selector.register(conn.fileno(), events)
                    registered_events = events

                timeout = deadline - time.monotonic()
                if timeout <= 0 or not selector.select(timeout):
                    raise TimeoutError('PostgreSQL health check timed out')

    def is_host_unreachable(self, primary: str | None = None, check_primary: bool = True) -> bool:
        """
        Check if a host is NOT accessible via the postgres protocol.

        Returns True if the host is unreachable (dead), False if it is reachable.
        When *primary* is not provided, the primary FQDN is resolved via
        ``get_primary_fqdn()``; if that returns an empty value, False is
        returned (no primary to check — treat as reachable).
        """
        if not primary:
            primary = self.get_primary_fqdn()
            if not primary:
                return False
        append = self.config.append_primary_conn_string
        if check_primary and ('target_session_attrs' not in append):
            ensure_connect_primary = 'target_session_attrs=primary'
        else:
            ensure_connect_primary = ''

        conn = None
        try:
            deadline = time.monotonic() + self.config.iteration_timeout
            conn = psycopg2.connect(
                'host=%s %s %s' % (primary, append, ensure_connect_primary),
                async_=True,
            )
            self._wait_async_connection(conn, deadline)
            cur = conn.cursor()
            cur.execute('SELECT 42')
            self._wait_async_connection(conn, deadline)
            result = cur.fetchone()
            if result and result[0] == 42:
                return False
            return True
        except Exception as err:
            logging.debug('%s while trying to check primary health.', str(err))
            return True
        finally:
            if conn is not None:
                conn.close()

    def reload(self):
        return not bool(self._cmd_manager.reload_postgresql(self.pgdata))


def build_postgres_config(config: RawConfigParser) -> PostgresConfig:
    """Build PostgresConfig from the 'global' section of an INI config."""
    postgres_config = PostgresConfig(
        conn_string=config.get('global', 'local_conn_string'),
        working_dir=config.get('global', 'working_dir'),
        recovery_filepath=config.get('global', 'recovery_conf_rel_path'),
        use_replication_slots=config.getboolean('global', 'use_replication_slots'),
        standalone_pooler=config.getboolean('global', 'standalone_pooler'),
        pooler_addr=config.get('global', 'pooler_addr'),
        pooler_port=config.getint('global', 'pooler_port'),
        pooler_conn_timeout=config.getfloat('global', 'pooler_conn_timeout'),
        postgres_timeout=config.getfloat('global', 'postgres_timeout'),
        iteration_timeout=config.getfloat('global', 'iteration_timeout'),
        append_primary_conn_string=config.get('global', 'append_primary_conn_string', fallback=''),
        wals_to_upload=config.getint('global', 'wals_to_upload'),
        use_lwaldump=(
            config.getboolean('global', 'use_lwaldump', fallback=False)
            or config.getboolean('global', 'quorum_commit', fallback=False)
        ),
        wal_barrier_timeout=config.getfloat(
            'global', 'wal_barrier_timeout', fallback=60.0,
        ),
    )
    if postgres_config.wal_barrier_timeout <= 0:
        raise ValueError('wal_barrier_timeout must be positive')
    return postgres_config


def create_postgres(config: RawConfigParser, cmd_manager: CommandManager) -> Postgres:
    """Factory: build a Postgres instance from config and a CommandManager."""
    return Postgres(config=build_postgres_config(config), cmd_manager=cmd_manager)
