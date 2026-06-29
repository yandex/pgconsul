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
import signal
import socket
import time
import traceback
from typing import Callable

import psycopg2
from psycopg2.sql import SQL, Identifier

from . import helpers, exceptions
from .command_manager import CommandManager
from .plugin import PluginRunner
from .types import PluginsConfig, ReplicaInfos

DEC2INT_TYPE = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values, 'DEC2INT', lambda value, curs: int(value) if value is not None else None
)

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
    use_lwaldump: bool
    working_dir: str
    recovery_filepath: str
    use_replication_slots: bool
    standalone_pooler: bool
    pooler_conn_timeout: float
    pooler_addr: str
    pooler_port: int
    postgres_timeout: float
    iteration_timeout: float
    plugins: PluginsConfig


class Postgres(object):
    """
    Postgres class
    """

    DB_STATE_CACHE_FILENAME = '.pgconsul_db_state.cache'
    DISABLED_ARCHIVE_COMMAND = '/bin/false'
    DISABLED_RESTORE_COMMAND = '/bin/false'
    LOCAL_CONNECT_TIMEOUT_INITIAL = 1
    LOCAL_CONNECT_TIMEOUT_MAX = 10

    def __init__(self, config: PostgresConfig, plugins: PluginRunner, cmd_manager: CommandManager):
        self.config = config
        self._plugins = plugins
        self._cmd_manager = cmd_manager

        self.state: dict[str, object] = {}

        self.conn_local: psycopg2.extensions.connection | None = None
        self.role: str | None = None
        self.pgdata = ''
        self.pg_version = None
        # Backoff counter for connect_timeout (1→2→4→8→10s). Reset on success.
        # Independent from _pg_timeout_count in main.py (restart threshold).
        self._conn_timeout_count = 0
        self._base_conn_string = self._strip_connect_timeout(config.conn_string)
        self._offline_detect_pgdata()
        self.reconnect()

    def _create_cursor(self):
        def _open_cursor():
            if self.conn_local is None:
                raise RuntimeError('Local conn is dead in _open_cursor()')
            cursor = self.conn_local.cursor()
            cursor.execute('SELECT 1;')
            return cursor

        try:
            if self.conn_local:
                return _open_cursor()
            raise RuntimeError('Local conn is dead')
        except Exception:
            for line in traceback.format_exc().split('\n'):
                logging.debug(line.rstrip())
            try:
                self.reconnect()
            except exceptions.PGConnectionTimeout:
                # Timeout already logged and counted in reconnect(); re-raise so
                # that callers can react to it (e.g. stop retrying) instead of
                # silently incrementing _conn_timeout_count with no visible traceback.
                raise
            # reconnect() succeeded — create a fresh cursor and return it.
            # Without this, _create_cursor() would fall through and return None.
            if self.conn_local:
                return _open_cursor()
            raise RuntimeError('Local conn is dead after reconnect')

    def _exec_query(self, query, **kwargs):
        # _create_cursor() never returns None: it either returns a cursor or raises.
        cur = self._create_cursor()
        cur.execute(query, kwargs)
        return cur

    def _get(self, query, **kwargs):
        with contextlib.closing(self._exec_query(query, **kwargs)) as cur:
            records = list(_plain_format(cur))
            return records

    def _exec_without_result(self, query):
        try:
            self._exec_query(query)
            return True
        except Exception:
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())
            return False

    def get_data_from_control_file(self, parameter, preproc=None, log=True):
        """
        Run pg_controldata and grep it's output
        """
        return self._cmd_manager.get_control_parameter(self.pgdata, parameter, preproc, log)

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
                if state.get('pg_version'):
                    logging.error('Found more than one cluster on %s port', need_port)
                    return
                self.pg_version = state['pg_version'] = version
                self.role = state['role'] = 'replica' if 'recovery' in pgstate else 'primary'
                self.pgdata = state['pgdata'] = pgdata
        except Exception:
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())

    @helpers.return_none_on_error
    def get_replication_slots(self):
        res = self._exec_query('SELECT slot_name FROM pg_replication_slots;').fetchall()
        return [i[0] for i in res]

    def _create_replication_slot(self, slot_name):
        logging.debug('ACTION. Creating slot %s.', slot_name)
        query = f"SELECT pg_create_physical_replication_slot('{slot_name}', true)"
        return self._exec_without_result(query)

    def _drop_replication_slot(self, slot_name):
        logging.debug('ACTION. Dropping slot %s.', slot_name)
        query = f"SELECT pg_drop_replication_slot('{slot_name}')"
        return self._exec_without_result(query)

    @staticmethod
    def _strip_connect_timeout(conn_string: str) -> str:
        """Remove connect_timeout from conn_string to allow dynamic override."""
        parts = [p for p in conn_string.split() if not p.startswith('connect_timeout=')]
        return ' '.join(parts)

    def _get_current_timeout(self) -> int:
        """Compute connect_timeout using exponential backoff (capped at LOCAL_CONNECT_TIMEOUT_MAX)."""
        return min(
            self.LOCAL_CONNECT_TIMEOUT_INITIAL * (2 ** self._conn_timeout_count),
            self.LOCAL_CONNECT_TIMEOUT_MAX,
        )

    def _log_connection_failure_diagnostics(self, connect_timeout: int):
        """Log system diagnostics when PostgreSQL connection fails with timeout."""
        try:
            pg_status = self.get_postgresql_status()
            logging.warning('Connection timeout diagnostics: pg_status=%s', pg_status)
        except Exception:
            logging.warning('Connection timeout diagnostics: could not get pg_status')
        try:
            cpu_count = os.cpu_count() or 'unknown'
            with open('/proc/loadavg', 'r') as f:
                logging.warning(
                    'Connection timeout diagnostics: loadavg=%s cpu_count=%s',
                    f.read().strip(),
                    cpu_count,
                )
        except Exception:
            pass
        for pressure_file in ('/proc/pressure/cpu', '/proc/pressure/io'):
            try:
                with open(pressure_file, 'r') as f:
                    logging.warning(
                        'Connection timeout diagnostics: %s=%s', pressure_file, f.read().strip()
                    )
            except Exception:
                pass
        logging.warning(
            'Connection timeout diagnostics: conn_timeout_count=%d, current_timeout=%d',
            self._conn_timeout_count,
            connect_timeout,
        )

    def reconnect(self):
        """
        Reestablish connection with local postgresql
        """
        logging.debug('Trying to reconnect to postgres')
        nonfatal_errors = {
            'FATAL:  the database system is starting up': exceptions.PGIsStartingUp,
            'FATAL:  the database system is shutting down': exceptions.PGIsShuttingDown,
        }
        connect_timeout = self._get_current_timeout()
        conn_str = f'{self._base_conn_string} connect_timeout={connect_timeout}'
        try:
            if self.conn_local:
                self.conn_local.close()
            if not self.state.get('running', False):
                logging.error('PostgreSQL is dead. Unable to reconnect.')
                self.conn_local = None
                return
            self.conn_local = psycopg2.connect(conn_str)
            self.conn_local.autocommit = True

            self._conn_timeout_count = 0
            self.role = self.get_role()
            self.pg_version = self._get_pg_version()
            self.pgdata = self._get_pgdata_path()
        except psycopg2.OperationalError as exception:
            logging.error('Could not connect to "%s".', conn_str)
            self.conn_local = None
            error_lines = traceback.format_exc().split('\n')
            for line in error_lines:
                logging.error(line.rstrip())
            for line in error_lines:
                for substr, exc in nonfatal_errors.items():
                    if substr in line:
                        raise exc()
            if 'timeout' in str(exception).lower():
                self._conn_timeout_count += 1
                self._log_connection_failure_diagnostics(connect_timeout)
                raise exceptions.PGConnectionTimeout(self._conn_timeout_count)

    @property
    def _db_state_cache_path(self) -> str:
        return os.path.join(self.config.working_dir, self.DB_STATE_CACHE_FILENAME)

    def get_cached_state(self):
        """Read previously cached db_state from disk (or None if unavailable)."""
        try:
            with open(self._db_state_cache_path, 'r') as fobj:
                return json.loads(fobj.read())
        except Exception:
            return None

    def get_state(self):
        """
        Get current database state (if possible)
        """
        data = {'alive': False, 'prev_state': self.get_cached_state()}
        try:
            try:
                is_db_alive, terminal_state = self.is_alive_and_in_terminal_state()
                if terminal_state:
                    data['running'] = is_db_alive
                    data['alive'] = is_db_alive
                else:
                    data['running'] = True
                    data['alive'] = False
            except exceptions.PGConnectionTimeout:
                raise
            except Exception:
                data['running'] = False
                data['alive'] = False
            # Explicitly update "running" to avoid dead loop
            self.state['running'] = data['running']

            if not data['alive']:
                raise RuntimeError('PostgreSQL is dead')
            data['role'] = self.get_role()
            self.role = data['role']
            data['pg_version'] = self._get_pg_version()
            data['pgdata'] = self._get_pgdata_path()
            data['opened'] = self.pgpooler('status')[1]
            data['timeline'] = self.get_data_from_control_file('Latest checkpoint.s TimeLineID', preproc=int, log=False)
            data['wal_receiver'] = self._get_wal_receiver_info()

            if data['role'] == 'primary':
                data['replics_info'] = self.get_replics_info('primary')
                data['replication_state'] = self.get_replication_state()
                data['sessions_ratio'] = self.get_sessions_ratio()
            elif data['role'] == 'replica':
                data['primary_fqdn'] = self.get_primary_fqdn()
                data['replics_info'] = self.get_replics_info('replica')

            #
            # We ask health of PostgreSQL one more time since it could die
            # while we were asking all other things here. It can lead to
            # unpredictable results.
            #
            data['alive'] = self.is_alive()
        except exceptions.PGConnectionTimeout:
            raise
        except Exception:
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())

        if data['alive']:
            try:
                with open(self._db_state_cache_path, 'w') as fobj:
                    save_data = data.copy()
                    del save_data['prev_state']
                    fobj.write(json.dumps(save_data))
            except IOError:
                logging.warning('Could not write cache file. Skipping it.')

        self.state = data
        return data

    def is_alive(self):
        return self.is_alive_and_in_terminal_state()[0]

    def is_alive_and_in_terminal_state(self):
        """
        Check that postgresql is alive.

        Raises PGConnectionTimeout when a connection attempt timed out — the caller
        is responsible for the restart policy (how many timeouts to tolerate).
        """
        try:
            # In order to check that postgresql is really alive
            # we need to check if service is running then
            # drop current connection and establish a new one
            if self.state.get('running', False):
                self.reconnect()
                res = self._exec_query('SELECT 42;').fetchone()
                if res[0] == 42:
                    return True, True
            else:
                self.state['running'] = self.is_postgresql_running()
            return False, True
        except (exceptions.PGIsShuttingDown, exceptions.PGIsStartingUp):
            return False, False
        except exceptions.PGConnectionTimeout:
            raise
        except Exception:
            for line in traceback.format_exc().split('\n'):
                logging.debug(line.rstrip())
            return False, True

    def get_role(self):
        """
        Get role of local postgresql (replica, primary or None if dead)
        """
        try:
            res = self._exec_query('SELECT pg_is_in_recovery();')
            if res is None:
                return None
            elif res.fetchone()[0]:
                return 'replica'
            else:
                return 'primary'
        except Exception:
            return None

    @helpers.return_none_on_error
    def _get_pg_version(self):
        """
        Get local postgresql version
        """
        res = self._exec_query("SHOW server_version_num")
        return int(res.fetchone()[0])

    @helpers.return_none_on_error
    def _get_pgdata_path(self):
        """
        Get local pg_data
        """
        res = self._exec_query('SHOW data_directory;').fetchone()
        return res[0]

    @helpers.return_none_on_error
    def get_replics_info(self, role) -> ReplicaInfos | None:
        """
        Get replicas from pg_stat_replication
        """
        current_lsn = {'primary': 'pg_current_wal_lsn()', 'replica': 'pg_last_wal_replay_lsn()'}
        wal_func = {
            'current_lsn': current_lsn[role],
            'diff_lsn': 'pg_wal_lsn_diff',
            'app_name': 'pg_receivewal',
            'sent_lsn': 'sent_lsn',
            'write_lsn': 'write_lsn',
            'replay_lsn': 'replay_lsn',
        }
        replay_lag = 'COALESCE(1000*EXTRACT(epoch from replay_lag), 0)::bigint AS replay_lag_msec,'
        query = """SELECT pid, application_name,
                    client_hostname, client_addr, state,
                {current_lsn}
                    AS primary_location,
                {diff_lsn}({current_lsn}, {sent_lsn})
                    AS sent_location_diff,
                {diff_lsn}({current_lsn}, {write_lsn})
                    AS write_location_diff,
                {diff_lsn}({current_lsn},
                    {replay_lsn})
                    AS replay_location_diff,
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
            replay_lag=replay_lag,
            replay_lsn=wal_func['replay_lsn'],
        )
        return self._get(query)

    @helpers.return_none_on_error
    def _get_wal_receiver_info(self):
        """
        Get wal_receiver info from pg_stat_wal_receiver
        """
        query = """SELECT pid, status, slot_name,
                   COALESCE(1000*EXTRACT(epoch FROM last_msg_receipt_time), 0)::bigint AS last_msg_receipt_time_msec,
                   conninfo FROM pg_stat_wal_receiver"""
        result = self._get(query)
        if result:
            return result[0]

    @helpers.return_none_on_error
    def get_replication_state(self):
        """
        Get replication type (sync/async)
        """
        res = self._exec_query('SHOW synchronous_standby_names;').fetchone()
        res = ('async', None) if res[0] == '' else ('sync', res[0])
        return res

    @helpers.return_none_on_error
    def get_sessions_ratio(self):
        """
        Get ratio of active sessions/max sessions (in percents)
        """
        cur = self._exec_query("SELECT count(*) FROM pg_stat_activity WHERE state!='idle';")
        cur = cur.fetchone()[0]
        max_sessions = self._exec_query('SHOW max_connections;').fetchone()[0]
        return (cur / int(max_sessions)) * 100

    @helpers.return_none_on_error
    def lwaldump(self):
        """Protected from kill -9 postgres"""
        query = """SELECT pg_wal_lsn_diff(
                lwaldump(),
                '0/00000000')::bigint"""
        return self._exec_query(query).fetchone()[0]

    @helpers.return_none_on_error
    def get_wal_receive_lsn(self):
        if self.config.use_lwaldump:
            return self.lwaldump()
        query = """SELECT pg_wal_lsn_diff(
                pg_last_wal_receive_lsn(),
                '0/00000000')::bigint"""
        return self._exec_query(query).fetchone()[0]

    def check_walsender(self, replics_info: ReplicaInfos, holder_fqdn):
        """
        Check walsender in sync state and sync holder is same
        """
        if not replics_info:
            return True
        holder_app_name = helpers.app_name_from_fqdn(holder_fqdn)
        for replica in replics_info:
            try:
                if replica['sync_state'] == 'sync' and replica['application_name'] != holder_app_name:
                    logging.warning('It seems sync replica and sync replica holder are different. Killing walsender.')
                    os.kill(int(replica['pid']), signal.SIGTERM)
                    break
            except Exception as exc:
                logging.error('Check walsender error: %s', repr(exc))
        return True

    def check_walreceiver(self):
        """
        Check if walreceiver is running using pg_stat_wal_receiver view
        """
        try:
            cur = self._exec_query('SELECT pid FROM pg_stat_wal_receiver WHERE status = \'streaming\'')
        except Exception as exc:
            logging.error('Unable to get walreceiver state: %s', repr(exc))
            return False
        return bool(cur.fetchall())

    def is_ready_for_pg_rewind(self):
        """
        Check if pg_rewind could be used on local postgresql
        """
        res = self.get_data_from_control_file('Data page checksum version', preproc=int)
        if res:
            logging.info("Checksums are enabled, host is ready for pg_rewind.")
            return True

        res = self.get_data_from_control_file('wal_log_hints setting')
        if res == 'on':
            logging.info("Checksums are disabled but wal_log_hints = on, host is ready for pg_rewind.")
            return True

        logging.error("Checksums or wal_log_hints should be enabled for pg_rewind to work properly.")
        return False

    @helpers.return_none_on_error
    def get_replay_diff(self, diff_from='0/00000000'):
        query = f"""SELECT pg_wal_lsn_diff(
                pg_last_wal_replay_lsn(),
                '{diff_from}')::bigint"""
        return self._exec_query(query).fetchone()[0]

    def get_primary_fqdn(self) -> str | None:
        # Single source for primary FQDN: runtime primary_conninfo takes priority
        # (more reliable than stale recovery.conf), recovery.conf is used as a fallback.
        try:
            primary_fqdn = helpers.extract_host(self._get_param_value('primary_conninfo'))
        except Exception as exc:
            logging.debug('Could not read runtime primary_conninfo, will fall back to recovery.conf: %s', exc)
            primary_fqdn = None
        return primary_fqdn or self.recovery_conf('get_primary')

    def recovery_conf(self, action, primary_host=None) -> str | None:
        """
        Perform recovery conf action (create, remove, get_primary)
        """
        recovery_filepath = os.path.join(self.pgdata, self.config.recovery_filepath)

        if action == 'create':
            self._plugins.run('before_populate_recovery_conf', primary_host)
            res = self._cmd_manager.generate_recovery_conf(recovery_filepath, primary_host)
            self._plugins.run('after_populate_recovery_conf', primary_host)
            return res
        elif action == 'remove':
            cmd = 'rm -f ' + recovery_filepath
            return helpers.subprocess_call(cmd)
        else:
            if os.path.exists(recovery_filepath):
                with open(recovery_filepath, 'r') as recovery_file:
                    for i in recovery_file.read().split('\n'):
                        if 'primary_conninfo' in i:
                            return helpers.extract_host(i)
            return None

    def promote(self) -> bool:
        """
        Make local postgresql primary
        """
        # TODO : potential split brain here in this case:
        # 1. We requested for switchover
        # 2. Host A was chosen to become a new primary
        # 3. Host A promote took too much time, so old primary decided to rollback switchover
        # 4. After switchover rollback and old primary returned back as a primary promote finished
        # 5. In the end we have old primary with open pooler and host A as a primary with open pooler.
        self._plugins.run('before_promote', self.conn_local, self.config)

        # We need to stop archiving WAL and resume after promote
        # to prevent wrong history file in archive in case of failure
        if not self.stop_archiving_wal():
            logging.error('ACTION-FAILED. Could not stop archiving WAL')
            return False

        # We need to resume replaying WAL before promote
        self.pg_wal_replay_resume()

        logging.info('ACTION. Starting promote')
        promoted = self._cmd_manager.promote(self.pgdata) == 0
        if promoted:
            if not self.resume_archiving_wal():
                logging.error('ACTION-FAILED. Could not resume archiving WAL')
            if self._wait_for_primary_role():
                self._plugins.run('after_promote', self.conn_local, self.config.plugins)
        return promoted

    def _wait_for_primary_role(self):
        """
        Wait until promotion succeeds
        """
        role = self.get_role()
        while role != 'primary':
            logging.info('Our role should be primary but we are now "%s".', role)
            if role is None:
                return False
            logging.info('Waiting %.1f second(s) to become primary.', self.config.iteration_timeout)
            time.sleep(self.config.iteration_timeout)
            role = self.get_role()

        return True

    def pgpooler(self, action):
        """
        Start/stop/status pooler wrapper
        """
        if action == 'stop':
            if self._get_pooler_status():
                return True
            self._plugins.run('before_close_from_load')
            res = self._cmd_manager.stop_pooler()
            after = 'after_close_from_load'
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
            self._plugins.run('before_open_for_load')
            res = self._cmd_manager.start_pooler()
            after = 'after_open_for_load'
        else:
            raise RuntimeError('Unknown pooler action: %s' % action)
        if res == 0:
            self._plugins.run(after)
            return True
        return False

    def _get_pooler_status(self) -> bool:
        result = self._cmd_manager.get_pooler_status()
        logging.debug('Pooler status: %s, %s', result, bool(result))
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

        logging.info('ACTION. Starting pg_rewind')
        res = self._cmd_manager.rewind(self.pgdata, primary_host)

        if self.config.use_replication_slots and res == 0:
            if os.path.exists('/tmp/pgconsul_replslots_backup'):
                try:
                    helpers.backup_dir('/tmp/pgconsul_replslots_backup', '%s/pg_replslot' % self.pgdata)
                except Exception:
                    logging.warning('Could not restore replication slots after rewinding. Skipping it.')
        return res

    def _get_param_value(self, param):
        cursor = self._exec_query(f'SHOW {param}')
        (value,) = cursor.fetchone()
        return value

    def _alter_system_set_param(self, param: str, value=None, reset=False) -> bool:
        def equal() -> bool:
            return self._get_param_value(param) == value

        def unequal(prev_value) -> bool:
            return self._get_param_value(param) != prev_value

        if self.conn_local is None:
            logging.error("No database connection")
            return False

        try:
            if reset:
                prev_value = self._get_param_value(param)
                logging.info(f'ACTION. Resetting {param} with ALTER SYSTEM')
                query = SQL("ALTER SYSTEM RESET {param}").format(param=Identifier(param))
                self._exec_query(query.as_string(self.conn_local))
                await_func: Callable[[], bool] = partial(unequal, prev_value)
                await_message = f'{param} is reset after reload'
            else:
                logging.info(f'ACTION. Setting {param} to {value} with ALTER SYSTEM')
                query = SQL("ALTER SYSTEM SET {param} TO %(value)s").format(param=Identifier(param))
                self._exec_query(query.as_string(self.conn_local), value=value)
                await_func = equal
                await_message = f'{param} is set to {value} after reload'
        except Exception:
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())
            return False
        reload_result = self._cmd_manager.reload_postgresql(self.pgdata)
        if reload_result:
            logging.debug(f'Reload has failed, not waiting for param {param} change')
            return False

        return helpers.await_for(await_func, self.config.postgres_timeout, await_message)

    def change_replication_type(self, synchronous_standby_names):
        return self._alter_system_set_param('synchronous_standby_names', synchronous_standby_names)

    def ensure_pooler_started(self):
        pooler_port_available, pooler_service_running = self.pgpooler('status')
        if pooler_service_running and not pooler_port_available:
            logging.warning('Service alive, but pooler not accepting connections, restarting.')
            self.pgpooler('stop')
            self.pgpooler('start')
        elif not pooler_service_running:
            logging.debug('Here we should open for load.')
            self.pgpooler('start')

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
        config = self._get_postgresql_auto_conf()
        if config.get('archive_command') == self.DISABLED_ARCHIVE_COMMAND:
            logging.info('ACTION. Archive command was disabled in postgresql.auto.conf, resetting it')
            self.resume_archiving_wal()

    def stop_archiving_wal(self):
        return self._alter_system_set_param('archive_command', self.DISABLED_ARCHIVE_COMMAND)

    def resume_archiving_wal(self):
        return self._alter_system_set_param('archive_command', reset=True)

    def stop_archiving_wal_stopped(self):
        return self._alter_system_stopped('archive_command', self.DISABLED_ARCHIVE_COMMAND)

    def stop_restoring_wal(self):
        return self._alter_system_set_param('restore_command', self.DISABLED_RESTORE_COMMAND)

    def resume_restoring_wal(self):
        return self._alter_system_set_param('restore_command', reset=True)

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

    #
    # We do it with writing to file and not with ALTER SYSTEM command since
    # PostgreSQL is stopped when this method is called.
    # We are not afraid of future rewriting postgresql.auto.conf with ALTER
    # SYSTEM command since this change is temporary.
    #
    def _alter_system_stopped(self, param, set_value):
        """
        Set param to value while PostgreSQL is stopped.
        Method should be called only with stopped PostgreSQL.
        """
        try:
            logging.info(f'ACTION. Setting {param} to {set_value} in postgresql.auto.conf')
            config = self._get_postgresql_auto_conf()
            current_file = os.path.join(self.pgdata, 'postgresql.auto.conf')
            new_file = os.path.join(self.pgdata, 'postgresql.auto.conf.new')
            old_value = config.get(param)
            if old_value == set_value:
                logging.debug(f'Param {param} already has value {set_value} in postgresql.auto.conf')
                return True
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
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())
            return False

    def checkpoint(self, query=None):
        """
        Perform checkpoint
        """
        logging.warning('ACTION. Initiating checkpoint')
        if not query:
            query = 'CHECKPOINT'
        return self._exec_without_result(query)

    def start_postgresql(self, timeout=60):
        """
        Start PG server on current host
        """
        return self._cmd_manager.start_postgresql(timeout, self.pgdata)

    def get_postgresql_status(self):
        """
        Returns PG status on current host
        """
        return self._cmd_manager.get_postgresql_status(self.pgdata)

    def is_postgresql_running(self) -> bool:
        """Returns True if PostgreSQL process is running (systemctl status == 0)."""
        return self.get_postgresql_status() == 0

    def stop_postgresql(self, timeout=60, wait=True):
        """
        Stop PG server on current host

        If synchronous replication is ON, but sync replica is dead, then we aren't able to stop PG.
        """
        return self._cmd_manager.stop_postgresql(timeout, self.pgdata, wait=wait)

    def create_replication_slots(self, slots: list[str], verbose=True):
        if len(slots) == 0:
            return True
        logging.info('Creating slots: %s', slots)
        current = self.get_replication_slots()
        for slot in slots:
            if current and slot in current:
                if verbose:
                    logging.debug('Slot %s already exists.', slot)
                continue
            if not self._create_replication_slot(slot):
                return False
        return True

    def drop_replication_slots(self, slots, verbose=True):
        if len(slots) == 0:
            return True
        logging.info('ACTION. Dropping slots: %s', slots)
        current = self.get_replication_slots()
        for slot in slots:
            if current is not None and slot not in current:
                if verbose:
                    logging.debug('Slot %s does not exist.', slot)
                continue
            if not self._drop_replication_slot(slot):
                return False
        return True

    def is_replaying_wal(self, check_time):
        prev_replay_diff = self.get_replay_diff()
        time.sleep(check_time)
        replay_diff = self.get_replay_diff()
        return prev_replay_diff < replay_diff

    def pg_wal_replay_pause(self) -> bool:
        try:
            if self._disable_wal_receiver():
                self._pg_wal_replay("pause")
        except psycopg2.errors.ObjectNotInPrerequisiteState as exc:
            # pg_wal_replay_pause() cannot be executed after promotion is triggered
            # so we just leave iteration
            logging.error('Could not replay pause. %s', str(exc))
            return False
        except Exception as exc:
            logging.error('Could not replay pause. Unexpected error.')
            logging.exception(exc)
            return False
        return True

    def pg_wal_replay_resume(self):
        if self.is_wal_replay_paused():
            logging.debug('WAL replay is paused. So we resume it')
            self._pg_wal_replay("resume")

    def is_wal_replay_paused(self):
        return self._exec_query('SELECT pg_is_wal_replay_paused();').fetchone()[0]

    def ensure_replaying_wal(self):
        self.enable_wal_receiver_if_disabled()
        self.pg_wal_replay_resume()

    def _disable_wal_receiver(self):
        """
        Disable walreceiver
        """
        try:
            if self._exec_query('SHOW primary_conninfo;').fetchone()[0] == '':
                logging.debug('walreceiver is already disabled')

            logging.info('ACTION. Disabling walreceiver.')

            self._alter_system_set_param('primary_conninfo', '')
            self.reload()
        except Exception as exc:
            logging.error('Could not disable walreceiver. Unexpected error.')
            logging.exception(exc)

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

    def _wal_receiver_timeout(self) -> int:
        cursor = self._exec_query("SELECT setting::int/1000 from pg_settings where name = 'wal_receiver_timeout';")
        return int(cursor.fetchone()[0])

    def is_wal_receiver_disabled(self) -> bool:
        if self._get_param_value('primary_conninfo') == '':
            logging.debug('walreceiver is disabled')
            return True

        logging.debug('walreciever is enabled')
        return False

    def terminate_backend(self, pid):
        """
        Send sigterm to backend by pid
        """
        # Note that pid could be already dead by this moment
        # So we do not check result
        self._exec_without_result(f'SELECT pg_terminate_backend({pid})')

    def _pg_wal_replay(self, pause_or_resume):
        logging.info('ACTION. WAL replay: %s', pause_or_resume)
        self._exec_query(f'SELECT pg_wal_replay_{pause_or_resume}();')

    def check_extension_installed(self, name):
        cur = self._exec_query(f"SELECT * FROM pg_extension WHERE extname = '{name}';")
        result = cur.fetchall()
        return len(result) == 1

    def reload(self):
        return not bool(self._cmd_manager.reload_postgresql(self.pgdata))
