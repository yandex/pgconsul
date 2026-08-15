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
import struct
import time
from typing import Callable

import psycopg2
from psycopg2.sql import SQL, Identifier

from . import helpers
from .command_manager import CommandManager
from .exceptions import PostgresConnectionError
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
    append_primary_conn_string: str = ''
    wals_to_upload: int = 20

    @property
    def db_state_path(self):
        return '%s/.pgconsul_db_state.cache' % self.working_dir


class Postgres(object):
    """
    Postgres class
    """

    DISABLED_ARCHIVE_COMMAND = '/bin/false'
    DISABLED_RESTORE_COMMAND = '/bin/false'

    def __init__(self, config: PostgresConfig, cmd_manager: CommandManager):
        self.config = config
        self._cmd_manager = cmd_manager
        self.conn_local: psycopg2.extensions.connection | None = None
        self._wals_to_upload = self.config.wals_to_upload
        self.role: str | None = None
        self.pgdata = ''
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

    def get_timeline(self):
        return self._get_data_from_control_file('Latest checkpoint.s TimeLineID', preproc=int, log=False)

    def get_database_cluster_state(self):
        return self._get_data_from_control_file('Database cluster state')

    def get_data_page_checksum_version(self):
        return self._get_data_from_control_file('Data page checksum version', preproc=int)

    def get_wal_log_hints_settings(self):
        return self._get_data_from_control_file('wal_log_hints setting')

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
        Raises KeyError if cache is missing/invalid and DB is dead.
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
        if not prev_state:
            raise KeyError('DB state cache file is missing or invalid')
        self.role = prev_state['role']
        self.pgdata = prev_state['pgdata']
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

    def get_sessions_ratio(self):
        """Get ratio of active sessions/max sessions (in percents).

        Raises:
            PostgresConnectionError: if the DB connection is lost.
        """
        cur = self._exec_query("SELECT count(*) FROM pg_stat_activity WHERE state!='idle';")
        cur = cur.fetchone()[0]
        max_sessions = self._exec_query('SHOW max_connections;').fetchone()[0]
        return (cur / int(max_sessions)) * 100

    def lwaldump(self):
        """Protected from kill -9 postgres"""
        query = """SELECT pg_wal_lsn_diff(
                lwaldump(),
                '0/00000000')::bigint"""
        return self._exec_query(query).fetchone()[0]

    def get_wal_receive_lsn(self):
        """Get WAL receive LSN as an integer offset.

        When use_lwaldump=True, lwaldump() crashes the DB session once the
        walreceiver has been disabled (primary_conninfo cleared). In that case
        we reconnect and fall back to pg_last_wal_receive_lsn() which works
        without an active walreceiver (MDB-41951).

        Only PostgresConnectionError is caught — _exec_query translates all
        psycopg2.OperationalError (the only lwaldump failure mode) into it.
        Other errors (e.g. ProgrammingError) indicate a bug and must propagate.

        Raises:
            PostgresConnectionError: if the DB connection is lost and the
                fallback also fails.
        """
        if self.config.use_lwaldump:
            try:
                return self.lwaldump()
            except PostgresConnectionError:
                logging.warning('lwaldump() crashed — falling back to pg_last_wal_receive_lsn')
                self.reconnect()
                return self._pg_last_wal_receive_lsn()
        return self._pg_last_wal_receive_lsn()

    def _pg_last_wal_receive_lsn(self):
        """Read LSN via pg_last_wal_receive_lsn (works after walreceiver disabled)."""
        query = """SELECT pg_wal_lsn_diff(
                pg_last_wal_receive_lsn(),
                '0/00000000')::bigint"""
        return self._exec_query(query).fetchone()[0]

    def check_walsender(self, replics_info: ReplicaInfos, holder_fqdn):
        """Check walsender in sync state and sync holder is same."""
        if not replics_info:
            return True
        holder_app_name = helpers.app_name_from_fqdn(holder_fqdn)
        for replica in replics_info:
            if replica['sync_state'] == 'sync' and replica['application_name'] != holder_app_name:
                logging.warning('It seems sync replica and sync replica holder are different. Killing walsender.')
                try:
                    os.kill(int(replica['pid']), signal.SIGTERM)
                except (ValueError, ProcessLookupError, PermissionError) as exc:
                    logging.error('Failed to kill walsender: %s', repr(exc))
                break
        return True

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

    def get_primary_fqdn(self) -> str | None:
        # Single source for primary FQDN: runtime primary_conninfo takes priority
        # (more reliable than stale recovery.conf), recovery.conf is used as a fallback.
        # PostgresConnectionError from _get_param_value propagates to run_iteration().
        primary_fqdn = helpers.extract_host(self._get_param_value('primary_conninfo'))
        logging.debug('Primary FQDN: %s', primary_fqdn)
        return primary_fqdn or self.recovery_conf('get_primary')

    def recovery_conf(self, action, primary_host=None) -> str | None:
        """
        Perform recovery conf action (create, remove, get_primary)
        """
        recovery_filepath = os.path.join(self.pgdata, self.config.recovery_filepath)

        if action == 'create':
            res = self._cmd_manager.generate_recovery_conf(recovery_filepath, primary_host)
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
                self._upload_wals()
        return promoted

    def _wait_for_primary_role(self):
        """
        Wait until promotion succeeds.

        Post-promote critical section (ADR-0002 §2): promote() has already run.
        get_role() raises PostgresConnectionError on connection loss (ADR-0001);
        we absorb it here (return False, skip WAL upload) rather than propagate
        through promote() and mislead callers.
        """
        try:
            role = self.get_role()
            while role != 'primary':
                logging.info('Our role should be primary but we are now "%s".', role)
                logging.info('Waiting %.1f second(s) to become primary.', self.config.iteration_timeout)
                time.sleep(self.config.iteration_timeout)
                role = self.get_role()
        except PostgresConnectionError:
            logging.warning('Lost DB connection while waiting for primary role; skipping WAL upload', exc_info=True)
            return False

        return True

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
                helpers.subprocess_call(cmd)

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

        logging.info('ACTION. Starting pg_rewind')
        res = self._cmd_manager.rewind(self.pgdata, primary_host)

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
            logging.exception('Error writing PostgreSQL config file')
            return False

    def checkpoint(self, query=None):
        """Perform checkpoint.

        Raises:
            PostgresConnectionError: if the DB connection is lost (propagates
                from _exec_without_result to the caller).
        """
        logging.info('ACTION. Initiating checkpoint')
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

    def stop_postgresql(self, timeout=60, wait=True):
        """
        Stop PG server on current host

        If synchronous replication is ON, but sync replica is dead, then we aren't able to stop PG.
        """
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
        try:
            if self._exec_query('SHOW primary_conninfo;').fetchone()[0] != '':
                logging.info('ACTION. Disabling walreceiver.')
                self._alter_system_set_param('primary_conninfo', '')
                if not self.reload():
                    logging.error('Could not reload PostgreSQL after disabling walreceiver.')
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
        except Exception as exc:
            logging.error('Could not disable walreceiver. Unexpected error.')
            logging.exception(exc)
            return False

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
        return self._get_param_value('primary_conninfo') == ''

    def _pg_wal_replay(self, pause_or_resume):
        logging.info('ACTION. WAL replay: %s', pause_or_resume)
        self._exec_query(f'SELECT pg_wal_replay_{pause_or_resume}();')

    def check_extension_installed(self, name):
        cur = self._exec_query(f"SELECT * FROM pg_extension WHERE extname = '{name}';")
        result = cur.fetchall()
        return len(result) == 1

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

        try:
            conn = psycopg2.connect('host=%s %s %s' % (primary, append, ensure_connect_primary))
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute('SELECT 42')
            result = cur.fetchone()
            if result and result[0] == 42:
                return False
            return True
        except Exception as err:
            logging.debug('%s while trying to check primary health.', str(err))
            return True

    def reload(self):
        return not bool(self._cmd_manager.reload_postgresql(self.pgdata))


def build_postgres_config(config: RawConfigParser) -> PostgresConfig:
    """Build PostgresConfig from the 'global' section of an INI config."""
    return PostgresConfig(
        conn_string=config.get('global', 'local_conn_string'),
        use_lwaldump=config.getboolean('global', 'use_lwaldump') or config.getboolean('global', 'quorum_commit'),
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
    )


def create_postgres(config: RawConfigParser, cmd_manager: CommandManager) -> Postgres:
    """Factory: build a Postgres instance from config and a CommandManager."""
    return Postgres(config=build_postgres_config(config), cmd_manager=cmd_manager)
