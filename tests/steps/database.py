#!/usr/bin/env python
# -*- coding: utf-8 -*-

# We are using deepcopy in every data return to help
# python run destructor on connection to prevent
# database connection leakage
from copy import deepcopy

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.errors import DuplicateObject
import select


class Postgres(object):
    def __init__(
        self, host='localhost', dbname='postgres', user='postgres', port='5432', autocommit=True, async_=False
    ):
        self.conn = None
        try:
            self.conn = psycopg2.connect(user=user, host=host, port=port, dbname=dbname, async_=async_)

            if async_:
                self.wait(self.conn)
            else:
                self.conn.autocommit = autocommit
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            # Sometimes we leave connections closed on our side
            # but open in PostgreSQL (some issue with docker?)
            # This query ensures that none of such connections
            # will leak
            if dbname != 'pgbouncer':
                self.cursor.execute(
                    """-- noinspection SqlResolveForFile

                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE client_addr IS NOT NULL
                    AND state = 'idle'
                    AND pid != pg_backend_pid()
                """
                )
                if async_:
                    self.wait(self.cursor.connection)
        except psycopg2.OperationalError as error:
            assert False, error
        except psycopg2.DatabaseError as error:
            assert False, error

    def __del__(self):
        if self.conn:
            self.conn.close()

    def ping(self):
        try:
            self.cursor.execute(
                """
                SELECT true AS ping
            """
            )
            return deepcopy(self.cursor.fetchone())['ping']
        except psycopg2.OperationalError:
            return False

    def pg_sleep(self, timeout=1):
        self.cursor.execute(
            """
            SELECT pg_sleep({timeout})
        """.format(
                timeout=timeout
            )
        )
        return None

    def is_primary(self):
        self.cursor.execute(
            """
            SELECT pg_is_in_recovery() AS in_recovery
        """
        )
        return self.cursor.fetchone()['in_recovery'] is False

    def get_replication_stat(self):
        self.cursor.execute(
            """-- noinspection SqlResolveForFile
            SELECT * FROM pg_stat_replication
        """
        )
        return deepcopy(self.cursor.fetchall())

    def get_replication_state(self):
        self.cursor.execute('SHOW synchronous_standby_names;')
        res = self.cursor.fetchone()['synchronous_standby_names']
        res = ('async', None) if res == '' else ('sync', res)
        return res

    def get_walreceiver_stat(self):
        self.cursor.execute(
            """-- noinspection SqlResolveForFile
            SELECT * FROM pg_stat_wal_receiver
        """
        )
        return deepcopy(self.cursor.fetchone())

    def get_config_option(self, option):
        self.cursor.execute(
            """
            SELECT current_setting(%(option)s) AS opt
        """,
            {'option': option},
        )
        return deepcopy(self.cursor.fetchone())['opt']

    def create_replication_slot(self, slot_name):
        try:
            self.cursor.execute(
                """
                SELECT pg_create_physical_replication_slot(%(name)s)
            """,
                {'name': slot_name},
            )
            return deepcopy(self.cursor.fetchone())
        except DuplicateObject:
            return [True]

    def get_replication_slots(self):
        self.cursor.execute(
            """-- noinspection SqlResolveForFile
            SELECT * FROM pg_replication_slots
        """
        )
        return deepcopy(self.cursor.fetchall())

    def drop_replication_slot(self, slot_name):
        self.cursor.execute(
            """
            SELECT pg_drop_replication_slot(%(name)s)
        """,
            {'name': slot_name},
        )
        return deepcopy(self.cursor.fetchone())

    def switch_and_get_wal(self):
        self.cursor.execute(
            """
            SELECT pg_walfile_name(pg_switch_wal())
        """
        )
        return deepcopy(self.cursor.fetchone())['pg_walfile_name']

    def disable_archiving(self):
        self.cursor.execute(
            """
            ALTER SYSTEM SET archive_command = '/bin/true'
        """
        )
        self.cursor.execute(
            """
            SELECT pg_reload_conf()
        """
        )
        return deepcopy(self.cursor.fetchone())

    def get_start_time(self):
        self.cursor.execute(
            """
            SELECT pg_postmaster_start_time() AS time
        """
        )
        return deepcopy(self.cursor.fetchone())['time']

    def is_wal_replay_paused(self):
        self.cursor.execute(
            """
            SELECT pg_is_wal_replay_paused() as paused
        """
        )
        return deepcopy(self.cursor.fetchone())['paused']

    def wal_replay_pause(self):
        self.cursor.execute(
            """
            SELECT pg_wal_replay_pause()
        """
        )

    def wait(self, conn):
        while True:
            state = conn.poll()
            if state == psycopg2.extensions.POLL_OK:
                break
            elif state == psycopg2.extensions.POLL_WRITE:
                select.select([], [conn.fileno()], [])
            elif state == psycopg2.extensions.POLL_READ:
                select.select([conn.fileno()], [], [])
            else:
                raise psycopg2.OperationalError("poll() returned %s" % state)
