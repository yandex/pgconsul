"""
WAL uploader for uploading WAL files after promote.
"""
from pgconsul import helpers

import os
import struct
import logging


class WALUploader:
    """
    Handles uploading of WAL files after promote operation.
    """

    def __init__(self, config, conn):
        """
        Initialize WAL uploader.

        Args:
            config: Configuration object with plugins settings
            conn: Database connection
        """
        self._config = config
        self._conn = conn
        self._wals_to_upload = 20
        if hasattr(config, 'plugins') and config.plugins.get('wals_to_upload'):
            self._wals_to_upload = int(config.plugins.get('wals_to_upload', 20))

    def after_promote(self):
        """
        Upload WAL files that were not archived during promote.
        """
        # We should finish promote if upload_wals fails
        try:
            wals_to_upload = self._wals_to_upload

            with self._conn.cursor() as cur:
                cur.execute("SELECT pg_walfile_name(pg_current_wal_lsn())")
                current_wal = cur.fetchone()[0]
                cur.execute("SHOW archive_command")
                archive_command = cur.fetchone()[0]
                # wal-g upload in parallel by default
                if 'envdir' in archive_command:
                    archive_command = "/usr/bin/envdir /etc/wal-g/envdir sh -c 'WALG_UPLOAD_CONCURRENCY=1 {}'".format(
                        archive_command.replace('/usr/bin/envdir /etc/wal-g/envdir ', '')
                    )
                cur.execute("SHOW data_directory")
                pgdata = cur.fetchone()[0]
            wals = os.listdir('{pgdata}/pg_wal/'.format(pgdata=pgdata))
            wals.sort()
            wals_to_upload_list = []
            for wal in wals:
                if wal < current_wal:
                    try:
                        logging.info(wal)
                        struct.unpack('>3I', bytearray.fromhex(wal))
                        wals_to_upload_list.append(wal)
                    except (struct.error, ValueError):
                        continue

            for wal in wals_to_upload_list[-wals_to_upload:]:
                path = '{pgdata}/pg_wal/{wal}'.format(pgdata=pgdata, wal=wal)
                cmd = archive_command.replace('%p', path).replace('%f', wal)
                helpers.subprocess_call(cmd)
        except Exception as error_message:
            logging.info(error_message)
