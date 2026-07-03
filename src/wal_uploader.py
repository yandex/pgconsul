"""
WAL uploader for uploading WAL files after promote.
"""
from . import helpers

import os
import struct
import logging


class WALUploader:
    """
    Handles uploading of WAL files after promote operation.
    """

    def __init__(self, config):
        """
        Initialize WAL uploader.

        Args:
            config: Configuration object with plugins settings
            conn: Database connection
        """
        self._config = config
        self._wals_to_upload = 20
        if hasattr(config, 'plugins') and config.plugins.get('wals_to_upload'):
            self._wals_to_upload = int(config.plugins.get('wals_to_upload', 20))
        logging.debug(f"WALUploader initialized with wals_to_upload={self._wals_to_upload}")

    def upload(self, conn):
        """
        Upload WAL files that were not archived during promote.

        Args:
            conn: Database connection to use for queries
        """
        logging.info("Starting WAL upload after promote")
        # We should finish promote if upload_wals fails
        try:
            wals_to_upload = self._wals_to_upload
            logging.debug(f"Will upload up to {wals_to_upload} WAL files")

            with conn.cursor() as cur:
                cur.execute("SELECT pg_walfile_name(pg_current_wal_lsn())")
                current_wal = cur.fetchone()[0]
                logging.info(f"Current WAL file: {current_wal}")

                cur.execute("SHOW archive_command")
                archive_command = cur.fetchone()[0]
                logging.debug(f"Original archive_command: {archive_command}")

                # wal-g upload in parallel by default
                if 'envdir' in archive_command:
                    archive_command = "/usr/bin/envdir /etc/wal-g/envdir sh -c 'WALG_UPLOAD_CONCURRENCY=1 {}'".format(
                        archive_command.replace('/usr/bin/envdir /etc/wal-g/envdir ', '')
                    )
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
            logging.error(f"WAL upload failed with error: {error_message}", exc_info=True)
