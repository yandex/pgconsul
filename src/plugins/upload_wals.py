from pgconsul import helpers
from pgconsul import plugin
from pgconsul.types import PluginsConfig

import os
import struct
import logging


class UploadWals(plugin.PostgresPlugin):
    def after_promote(self, conn, config: PluginsConfig):
        # We should finish promote if upload_wals is fail
        try:
            with conn.cursor() as cur:
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
            wals_to_upload = []
            for wal in wals:
                if wal < current_wal:
                    try:
                        logging.info(wal)
                        struct.unpack('>3I', bytearray.fromhex(wal))
                        wals_to_upload.append(wal)
                    except (struct.error, ValueError):
                        continue

            wals_count = int(config.get('wals_to_upload', 20))
            for wal in wals_to_upload[-wals_count:]:
                path = '{pgdata}/pg_wal/{wal}'.format(pgdata=pgdata, wal=wal)
                cmd = archive_command.replace('%p', path).replace('%f', wal)
                helpers.subprocess_call(cmd)
        except Exception as error_message:
            logging.info(error_message)
