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
                cur.execute("SHOW server_version_num")
                pg_version = cur.fetchone()[0]
                queries = {"pgdata": "SHOW data_directory;", "archive_command": "SHOW archive_command;"}
                if int(pg_version) >= 100000:
                    queries["wal_location"] = "SELECT pg_walfile_name(pg_current_wal_lsn())"
                    wal_dir = 'pg_wal'
                    logging.info(queries)
                else:
                    queries["wal_location"] = "SELECT pg_xlogfile_name(pg_current_xlog_location())"
                    wal_dir = 'pg_xlog'
                cur.execute(queries['wal_location'])
                current_wal = cur.fetchone()[0]
                cur.execute(queries['archive_command'])
                archive_command = cur.fetchone()[0]
                # wal-g upload in parallel by default
                if 'envdir' in archive_command:
                    archive_command = "/usr/bin/envdir /etc/wal-g/envdir sh -c 'WALG_UPLOAD_CONCURRENCY=1 {}'".format(
                        archive_command.replace('/usr/bin/envdir /etc/wal-g/envdir ', '')
                    )
                cur.execute(queries['pgdata'])
                pgdata = cur.fetchone()[0]
            wals = os.listdir('{pgdata}/{wal_dir}/'.format(pgdata=pgdata, wal_dir=wal_dir))
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

            wals_count = config.get('wals_to_upload', 20)
            for wal in wals_to_upload[-wals_count:]:
                path = '{pgdata}/{wal_dir}/{wal}'.format(pgdata=pgdata, wal_dir=wal_dir, wal=wal)
                cmd = archive_command.replace('%p', path).replace('%f', wal)
                helpers.subprocess_call(cmd)
        except Exception as error_message:
            logging.info(error_message)
