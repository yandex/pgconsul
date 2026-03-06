#!/usr/bin/env python3
"""
Simple PostgreSQL client that connects to the current master using target_session_attrs
and runs periodic INSERTs. Used in functional tests to verify client connectivity
through failovers and switchovers.
"""
import os
import sys
import time
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def main():
    hosts = os.environ.get('PGHOST', 'postgresql1,postgresql2,postgresql3')
    port = os.environ.get('PGPORT', '6432')
    database = os.environ.get('PGDATABASE', 'postgres')
    user = os.environ.get('PGUSER', 'postgres')
    password = os.environ.get('PGPASSWORD', '')
    interval = float(os.environ.get('WOODPECKER_INTERVAL', '0.1'))

    # Build multi-host connection string with target_session_attrs=primary
    # to connect only to the current master (libpq will try hosts until it finds one)
    conninfo = (
        f"host={hosts} port={port} dbname={database} user={user} "
        f"password={password} target_session_attrs=primary connect_timeout=1"
    )

    conn = None
    counter = 0
    while True:
        try:
            conn = psycopg2.connect(conninfo)
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS woodpecker_inserts "
                    "(id SERIAL PRIMARY KEY, ts TIMESTAMPTZ DEFAULT now(), n INT)"
                )
            while True:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO woodpecker_inserts (n) VALUES (%s)",
                        (counter,)
                    )
                time.sleep(interval)
                counter += 1
        except Exception as e:
            print(f"Insert failed: {e}", file=sys.stderr)
        finally:
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(interval)

if __name__ == '__main__':
    main()
