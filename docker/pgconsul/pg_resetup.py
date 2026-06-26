#!/usr/bin/env python3
"""
pg_resetup.py — daemon that watches for .pgconsul_rewind_fail.flag.

Runs as a long-lived process under supervisor.  Every POLL_INTERVAL seconds
it checks (under a file lock) whether the rewind-fail flag exists.  When the
flag is detected the daemon rebuilds the local database via pg_basebackup
from the current primary, removes the flag, and restarts pgconsul.

The primary is discovered by iterating over database hosts obtained from
the local pgconsul status file and querying pg_is_in_recovery().
"""

import fcntl
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import time

WORKING_DIR = '/tmp'
FLAG_FILE = os.path.join(WORKING_DIR, '.pgconsul_rewind_fail.flag')
STATUS_FILE = os.path.join(WORKING_DIR, 'pgconsul.status')
LOCK_FILE = os.path.join(WORKING_DIR, '.pg_resetup.lock')

PG_MAJOR = os.environ.get('PG_MAJOR', '13')
DEFAULT_PGDATA = f'/var/lib/postgresql/{PG_MAJOR}/main'

REPL_USER = 'repl'

POLL_INTERVAL = 30
PG_START_TIMEOUT = 60

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [pg_resetup] %(levelname)s %(message)s',
    stream=sys.stdout,
)
log = logging.getLogger('pg_resetup')

# Global flag for graceful shutdown
_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info('Received signal %s, shutting down...', signum)
    _shutdown = True


def run_cmd(cmd: list[str], *, check: bool = True, timeout: int = 300, **kwargs) -> subprocess.CompletedProcess:
    """Run a command and return the CompletedProcess result."""
    log.debug('Running: %s', ' '.join(cmd))
    return subprocess.run(cmd, capture_output=True, text=True, check=check, timeout=timeout, **kwargs)


def run_as_postgres(cmd: str, *, check: bool = True, timeout: int = 300) -> subprocess.CompletedProcess:
    """Run a shell command as the postgres user via ``su``."""
    return run_cmd(['su', '-', 'postgres', '-c', cmd], check=check, timeout=timeout)


def supervisorctl(action: str, service: str) -> None:
    """Execute a supervisorctl action, ignoring errors."""
    try:
        run_cmd(['supervisorctl', action, service], check=False, timeout=30)
    except Exception:
        log.warning('supervisorctl %s %s failed, ignoring', action, service)


# ---------------------------------------------------------------------------
# Host discovery
# ---------------------------------------------------------------------------

def get_hosts_from_status_file() -> list[str]:
    """Extract database host list from the local pgconsul status file.

    The status file is written by pgconsul (helpers.write_status_file) and
    contains JSON with the structure::

        {
          "zk_state": {
            "synchronous_standby_names": { "<host>": ..., ... },
            ...
          },
          ...
        }

    Keys of ``synchronous_standby_names`` are the FQDNs of all cluster
    members.
    """
    if not os.path.isfile(STATUS_FILE):
        log.warning('Status file %s not found', STATUS_FILE)
        return []

    try:
        with open(STATUS_FILE) as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, OSError) as exc:
        log.warning('Failed to read status file: %s', exc)
        return []

    zk_state = data.get('zk_state') or {}
    ssn = zk_state.get('synchronous_standby_names') or {}
    hosts = list(ssn.keys())
    if hosts:
        return hosts

    log.warning('No hosts found in synchronous_standby_names')
    return []


def find_primary(hosts: list[str]) -> str | None:
    """Iterate over *hosts* and return the one that is the primary.

    Connects to each host on port 5432 and checks ``pg_is_in_recovery()``.
    The host where this returns ``false`` is the primary.
    """
    for host in hosts:
        try:
            result = run_cmd(
                [
                    'psql',
                    f'host={host} port=5432 dbname=postgres user={REPL_USER} connect_timeout=5',
                    '-At',
                    '-c',
                    'SELECT pg_is_in_recovery();',
                ],
                check=False,
                timeout=10,
            )
            if result.returncode == 0 and result.stdout.strip() == 'f':
                return host
        except Exception as exc:
            log.debug('Failed to query %s: %s', host, exc)
    return None


# ---------------------------------------------------------------------------
# PGDATA helpers
# ---------------------------------------------------------------------------


def get_slot_name() -> str:
    """Return the replication slot name for this host.

    Matches the logic in ``gen_rec_conf.sh``: the FQDN with dots and
    dashes replaced by underscores.
    """
    fqdn = socket.getfqdn()
    return fqdn.replace('.', '_').replace('-', '_')


# ---------------------------------------------------------------------------
# Rebuild
# ---------------------------------------------------------------------------

def stop_services(pgdata: str) -> None:
    """Stop pgconsul, pgbouncer and postgresql."""
    log.info('Stopping pgconsul and pgbouncer...')
    supervisorctl('stop', 'pgconsul')
    supervisorctl('stop', 'pgbouncer')

    log.info('Stopping postgresql...')
    run_cmd(
        ['pg_ctlcluster', PG_MAJOR, 'main', 'stop', '--', '-m', 'immediate'],
        check=False,
        timeout=60,
    )
    # Belt-and-suspenders: try pg_ctl directly
    run_as_postgres(
        f'/usr/bin/postgresql/pg_ctl stop -m immediate -D {pgdata}',
        check=False,
        timeout=60,
    )


def clean_pgdata(pgdata: str) -> None:
    """Remove all contents of PGDATA."""
    log.info('Cleaning %s...', pgdata)
    for entry in os.listdir(pgdata):
        path = os.path.join(pgdata, entry)
        if os.path.isdir(path):
            subprocess.run(['rm', '-rf', path], check=True)
        else:
            os.remove(path)


def recreate_replication_slot(primary: str, slot_name: str) -> None:
    """Drop and recreate the replication slot on the primary."""
    log.info('Recreating replication slot %s on %s...', slot_name, primary)
    connstr = f'host={primary} port=5432 dbname=postgres user={REPL_USER}'
    run_cmd(
        ['psql', connstr, '-c', f"SELECT pg_drop_replication_slot('{slot_name}');"],
        check=False,
        timeout=30,
    )
    run_cmd(
        ['psql', connstr, '-c', f"SELECT pg_create_physical_replication_slot('{slot_name}');"],
        check=False,
        timeout=30,
    )


def run_basebackup(primary: str, pgdata: str) -> None:
    """Run pg_basebackup from the primary."""
    log.info('Running pg_basebackup from %s...', primary)
    connstr = f'host={primary} port=5432 dbname=postgres user={REPL_USER}'
    run_as_postgres(
        f'pg_basebackup --pgdata={pgdata} --wal-method=fetch --dbname="{connstr}"',
        timeout=600,
    )


def generate_recovery_conf(primary: str, pgdata: str) -> None:
    """Generate recovery configuration (recovery.conf + standby.signal)."""
    log.info('Generating recovery configuration...')
    conf_dir = os.path.join(pgdata, 'conf.d')
    os.makedirs(conf_dir, exist_ok=True)
    # Ensure postgres owns it
    run_cmd(['chown', 'postgres:postgres', conf_dir], check=False)
    recovery_conf_path = os.path.join(conf_dir, 'recovery.conf')
    run_as_postgres(f'/usr/local/bin/gen_rec_conf.sh {primary} {recovery_conf_path}')


def start_postgresql() -> None:
    """Start postgresql and wait for it to become ready."""
    log.info('Starting postgresql...')
    run_cmd(['pg_ctlcluster', PG_MAJOR, 'main', 'start'], check=True, timeout=60)

    log.info('Waiting for postgresql to become ready...')
    for attempt in range(PG_START_TIMEOUT):
        result = run_as_postgres(
            "psql --set ON_ERROR_STOP=1 -c 'SELECT 1'",
            check=False,
            timeout=10,
        )
        if result.returncode == 0:
            log.info('PostgreSQL is ready')
            return
        time.sleep(1)

    raise RuntimeError(f'PostgreSQL did not become ready within {PG_START_TIMEOUT} seconds')


def rebuild_from_primary(primary: str, pgdata: str) -> None:
    """Full rebuild sequence: stop → clean → basebackup → recover → start."""
    slot_name = get_slot_name()
    log.info(
        'Rebuilding from primary=%s, pgdata=%s, slot=%s',
        primary,
        pgdata,
        slot_name,
    )

    stop_services(pgdata)
    clean_pgdata(pgdata)
    recreate_replication_slot(primary, slot_name)
    run_basebackup(primary, pgdata)
    generate_recovery_conf(primary, pgdata)
    start_postgresql()


# ---------------------------------------------------------------------------
# Single iteration
# ---------------------------------------------------------------------------

def check_and_resetup() -> None:
    """One iteration: check the flag under lock and rebuild if needed."""
    # Acquire an exclusive file lock (non-blocking)
    lock_fd = open(LOCK_FILE, 'w')
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log.info('Another instance holds the lock, skipping this cycle')
        return
    finally:
        # If we didn't get the lock, close and return
        pass

    try:
        # Check flag under lock
        if not os.path.isfile(FLAG_FILE):
            return

        log.info('Rewind fail flag detected, attempting rebuild...')

        # Get host list
        hosts = get_hosts_from_status_file()
        if not hosts:
            log.error('Could not determine host list from status file')
            return
        log.info('Cluster hosts: %s', hosts)

        # Find the primary
        primary = find_primary(hosts)
        if not primary:
            log.error('Could not find a primary among the cluster hosts')
            return
        log.info('Found primary: %s', primary)

        # Determine PGDATA
        pgdata = DEFAULT_PGDATA
        log.info('Using PGDATA: %s', pgdata)

        # Rebuild
        rebuild_from_primary(primary, pgdata)

        # Remove the rewind fail flag (must happen before starting pgconsul,
        # because pgconsul checks for this flag on startup and exits if found).
        log.info('Rebuild successful, removing rewind fail flag')
        os.remove(FLAG_FILE)

        # Start pgconsul
        log.info('Starting pgconsul...')
        supervisorctl('start', 'pgconsul')

        log.info('pg_resetup completed successfully')
    except Exception:
        log.exception('Rebuild failed, will retry on next cycle')
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    log.info('pg_resetup daemon started (poll interval: %ds)', POLL_INTERVAL)

    while not _shutdown:
        try:
            check_and_resetup()
        except Exception:
            log.exception('Unexpected error in main loop')

        # Sleep in small increments so we can react to SIGTERM quickly
        for _ in range(POLL_INTERVAL):
            if _shutdown:
                break
            time.sleep(1)

    log.info('pg_resetup daemon stopped')


if __name__ == '__main__':
    main()
