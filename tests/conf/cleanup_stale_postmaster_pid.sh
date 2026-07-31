#!/bin/sh
# Remove stale $PGDATA/postmaster.pid left after abrupt container kill.
# Usage: cleanup_stale_postmaster_pid.sh <pgdata>

set -e

if [ -z "$1" ]; then
    echo "usage: $0 <pgdata>" >&2
    exit 1
fi

pgdata=$1
pidfile="${pgdata}/postmaster.pid"

if [ ! -f "$pidfile" ]; then
    exit 0
fi

pid=$(head -n 1 "$pidfile" 2>/dev/null || true)

# Non-numeric / empty PID — treat as stale.
case "$pid" in
    ''|*[!0-9]*)
        echo "Removing invalid postmaster.pid in ${pgdata} (pid='${pid}')" >&2
        rm -f "$pidfile"
        rm -f /var/run/postgresql/.s.PGSQL.5432.lock
        rm -f /var/run/postgresql/postmaster.pid
        sleep 1
        exit 0
        ;;
esac

if [ ! -d "/proc/${pid}" ]; then
    echo "Removing stale postmaster.pid in ${pgdata} (pid ${pid} is not running)" >&2
    rm -f "$pidfile"
    rm -f /var/run/postgresql/.s.PGSQL.5432.lock
    rm -f /var/run/postgresql/postmaster.pid
    sleep 1
    exit 0
fi

# Postmaster cmdline looks like ".../postgres -D <pgdata>"; backends look like "postgres: ...".
cmdline=$(tr '\0' ' ' < "/proc/${pid}/cmdline" 2>/dev/null || true)
case "$cmdline" in
    *'/postgres '*|*/postgres|*/postmaster*|*'postmaster '*)
        case "$cmdline" in
            *': '*)
                ;;
            *)
                exit 0
                ;;
        esac
        ;;
esac

echo "Removing stale postmaster.pid in ${pgdata} (pid ${pid} is not a postmaster: '${cmdline}')" >&2
rm -f "$pidfile"
rm -f /var/run/postgresql/.s.PGSQL.5432.lock
rm -f /var/run/postgresql/postmaster.pid
sleep 1
exit 0
