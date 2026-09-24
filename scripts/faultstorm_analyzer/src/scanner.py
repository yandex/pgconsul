"""Collect bounded evidence from node logs."""

import re
from typing import Any

from .parsing import evidence, lines, timestamp


RULES = {
    'startup_zk_failure': ('ZooKeeper connection failure during startup', 'Could not initialize ZooKeeper connection'),
    'zk_connected': ('ZooKeeper connection established', 'Successfully connected to ZooKeeper|Reconnected to ZK'),
    'rewind_flag': ('Iteration skipped because the rewind-fail flag is set', 'Rewind fail flag is set'),
    'already_running': ('New startup detects an existing process', 'Already running!'),
    'no_primary': ('Resetup could not find a primary', 'Could not find a primary among the cluster hosts'),
    'spawn_error': ('Supervisor failed to start the process', 'ERROR \\(spawn error\\)'),
    'supervisor_stopped': ('Supervisor reported pgconsul stopped', 'stdout=pgconsul: stopped'),
    'resetup_complete': ('Resetup reported successful completion', 'pg_resetup completed successfully'),
    'no_processes': ('Freezer found no matching processes', 'No matching processes found'),
    'pg_shutdown': ('PostgreSQL reported shutdown', 'database system is shut down'),
    'pg_ready': ('PostgreSQL is ready to accept connections', 'database system is ready to accept'),
    'pg_fatal': ('PostgreSQL error', r'\b(?:PANIC|FATAL):'),
    'pooler_error': ('Pooler connection error', 'server login failed|server conn crashed|server connection timed out'),
    'zk_expired': ('ZooKeeper session expired', 'Expiring session|Session .* expired'),
}
PATTERNS = {key: re.compile(pattern) for key, (_, pattern) in RULES.items()}
PROCESS = re.compile(r'SIG(?:STOP|CONT) -> PID (\d+) \([^)]*python[^)]*/(?:bin|usr/local/bin)/pgconsul\b')
LOG_NAMES = ('pgconsul.log', 'pg_resetup.log', 'process_freezer.log', 'postgresql.log', 'pgbouncer.log', 'zk.log')


def scan_log(path):
    groups: dict[str, dict[str, Any]] = {}
    first = last = None
    last_line = ''
    total = 0
    for total, text in lines(path):
        last_line = text
        if ' DEBUG' in text and path.name in ('postgresql.log', 'pgbouncer.log'):
            continue
        when = timestamp(text)
        if when:
            ref = evidence(path, total, text, when)
            if first is None or when < first['time']:
                first = ref
            if last is None or when > last['time']:
                last = ref
        kinds = [key for key, pattern in PATTERNS.items() if pattern.search(text)]
        if match := PROCESS.search(text):
            kinds.append(f'pgconsul_process:{match[1]}')
        for key in kinds:
            ref = evidence(path, total, text, when)
            group = groups.setdefault(key, {'kind': key, 'count': 0, 'first': ref, 'last': ref})
            group['count'] += 1
            group['last'] = ref
    if when := timestamp(last_line):
        last = evidence(path, total, last_line, when)
    return dict(path=str(path), lines=total, first=first, last=last, groups=list(groups.values()))
