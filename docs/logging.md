# Logging in pgconsul

## Overview

pgconsul uses Python's standard `logging` module. All log output goes to stdout/stderr and is captured by the system journal or container runtime.

## Log Levels

### ERROR
Critical failures that require immediate attention:
- Connection failures to ZooKeeper or PostgreSQL
- Failed critical operations (promote, rewind, stop)
- Inconsistent cluster state (timeline mismatch, nobody holds leader lock)

Examples:
```
ERROR Could not connect to "host=localhost port=5432 ...".
ERROR Could not promote me as a new primary.
ERROR ZK timeline is newer than local. Releasing leader lock
```

### WARNING
Recoverable issues and important state changes:
- Temporary connectivity problems (retries will follow)
- Lock acquisition failures
- Unexpected but non-fatal cluster states

Examples:
```
WARNING Being disconnected from ZK. (Kazoo)
WARNING Unable to obtain lock leader within timeout (10 s)
WARNING Seems that we are not really streaming WAL from host2.
```

### INFO
Normal operational events — the main content of the log:
- Iteration start/end
- Role changes
- Start and completion of key procedures (SWITCHOVER, FAILOVER, REWIND, RESETUP)
- Successful operations

Examples:
```
INFO Start iteration on host: host1.example.com
INFO Role: primary
INFO ACTION. Starting promote
INFO Finished iteration ==============================
```

### DEBUG
Detailed execution information for troubleshooting:
- SQL queries executed against PostgreSQL
- Subprocess commands and their exit codes
- Intermediate states and checks
- ZooKeeper node operations

Examples:
```
DEBUG Running command: pg_ctlcluster 14 main status
DEBUG Command finished with exit code 0: pg_ctlcluster 14 main status
DEBUG Executing SQL: SELECT pg_is_in_recovery();
DEBUG No lock instance for leader. Creating one.
```

## Key Events Highlighting

Critical cluster events are highlighted with separator lines for easy visual scanning:

```
============================================================
SWITCHOVER STARTED: host1 → host2
============================================================
... switchover procedure ...
============================================================
SWITCHOVER COMPLETED
============================================================
```

The following events use this format:
- **SWITCHOVER** — scheduled primary switch
- **FAILOVER** — unplanned primary switch after primary death
- **REWIND** — pg_rewind execution
- **RESETUP** — full replica re-setup
- **MAINTENANCE** — cluster maintenance mode enter/exit

## State Logging

### db_state

At the start of each iteration, the database state is logged in a structured format:

```
DEBUG DB State:
DEBUG   Role: PRIMARY
DEBUG   Timeline: 5
DEBUG   LSN: 0/5A3F000
DEBUG   PostgreSQL: running
DEBUG   Bouncer: running
DEBUG   Replication: sync
DEBUG   SSN: host2
DEBUG   Replicas (1):
DEBUG     - host2.example.com: state=streaming, sync=sync, lag=3ms, sent_lsn=0/5A3F000, replay_lsn=0/5A3EFF0
```

Fields:
- `Role` — current PostgreSQL role (`PRIMARY` / `REPLICA` / `UNKNOWN`)
- `Timeline` — current WAL timeline
- `LSN` — current write-ahead log position
- `PostgreSQL` — whether PostgreSQL process is running (`running` / `stopped`)
- `Bouncer` — whether the connection pooler is running (`running` / `stopped`)
- `Replication` — replication type: `sync` or `async` (primary only, when set)
- `SSN` — synchronous standby name (primary only, when set)
- `Archive command` — current archive_command value (when set)
- `Replicas` — list of streaming replicas with state, sync mode, lag and LSN positions

### zk_state

ZooKeeper state is logged at DEBUG level at the start of each iteration:

```
DEBUG ZK State:
DEBUG   Timeline: 5
DEBUG   Leader lock: host1.example.com
DEBUG   Quorum locks (2): host1.example.com, host2.example.com
DEBUG   Alive locks (2): host1.example.com, host2.example.com
```

With active failover:
```
DEBUG ZK State:
DEBUG   Timeline: 5
DEBUG   Leader lock: NONE
DEBUG   Quorum locks: NONE
DEBUG   Failover state: promoting
DEBUG   Promoting host: host2.example.com
```

Fields:
- `Timeline` — timeline stored in ZooKeeper
- `Leader lock` — current lock holder (primary), or `NONE` if no primary
- `Quorum locks` — list of hosts holding quorum lock, or `NONE`
- `Alive locks` — list of hosts holding alive lock, or `NONE` if empty
- `Maintenance` — maintenance mode holder (shown only when active)
- `Switchover state` / `Switchover candidate` — shown only during switchover
- `Failover state` / `Promoting host` — shown only during failover

## Module: src/log_formatters.py

Helper functions for structured log output.

### `format_db_state_for_log(db_state: dict) -> str`

Formats `db_state` dict into a multi-line human-readable string for logging.

```python
from .log_formatters import format_db_state_for_log
if logging.getLogger().isEnabledFor(logging.DEBUG):
    logging.debug(format_db_state_for_log(db_state))
```

### `format_zk_state_for_log(zk_state: dict) -> str`

Formats `zk_state` dict into a multi-line human-readable string for logging.

```python
from .log_formatters import format_zk_state_for_log
if logging.getLogger().isEnabledFor(logging.DEBUG):
    logging.debug(format_zk_state_for_log(zk_state))
```

### `format_replics_info_for_log(replics_info: list) -> str`

Formats replica info list into a compact multi-line string.

```python
from .log_formatters import format_replics_info_for_log
logging.debug('replics_info:\n%s', format_replics_info_for_log(replics_info))
```

### `log_event(event: str, detail: str = '', level: str = 'warning', char: str = '=', length: int = 60)`

Logs a key cluster event surrounded by separator lines.

```python
from .log_formatters import log_event
log_event('FAILOVER: Primary has died, starting failover procedure', level='error')
log_event('SWITCHOVER STARTED', detail=f'{old_primary} → {new_primary}')
```

Output:
```
============================================================
FAILOVER: Primary has died, starting failover procedure
============================================================
```

### `log_separator(logger, level: str = 'info', char: str = '=', length: int = 60)`

Logs a single separator line.

```python
from .log_formatters import log_separator
log_separator(logging, level='info')
```

## Subprocess Logging

All subprocess calls log the command at DEBUG level before execution and the exit code after:

```
DEBUG Running command: pg_ctlcluster 14 main promote
DEBUG Command finished with exit code 0 in 0.123s: pg_ctlcluster 14 main promote
```

On failure (non-zero exit code), stdout and stderr are logged at ERROR level:

```
DEBUG Command finished with exit code 1 in 2.456s: pg_ctlcluster 14 main stop
ERROR <stdout line 1>
ERROR <stderr line 1>
```

## SQL Query Logging

All SQL queries executed via `_exec_query()` are logged at DEBUG level:

```
DEBUG Executing SQL: SELECT pg_is_in_recovery();
DEBUG Executing SQL: SHOW synchronous_standby_names;
DEBUG Executing SQL: SELECT pg_wal_replay_pause();
```

## Tests

Unit tests for `log_formatters.py` are in [`tests/test_log_formatters.py`](../tests/test_log_formatters.py).

Run with:
```bash
python -m pytest tests/test_log_formatters.py -v
```

26 tests cover all formatting functions and edge cases (empty dicts, None values, multiple replicas, etc.).
