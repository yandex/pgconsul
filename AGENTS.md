# AGENTS.md — AI Agent Guide for the pgconsul Project

## Project Overview

**pgconsul** is a tool for maintaining High-Availability PostgreSQL cluster configurations. It is responsible for automatic cluster recovery in emergency situations, using ZooKeeper as a distributed coordinator.

**Language:** Python 3  
**License:** PostgreSQL  
**Installation path:** `/opt/yandex/pgconsul` (venv)

---

## Architecture

### Directory Structure

```
src/                    # Main source code (pgconsul package)
├── main.py             # Main pgconsul class, primary iteration loop
├── pg.py               # PostgreSQL interaction (psycopg2)
├── zk.py               # ZooKeeper interaction (kazoo)
├── replication_manager.py         # Replication mode management (sync/async/quorum)
├── replication_manager_factory.py # ReplicationManager factory and configuration
├── failover_election.py           # Failover election logic
├── helpers.py          # Utility functions
├── utils.py            # Switchover, Failover classes
├── command_manager.py  # External command management
├── cli.py              # CLI interface (pgconsul-util)
├── types.py            # Type aliases
├── exceptions.py       # Custom exceptions
├── list_removal_strategy.py       # Quorum list removal strategy
├── ssn_manager.py      # SSN (Sync Standby Names) management
├── log_formatters.py   # Log formatting
├── async_logging.py    # Asynchronous logging
└── sdnotify.py         # systemd integration
```

### Core Components

| Component | File | Description |
|-----------|------|-------------|
| `pgconsul` | `src/main.py` | Main class, primary loop (`run_iteration`) |
| `Postgres` | `src/pg.py` | PostgreSQL abstraction layer |
| `Zookeeper` | `src/zk.py` | ZooKeeper abstraction layer |
| `ReplicationManager` | `src/replication_manager.py` | Replication type management |
| `FailoverElection` | `src/failover_election.py` | New primary election |
| `CommandManager` | `src/command_manager.py` | External command execution |

### Data Flow (Main Loop)

Every second, `pgconsul` executes `run_iteration()`:
1. Fetches database state (`db.get_state()`)
2. Fetches ZooKeeper state (`zk.get_state()`)
3. Updates maintenance status
4. Depending on the current role, calls:
   - `primary_iter()` — if the node is the primary
   - `replica_iter()` — if the node is an HA replica
   - `non_ha_replica_iter()` — if the node is a cascading replica
   - `dead_iter()` — if PostgreSQL is unavailable

---

## Testing

### Unit Tests (pytest)

Unit tests are located in `tests/unit/` directory.

```bash
# Run all unit tests
make unit_test

# Or run directly with pytest
pytest tests/unit/ -v
pytest tests/unit/ --cov=src --cov-report=html --cov-report=term
```

### Integration BDD Tests (behave)

```bash
# All tests
make check_test

# Specific feature file
TEST_ARGS='-i archive.feature' make check_test

# Specific scenario by line number
TEST_ARGS='-i kill_primary.feature:108' make check_test

# By tag
TEST_ARGS='--tags @fail_replication_source -i cascade.feature' make check_test

# With debug logs
DEBUG=1 TEST_ARGS='--tags @fail_replication_source -i cascade.feature' make check_test

# Continue on failure (unstoppable)
tox -e behave_unstoppable -- tests/features cascade.feature
```

### Test Logs

- `logs/debug/test_execution.log` — test execution details, timing, retries
- `logs/<feature_file>/<line_number>/<hostname>/` — container logs on failure

---

## Linting and Static Analysis

```bash
tox -e mypy
```

> **Note:** `yapf`, `flake8`, `pylint`, and `bandit` are currently broken and should not be run.
> Do not use `make lint`. Only `mypy` is required.

### Style Rules

- **Maximum line length:** 200 characters (`.flake8`)
- **Type checking:** mypy with `ignore_missing_imports = True`, `check_untyped_defs = True`
- All new code must pass: `mypy`

---

## Configuration

Configuration is stored in an INI file (default: `/etc/pgconsul.conf`). Main sections:

| Section | Description |
|---------|-------------|
| `[global]` | General parameters (ZK address, timeouts, priority, replication mode) |
| `[primary]` | Primary behavior (replication type switching, quorum) |
| `[replica]` | Replica behavior (recovery timeouts, failover) |
| `[commands]` | External commands (promote, rewind, pg_start/stop, etc.) |
| `[plugins]` | Plugin configuration |

Full reference: [`docs/CONFIG.md`](docs/CONFIG.md)

---

## Important Conventions

### Comments

- All added comments must be concise

### Working with ZooKeeper

- All ZK paths are defined as constants in the `Zookeeper` class (`src/zk.py`)
- The primary lock is stored at `<prefix>/master` (`PRIMARY_LOCK_PATH`)
- Cluster state is synchronized via ZK on every iteration
- When ZK connectivity is lost, the primary stops the pooler and halts WAL archiving

### Replication

- Supported modes: `sync`, `async`, `quorum`
- `ReplicationManager` handles switching between modes
- `quorum_removal_delay` (0–120 sec) — delay before removing a replica from the quorum list
- When `quorum_commit = true`, either `use_lwaldump = true` or `allow_potential_data_loss = true` is required

### Failover vs Switchover

- **Failover** — automatic emergency switch triggered when the primary becomes unavailable
- **Switchover** — planned switch initiated via `pgconsul-util switchover`
- Both processes are coordinated through ZK (`FAILOVER_STATE_PATH`, `SWITCHOVER_STATE_PATH`)

### Rewind-fail Flag

- If `pg_rewind` fails more than `max_rewind_retries` times, the file `.pgconsul_rewind_fail.flag` is created
- When this flag exists, pgconsul refuses to start — manual intervention is required

---

## Common Agent Tasks

### Adding a New Configuration Parameter

1. Add the parameter to `src/main.py` (read via `self.config.get/getint/getfloat/getboolean`)
2. If the parameter belongs to `ReplicationManager` — add it to [`ReplicationManagerConfig`](src/replication_manager_factory.py) and [`build_replication_manager_config()`](src/replication_manager_factory.py)
3. Update the documentation in [`docs/CONFIG.md`](docs/CONFIG.md)
4. Add a default value to the test config [`tests/conf/pgconsul.conf`](tests/conf/pgconsul.conf)

### Adding a Unit Test

- Test files: `tests/unit/test_*.py`
- Run: `pytest tests/unit/ -v` or `make unit_test`
- Uses standard `pytest`; mocking via `unittest.mock`

### Adding a BDD Test

- Feature files: `tests/features/*.feature`
- Step definitions: `tests/steps/*.py`
- Run: `TEST_ARGS='-i <feature>.feature' make check_test`

### Changing Replication Logic

- Core logic: [`src/replication_manager.py`](src/replication_manager.py)
- Configuration: [`src/replication_manager_factory.py`](src/replication_manager_factory.py)
- SSN management: [`src/ssn_manager.py`](src/ssn_manager.py)
- Tests: `tests/unit/test_replication_manager_*.py`, `tests/unit/test_ssn_manager.py`
