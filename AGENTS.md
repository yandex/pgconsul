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
├── __init__.py         # Daemon bootstrap, configuration defaults, and logging
├── main.py             # Main pgconsul class, primary iteration loop
├── pg.py               # PostgreSQL interaction (psycopg2)
├── zk.py               # ZooKeeper domain operations and locks
├── zk_client.py        # Low-level KazooClient wrapper (ZK connection management)
├── replication_manager.py         # Replication mode management (sync/async/quorum)
├── failover_election.py           # Failover election logic
├── helpers.py          # Utility functions
├── utils.py            # Switchover, Failover classes
├── command_manager.py  # External command management
├── cli.py              # CLI interface (pgconsul-util)
├── types.py            # Type aliases
├── exceptions.py       # Custom exceptions
├── list_removal_strategy.py       # Quorum list removal strategy
├── ssn_manager.py      # SSN (Sync Standby Names) management
├── slot_manager.py     # Replication slot lifecycle management
├── timings.py          # Failover/switchover timing management
├── log_formatters.py   # Log formatting
├── async_logging.py    # Asynchronous logging
├── yapf_check.py       # YAPF style-check helper
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

- All added comments must be brief and in English

### Error Handling

#### PostgreSQL Errors (`src/exceptions.py`)

PostgreSQL errors propagate as typed exceptions by default:

| Exception | When to raise |
|-----------|---------------|
| `PostgresException` | Base class; do not raise directly |
| `PostgresConnectionError` | Connection unavailable or dropped (`psycopg2.OperationalError`) |
| `PostgresQueryError` | Reserved for unexpected/invalid query results; not yet raised in production code |

**Key convention:** `pg.py` translates `psycopg2.OperationalError` into `PostgresConnectionError`
and propagates it. Local handlers in `pg.py` are limited to the
[ADR-0001 exceptions](adr/ADR-0001-typed-postgres-exception-hierarchy.md).
Callers may handle errors in critical sections and Best-Effort operations listed in
[ADR-0002](adr/ADR-0002-exception-propagation-to-iteration-boundary.md).
Otherwise, errors propagate through `run_iteration()` to `start()`, which logs them and starts the next iteration.

**`@helpers.return_none_on_error` is allowed only on `zk.noexcept_get()`.** Other methods may
return `None` for absent data if errors propagate as exceptions.

#### ZooKeeper Errors

- `zk.get()` / `zk.write()` raise `ZookeeperException` — callers decide to propagate or handle.
- `zk.noexcept_get()` swallows exceptions and returns `None` — valid "soft" API for optional reads.

### Working with ZooKeeper

- All ZK paths are defined as constants in the `Zookeeper` class (`src/zk.py`)
- The primary lock is stored at `<prefix>/master` (`PRIMARY_LOCK_PATH`)
- Cluster state is synchronized via ZK on every iteration
- When ZK connectivity is lost, the primary stops the pooler and halts WAL archiving
- **Layering (ADR-0003):** `ZkClient` (`src/zk_client.py`) is the transport layer — KazooClient
  lifecycle, primitive data ops, kazoo→`ZkClientError` exception translation. `Zookeeper`
  (`src/zk.py`) is the domain layer — path constants, lock ownership, business operations,
  `ZkClientError → ZookeeperException` translation. New business operations go in `zk.py`;
  new transport primitives go in `zk_client.py`. `Zookeeper` must not import `kazoo.*` directly.

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

## Architecture Decision Records (ADR)

Architectural decisions are documented in `adr/` as Markdown files named `ADR-NNNN-<slug>.md`.

### Existing ADRs

| File | Title | Status |
|------|-------|--------|
| [`adr/ADR-0001-typed-postgres-exception-hierarchy.md`](adr/ADR-0001-typed-postgres-exception-hierarchy.md) | Typed Exception Hierarchy for the PostgreSQL Layer | Accepted |
| [`adr/ADR-0002-exception-propagation-to-iteration-boundary.md`](adr/ADR-0002-exception-propagation-to-iteration-boundary.md) | Exception Propagation Strategy to the Iteration Boundary | Accepted |
| [`adr/ADR-0003-zk-client-zk-layering.md`](adr/ADR-0003-zk-client-zk-layering.md) | Layering and Responsibility Split between `ZkClient` and `Zookeeper` | Accepted |
| [`adr/ADR-0004-factory-config-builder-convention.md`](adr/ADR-0004-factory-config-builder-convention.md) | Factory + Config-Builder Convention for Infrastructure Components | Accepted |

### When to create a new ADR

Create a new ADR when making a decision that:
- Changes the error-handling contract of a module (e.g. exceptions vs. return values)
- Introduces or removes a cross-cutting mechanism (decorator, base class, protocol)
- Establishes a new convention that all contributors must follow
- Has non-obvious trade-offs that future maintainers should understand

### ADR structure

Each ADR must contain the following sections:
`# Context` → `# Decision` → `# Alternatives` → `# Consequences` → `# Links`

---

## Common Agent Tasks

### Adding a New Configuration Parameter

1. Add the parameter to the owning component's `*Config` and config builder, following [ADR-0004](adr/ADR-0004-factory-config-builder-convention.md).
2. For `ReplicationManager`, update `ReplicationManagerConfig` and `build_replication_manager_config()` in [`src/replication_manager.py`](src/replication_manager.py). For the orchestrator, update `PgconsulConfig` and `build_pgconsul_config()` in `src/main.py`.
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
- Configuration: `ReplicationManagerConfig` and `build_replication_manager_config()` in [`src/replication_manager.py`](src/replication_manager.py)
- SSN management: [`src/ssn_manager.py`](src/ssn_manager.py)
- Replication slot lifecycle: [`src/slot_manager.py`](src/slot_manager.py)
- Tests: `tests/unit/test_replication_manager_*.py`, `tests/unit/test_ssn_manager.py`, `tests/unit/test_slot_manager.py`
