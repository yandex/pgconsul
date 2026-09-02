# AGENTS.md — AI Agent Guide for the pgconsul Project

## Project Overview

**pgconsul** is a tool for maintaining High-Availability PostgreSQL cluster configurations. It is responsible for automatic cluster recovery in emergency situations, using ZooKeeper as a distributed coordinator.

**Language:** Python 3  
**License:** PostgreSQL  
**Installation path:** `/opt/yandex/pgconsul` (venv)

---

## Data Safety Contract

[`docs/DATA_SAFETY.md`](docs/DATA_SAFETY.md) is the foundational data-safety
document for pgconsul. It must not be changed in automatic mode without the
explicit consent of a human for that specific change.

All pgconsul code must conform to the requirements stated in
[`docs/DATA_SAFETY.md`](docs/DATA_SAFETY.md).

---

## Architecture

### Directory Structure

```
src/                    # Main source code (pgconsul package)
├── main.py             # Main pgconsul class, primary iteration loop
├── pg.py               # PostgreSQL interaction (psycopg2)
├── zk.py               # ZooKeeper interaction (kazoo)
├── replication_manager.py         # Replication mode management (sync/async/quorum)
├── commands.py                   # Command dataclasses (Plan = list[Command]) — ADR-0006
├── command_executor.py           # Imperative shell — dispatches commands to infra — ADR-0006
├── switchover/                    # Manager-owned switchover protocol types (ADR-0014)
│   └── types.py                  #   SwitchoverPhase, SwitchoverRecord
├── failover/                      # Failover state machines (ADR-0007)
│   ├── coordinator.py            #   FailoverCoordinatorMachine
│   ├── participant.py            #   FailoverParticipantMachine
│   └── types.py                  #   FailoverPhase, FailoverObservation, FailoverRecord
├── return_to_cluster/            # Return-to-cluster state machine (ADR-0006, stateless)
│   ├── machine.py                #   ReturnToClusterMachine
│   └── types.py                  #   ReturnPhase, ReturnObservation
├── maintenance.py                # Maintenance-mode handler
├── debug.py                      # DebugFailure — fault injection for testing
├── helpers.py          # Utility functions
├── utils.py            # Switchover, Failover classes
├── command_manager.py  # External command management
├── cli.py              # CLI interface (pgconsul-util)
├── types.py            # Type aliases
├── exceptions.py       # Custom exceptions
├── list_removal_strategy.py       # Quorum list removal strategy
├── ssn_manager.py      # SSN (Sync Standby Names) management
├── slot_manager.py     # Replication slot lifecycle management
├── timings.py          # TimingTracker — downtime/failover/switchover timing via ZK
├── log_formatters.py   # Log formatting
├── async_logging.py    # Asynchronous logging
├── zk_client.py        # Low-level KazooClient wrapper (ZK connection management)
├── sdnotify.py         # systemd integration
└── yapf_check.py       # yapf style-check helper script
```

### Core Components

| Component | File | Description |
|-----------|------|-------------|
| `pgconsul` | `src/main.py` | Main class, primary loop (`run_iteration`) |
| `Postgres` | `src/pg.py` | PostgreSQL abstraction layer |
| `Zookeeper` | `src/zk.py` | ZooKeeper abstraction layer |
| `ReplicationManager` | `src/replication_manager.py` | Replication type management |
| `FailoverCoordinatorMachine` | `src/failover/coordinator.py` | Failover coordinator state machine (ADR-0007) |
| `FailoverParticipantMachine` | `src/failover/participant.py` | Failover participant state machine (ADR-0007) |
| `CommandExecutor` | `src/command_executor.py` | Imperative shell — command dispatch (ADR-0006) |
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

# Specific scenario by tag (preferred — behave's -i does NOT support :line filtering)
TEST_ARGS='-i anywhere_switchover.feature --tags=@switchover_failed_promote' make check_test

# By tag only
TEST_ARGS='--tags @fail_replication_source -i cascade.feature' make check_test

# With debug logs
DEBUG=1 TEST_ARGS='-i anywhere_switchover.feature --tags=@switchover_failed_promote' make check_test

# Continue on failure (unstoppable)
tox -e behave_unstoppable -- tests/features cascade.feature
```

> **Note:** behave's `-i` (include) filter matches by feature **file name** only.
> The `feature:line` syntax (e.g. `kill_primary.feature:108`) does **not** work
> with `-i` — it silently matches zero scenarios. To run a single scenario, use
> `--tags=@<tag>` together with `-i <feature>.feature`.

### Debugging Tests

#### Step 1 — Identify the failed test

Open [`logs/debug/test_execution.log`](logs/debug/test_execution.log). This file is written by
[`setup_debug_logging()`](tests/steps/helpers.py) and contains a chronological record of every
step with its status and duration:

```
2026-08-09 18:53:53 - helpers - INFO - Finished step: Then ... (status=failed, duration=12.345s)
```

Search for `status=failed` to find the failing step. The log entry includes the step keyword, name,
status, and duration — enough to identify the exact feature file and line.

> **Heuristic — stuck test without an explicit failure:**
> If `status=failed` is not found but the log shows many repeating entries that continue for
> more than 2 minutes, the test is likely stuck and will fail on timeout. Treat the last
> repeating step as the failing one and proceed to inspect its container logs.

> **Tip:** The log format is
> `%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s`
> (see [`setup_debug_logging()`](tests/steps/helpers.py) — the `Formatter` call).

#### Step 2 — Read container logs saved on failure

When a step fails, [`after_step()`](tests/environment.py:152) in
[`tests/environment.py`](tests/environment.py) automatically extracts logs from every container
into:

```
logs/<feature_file>/<line_number>/<hostname>/
```

Contents per container type:

| Container type | Files extracted |
|----------------|-----------------|
| PostgreSQL (`postgresql*`) | `pgconsul.log`, `postgresql.log`, `pgbouncer.log`, `rsync.log` |
| ZooKeeper (`zookeeper*`) | `zookeeper--server-<hostname>.log` |
| Backup (`backup*`) | `rsync.log` |

Read these files to inspect the cluster state at the moment of failure.

#### Step 3 — Inspect live container logs

If the test environment is still running (e.g. after `make check_test` or a manual test session),
read logs directly from the containers:

```bash
# List running containers (project name: pgconsul)
docker ps

# Follow pgconsul log on a specific node
docker logs -f postgresql1

# Read a specific log file inside a container
docker exec postgresql1 cat /var/log/pgconsul/pgconsul.log
docker exec postgresql1 cat /var/log/postgresql/postgresql.log
docker exec postgresql1 cat /var/log/postgresql/pgbouncer.log

# ZooKeeper logs
docker exec zookeeper1 cat /var/log/zookeeper/zookeeper--server-pgconsul_zookeeper1_1.log
```

Container names follow the pattern `<service><N>` (e.g. `postgresql1`,
`zookeeper2`, `backup1`). The *hostname* inside each container follows the
pattern `pgconsul_<service><N>_1` (e.g. `pgconsul_postgresql1_1`) — this is
the FQDN used by pgconsul and ZooKeeper, not the `docker` container name.
See [`docker-compose.yml`](docker-compose.yml) for the full list.

#### Debug flags

| Flag | Effect |
|------|--------|
| `DEBUG=1` | Save container logs for **all** steps (not only failed). Enables `pdb` post-mortem on failure. |
| `DEBUG_LOG_DIR=<path>` | Override the debug log directory (default: `logs/debug`). |

```bash
# Run with full debug logging and pdb on failure
# Use --tags=@<tag> to select a scenario (see note above: -i does not support :line)
DEBUG=1 TEST_ARGS='-i kill_primary.feature --tags=@some_tag' make check_test
```

#### Debugging workflow summary

1. Check `logs/debug/test_execution.log` → find the `status=failed` entry.
   If no failure is logged but repeating entries persist for more than 2 minutes,
   treat the last repeating step as the failing one (stuck test → timeout).
2. Read saved container logs in `logs/<feature_file>/<line_number>/<hostname>/`.
3. If containers are still running, inspect live logs via `docker logs` / `docker exec`.
4. Use `DEBUG=1` to capture logs for all steps or to drop into `pdb` on failure.

> Steps 1–2 can be automated — see the [Automated failure analysis script](#automated-failure-analysis-script) below.

#### Automated failure analysis script

Steps 1–2 above can be automated with [`scripts/analyze_failed_scenario.py`](scripts/analyze_failed_scenario.py).
The script scans `test_execution.log` for failed steps, locates the corresponding
container logs, and searches them for known failure patterns (WAL divergence,
pg_rewind failure, connection errors, timeline mismatch, stuck loops, etc.).
It ranks findings by likelihood and prints a concise root-cause summary.

For full usage, CLI reference, and output-stream details see
[`scripts/README.md`](scripts/README.md). Architecture and extension guide for
the underlying `failure_analyzer` package:
[`scripts/failure_analyzer/README.md`](scripts/failure_analyzer/README.md).

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

The codebase uses a typed exception hierarchy for PostgreSQL errors — never return `None` to signal
a DB error:

| Exception | When to raise |
|-----------|---------------|
| `PostgresException` | Base class; do not raise directly |
| `PostgresConnectionError` | Connection unavailable or dropped (`psycopg2.OperationalError`) |
| `PostgresQueryError` | Query executed but returned an unexpected/invalid result |

**Key convention:** `pg.py` internal methods translate `psycopg2.OperationalError` into
`PostgresConnectionError` and **let it propagate to the caller**. Callers in `main.py` /
`replication_manager.py` decide: use `try/except PostgresConnectionError` only in critical
scenarios (switchover, failover, reconnect) where restarting the iteration is not safe.
In all other cases the exception propagates up to `run_iteration()`, which restarts the iteration.

**PROHIBITED in `pg.py` methods:** catching `PostgresConnectionError` inside the method itself
and returning a safe default (e.g. `return []`, `return ('async', None)`, `return None`).
This pattern hides DB errors from the iteration loop and prevents proper restart.
The **only** allowed exception: `reconnect()`, which must catch connection errors by definition.

**`@helpers.return_none_on_error` is intentionally kept only on `zk.noexcept_get()`** — that is
the only place where `None` as a return value is a valid "no data" signal. Do **not** apply this
decorator to new `pg.py` methods; raise `PostgresConnectionError` instead.

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
- Safe quorum failover reads the valid local `pg_wal` endpoint with `lwaldump()` after fencing external WAL sources. It never falls back to receive/replay LSN: after a PostgreSQL restart those SQL positions may be behind WAL still present on disk.

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
| [`adr/ADR-0002-exception-propagation-to-run-iteration.md`](adr/ADR-0002-exception-propagation-to-run-iteration.md) | Exception Propagation Strategy to `run_iteration()` | Accepted |
| [`adr/ADR-0003-zk-client-zk-layering.md`](adr/ADR-0003-zk-client-zk-layering.md) | Layering and Responsibility Split between `ZkClient` and `Zookeeper` | Accepted |
| [`adr/ADR-0004-factory-config-builder-convention.md`](adr/ADR-0004-factory-config-builder-convention.md) | Factory + Config-Builder Convention for Infrastructure Components | Accepted |
| [`adr/ADR-0005-idempotent-iterations.md`](adr/ADR-0005-idempotent-iterations.md) | Idempotent Iterations — Level-Triggered Reconciliation for Cluster Operations | Accepted |
| [`adr/ADR-0006-switchover-machine-command-plan.md`](adr/ADR-0006-switchover-machine-command-plan.md) | Cluster-Op State Machines — Pure Handlers with Command Plans (Functional Core / Imperative Shell) | Accepted |
| [`adr/ADR-0007-failover-state-machine.md`](adr/ADR-0007-failover-state-machine.md) | Failover State Machine — Coordinator + Participant | Accepted |

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
- **Switchover/failover tests must not rely on priority** to determine which replica
  becomes primary. Use the `we remember which of "<replicas>" became primary as
  "<tag_primary>" and the other as "<tag_replica>"` step and reference the resulting
  tags (`<tag_primary>`, `<tag_replica>`) in subsequent assertions. Do not hardcode a
  specific replica as the expected new primary. See
  [`anywhere_switchover.feature`](tests/features/anywhere_switchover.feature) for a
  reference.

### Reproducing a Behave Failure as a Unit Test (Red-Green Workflow)

When a behave (BDD) integration test fails, the bug must be reproduced as a unit
test **before** fixing the code. This follows the Red-Green discipline and keeps
regressions pinned by fast, deterministic tests:

1. **Diagnose** — Identify the failing behave step via
   [`logs/debug/test_execution.log`](logs/debug/test_execution.log) and the
   container logs (see [Debugging Tests](#debugging-tests)). Determine the root
   cause and the exact code path that misbehaves.
2. **Write a red unit test** — Add a unit test in `tests/unit/` that reproduces
   the same situation in isolation (mocks via `unittest.mock`, no
   Docker/ZK/PostgreSQL). The test **must fail (red)** against the current
   unfixed code — this proves it actually exercises the bug. Name the test after
   the scenario it reproduces and reference the feature file + line in a
   docstring or comment.
3. **Fix the code** — Change the source so the unit test passes (green). The fix
   must make the red test green without weakening the assertion.
4. **Verify** — Run `pytest tests/unit/ -v` to confirm the new test is green,
   then re-run the original behave scenario
   (`TEST_ARGS='-i <feature>.feature --tags=@<tag>' make check_test`) to confirm
   the integration failure is resolved. See the note above: `-i` matches by
   file name only; use `--tags=@<tag>` to select a single scenario.

**Rules:**

- Never fix code for a behave failure without a red unit test that reproduces
  it. A green behave run alone is not sufficient — the regression must be pinned
  by a fast, deterministic unit test.
- The unit test must be deterministic and fast: no real containers, network, or
  sleeps. Use `unittest.mock` to stub `Postgres` / `Zookeeper` / external
  commands.
- If the bug cannot be reproduced at the unit level (e.g. it requires real
  PostgreSQL/ZK interaction), document the reason in the PR and add the
  reproduction as a behave scenario instead.

### Changing Replication Logic

- Core logic: [`src/replication_manager.py`](src/replication_manager.py)
- Configuration: [`src/replication_manager_factory.py`](src/replication_manager_factory.py)
- SSN management: [`src/ssn_manager.py`](src/ssn_manager.py)
- Replication slot lifecycle: [`src/slot_manager.py`](src/slot_manager.py)
- Tests: `tests/unit/test_replication_manager_*.py`, `tests/unit/test_ssn_manager.py`, `tests/unit/test_slot_manager.py`

---

## Known Gotchas & Debugging Notes

### `is_host_alive()` with `timeout=0.0` always returns False

[`zk.is_host_alive()`](src/zk.py) defaults to `timeout=0.0`. It delegates to
[`helpers.await_for()`](src/helpers.py) → [`helpers.get_exponentially_retrying()`](src/helpers.py),
which computes `retrying_end = time.time() + timeout`. When `timeout == 0`, the
`while time.time() < retrying_end` loop body **never executes** — the function
immediately returns `False` and logs `"Retrying timeout expired."` without ever
calling the check.

**Rule:** never call `is_host_alive(host)` without an explicit `timeout` argument.
A value of `1` second is sufficient for local Docker tests; production callers
(e.g. `utils.py`) pass `self.timeout / 2`.

### Host-side logs are truncated when the test is stuck

The logs in `logs/tests/features/<feature>/<line>/` are extracted **only on step
failure** (via `after_step` in `tests/environment.py`). If the test is stuck
(timeout, not yet failed), those files are truncated at the moment the test
started waiting. To see the full picture, read logs directly from the running
containers:

```bash
docker exec postgresql1 grep -i "switchover\|SWITCHOVER\|scheduled\|sync_set\|initiated\|candidate_found\|primary_shut\|promoted\|failed\|Dropping stale" /var/log/pgconsul/pgconsul.log | tail -40
```

### Quick-fix iteration without full image rebuild

To test a source change without waiting for `make build_pgconsul`, copy the file
into running containers and restart pgconsul:

```bash
for c in postgresql1 postgresql2 postgresql3; do
  docker cp src/<file>.py ${c}:/opt/yandex/pgconsul/lib/python3.10/site-packages/pgconsul/<file>.py
  docker exec ${c} supervisorctl restart pgconsul
done
```

This is **not** a substitute for a full `make check_test` run (the image must be
rebuilt for the actual test), but it is useful for fast manual verification of a
fix against an already-running cluster.

### ZK access from test containers

- ZK listens on port **2281** (not the default 2181) inside the `pgconsul_net`
  Docker network.
- `zkCli.sh` is at `/opt/zookeeper/bin/zkCli.sh` in the `zookeeper*` containers,
  but direct CLI access is restricted (`Insufficient permission`). Use the
  pgconsul venv Python (`/opt/yandex/pgconsul/bin/python3`) with `kazoo` for
  programmatic inspection — but note that network latency between DCs
  (60–70 ms in tests) can cause connection timeouts; use a generous `timeout=`
  on `KazooClient.start()`.

#### Dumping all ZooKeeper records

[`scripts/dump_zk.py`](scripts/dump_zk.py) is a standalone script that dumps
every ZK node (path + value) under the configured `zk_lockpath_prefix`. It
reads `/etc/pgconsul.conf` and connects exactly the way pgconsul does — SSL
(port 2281) + digest auth — so it works out of the box inside a postgresql
container, which already has `python3-kazoo` and the SSL certs in
`/etc/zk-ssl/`.

For full usage, CLI reference, and output examples see
[`scripts/README.md`](scripts/README.md).
