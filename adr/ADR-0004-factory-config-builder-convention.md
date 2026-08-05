# ADR-0004: Factory + Config-Builder Convention for Infrastructure Components

**Status:** Accepted
**Date:** 2026-08-05
**Deciders:** kopylov74
**Ticket:** MDB-41951

---

## Context

`main.py` is a God Object (~2200 lines). Part of the bloat comes from
constructor-methods (`_commands()`, `_postgres_config()`) that parse
`RawConfigParser` and know the details of INI sections (`global`,
`commands`, etc.). During the decomposition driven by MDB-41951, a
consistent pattern for creating components is needed so that `main.py`
remains a thin orchestrator.

A pattern has already emerged organically across five infrastructure
components:

| Component | Factory | Config builder | File |
|-----------|---------|-----------------|------|
| `Zookeeper` | `create_zk(config)` | (inline) | `zk.py` |
| `ReplicationManager` | `create_replication_manager(config, db, zk)` | `build_replication_manager_config(config)` | `replication_manager.py` |
| `SlotManager` | `create_replication_slot_manager(config, db, zk)` | (inline) | `slot_manager.py` |
| `Postgres` | `create_postgres(config, cmd_manager)` | `build_postgres_config(config)` | `pg.py` |
| `CommandManager` | `create_command_manager(config)` | `build_command_manager_config(config)` | `command_manager.py` |

The standalone `replication_manager_factory.py` module was deleted in
favour of co-locating the factory with the class, and
`create_postgres` / `create_command_manager` were introduced following the
same pattern.

The upcoming stages of MDB-41951 will add 6+ new classes
(`SwitchoverStateMachine`, `FailoverManager`, `ReturnToClusterManager`,
`ClusterRepairer`, `MaintenanceHandler`, `TimingTracker`). Without a
documented convention, the first contributor who has not studied all five
precedents will likely break the pattern.

---

## Decision

Every infrastructure component of pgconsul is created through a pair of
functions located in the **same module** as the class definition:

### 1. `build_<name>_config(config: RawConfigParser) -> <Name>Config`

- Parses one or more INI sections and returns a `@dataclass` config object.
- Validates values (ranges, required fields) and raises `ValueError` on
  invalid input.
- Is testable **without** instantiating the component itself (no DB/ZK
  mocks required for config-parsing tests).

### 2. `create_<name>(config, *dependencies) -> <Name>`

- Calls `build_<name>_config(config)` to obtain the config.
- Passes the config and already-constructed dependencies into the class
  constructor.
- Dependencies are injected as **ready objects**, never as `RawConfigParser`
  and never via a service locator.

### Rules

| Rule | Rationale |
|------|-----------|
| The factory lives in the **same file** as the class (not in a separate `*_factory.py`) | Avoids an extra module, circular imports, and deferred-import workarounds |
| `main.py` calls `create_*` and **does not parse config** itself | Keeps `main.py` a thin orchestrator; section names are an implementation detail of the component |
| The class constructor accepts a ready `*Config` + dependencies, **not** `RawConfigParser` | Separates parsing from construction; enables unit tests without an INI file |
| `*Config` is a `@dataclass` (or `pydantic.BaseModel`), **not** a `dict` | Type safety, `mypy --strict` compatibility, explicit field declarations |
| `build_*_config` and `create_*` are **separate functions** | Config parsing can be tested in isolation; the factory is a thin wrapper over the builder |

### Exception: single-parameter components

When a component needs **exactly one** scalar configuration value, the
`build_*_config` / `create_*` pair and the `*Config` dataclass are
disproportionate boilerplate. In that case:

- The class constructor accepts the scalar value directly (typed, e.g.
  `log_timing_command: str | None`), **not** `RawConfigParser` and **not**
  a one-field `*Config` dataclass.
- No factory function is added.
- `main.py` reads the single value and passes it to the constructor:
  `TimingTracker(self.zk, self.config.get('commands', 'log_timing', fallback=None))`.

This is the only situation where `main.py` parses config for a component.
The exception exists because a one-field dataclass + two wrapper functions
add noise without improving testability or separation of concerns.

Precedent: `TimingTracker` (`src/timings.py`).

### Dependency injection direction

Dependencies flow inward per the Dependency Rule (Clean Architecture):

```
main.py  →  create_<name>(config, db, zk, ...)
              │
              ├─ build_<name>_config(config) → <Name>Config
              │
              └─ <Name>(config, db, zk, ...)
```

The component never reaches outward to create its own dependencies; they
are handed in by the caller (the factory, invoked by `main.py`).

---

## Alternatives

### A1. Separate `*_factory.py` module

Each component gets a dedicated `foo_factory.py` containing the config
dataclass, the builder, and the factory.

**Rejected:**
- Produces an extra module with no benefit.
- Required a deferred import (`from .replication_manager import
  ReplicationManager` inside the factory function) to break a circular
  dependency — a code smell that disappears when the factory is co-located
  with the class.
- Five existing components already follow the co-location pattern; a
  separate module would be inconsistent.

### A2. Constructor parses `RawConfigParser` directly

The class `__init__` accepts `config: RawConfigParser` and reads sections
internally.

**Rejected:**
- Breaks testability: you cannot construct the object without a full INI
  file, even in a unit test that only checks behaviour.
- Violates SRP: the class is both a domain component and a config parser.
- Makes it impossible to test config parsing in isolation (you would need
  to instantiate the real component, requiring DB/ZK mocks).

### A3. Service locator / DI container

A central registry holds all components; classes resolve dependencies by
key at runtime.

**Rejected:**
- Overkill for a single-process daemon with a fixed, small set of
  components.
- No precedent in the codebase; introduces a new abstraction layer and
  new failure modes (missing key, wrong type).
- Hides the dependency graph, making it harder to reason about startup
  order — which is currently explicit in `main.py.__init__`.

### A4. Do not document; rely on convention and code review

**Rejected:**
- The convention is not self-evident: the choice of co-location (A1 vs.
  current), the separation of `build_*_config` from `create_*`, and the
  prohibition on `main.py` parsing config are all non-obvious decisions
  with trade-offs.
- ADR-0001 and ADR-0002 already established that pgconsul conventions are
  documented formally, not left to oral tradition.
- MDB-41951 stages 3–6 will add 6+ new classes; without a reference, the
  pattern will fragment.

---

## Consequences

### Positive

- `main.py` does not know the details of component configuration (section
  names, field names, validation rules).
- Config parsing is testable in isolation — `TestBuildPostgresConfig` and
  `TestBuildCommandManagerConfig` run without DB/ZK mocks.
- New components follow a single pattern — fewer "how do I create this?"
  decisions for contributors.
- The dependency graph remains explicit in `main.py.__init__`, preserving
  startup-order clarity.

### Negative

- Each component requires two functions plus a `@dataclass` config — more
  boilerplate than a single constructor.
- Dependencies must be created **before** calling the factory, so the
  order of `create_*` calls in `main.py.__init__` matters (e.g.
  `create_postgres` needs `cmd_manager`, so `create_command_manager` must
  come first). This is intentional and visible, but must be maintained.
- A component that needs a dynamically-resolved dependency (e.g. "create
  `db` only after ZK is connected") does not fit the static factory model
  cleanly — such cases require a lazy factory or a two-phase init, which
  this ADR does not cover.

---

## Scope

**In scope:** creation of infrastructure components (`Postgres`,
`Zookeeper`, `CommandManager`, `ReplicationManager`, `SlotManager`, and
all future classes from the MDB-41951 decomposition plan).

**Out of scope:**
- Class naming conventions (`CamelCase`, etc.) — style, not architecture.
- Error-handling contracts — covered by ADR-0001 and ADR-0002.
- Iteration operation order — covered by the idempotent-iterations ADR
  (draft).
- Module structure (`iteration/*.py`) — covered by the architecture
  proposal for MDB-41951.

---

## Links

- **Related ADRs:**
  - [ADR-0001](ADR-0001-typed-postgres-exception-hierarchy.md) — Typed exception hierarchy (sets the tone for formal conventions)
  - [ADR-0002](ADR-0002-exception-propagation-to-run-iteration.md) — Exception propagation strategy
- **Related code:**
  - [`src/pg.py`](../src/pg.py) — `build_postgres_config`, `create_postgres`
  - [`src/command_manager.py`](../src/command_manager.py) — `build_command_manager_config`, `create_command_manager`
  - [`src/replication_manager.py`](../src/replication_manager.py) — `build_replication_manager_config`, `create_replication_manager`
  - [`src/zk.py`](../src/zk.py) — `create_zk`
  - [`src/slot_manager.py`](../src/slot_manager.py) — `create_replication_slot_manager`
  - [`src/timings.py`](../src/timings.py) — `TimingTracker` (single-parameter exception)
- **Related tickets:**
  - MDB-41951 — idempotency algorithm; driver of the decomposition that
    motivated this convention.
