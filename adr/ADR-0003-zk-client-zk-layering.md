# ADR-0003: Layering and Responsibility Split between `ZkClient` and `Zookeeper`

**Status:** Accepted  
**Date:** 2026-08-05  
**Deciders:** kopylov74, mialinx  
**Ticket:** MDB-41951 (parent: MDB-46662)

---

## Context

Historically `src/zk.py` (`Zookeeper`) was a single class that both managed the KazooClient
connection lifecycle and implemented all pgconsul business semantics (cluster paths, locks,
failover/switchover state, elections). This mixed two distinct responsibilities:

- **Transport:** KazooClient lifecycle, reconnection backoff, kazoo exception translation.
- **Domain:** pgconsul path constants, lock ownership semantics, cluster state aggregation,
  failover/switchover coordination.

The mixed class imported `kazoo.*` directly and caught `kazoo.exceptions.*` throughout business
methods, making it impossible to reason about error boundaries: a `NoNodeError` (valid "no data")
was indistinguishable from a `ConnectionClosedError` (transport failure) at the call site — the
same ambiguity that [ADR-0001](ADR-0001-typed-postgres-exception-hierarchy.md) resolved for the
PostgreSQL layer.

`src/zk_client.py` (`ZkClient`) was extracted as a low-level wrapper. The split is now in code,
but the **convention** — what belongs in which layer, and the exception translation contract — is
not documented. Without a recorded decision, new contributors have no rule to follow and the
layers will gradually re-mix, reproducing the pre-ADR-0001 problem for the ZK layer.

---

## Decision

Formalize a **two-layer architecture** for ZooKeeper access with an explicit responsibility
boundary and exception translation contract.

### §1. Layer responsibilities

| Layer | File | Class | Responsibility |
|-------|------|-------|----------------|
| Infrastructure (Transport) | `src/zk_client.py` | `ZkClient`, `LockHandle` | KazooClient lifecycle, reconnection, path-prefix resolution, primitive data operations, lock-recipe factories, kazoo→domain exception translation |
| Domain | `src/zk.py` | `Zookeeper` | pgconsul path constants, lock ownership semantics, cluster state aggregation, business operations (elections, switchover, failover, maintenance, SSN), `ZkClientError → ZookeeperException` translation |

### §2. Dependency rule

`Zookeeper` (Domain) **must not** import `kazoo.*` directly. All Kazoo access goes through
`ZkClient`. `ZkClient` (Infrastructure) **must not** know pgconsul business semantics — no path
constants, no lock ownership logic, no cluster-state aggregation.

```
main.py ──► Zookeeper (Domain) ──► ZkClient (Infra) ──► KazooClient
              raises                    raises              raises
          ZookeeperException        ZkClientError        kazoo.*
```

### §3. Exception translation contract

1. `ZkClient` translates all `kazoo.exceptions.*` into the `ZkClientError` hierarchy
   (`ZkNoNodeError`, `ZkSessionExpiredError`, `ZkConnectionClosedError`, `ZkLockTimeout`,
   `ZkClientError`). Raw kazoo exceptions **must not** escape `zk_client.py`.
2. `Zookeeper` catches `ZkClientError` and translates to `ZookeeperException` for domain callers.
3. `Zookeeper.noexcept_get()` is the **only** place where `@helpers.return_none_on_error` is
   permitted in the ZK layer — `None` is a valid "no data" signal there (mirrors the single
   exception granted in [ADR-0002](ADR-0002-exception-propagation-to-run-iteration.md) §3 for the
   PG layer). Do **not** apply this decorator to new `ZkClient` or `Zookeeper` methods.

### §4. Placement rule for new code

| New code | Goes into | Rationale |
|----------|-----------|-----------|
| New ZK path constant | `zk.py` (`Zookeeper` class attribute) | Paths are domain semantics |
| New primitive data operation (e.g. transactional multi-write) | `zk_client.py` | Transport-level, no business meaning |
| New business operation (e.g. idempotency marker read/write) | `zk.py` | Composes `ZkClient` primitives with domain logic |
| New kazoo exception type to handle | `zk_client.py` (add to hierarchy + translate) | Keeps kazoo types from leaking to Domain |
| New lock semantics (e.g. conditional acquire) | `zk.py` | Lock ownership is domain policy |

### §5. Reconnection ownership

- `ZkClient.reconnect()` — **connection only**: rebuilds KazooClient with backoff, does not touch
  locks (documented in code: "Connection-only: does not touch locks").
- `Zookeeper.reconnect()` — **connection + locks**: drops stale locks, re-inits
  `PRIMARY_LOCK_PATH` only; other locks re-acquired lazily.

This split prevents `ZkClient` from needing knowledge of which locks exist (a domain concern).

---

## Alternatives

### A. Single class (status quo before extraction)

Keep all logic in `Zookeeper`, importing `kazoo.*` directly.

**Rejected:** Reproduces the ADR-0001 ambiguity (transport error vs. "no data") for the ZK layer.
Makes the class ~1400 lines, untestable in isolation, and forces every business method to know
kazoo exception types.

### B. Three layers (Transport / Repository / Domain)

Introduce an intermediate "ZK Repository" layer that maps domain operations to ZK paths, with
`ZkClient` as pure transport and `Zookeeper` as pure domain orchestration.

**Rejected:** Over-engineering for the current scale. The path constants and business operations
are tightly coupled (paths are defined next to the operations that use them); a separate repository
layer would add indirection without clarifying the boundary. Revisit if pgconsul grows a second
ZK-backed domain (e.g. a separate coordinator for metrics).

### C. Document in AGENTS.md only, no ADR

Add a paragraph to `AGENTS.md` describing the split.

**Rejected:** `AGENTS.md` is an agent guide, not a decision record. It does not capture context,
alternatives, and consequences. ADR-0001/0002 set the precedent that error-handling contracts are
ADR-level decisions; the ZK layering is the same class of decision.

---

## Consequences

### Positive

- **Clear placement rule** for new ZK code — eliminates "where do I add this?" ambiguity.
- **Testable in isolation:** `ZkClient` is unit-tested with mocked `KazooClient`
  (`tests/unit/test_zk_client.py`); `Zookeeper` is tested with mocked `ZkClient`
  (`tests/unit/test_zk_*.py`).
- **Exception boundary is explicit:** domain callers catch `ZookeeperException`, never `kazoo.*`
  or `ZkClientError` directly.
- **Mirrors ADR-0001/0002** — consistent error-handling philosophy across PG and ZK layers.

### Negative

- **Two files to touch** for a new business operation: a primitive in `zk_client.py` (if needed)
  + a domain method in `zk.py`. Slightly more boilerplate.
- **`noexcept_get` remains a special case** — requires the same ongoing discipline as the
  ADR-0002 §3 best-effort exceptions: documented, justified, not copied.

### Neutral

- `path_prefix` resolution lives in `ZkClient._resolve_path()`, but path **constants** live in
  `Zookeeper`. This is intentional: the transport layer resolves, the domain layer names.

---

## Links

- **Related ADRs:**
  - [ADR-0001](ADR-0001-typed-postgres-exception-hierarchy.md) — typed exception hierarchy
    for the PostgreSQL layer (same pattern, applied to ZK)
  - [ADR-0002](ADR-0002-exception-propagation-to-run-iteration.md) — exception propagation
    strategy; `noexcept_get` is the ZK analogue of the best-effort exception
- **Code:**
  - `src/zk_client.py` — `ZkClient`, `LockHandle`, `ZkClientError` hierarchy
  - `src/zk.py` — `Zookeeper`, `ZookeeperException`
- **Tests:**
  - `tests/unit/test_zk_client.py`
  - `tests/unit/test_zk_*.py` — domain-layer tests with mocked `ZkClient`
- **Related Tickets:**
  - MDB-41951 — idempotency algorithm (parent: MDB-46662)
