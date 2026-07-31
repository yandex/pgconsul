# ADR-0003: Best-Effort Operations — Allowed PostgresConnectionError Handling Outside Critical Sections

**Status:** Accepted  
**Date:** 2026-07-31  
**Deciders:** kopylov74  
**Ticket:** MDB-41953 (parent: MDB-46662)

---

## Context

ADR-0002 §1 establishes the default rule: `PostgresConnectionError` must propagate to
`run_iteration()`. The only explicitly allowed exception is `reconnect()`.

However, certain operations in pgconsul are **non-critical maintenance tasks** that:
- Run on every iteration (automatic retry)
- Do not affect cluster availability or data integrity if skipped
- Have a well-defined fallback (skip and retry next iteration)

Catching `PostgresConnectionError` for these operations avoids polluting `run_iteration()`
with noise from transient DB outages on non-critical paths.

---

## Decision

Introduce the concept of **Best-Effort Operations** — operations that may legitimately catch
`PostgresConnectionError` and return early, as an explicit exception to ADR-0002 §1.

### Criteria for Best-Effort classification

An operation qualifies as Best-Effort if **all** of the following are true:

| # | Criterion | Rationale |
|---|-----------|-----------|
| 1 | **Non-blocking:** Skipping the operation does not prevent the iteration from completing normally | The cluster continues operating even if the operation is skipped |
| 2 | **Self-healing:** The operation is called on every iteration (or on a short, bounded interval) | A transient DB outage will be retried automatically without manual intervention |
| 3 | **No data loss:** Skipping the operation cannot cause data divergence, split-brain, or loss of availability | The operation is maintenance, not a correctness-critical step |
| 4 | **No critical section:** The operation is not called from within a switchover or failover critical section | Critical sections are already covered by ADR-0002 §2 |

### Operations currently classified as Best-Effort

| Operation | Location | Rationale |
|-----------|----------|-----------|
| Replication slot sync | `slot_manager.handle_slots()` | Non-critical maintenance; skipped slots are created on next iteration; no data loss if skipped |

### Rules for Best-Effort exception handling

When catching `PostgresConnectionError` in a Best-Effort operation:

1. **Only** `PostgresConnectionError` may be caught — other exceptions must propagate
2. The catch block **must** log at `warning` level with `exc_info=True` (full traceback preserved)
3. The catch block **must** return early (not continue with partial state)
4. The operation **must not** be called from a switchover/failover critical section

---

## Alternatives

### A1. Propagate all PostgresConnectionError to run_iteration()

Remove all Best-Effort exception handling and let every `PostgresConnectionError` reach
`run_iteration()`.

**Against:**
- `run_iteration()` already catches all exceptions with `except Exception` (see N-1), so the
  end result is the same — iteration restarts. Best-Effort handling avoids this extra round-trip.
- Non-critical DB outages on maintenance paths would trigger iteration restarts, adding overhead
  and noise without improving correctness.

### A2. Expand the list of critical sections in ADR-0002

Add Best-Effort operations to the list of critical sections in ADR-0002 §2.

**Against:**
- ADR-0002 §2 defines critical sections by their semantics (switchover/failover), not by
  enumerating individual operations. Adding operations by name would mix two different
  classification schemes.

---

## Consequences

### Positive
- ✅ Non-critical DB outages are handled locally without polluting `run_iteration()`
- ✅ Clear, documented criteria for when Best-Effort classification is appropriate
- ✅ Self-healing: transient failures auto-retry on next iteration

### Negative
- ❌ Introduces a second exception-handling pattern alongside the default propagate rule
- ❌ Any new Best-Effort operation must be reviewed against the criteria and added to this ADR

---

## Revisit Criteria

Reconsider if:
1. A Best-Effort operation is moved to a critical section — its exception handling must be removed
2. The self-healing property is lost (e.g., operation is no longer called every iteration) —
   reclassify as blocking
3. A new category of non-critical operations emerges — update this ADR with the criteria
   and add the operation to the table

---

## Links

- **Related ADR:**
  - [ADR-0001](ADR-0001-typed-postgres-exception-hierarchy.md) — Typed Exception Hierarchy
  - [ADR-0002](ADR-0002-exception-propagation-to-run-iteration.md) — Exception propagation to `run_iteration()`

- **Related Code:**
  - [`src/slot_manager.py`](../src/slot_manager.py) — `handle_slots()` — Best-Effort operation
  - [`src/main.py`](../src/main.py) — `replica_iter()`, `sync_replica_iter()` — callers of Best-Effort operations

- **Related Tickets:**
  - MDB-41953 — this ticket
  - MDB-46662 — parent refactoring epic
