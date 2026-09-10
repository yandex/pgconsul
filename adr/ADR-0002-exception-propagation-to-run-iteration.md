# ADR-0002: Exception Propagation Strategy to `run_iteration()`

**Status:** Accepted  
**Date:** 2026-07-22  
**Deciders:** kopylov74, mialinx  
**Ticket:** MDB-41953 (parent: MDB-46662)

---

## Context

`pgconsul` operates as an infinite loop: every second `run_iteration()` is called, which
determines the node role and dispatches to `primary_iter()`, `replica_iter()`,
`non_ha_replica_iter()`, or `dead_iter()`.

```python
while should_run():
    run_iteration()   # restart on any unhandled exception
    timer.sleep(...)
```

With the introduction of typed PostgreSQL exceptions (see [ADR-0001]) the question becomes:
**who should catch `PostgresConnectionError` / `PostgresQueryError`, and where?**

Two fundamentally different strategies exist:

| Strategy | Description |
|----------|-------------|
| **"Python-way"** | Let exceptions propagate freely to `run_iteration()`; add selective `try/except` only in critical sections where restarting the iteration is unsafe |
| **"Go-way"** | Handle exceptions at the call site; return `None` or a fallback value; guard every call with `if res is None` |

The "Go-way" was the previous approach (via `@return_none_on_error`) and is being retired
per ADR-0001. The new policy must be stated explicitly so that all contributors follow
the same convention.

However, certain operations in pgconsul are **non-critical maintenance tasks** that:
- Run on every iteration (automatic retry)
- Do not affect cluster availability or data integrity if skipped
- Have a well-defined fallback (skip and retry next iteration)

For these operations, propagating `PostgresConnectionError` to `run_iteration()` adds noise
without improving correctness — the iteration would restart only to retry the same
maintenance task on the next cycle.

---

## Decision

Adopt the **"Python-way" exception propagation** model with three tiers:

### §1. Default: let exceptions propagate to `run_iteration()`

Methods in `pg.py` raise `PostgresConnectionError` or `PostgresQueryError`.
Callers in `main.py` / `replication_manager.py` do **not** catch these exceptions unless
they are in a critical section (§2) or a best-effort operation (§3).
`run_iteration()` catches any unhandled exception, logs it, and starts the next iteration.

### §2. Critical sections that cannot safely restart the iteration

Some operations are stateful and cannot be interrupted mid-flight:
- **Switchover** (`utils.Switchover`) — the cluster is already transitioning; restarting
  the iteration without completing or cleanly aborting the switchover would leave the
  cluster in an inconsistent state.
- **Failover election** (`failover_election.py`) — the election protocol has timing
  invariants; a silent restart could cause split-brain.
- **Post-promote WAL upload** (`pg._upload_wals()`) — called after the node has already
  become primary; any unhandled exception would propagate through `promote()` and could
  mislead callers into thinking promote failed. The broad `except Exception` here is
  intentional and documented.

In these sections, callers **must** explicitly `try/except PostgresConnectionError` (and/or
`PostgresQueryError`) and either raise a domain-specific exception
(`SwitchoverException`, `FailoverException`) or take a safe compensating action.

The switchover critical section starts **after** `zk.try_acquire_lock()`. Pre-lock calls
(e.g. `slot_manager.create_slots_for_hosts()`) are not critical: a `PostgresConnectionError`
propagates to `run_iteration()` (§1), no lock is held, and missing slots are recreated by
the Best-Effort `handle_slots()` (§3). No explicit `try/except` is needed there.

### §3. Best-Effort operations

**Best-Effort operations** are non-critical maintenance tasks that may legitimately catch
`PostgresConnectionError` and return early, as an explicit exception to §1.

#### Criteria for Best-Effort classification

An operation qualifies as Best-Effort if **all** of the following are true:

| # | Criterion | Rationale |
|---|-----------|-----------|
| 1 | **Non-blocking:** Skipping does not prevent the iteration from completing normally | The cluster continues operating even if the operation is skipped |
| 2 | **Self-healing:** The operation is called on every iteration (or on a short, bounded interval) | A transient DB outage will be retried automatically without manual intervention |
| 3 | **No data loss:** Skipping cannot cause data divergence, split-brain, or loss of availability | The operation is maintenance, not a correctness-critical step |
| 4 | **No critical section:** The operation is not called from within a switchover or failover critical section | Critical sections are already covered by §2 |

#### Operations currently classified as Best-Effort

| Operation | Location | Rationale |
|-----------|----------|-----------|
| Replication slot sync | `slot_manager.handle_slots()` | Non-critical maintenance; skipped slots are created on next iteration; no data loss if skipped |
| Sessions ratio for load-based replication type | `replication_manager._get_needed_replication_type_without_await_before_async()` | Optional metric; skipping returns conservative 'sync' default; no data loss; retried every iteration |
| Streaming check in recovery loop | `main._check_postgresql_streaming()` | Post-failover/switchover recovery (`_wait_for_streaming`); returns None on DB loss so the `await_for` loop retries; self-healing; no data loss |

`_check_postgresql_streaming()` is an exception to criterion #4: it is called from critical
sections (`_do_primary_switchover`, `_accept_failover`), but its Best-Effort behaviour is a
**wait** (return `None` → retry), not an abort. A DB loss keeps the transition waiting for
streaming to resume — the desired compensating action — instead of cancelling it.

#### Rules for Best-Effort exception handling

When catching `PostgresConnectionError` in a Best-Effort operation:

1. **Only** `PostgresConnectionError` may be caught — other exceptions must propagate
2. The catch block **must** log at `warning` level with `exc_info=True` (full traceback preserved)
3. The catch block **must** return early (not continue with partial state)
4. The operation **must not** be called from a switchover/failover critical section

### Decision rule (applied per call site)

```
Is the caller inside a critical section (switchover / failover election)?
├── YES → add try/except PostgresConnectionError; raise domain exception or handle explicitly
└── NO  → Is this a Best-Effort operation (meets all 4 criteria)?
          ├── YES → catch PostgresConnectionError, log warning, return early
          └── NO  → do not catch; let the exception propagate to run_iteration()
```

### What the iteration loop does on an unhandled exception

The restart boundary lives in the `run()` loop that drives `run_iteration()`, not inside
`run_iteration()` itself (see [`src/main.py`](../src/main.py)):

```python
while should_run():
    try:
        self.run_iteration(my_prio)
    except PostgresConnectionError as e:
        # Expected transient DB errors: log as warning, restart iteration.
        logging.warning('PostgreSQL error during iteration, will retry: %s', e)
    except Exception:
        logging.exception('Unexpected error during run_iteration')
```

Two handler tiers are intentional:
- `PostgresConnectionError` — an **expected** transient DB outage; logged at `warning`
  and the loop restarts the iteration. It is the typed exception raised by `pg.py` per
  ADR-0001 and propagated per §1.
- `except Exception` — any **unexpected** error; logged at `exception` level with a full
  traceback.

`PostgresQueryError` is intentionally **not** caught here (see ADR-0001 §Revisit Criteria):
no `pg.py` method raises it yet, so it would fall through to `except Exception` and be
logged with a traceback if it ever appears.

This guarantees that any DB error that escapes a non-critical caller is logged and the
daemon continues on the next iteration — the safest possible default.

---

## Alternatives

### A1. "Go-way": handle at every call site, return `None` on error

Every `pg.py` caller checks `if res is None` and returns early.

**Against:**
- Perpetuates the root cause of MDB-41953 (see ADR-0001)
- Callers must be aware of the `None`-means-error convention
- Errors are silently swallowed; no traceback at the call site
- Logic that depends on an empty result vs. an error behaves incorrectly

### A2. Catch all exceptions in `primary_iter()` / `replica_iter()` top-level

Add a single `try/except` at the top of each `*_iter()` method.

**Against:**
- Equivalent to the current behaviour (swallows exceptions one level higher)
- Does not propagate context to `run_iteration()` for uniform logging
- Still does not distinguish "connection error" from "logic error"

### A3. Catch only `PostgresConnectionError` everywhere, re-raise others

**Against:**
- Creates a large number of identical boilerplate `try/except` blocks
- Violates the single-responsibility principle: each method handles its own error
  *and* the iteration-restart policy
- The same goal is achieved more cleanly by propagating to `run_iteration()`

### A4. Propagate all `PostgresConnectionError` including Best-Effort operations

Remove Best-Effort exception handling and let every `PostgresConnectionError` reach
`run_iteration()`.

**Against:**
- `run_iteration()` already catches all exceptions with `except Exception`, so the
  end result is the same — iteration restarts. Best-Effort handling avoids this extra round-trip.
- Non-critical DB outages on maintenance paths would trigger iteration restarts, adding overhead
  and noise without improving correctness.

---

## Consequences

### Positive
- ✅ **Uniform error handling:** all DB errors are logged at a single point (`run_iteration()`) with a consistent format
- ✅ **Less boilerplate:** callers do not need per-call `if res is None` guards
- ✅ **mypy-friendly:** return types of `pg.py` methods no longer need `Optional[T]` where `None` signalled an error
- ✅ **Explicit critical sections:** the need for `try/except` in switchover/failover code is documented and intentional, not accidental
- ✅ **Best-Effort clarity:** non-critical maintenance operations have documented criteria and handling rules

### Negative
- ❌ **Transition risk:** existing callers that rely on `None`-as-error must be audited before removing `@return_none_on_error`; a missing audit causes an unhandled exception to surface in `run_iteration()` — a **visible** failure, but still a failure
- ❌ **Learning curve:** contributors must understand which call sites are "critical" and which are "best-effort"
- ❌ **Any new Best-Effort operation must be reviewed** against the criteria and added to the table in §3

### Technical Debt Introduced
- A catalogue of "critical sections" must be maintained (currently: switchover, failover election). New critical sections must be identified and documented when added.
- A catalogue of "Best-Effort operations" must be maintained (§3 table). New operations must be classified explicitly.

### Technical Debt Resolved
- Implicit `None`-propagation through `@return_none_on_error` in non-critical paths

---

## Revisit Criteria

Reconsider if:
1. A new operation is introduced that is neither a full iteration nor a named critical section (e.g. a background thread) — define its error boundary explicitly.
2. `run_iteration()` is split into smaller autonomous units — re-evaluate where the "restart" boundary sits.
3. A Best-Effort operation is moved to a critical section — its exception handling must be removed.
4. The self-healing property of a Best-Effort operation is lost (e.g., operation is no longer called every iteration) — reclassify as blocking.

---

## Links

- **Related ADR:**
  - [ADR-0001](ADR-0001-typed-postgres-exception-hierarchy.md) — Typed exception hierarchy for the PostgreSQL layer

- **Related Code:**
  - [`src/main.py`](../src/main.py) — `run_iteration()`, `primary_iter()`, `replica_iter()`
  - [`src/utils.py`](../src/utils.py) — `Switchover`, `Failover` classes
  - [`src/failover_election.py`](../src/failover_election.py) — failover election logic
  - [`src/exceptions.py`](../src/exceptions.py) — `SwitchoverException`, `FailoverException`, `PostgresConnectionError`
  - [`src/slot_manager.py`](../src/slot_manager.py) — `handle_slots()` — Best-Effort operation
  - [`src/replication_manager.py`](../src/replication_manager.py) — Best-Effort operation (sessions ratio)

- **Related Tickets:**
  - MDB-41953 — this ticket
  - MDB-41954 — switchover protocol refactoring (tightly coupled)
  - MDB-46662 — parent refactoring epic
