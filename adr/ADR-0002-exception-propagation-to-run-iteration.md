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
per ADR-0003. The new policy must be stated explicitly so that all contributors follow
the same convention.

---

## Decision

Adopt the **"Python-way" exception propagation** model:

1. **Default: let exceptions propagate to `run_iteration()`.**  
   Methods in `pg.py` raise `PostgresConnectionError` or `PostgresQueryError`.
   Callers in `main.py` / `replication_manager.py` do **not** catch these exceptions unless
   they are in a critical section (see below).
   `run_iteration()` catches any unhandled exception, logs it, and starts the next iteration.

2. **Exception: critical sections that cannot safely restart the iteration.**  
   Some operations are stateful and cannot be interrupted mid-flight:
   - **Switchover** (`utils.Switchover`) — the cluster is already transitioning; restarting
     the iteration without completing or cleanly aborting the switchover would leave the
     cluster in an inconsistent state.
   - **Failover election** (`failover_election.py`) — the election protocol has timing
     invariants; a silent restart could cause split-brain.
   
   In these sections, callers **must** explicitly `try/except PostgresConnectionError` (and/or
   `PostgresQueryError`) and either raise a domain-specific exception
   (`SwitchoverException`, `FailoverException`) or take a safe compensating action.

3. **`reconnect()` is the only method in `pg.py` that may catch `PostgresConnectionError`.**  
   This is structurally necessary: `reconnect()` is the recovery path for lost connections.

### Decision rule (applied per call site)

```
Is the caller inside a critical section (switchover / failover election)?
├── YES → add try/except PostgresConnectionError; raise domain exception or handle explicitly
└── NO  → do not catch; let the exception propagate to run_iteration()
```

### What `run_iteration()` does on an unhandled exception

```python
def run_iteration(self):
    try:
        ...
    except Exception:
        logging.exception('Unhandled exception in run_iteration')
        # iteration ends; the loop starts the next one after sleep
```

This guarantees that any DB error that escapes a non-critical caller is logged with a full
traceback and the daemon continues on the next iteration — the safest possible default.

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

---

## Consequences

### Positive
- ✅ **Uniform error handling:** all DB errors are logged at a single point (`run_iteration()`) with a consistent format
- ✅ **Less boilerplate:** callers do not need per-call `if res is None` guards
- ✅ **mypy-friendly:** return types of `pg.py` methods no longer need `Optional[T]` where `None` signalled an error
- ✅ **Explicit critical sections:** the need for `try/except` in switchover/failover code is documented and intentional, not accidental

### Negative
- ❌ **Transition risk:** existing callers that rely on `None`-as-error must be audited before removing `@return_none_on_error`; a missing audit causes an unhandled exception to surface in `run_iteration()` — a **visible** failure, but still a failure
- ❌ **Learning curve:** contributors must understand which call sites are "critical" and require explicit error handling

### Technical Debt Introduced
- A catalogue of "critical sections" must be maintained (currently: switchover, failover election). New critical sections must be identified and documented when added.

### Technical Debt Resolved
- Implicit `None`-propagation through `@return_none_on_error` in non-critical paths

---

## Revisit Criteria

Reconsider if:
1. A new operation is introduced that is neither a full iteration nor a named critical section (e.g. a background thread) — define its error boundary explicitly.
2. `run_iteration()` is split into smaller autonomous units — re-evaluate where the "restart" boundary sits.

---

## Links

- **Related ADR:**
  - [ADR-0001](ADR-0001-typed-postgres-exception-hierarchy.md) — Typed exception hierarchy for the PostgreSQL layer

- **Related Code:**
  - [`src/main.py`](../src/main.py) — `run_iteration()`, `primary_iter()`, `replica_iter()`
  - [`src/utils.py`](../src/utils.py) — `Switchover`, `Failover` classes
  - [`src/failover_election.py`](../src/failover_election.py) — failover election logic
  - [`src/exceptions.py`](../src/exceptions.py) — `SwitchoverException`, `FailoverException`, `PostgresConnectionError`

- **Related Tickets:**
  - MDB-41953 — this ticket
  - MDB-41954 — switchover protocol refactoring (tightly coupled)
  - MDB-46662 — parent refactoring epic
