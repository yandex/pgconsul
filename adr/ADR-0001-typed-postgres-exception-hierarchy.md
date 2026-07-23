# ADR-0001: Typed Exception Hierarchy for the PostgreSQL Layer

**Status:** Accepted  
**Date:** 2026-07-22  
**Deciders:** kopylov74, mialinx  
**Ticket:** MDB-41953 (parent: MDB-46662)

---

## Context

`src/pg.py` contains methods that query PostgreSQL via `psycopg2`. Historically, these methods
were annotated with `@helpers.return_none_on_error` — a decorator that catches **any** exception
and returns `None` instead of propagating it:

```python
def return_none_on_error(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            logging.exception('Unhandled exception in %s', func.__name__)
            return None
    return wrapper
```

This approach creates a critical ambiguity: the return value `None` carries two completely
different meanings in the codebase:

| Meaning | Example |
|---------|---------|
| "no data" (valid empty result) | `get_replication_slots()` returns `None` / `[]` when no slots exist |
| "error" (connection lost, query failed) | `get_replication_slots()` returns `None` because `psycopg2.OperationalError` was swallowed |

Callers in `main.py` either:
- silently skip logic on `None` (treating both cases as "no data"), or
- perform an explicit `res is None` guard, but cannot distinguish the cause.

The codebase mixes two error-handling idioms — Python exceptions and Go-style
`None`-as-error sentinel — producing the worst properties of both: exceptions are invisible
to callers, and `None`-checks require the caller to "know" whether `None` means "empty" or "error".

---

## Decision

Introduce a **typed exception hierarchy** for all PostgreSQL-related errors in `src/exceptions.py`
and consistently raise these exceptions from `pg.py` methods instead of returning `None`.

### Exception hierarchy (added to `src/exceptions.py`)

```python
class PostgresException(pgconsulException):
    """Base class for all PostgreSQL errors. Do not raise directly."""

class PostgresConnectionError(PostgresException):
    """Connection to PostgreSQL is unavailable or dropped.
    Wraps psycopg2.OperationalError."""

class PostgresQueryError(PostgresException):
    """Query executed but returned an unexpected or invalid result."""
```

### Mapping rules for `pg.py`

| Situation | Before | After |
|-----------|--------|-------|
| `psycopg2.OperationalError` (connection lost) | `return None` | raise `PostgresConnectionError` |
| Query returns logically invalid data | `return None` | raise `PostgresQueryError` |
| Empty but valid result (e.g. no slots) | `return []` | `return []` (unchanged) |
| Normal result | `return value` | `return value` (unchanged) |

### Prohibition on catching `PostgresConnectionError` inside `pg.py`

Methods in `pg.py` **must not** catch `PostgresConnectionError` internally and return a safe
default. Doing so hides DB errors from the iteration loop and prevents proper iteration restart.
The **only** allowed exception is `reconnect()`, which must handle connection errors by definition.

### `@helpers.return_none_on_error` retention policy

The decorator **must not** be applied to any new `pg.py` methods.
It is intentionally retained only on `zk.noexcept_get()` — the one place where `None` is a
valid "no data" signal (ZK optional reads are non-blocking by design).

---

## Alternatives

### A1. Keep `@return_none_on_error` + add explicit `None` guards everywhere

Callers in `main.py` check `if res is None: return` or `if res is None: raise ...`.

**Against:**
- Perpetuates the ambiguity between "empty result" and "error"
- Not idiomatic Python — mixes two incompatible paradigms
- Every new caller must remember to guard against `None`
- Error context is logged at the decorator level; caller loses the traceback

### A2. Introduce a sentinel object (e.g. `MISSING = object()`)

Methods return `MISSING` on error and a real value otherwise.

**Against:**
- Adds a new abstraction that still requires caller-side checks
- Does not carry error information (type, message, traceback)
- Not standard Python practice; harder to integrate with `mypy`

### A3. Return `Optional[T]` and document the convention clearly

Keep `None` returns, but strictly document "None = error, [] = empty".

**Against:**
- Documentation drift is guaranteed over time
- `mypy` cannot distinguish the two `None` meanings at the type level
- Does not fix the root cause; formalises the ambiguity without resolving it

---

## Consequences

### Positive
- ✅ **Disambiguation:** connection errors are distinguishable from empty results at the type level
- ✅ **Observability:** exceptions carry a full traceback; callers get complete context
- ✅ **mypy compatibility:** `Optional[T]` return types can be narrowed where `None` was only returned on error
- ✅ **Fail-fast:** uncaught `PostgresConnectionError` propagates to `run_iteration()` and triggers iteration restart — the correct behaviour
- ✅ **Prevents a class of bugs** where empty-result logic was applied to error conditions

### Negative
- ❌ **Migration effort:** all call sites of `@return_none_on_error`-decorated methods must be audited and updated
- ❌ **Risk during transition:** if a call site is not updated, an unhandled `PostgresConnectionError` may surface as an unexpected exception — however this is a **safer** failure mode than silently operating on stale data

### Technical Debt Resolved
- `@helpers.return_none_on_error` usage on `pg.py` methods
- Mixed `None`/exception idiom across `pg.py` + `main.py`

---

## Revisit Criteria

Reconsider if:
1. A method genuinely needs to return `None` as a valid "no data" signal **and** can also fail — introduce a dedicated `Result` type or a domain-specific sentinel in that case.
2. A future refactoring merges `pg.py` and `zk.py` error handling into a unified infrastructure layer — revisit the hierarchy to avoid duplication.

---

## Links

- **Related ADR:**
  - [ADR-0002](ADR-0002-exception-propagation-to-run-iteration.md) — Exception propagation strategy to `run_iteration()`

- **Related Code:**
  - [`src/exceptions.py`](../src/exceptions.py) — exception hierarchy
  - [`src/helpers.py`](../src/helpers.py) — `return_none_on_error` decorator
  - [`src/pg.py`](../src/pg.py) — PostgreSQL abstraction layer

- **Related Tickets:**
  - MDB-41953 — this ticket
  - MDB-46662 — parent refactoring epic
