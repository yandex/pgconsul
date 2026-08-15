# ADR-0006: Cluster-Op State Machines — Pure Handlers with Command Plans (Functional Core / Imperative Shell)

**Status:** Accepted
**Date:** 2026-08-10
**Deciders:** kopylov74
**Ticket:** MDB-41951

---

## Context

ADR-0005 established the level-triggered reconciliation model: an iteration is a
function of observed state `f(db_state, zk_state)`, and multi-step processes
(switchover) are explicit state machines that persist their phase to ZK and take
one step per iteration.

The switchover machines introduced by that work — `PrimarySwitchoverMachine` and
`CandidateSwitchoverMachine` in [`src/switchover/`](../src/switchover/primary.py)
(`primary.py`, `candidate.py`, `types.py`) — are correct but **not pure**. Each
phase handler mixes three concerns:

1. **Observation** — reading fresh state mid-handler
   (`db.get_replics_info('primary')`, a second `zk.get_switchover_state()`,
   `zk.get_current_lock_holder()`, `zk.is_host_alive()`).
2. **Decision** — sanity gates, sync checks, idempotency fences.
3. **Effect** — writing ZK phase, stopping PostgreSQL, releasing the lock,
   starting timers, calling composite operations (`do_failover`,
   `rewind_from_source`).

Because effects are executed inline, dependencies are injected as a large bag of
callbacks. `PrimaryContext` carries ~19 fields (infra objects + bound `pgconsul`
methods); `CandidateContext` carries ~7. Consequences:

- **Tests require heavy mocking.** Every handler test builds a `MagicMock`
  context and asserts on `mock.method.assert_called_*`. The
  `_make_scheduled_context` helper in
  [`tests/unit/test_switchover_machine.py`](../tests/unit/test_switchover_machine.py)
  configures ~12 callbacks just to exercise the 8 sanity gates of
  `_handle_scheduled`. Tests verify *interactions*, not *decisions*.
- **The handler signature lies.** `step(record, db_state, zk_state)` suggests
  the handler is a function of the passed-in snapshots, but handlers ignore
  parts of them and read fresh state instead. The real inputs are implicit.
- **Idempotency and the "persist phase before action" fence live in prose and
  imperative code**, not in a reviewable data structure.

The upcoming Stage 6 work (`FailoverManager`, `ReturnToClusterManager`) will add
two more state machines. Their effect vocabulary overlaps heavily with
switchover (lock acquire/release, timers, `failover_state`, timeline,
stop/pooler, checkpoint). Without a fixed convention each machine would reinvent
the context-of-callbacks style and its own execution glue, entrenching the
mocking burden and duplicating I/O handling across the codebase.

The user's proposal: each handler should take observed state as input and
**return a set of commands** that a separate mechanism executes. This turns the
machine into a pure, mock-free, easily testable class and simplifies the code.

---

## Decision

Adopt the **Functional Core / Imperative Shell** pattern for switchover **and all
future cluster-op state machines** (failover, return-to-cluster). Effects become
**data** (Command objects); a **single, shared Executor** interprets them.

### 1. Observation (read-model) — the sole handler input

Introduce a typed, immutable snapshot that carries everything a handler needs.
It is assembled **once** at the start of the step by the shell (which owns the
infra objects), so the handler performs **no I/O**.

```python
@dataclass(frozen=True)
class SwitchoverObservation:
    record: SwitchoverRecord          # phase + hostname/timeline/candidate/side_replicas
    my_hostname: str
    role: str | None                  # db.get_role()
    zk_timeline: int | None
    failover_state: str | None
    last_failover_ts: float | None
    last_switchover_ts: float | None
    ha_replics: frozenset[str] | None
    replics_info: ReplicaInfos        # fresh, from the correct source for the phase
    streaming_replicas: tuple[str, ...]
    live_switchover_state: SwitchoverPhase | None   # fresh re-read of switchover/state;
    #   lets the primary detect the candidate's INITIATED→CANDIDATE_FOUND
    #   transition without persisting its own phase for it (see plan_initiated).
    candidate_alive: bool | None
    lock_holder: str | None
    switchover_timer_started: bool
    downtime_timer_started: bool
    # ... extended incrementally, one field per read the handlers need
```

Rule: **the raw `db_state` / `zk_state` dicts are NOT the handler contract.**
They are start-of-iteration snapshots and lack phase-specific reads
(candidate aliveness, fresh `replics_info`, streaming replicas). The `plan()`
function consumes an `Observation`, not raw dicts. Each machine family may have
its own observation dataclass (`SwitchoverObservation`, later
`FailoverObservation`) since their read-sets differ; the **command vocabulary**
below is shared.

### 2. Commands — one shared vocabulary, logically grouped

Each effect a handler can request is a frozen dataclass with no behaviour. A
handler returns an ordered `Plan` (a list of commands, executed in order;
execution stops at the first failing command). Commands live in **one module**
(`src/commands.py`), grouped by scope so that switchover and failover machines
draw from the same namespace.

**Common commands** (used by every cluster-op machine):

```python
@dataclass(frozen=True)
class AcquireLock:         lock_type: str | None = None; allow_queue: bool = True; timeout: float = 0
@dataclass(frozen=True)
class ReleaseLock:         lock_type: str | None = None; wait: float = 0
@dataclass(frozen=True)
class StartTimer:          name: str; ts: float | None = None
@dataclass(frozen=True)
class StopTimer:           name: str; track_as: str | None = None
@dataclass(frozen=True)
class WriteFailoverState:  value: str
@dataclass(frozen=True)
class WriteTimeline:       timeline: int
@dataclass(frozen=True)
class StopPooler:          pass
@dataclass(frozen=True)
class StopPostgresql:      wait: bool = True; force_async: bool = False; timeout: float | None = None
@dataclass(frozen=True)
class Checkpoint:          pass
@dataclass(frozen=True)
class StoreReplicsInfo:    pass
@dataclass(frozen=True)
class LeaveSyncGroup:      pass
@dataclass(frozen=True)
class Sleep:               seconds: float          # WAL-drain delay only; NOT a cluster-event wait
@dataclass(frozen=True)
class Log:                 message: str; level: str = 'info'; event: bool = False
```

**Switchover-specific commands:**

```python
@dataclass(frozen=True)
class TransitionTo:        phase: SwitchoverPhase   # persists switchover/state
@dataclass(frozen=True)
class WriteCandidate:      candidate: str
@dataclass(frozen=True)
class WriteSideReplicas:   side_replicas: list[str]
@dataclass(frozen=True)
class SetSyncReplication:  host: str
@dataclass(frozen=True)
class CleanupSwitchover:   pass
```

**Failover-specific commands (Stage 6 preview):**

```python
@dataclass(frozen=True)
class Promote:             pass
@dataclass(frozen=True)
class MakeElection:        allow_data_loss: bool
@dataclass(frozen=True)
class SetSSNBeforePromote: old_primary: str | None
@dataclass(frozen=True)
class WriteCurrentPromotingHost: pass

Plan = list[Command]
```

`plan()` returns an **empty** `Plan` to mean "nothing to do this iteration,
condition not yet met — retry next time" (the level-triggered "wait").

### 3. Composite operations stay opaque (for now)

`do_failover`, `rewind_from_source`, and `_return_to_cluster` are themselves
multi-step, stateful mini-procedures with their own reads/writes/waits.
Decomposing them into primitive commands is out of scope for this ADR. They are
represented as **opaque commands** whose parameters are pure data:

```python
@dataclass(frozen=True)
class DoFailover:          old_primary: str | None
@dataclass(frozen=True)
class RewindFromSource:    new_primary: str; is_postgresql_dead: bool; limit: float
@dataclass(frozen=True)
class SetSimplePrimarySwitchTry: pass
@dataclass(frozen=True)
class DeleteHostOp:        pass
```

The Executor delegates these to the existing `pgconsul` methods unchanged.
Testing `assert DoFailover(old_primary='host1') in plan` is equivalent in power
to today's `ctx.do_failover.assert_called_once_with(...)`, but the *decision to
issue it* is now tested purely. Fully reifying these is deferred to Stage 6.

### 4. One read-set per handler — split read-decide-write chains

A pure handler must be **read-at-start → decide → emit**. Handlers that today
interleave a second read after a write (most notably `_handle_candidate_found`:
stop pooler → *fresh sync check* → stop PG → write phase → release lock) violate
this shape.

Resolution: **split such a handler into finer phases**, one observation per
phase. `candidate_found` becomes a small chain where each ZK-persisted
sub-phase corresponds to exactly one read-at-start handler. This is not overhead
— it is directly aligned with ADR-0005: finer phases make kill-9 recovery more
granular. Where a split is not warranted, the missing read is added to the
`Observation` instead.

> **Implementation note (MDB-41951):** `candidate_found` was **not** split into
> sub-phases in the final implementation. Instead, the primary's
> `plan_initiated` re-reads `live_switchover_state` from the `Observation` to
> detect the candidate's `INITIATED → CANDIDATE_FOUND` transition, then inlines
> `plan_candidate_found()` (pooler stop + transition) in the same iteration.
> The second read (fresh sync check) that motivated the split was folded into
> `plan_pooler_stopped`, which already runs on the next iteration with a fresh
> observation. See `src/switchover/primary.py` (`plan_initiated`,
> `plan_pooler_stopped`) and `docs/SWITCHOVER.md`.

### 5. Executor — a single imperative shell for all cluster-op machines

There is **one** `CommandExecutor`, not one per machine family. It owns the
infra objects (`zk`, `db`, `replication_manager`, `timings`) and the bound
opaque composite callbacks, and dispatches each command type to its effect. It:

1. Is handed an already-built `Observation` and the machine to run.
2. Calls `machine.plan(observation)` — pure, no I/O.
3. Interprets the returned `Plan` command-by-command, stopping on the first
   command whose effect fails (preserving today's "return True, retry next
   iteration" semantics).

**Why one Executor and not `SwitchoverExecutor` + `FailoverExecutor`:**

- The Executor is a thin interpreter (`dispatch by command type → infra call`),
  not domain logic. Splitting it would duplicate the interpretation of the
  large common-command set (lock, timers, `failover_state`, timeline, PG,
  pooler, checkpoint) — ~60% of both vocabularies.
- Both machine families depend on the same infra objects; a split Executor
  would re-inject the same dependencies twice.
- ADR-0002 wants a **single I/O boundary** for `PostgresConnectionError` /
  `ZookeeperException` handling. One Executor = one place that owns that
  contract; two Executors would duplicate error handling.
- Extensibility: `FailoverManager` (Stage 6) reuses the whole common set for
  free — it only adds its 3–4 specific commands and their dispatch branches.

The **Observation builders may differ per family** (switchover vs failover read
different state), and each machine emits only its relevant command subset — but
they all run through the one Executor.

The "persist phase before action" fence (ADR-0005 §3) becomes an **ordering
invariant of the Plan**: a `TransitionTo(X)` command precedes the commands that
perform X's action. The Executor honours list order, so the fence is expressed
declaratively and is visible in tests.

### 6. Boundary with `main.py`

`primary_iter` / `replica_iter` keep their current call sites
([`primary_iter`](../src/main.py), [`replica_iter`](../src/main.py)): build the
`SwitchoverRecord`, check `is_active()` / ownership, then delegate one step —
now `executor.run(machine, observation)`. The debug-failure hook
(`DebugFailure`) remains, injected into `plan()` as a pure predicate
`(str) -> bool` so that fault-injection points stay declarative.

---

## Alternatives

### A1. Keep callback-context handlers, add more unit tests
Rejected: does not remove the mocking burden; tests keep asserting on
interactions; the implicit-input problem (§1) remains.

### A2. Enriched observation only, keep inline effects
Rejected: makes inputs explicit but leaves effects entangled with decisions, so
"machine without mocks" is not achieved and the fence stays imperative.

### A3. Full effect system / free monad over all operations
Rejected: reifying `do_failover` / `rewind` / `return_to_cluster` into primitive
commands is a large, risky change touching the most dangerous code paths. §3
keeps them opaque; full reification is deferred to Stage 6 as separate tickets.

### A4. Return a single command instead of a list
Rejected: several phases legitimately emit an ordered set (e.g. write candidate
+ write side replicas + transition). A `Plan` list expresses ordering and the
phase-before-action fence directly.

### A5. Separate `SwitchoverExecutor` and `FailoverExecutor`
Rejected (§5): their command vocabularies overlap ~60%; separate interpreters
would duplicate common-command interpretation, re-inject identical
dependencies, and split the ADR-0002 I/O boundary in two. A single
`CommandExecutor` over a shared vocabulary, with per-family Observation
builders, gives full reuse. If a future machine ever needed a genuinely
disjoint effect set, an Executor split could be revisited — not the case for
switchover/failover.

---

## Consequences

**Positive:**
- **Handlers become pure and mock-free.** Tests call `plan(observation)` and
  assert on the returned `Plan` — no `MagicMock`, no `assert_called`. Decisions,
  not interactions, are verified.
- **Inputs are explicit and typed** (`Observation`); the handler signature no
  longer lies.
- **Idempotency fence and operation order become reviewable data** (command
  order in the `Plan`).
- **One shared vocabulary + one Executor** ⇒ `FailoverManager` /
  `ReturnToClusterManager` reuse the common command set and I/O handling for
  free.
- The Executor concentrates all I/O in one place, shrinking the critical section
  and aligning with ADR-0002 exception handling.

**Negative / Risks:**
- New indirection (Observation + Command + Executor) adds boilerplate for small
  handlers.
- The Executor still needs interaction/integration tests (behave already covers
  this); the "no mocks" benefit applies to the *core*, not the shell — this must
  not be over-claimed.
- Splitting `candidate_found` into finer phases (§4) touches a critical path and
  requires full behave verification, potentially a two-phase rollout if new
  `switchover/state` values are introduced (ADR-0005 §5).
- Command vocabulary must be kept minimal and stable to avoid a sprawling DSL;
  adding a command requires an Executor dispatch branch and a test.

**Neutral:**
- Composite operations remain opaque; testability gain there is marginal until
  Stage 6.

---

## Addendum: Stateless Variant and Two-Pass Shell (MDB-41951)

The `ReturnToClusterMachine` (`src/return_to_cluster/`) extends this ADR with
two patterns not covered by the original switchover machines:

### Stateless machine (in-memory phase)

Switchover machines persist their phase to ZK (`switchover/state`) — the phase
survives restarts and is visible to all cluster members. The
`ReturnToClusterMachine` is **stateless**: its phase is re-derived from the
observation on every `plan()` call (via `_derive_phase`). No ZK persistence.

This is safe because return-to-cluster is a single-call flow (not a multi-
iteration process like switchover). The machine runs to completion within one
`_return_to_cluster()` invocation; if the iteration restarts, the machine
re-derives its phase from fresh observations.

### Two-pass shell with `last_command_succeeded`

Switchover machines take one `executor.run()` per iteration. The
`ReturnToClusterMachine` requires **two passes**:

1. **Pass 1** (`simple_switch_tried=False`): SIMPLE_SWITCH phase — tries
   `SimplePrimarySwitch`. If it succeeds, done. If it fails (fail-fast),
   executor stops.
2. **Pass 2** (`simple_switch_tried=True`): CHECK_DIVERGENCE phase — checks
   timelines, decides retry (timelines match) vs rewind (divergence).

The shell uses `executor.last_command_succeeded` to distinguish "all commands
completed" (pass 1 succeeded) from "fail-fast stopped" (pass 2 needed). This
attribute is set by `run()` on every call.

### When to use the stateless variant

- The process completes within a single call (no multi-iteration state).
- The phase can be fully derived from observations (no external state needed).
- ZK persistence is unnecessary (no cross-node visibility required).

If the process spans multiple iterations or needs cross-node coordination,
use the stateful variant (persist phase to ZK) as switchover machines do.

---

## Links

- ADR-0005: Idempotent Iterations — level-triggered reconciliation; this ADR
  extends its purity guarantee from the iteration level down to the handler
  level.
- ADR-0004: Factory + Config-Builder convention — the Executor and Observation
  builders follow the same construction convention.
- ADR-0002: Exception Propagation — the single Executor is the one I/O boundary
  where `PostgresConnectionError` / `ZookeeperException` are handled per phase.
- Implementation report: `10-projects/pgconsul/MDB-41951-idempotency-algo/implement/21-switchover-command-plan-refactor.md`
- Return-to-cluster machine (stateless variant): `10-projects/pgconsul/MDB-41951-idempotency-algo/implement/44-return-to-cluster-machine-implementation.md`
