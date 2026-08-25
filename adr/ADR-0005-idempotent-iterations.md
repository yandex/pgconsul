# ADR-0005: Idempotent Iterations — Level-Triggered Reconciliation for Cluster Operations

**Status:** Accepted  
**Date:** 2026-07-31  
**Deciders:** kopylov74  
**Ticket:** MDB-41951

---

## Context

The main iteration functions of pgconsul (`primary_iter`, `replica_iter`, `non_ha_replica_iter`,
`dead_iter` in `src/main.py`) contain "one-shot" code paths — multi-step procedures
(switchover, failover, return-to-cluster) with blocking waits (`helpers.await_for`,
`time.sleep`) inside a single iteration.

If the process is interrupted (OS signal, unexpected error, ZK session loss), re-entering
the same point is impossible:

- `_check_primary_switchover` accepts only `scheduled` state; after writing `initiated`,
  an interrupted switchover cannot be resumed.
- `_drop_stale_switchover` runs before switchover check and treats any state `!= scheduled`
  as stale — destroying in-progress switchover.
- Cluster "repairs" are executed before main operations: an error in repair prevents
  reaching switchover/failover.

Result: switchover may "silently" not happen; operator/dbaas_worker waits until timeout.

---

## Decision

### 1. Iteration Model: level-triggered reconciliation

Each iteration is a function of observed state `f(db_state, zk_state)`:

1. Iteration **does not wait** for cluster events inside itself. "Waiting for X" = "condition X
   not met → write progress → exit; will check on next iteration".
2. `helpers.await_for` / `await_for_value` with `limit=-1` are prohibited.
3. `time.sleep` with cluster event waiting inside `*_iter` is prohibited
   (only short local retries at single-request level are allowed).

### 2. Operation Order Inside Iteration

```
safety gates (lock, timeline, destructive op)
  → maintenance
  → main operations (switchover / failover state machine)
  → repairs (slots, SSN, pooler, archiving, timings)
  → stale state cleanup (last)
```

An error in repairs should not block main operations of the next iteration;
cleanup runs only after main operations have had a chance to resume.

### 3. Multi-Step Processes — Explicit State Machines

- Process phase is persisted to ZK **before** executing the phase action.
- Each action is idempotent: before execution, check via ZK + PG state whether
  it has already been executed.
- Interruption at any point → next iteration reads phase and continues from it.
- Phase transitions are logged structurally (`log_event`), phase duration is
  **measured** (`TimingTracker`); separate per-phase timeouts are not introduced yet —
  existing ones are reused: `switchover_rollback_timeout`,
  `switchover_catchup_timeout`,
  `min_failover_timeout`, `primary_unavailability_timeout`,
  `walreceiver_disable_timeout`, `wal_drain_delay`
  (see `src/__init__.py` defaults and `docs/CONFIG.md`).

### 4. Stale Criterion

A process record is considered stale **only** if it cannot belong to a resumable process:

- record timeline < current PG timeline, **or**
- state is `failed`, **or**
- process timeout exceeded.

States `initiated` / `candidate_found` with matching timeline — **not** stale.

### 5. Backward Compatibility with Old Versions

New `switchover/state` values are introduced in two phases:

1. First, a version that **understands** new phases is rolled out (readers).
2. Then, a version that **writes** them (writers).

New values are chosen so that old code treats them safely
(as "not scheduled" → does not start parallel switchover). External contract
with dbaas_worker does not change: worker writes `scheduled` and waits for node cleanup.

---

## Alternatives

### A1. Keep blocking waits, add signal handlers / checkpoints
Rejected: does not solve unexpected errors problem; complicates code; state
still lives in process stack, not in ZK/PG.

### A2. Separate long-lived "switchover orchestrator" (process/thread)
Rejected: adds new component and new failure modes; contradicts
existing "one loop — one iteration" model.

### A3. Edge-triggered ZK events (watches)
Rejected: watches are unreliable during session breaks; level-triggered model is easier
to verify and is already partially used (op-nodes, failover_state).

---

## Consequences

**Positive:**
- Switchover/failover resumable after pgconsul crash/restart at any phase.
- Iteration always bounded in time → maintenance and holder change
  handled with predictable delay.
- Testing simplified: behave scenarios "kill -9 at phase X" + debug hooks on phases.

**Negative / Risks:**
- Multi-step processes span multiple iterations → total switchover time
  may increase (mitigated: several non-blocking steps per iteration).
- Expanding `switchover/state` values requires two-phase rollout (§5).
- Refactoring affects critical paths — introduced incrementally, each phase
  verified with full behave suite.

**Related Decisions:**
- ADR-0002: exceptions still propagate to `run_iteration()`; state machine
  reduces critical section size.
- ADR-0003: repairs remain best-effort operations and move to end of iteration.
