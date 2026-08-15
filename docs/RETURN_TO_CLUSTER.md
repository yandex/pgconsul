# RETURN TO CLUSTER

Return-to-cluster is the process of re-attaching a node to the cluster as a
replica after it has lost its primary role (failover) or been disconnected
(switchover, network split, PostgreSQL restart).

## Architecture overview (MDB-41951, ADR-0006)

Return-to-cluster is implemented as a **stateless state machine** —
`ReturnToClusterMachine` in `src/return_to_cluster/machine.py`. Unlike the
switchover machines, it does **not** persist phases to ZK. Instead, the phase
is **re-derived from the observation** on every call to `plan()`.

The machine follows the same "pure planner + imperative shell" pattern as
switchover (ADR-0006):

* All I/O is concentrated in `ReturnObservation.build()` in
  `src/return_to_cluster/types.py`.
* `plan()` is a pure function: receives an immutable observation, returns a
  Plan (list of commands).
* `CommandExecutor.run()` executes the commands, fail-fast on first error.

The key design goal is to **distinguish transient simple-switch failures from
real WAL divergence** to avoid unnecessary `pg_rewind`.

### Source files

| File | Purpose |
|------|---------|
| `src/return_to_cluster/types.py` | `ReturnPhase`, `ReturnObservation`, `ReturnMachineConfig`, `is_op_destructive`, `timelines_match` |
| `src/return_to_cluster/machine.py` | `ReturnToClusterMachine` — the stateless state machine |
| `src/return_to_cluster/__init__.py` | Re-exports public API |

## Phases (`ReturnPhase`)

Phases are **in-memory only** (not persisted to ZK). They are re-derived from
the observation on every call:

| Phase | Meaning |
|-------|---------|
| `INIT` | Initial phase (not used directly — `_derive_phase` always advances past it) |
| `SIMPLE_SWITCH` | Attempt simple primary switch (recovery.conf + restart, no pg_rewind) |
| `CHECK_DIVERGENCE` | Compare local and ZK timelines to decide: retry or rewind |
| `WAIT_CANDIDATE` | Wait for the new primary to become reachable (no-op, retry next iteration) |
| `RETRY_SIMPLE` | Restore archive recovery, then retry simple switch |
| `REWIND` | pg_rewind required — WAL has diverged |
| `DONE` | Terminal phase (return to cluster complete) |

## How phase derivation works: `_derive_phase()`

Unlike switchover (where the phase is read from ZK), return-to-cluster derives
the phase **purely from the observation**:

```python
def _derive_phase(self, obs: ReturnObservation) -> ReturnPhase:
    # 1. Force REWIND for former primaries or after destructive operations
    effective_role = obs.role or obs.fallback_role
    if effective_role == 'primary' or is_op_destructive(obs.last_op):
        return ReturnPhase.REWIND

    # 2. If simple switch was already tried — check divergence
    if obs.simple_switch_tried:
        return ReturnPhase.CHECK_DIVERGENCE

    # 3. Default — try simple switch first
    return ReturnPhase.SIMPLE_SWITCH
```

### The `fallback_role` mechanism

When PostgreSQL is dead, `db.get_role()` returns `None` — even for a former
primary. The `fallback_role` field (passed by `dead_iter()` as `self.db.role`,
the previous role before death) allows the machine to detect former primaries
and force `REWIND` instead of attempting `SIMPLE_SWITCH` (which would fail).

This prevents a dangerous scenario: a dead former primary tries simple switch,
fails, and wastes time instead of going straight to `pg_rewind`.

## The return-to-cluster process

The machine is driven from `Pgconsul._return_to_cluster()` in `src/main.py`,
which uses a **two-pass delegation**:

### Pass 1: Try simple switch (if not already tried)

```python
obs = ReturnObservation.build(..., simple_switch_tried=False)
consumed = self._executor.run(self._return_machine, obs)
# If simple switch succeeded (plan fully executed) — done.
# If it failed (fail-fast) — fall through to pass 2.
```

### Pass 2: Check divergence — rewind or retry

```python
obs = ReturnObservation.build(..., simple_switch_tried=True)
self._executor.run(self._return_machine, obs)
```

### Phase 1: SIMPLE_SWITCH

`plan_simple_switch()` attempts a simple primary switch (create recovery.conf,
restart PostgreSQL, wait for streaming — no pg_rewind):

```python
[SimplePrimarySwitch(new_primary, is_dead, limit),
 CheckDivergence()]
```

> **Naming note:** `CheckDivergence` here is a **command** (a dataclass in
> `src/commands.py`), not the `CHECK_DIVERGENCE` **phase**. The command is a
> no-op marker emitted at the end of the SIMPLE_SWITCH Plan; it tells the
> two-pass shell that pass 1 finished successfully and pass 2 (the
> `CHECK_DIVERGENCE` phase) is not needed. The `CHECK_DIVERGENCE` **phase**
> (see below) runs only when `SimplePrimarySwitch` fails and the caller
> re-invokes the machine with `simple_switch_tried=True`.

If `SimplePrimarySwitch` succeeds, the node is back in the cluster. If it
fails (fail-fast), `CheckDivergence` is not executed and the caller proceeds
to pass 2.

### Phase 2: CHECK_DIVERGENCE

`plan_check_divergence()` compares local and ZK timelines:

**Timelines match** (transient failure — network, timeout):
```python
[EnsureRestoringWal(),  # if archive_restore_disabled
 Log('timelines match, retrying simple switch')]
```
The machine will retry simple switch on the next call (pass 2 with
`simple_switch_tried=True` routes to `RETRY_SIMPLE`).

**Timelines diverge** (real WAL divergence — pg_rewind required):
```python
# Delegates to plan_rewind()
[SetSimplePrimarySwitchTry(),
 RewindFromSource(new_primary, is_postgresql_dead, limit)]
```

### Phase 3: RETRY_SIMPLE

`plan_retry_simple()` restores archive recovery if needed, then retries
simple switch:

```python
[EnsureRestoringWal(),  # if archive_restore_disabled
 SimplePrimarySwitch(new_primary, is_dead, limit)]
```

### Phase 4: REWIND

`plan_rewind()` marks that simple switch was tried, then delegates to
`RewindFromSource` (pg_rewind + attach to new primary):

```python
[SetSimplePrimarySwitchTry(),
 RewindFromSource(new_primary, is_postgresql_dead, limit)]
```

After successful rewind, the node is back in the cluster as a replica.

## Transition diagram

```
              +-------------------+
              | _derive_phase()   |
              +--------+----------+
                       |
           +-----------+-----------+
           |                       |
     former primary          replica or
     or destructive op       unknown role
           |                       |
           v                       v
      +---------+          +----------------+
      | REWIND  |          | SIMPLE_SWITCH  |
      +----+----+          +-------+--------+
           |                       |
           |                  fail-fast
           |                       |
           |                       v
           |          +----------------------+
           |          | CHECK_DIVERGENCE     |
           |          +----+------------+----+
           |               |            |
           |     timelines | match      | timelines
           |     diverge   |            | diverge
           |               v            |
           |     +----------------+     |
           |     | RETRY_SIMPLE   |     |
           |     +-------+--------+     |
           |             |              |
           |          success           |
           |             |              |
           v             v              v
      +---------+   +---------+   +---------+
      |  DONE   |   |  DONE   |   | REWIND  |
      +---------+   +---------+   +----+----+
                                       |
                                       v
                                  +---------+
                                  |  DONE   |
                                  +---------+
```

## Idempotency guarantees

| Mechanism | What it provides |
|-----------|-----------------|
| Stateless design | Phase re-derived from observation each call — no stale state |
| `fallback_role` | Former primaries detected even when PG is dead — forced to REWIND |
| Two-pass delegation | Pass 1 tries simple switch; pass 2 checks divergence — no wasted rewind |
| Timeline comparison | Transient failures retried without pg_rewind; real divergence gets rewind |
| `is_op_destructive` guard | Nodes with destructive last_op (rewind) go straight to REWIND |
| Fail-fast | Command failure stops the Plan; caller retries with fresh observation |

## Entry points from `main.py`

The machine is driven from `Pgconsul._return_to_cluster()` in `src/main.py`,
which is called from:

1. **`primary_iter()`** — when the current primary needs to release the lock
   and return as a replica (e.g., another host was promoted, timeline
   mismatch, `stream_from` configured).

2. **`replica_iter()`** — when a replica's primary has changed and it needs
   to re-attach to the new primary.

3. **`dead_iter()`** — when PostgreSQL is dead and the node needs to return
   to cluster (passes `fallback_role` so the machine can detect former
   primaries).

4. **`replica_return()`** — when a replica is not streaming and needs to
   re-attach.

## Scenarios

### Scenario 1: Replica with matching timelines

1. `role=replica`, `simple_switch_tried=False` -> `SIMPLE_SWITCH`
2. `SimplePrimarySwitch` fails (timeout)
3. `simple_switch_tried=True` -> `CHECK_DIVERGENCE`
4. Timelines match -> `RETRY_SIMPLE`
5. `SimplePrimarySwitch` succeeds -> `DONE`

### Scenario 2: Former primary (dead PG)

1. `role=None` (PG dead), `fallback_role=primary` -> `REWIND`
2. `pg_rewind` executes
3. After rewind: `role=replica`, `simple_switch_tried=True` ->
   `CHECK_DIVERGENCE`
4. Timelines match -> `RETRY_SIMPLE` -> `DONE`

### Scenario 3: Replica with diverged timelines

1. `role=replica`, `simple_switch_tried=False` -> `SIMPLE_SWITCH`
2. `SimplePrimarySwitch` fails
3. `simple_switch_tried=True` -> `CHECK_DIVERGENCE`
4. Timelines diverge -> `REWIND`
5. `pg_rewind` -> `DONE`
