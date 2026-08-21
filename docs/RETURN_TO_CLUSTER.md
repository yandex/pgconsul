# RETURN TO CLUSTER

Return-to-cluster is the process of re-attaching a node to the cluster as a
replica after it has lost its primary role (failover) or been disconnected
(switchover, network split, PostgreSQL restart).

## Architecture overview (MDB-41951, ADR-0006)

Return-to-cluster is implemented as a **pure decision function** —
`decide_return_action()` in `src/return_to_cluster/machine.py`. Unlike the
switchover machines, it does **not** persist state to ZK. Instead, the action
is **re-derived from the observation** on every call.

The design follows the "functional core / imperative shell" pattern (ADR-0006):

* All I/O is concentrated in `ReturnObservation.build()` in
  `src/return_to_cluster/types.py`.
* `decide_return_action()` is a pure function: receives an immutable
  observation, returns a `ReturnAction` (`SIMPLE_SWITCH` or `REWIND`).
* The shell (`_return_to_cluster` in `src/main.py`) executes the action
  directly — no `CommandExecutor` delegation.

The key design goal is to **distinguish transient simple-switch failures from
real WAL divergence** to avoid unnecessary `pg_rewind`.

### Source files

| File | Purpose |
|------|---------|
| `src/return_to_cluster/types.py` | `ReturnObservation`, `timelines_match` |
| `src/return_to_cluster/machine.py` | `ReturnAction`, `decide_return_action()` — the pure decision function |
| `src/return_to_cluster/__init__.py` | Re-exports public API |

## Actions (`ReturnAction`)

Actions are **in-memory only** (not persisted to ZK). They are re-derived from
the observation on every call:

| Action | Meaning |
|--------|---------|
| `SIMPLE_SWITCH` | Attempt simple primary switch (recovery.conf + restart, no pg_rewind) |
| `REWIND` | pg_rewind required — WAL has diverged or node is a former primary |

## How the decision works: `decide_return_action()`

Unlike switchover (where the phase is read from ZK), return-to-cluster derives
the action **purely from the observation**:

```python
def decide_return_action(obs: ReturnObservation) -> ReturnAction:
    # 1. Force REWIND for former primaries or after destructive operations
    effective_role = obs.role or obs.fallback_role
    if effective_role == 'primary' or is_op_destructive(obs.last_op):
        return ReturnAction.REWIND

    # 2. If simple switch was already tried — check divergence
    if obs.simple_switch_tried:
        if timelines_match(obs.local_timeline, obs.zk_timeline):
            return ReturnAction.SIMPLE_SWITCH  # retry (transient failure)
        return ReturnAction.REWIND  # real divergence

    # 3. Default — try simple switch first
    return ReturnAction.SIMPLE_SWITCH
```

### The `fallback_role` mechanism

When PostgreSQL is dead, `db.get_role()` returns `None` — even for a former
primary. The `fallback_role` field (passed by `dead_iter()` as `self.db.role`,
the previous role before death) allows the decision function to detect former
primaries and force `REWIND` instead of attempting `SIMPLE_SWITCH` (which
would fail).

This prevents a dangerous scenario: a dead former primary tries simple switch,
fails, and wastes time instead of going straight to `pg_rewind`.

## The return-to-cluster process

The decision function is called from `Pgconsul._return_to_cluster()` in
`src/main.py`. One action is executed per call:

```python
def _return_to_cluster(self, new_primary, role, is_dead=False, skip_check=False):
    # ... build observation ...
    action = decide_return_action(obs)

    # Both actions need archive recovery if it was disabled.
    if obs.archive_restore_disabled:
        self._ensure_restoring_wal()

    if action == ReturnAction.SIMPLE_SWITCH:
        if self._simple_primary_switch(limit, new_primary, is_dead):
            return None  # success
        self._set_simple_primary_switch_try()
        return None  # retry next iteration (will go to REWIND if timelines diverge)

    # action == ReturnAction.REWIND
    self._set_simple_primary_switch_try()
    self._rewind_from_source(is_postgresql_dead=is_dead, limit=limit, new_primary=new_primary)
    # ... check max_rewind_retries ...
```

**Key difference from the old two-pass design:** Pass 1 (simple switch) and
Pass 2 (rewind/retry) now happen on **different iterations** (1 second apart).
This is safe — both cases retry on the next iteration.

### SIMPLE_SWITCH action

If `archive_restore_disabled` is true, `_ensure_restoring_wal()` is called
first (before the action branch — both actions need archive recovery). Then
the shell calls `_simple_primary_switch()` directly (create recovery.conf,
restart PostgreSQL, wait for streaming — no pg_rewind).

If it succeeds, the node is back in the cluster. If it fails, the shell sets
the `simple_switch_tried` flag and returns — the next iteration will call
`decide_return_action` again, which will check timeline divergence.

### REWIND action

`_ensure_restoring_wal()` is called before the action branch (see above) if
`archive_restore_disabled` is true (pg_rewind uses `--restore-target-wal`).
Then the shell calls `_rewind_from_source()` directly (pg_rewind + attach to
new primary).

After successful rewind, the node is back in the cluster as a replica.

## Transition diagram

```
              +-----------------------+
              | decide_return_action() |
              +----------+------------+
                         |
              +----------+-----------+
              |                      |
       former primary           replica or
       or destructive op        unknown role
              |                      |
              v                      v
         +---------+        +----------------+
         | REWIND  |        | SIMPLE_SWITCH   |
         +----+----+        +-------+--------+
              |                     |
              |                fail (next iter:
              |                simple_switch_tried=True)
              |                     |
              |                     v
              |           +-------------------+
              |           | decide_return_action|
              |           | (next iteration)   |
              |           +----+----------+----+
              |                |          |
              |     timelines  | match    | timelines
              |     diverge    |          | diverge
              |                v          |
              |        +----------------+ |
              |        | SIMPLE_SWITCH   | |
              |        | (retry)         | |
              |        +-------+--------+ |
              |                |          |
              v                v          v
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
| Stateless design | Action re-derived from observation each call — no stale state |
| `fallback_role` | Former primaries detected even when PG is dead — forced to REWIND |
| One action per iteration | Simple switch and rewind happen on separate iterations — both retry safely |
| Timeline comparison | Transient failures retried without pg_rewind; real divergence gets rewind |
| `is_op_destructive` guard | Nodes with destructive last_op (rewind) go straight to REWIND |
| `simple_switch_tried` flag | Persisted in ZK — survives restarts, prevents infinite simple-switch loops |

## Entry points from `main.py`

The decision function is called from `Pgconsul._return_to_cluster()` in
`src/main.py`, which is called from the top-level iteration methods:

1. **`primary_iter()`** — when the current primary needs to release the lock
   and return as a replica (e.g., another host was promoted, timeline
   mismatch, `stream_from` configured). Reaches `_return_to_cluster()` via
   `release_lock_and_return_to_cluster()` and `resolve_zk_primary_lock()`.

2. **`replica_iter()`** — when a replica's primary has changed and it needs
   to re-attach to the new primary. Reaches `_return_to_cluster()` via
   `change_primary()`, `replica_return()` (nested helper for a non-streaming
   replica), and the switchover block (`skip_check=True`).

3. **`dead_iter()`** — when PostgreSQL is dead and the node needs to return
   to cluster (passes `fallback_role` so the decision function can detect
   former primaries).

4. **`non_ha_replica_iter()`** — when a cascading (non-HA) replica's
   replication source is dead and it needs to re-attach to the primary.

## Scenarios

### Scenario 1: Replica with matching timelines

1. `role=replica`, `simple_switch_tried=False` -> `SIMPLE_SWITCH`
2. `_simple_primary_switch()` fails (timeout)
3. Next iteration: `simple_switch_tried=True`, timelines match -> `SIMPLE_SWITCH` (retry)
4. `_simple_primary_switch()` succeeds -> done

### Scenario 2: Former primary (dead PG)

1. `role=None` (PG dead), `fallback_role=primary` -> `REWIND`
2. `pg_rewind` executes
3. After rewind: node is back as a replica -> done

### Scenario 3: Replica with diverged timelines

1. `role=replica`, `simple_switch_tried=False` -> `SIMPLE_SWITCH`
2. `_simple_primary_switch()` fails
3. Next iteration: `simple_switch_tried=True`, timelines diverge -> `REWIND`
4. `pg_rewind` -> done
