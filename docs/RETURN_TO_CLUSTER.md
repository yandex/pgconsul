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
  observation, returns a `ReturnAction`.
* The shell (`_return_to_cluster` in `src/main.py`) executes the action
  directly — no `CommandExecutor` delegation.

The key design goal is to **distinguish transient simple-switch failures from
real WAL divergence** to avoid unnecessary `pg_rewind`.

Return work aimed at the cluster primary is also bound to a target epoch:
the desired-primary `operation_id`, hostname, and cluster timeline captured
when the work starts. Pgconsul rechecks this identity before destructive
steps, while waiting for recovery or streaming, and after `pg_rewind`.
If the primary changes, the current attempt stops and the next iteration
rebuilds the observation for the new target. A blocking `pg_rewind` is allowed
to finish, but PostgreSQL is not started against its obsolete source.

Cascading replication may target a configured `stream_from` replica rather
than the current primary. Those call sites explicitly use the configured
source identity instead of the primary-epoch guard.

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
| `WAIT_HISTORY` | Keep restore fenced until the target timeline history is in the archive |
| `WAIT_ARCHIVE` | Keep restore fenced until the old timeline's `.partial` WAL file containing the forkpoint is in the archive |
| `SIMPLE_SWITCH` | Attempt simple primary switch (recovery.conf + restart, no pg_rewind) |
| `REWIND` | pg_rewind required — WAL has diverged or node is a former primary |

## How the decision works: `decide_return_action()`

Unlike switchover (where the phase is read from ZK), return-to-cluster derives
the action **purely from the observation**:

```python
def decide_return_action(obs: ReturnObservation) -> ReturnAction:
    # 1. A regular replica first tries the fast path with restore fenced.
    if timelines_differ(obs) and not obs.simple_switch_tried:
        return ReturnAction.SIMPLE_SWITCH

    # 2. A failed fast path or former primary waits for both archive barriers.
    if timelines_differ(obs):
        if obs.timeline_history is None:
            return ReturnAction.WAIT_HISTORY
        if not obs.required_wal_archived:
            return ReturnAction.WAIT_ARCHIVE
        if timeline_requires_rewind(obs):
            return ReturnAction.REWIND

    # 3. Force REWIND for former primaries or after destructive operations.
    effective_role = obs.role or obs.fallback_role
    if effective_role == 'primary' or is_op_destructive(obs.last_op):
        return ReturnAction.REWIND

    # 4. Matching timelines can retry a transient simple-switch failure.
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
def _return_to_cluster(self, new_primary, role, is_dead=False):
    target = self._capture_return_target(new_primary)
    # ... build observation ...
    action = decide_return_action(obs)

    if not self._return_target_is_current(target):
        return

    if action in (ReturnAction.WAIT_HISTORY, ReturnAction.WAIT_ARCHIVE):
        return

    if action == ReturnAction.SIMPLE_SWITCH:
        if self._simple_primary_switch(limit, new_primary, is_dead):
            if obs.archive_restore_disabled:
                self._ensure_restoring_wal()
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

The first attempt does not wait for S3. Archive restore remains disabled, so
the replica can receive WAL only from the new primary. After streaming is
established, archive restore is enabled.

If that attempt fails, the next iteration waits for the target history and the
old-timeline `.partial` WAL file containing the forkpoint. A safe replica gets another
simple switch; a replica past the forkpoint is rewound.

If it succeeds, the node is back in the cluster. If it fails, the shell sets
the `simple_switch_tried` flag and returns — the next iteration will call
`decide_return_action` again, which will check timeline divergence.

### REWIND action

The shell enables archive access while PostgreSQL is stopped, then calls
`pg_rewind --restore-target-wal`. It enables archive access again after rewind
because `pg_rewind` copies the winner's `postgresql.auto.conf`, including its
temporary restore fence. PostgreSQL starts only after both steps.

After successful rewind, the node is back in the cluster as a replica.

## Transition diagram

```
              +-----------------------+
              | decide_return_action() |
              +----------+------------+
                         |
              timelines differ?
                  /        \
                yes         no
                 |           |
          first replica try?  |
            /       \         |
          yes        no        |
           |          |        |
     SIMPLE_SWITCH  history + fork WAL ready?
                        /       \
                      no         yes
                      |           |
               WAIT_HISTORY/   past fork?
               WAIT_ARCHIVE     /     \
                              yes      no
                               |        |
                            REWIND  SIMPLE_SWITCH
```

## Idempotency guarantees

| Mechanism | What it provides |
|-----------|-----------------|
| Stateless design | Action re-derived from observation each call — no stale state |
| `fallback_role` | Former primaries detected even when PG is dead — forced to REWIND |
| One action per iteration | Simple switch and rewind happen on separate iterations — both retry safely |
| Archive barrier | A failed fast path waits for history and the old WAL segment containing the fork |
| Restore fence | A fast return receives WAL only from the winner until streaming is established |
| `is_op_destructive` guard | Nodes with destructive last_op (rewind) go straight to REWIND |
| `simple_switch_tried` flag | Persisted in ZK — survives restarts, prevents infinite simple-switch loops |
| Target epoch guard | A return cannot start PostgreSQL against an obsolete primary operation or timeline |

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

1. `role=None`, `fallback_role=primary`, and timelines differ.
2. Wait for the target history and the WAL segment containing its forkpoint.
3. Run `pg_rewind`, then attach as a replica.

### Scenario 3: Replica with diverged timelines

1. Local and cluster timelines differ; restore remains disabled.
2. Try direct streaming from the new primary immediately.
3. If it fails, wait for history and fork WAL in the archive.
4. Rewind only if the local durable LSN is past the forkpoint; otherwise retry
   the simple switch.
