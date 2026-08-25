# ADR-0009: Top-level blocking failover handler

**Status:** Accepted
**Date:** 2026-08-24
**Deciders:** munakoiso

# Context

Failover is currently entered from several role-specific branches in
`primary_iter()`, `replica_iter()`, and switchover fallback guards. An active
failover can therefore fall through into unrelated role-based reconciliation,
and every new role path must know how to detect and resume it.

Failover is a cluster operation with its own persistent state machine. While
that machine is active, normal reconciliation must not acquire or release the
primary lock, change replication source, repair slots, or start another
cluster operation.

The intended long-term iteration pipeline is ordered by operation priority:

```python
def run_iteration():
    write_iteration_state()
    if maintenance:
        return
    if handle_failover():
        return
    if handle_switchover():
        return
    if handle_local_rewind():
        return
    release_primary_lock_if_needed()
    handle_role_based_logic()
```

This ADR covers only the first extraction. Switchover and local rewind remain
in their current locations until later changes.

# Decision

Introduce a single top-level `handle_failover(db_state, zk_state) -> bool`
boundary in `run_iteration()`, after the common DB/ZK snapshot, liveness
refresh, service-node writes, and maintenance gate, but before role-based
dispatch.

The return value means **iteration claimed**, not operation succeeded:

- `True`: this iteration belongs to failover. The handler may have advanced a
  phase, waited, retried a failed command, or performed cleanup. No ordinary
  role-based logic may run afterward.
- `False`: there is no active failover and no failover was initiated. Normal
  iteration processing may continue.

Failures and empty command plans still return `True` while failover owns the
iteration. They are retried on the next iteration.

## Ownership rules

Outside maintenance mode, `handle_failover()` claims the iteration when any
of these conditions holds:

1. `failover_state` contains an in-progress phase;
2. `failover_state` is `finished` or `failed` and terminal cleanup is required;
3. `failover_must_be_reset` exists;
4. this iteration detects a valid failover trigger and starts the machine.

`finished` is a cleanup phase, not the idle failover value. Terminal cleanup
removes `failover_state`; absence of the node is the only idle state. This
gives cleanup an explicit retry boundary and keeps it out of ordinary
iterations.

When another host owns an active failover role, the local handler may produce
no commands, but it still returns `True`. Waiting is part of the failover; it
must not fall through to normal reconciliation.

## Maintenance precedence

Maintenance mode blocks all cluster reconciliation, including initiation,
resumption, and terminal cleanup of failover. After refreshing common state
and writing service nodes, `run_iteration()` finishes without calling
`handle_failover()`. Persistent failover state remains unchanged and is
resumed after maintenance mode is disabled.

Automatic failover initiation preserves the existing triggers and exclusions:

- an HA replica observes that the primary lock has no holder;
- non-HA replicas and single-node instances do not join an HA election.

Switchover fallback is owned by the switchover machine and is specified in
ADR-0010. Failover never reads switchover metadata. Once failover has a
persistent phase, its ZK state alone determines ownership.

## Handler responsibilities

The top-level handler owns the complete failover lifecycle:

- trigger detection;
- coordinator acquisition and recovery;
- coordinator/participant machine dispatch;
- winner promotion resumption after PostgreSQL has become primary;
- loser waiting while the global operation is active;
- `failed`/`finished` cleanup and `failover_must_be_reset` handling.

`failover_must_be_reset` is part of `FailoverObservation`. The coordinator
machine converts it into a `CleanupFailover` command even when
`failover_state` has already been deleted. `handle_failover()` must not call
cleanup directly. The command executor performs the ZK mutations and retries
them through the same machine path after a partial cleanup or process crash.

Terminal cleanup is run by the failover coordinator and includes election
votes/winner, timers, the reset marker, and the coordinator lock. It
must never release the winner's primary leader lock. The last cleanup action
deletes `failover_state`, making the next iteration ordinary. If the original
coordinator crashes, another HA participant may acquire the coordinator lock
and resume the same cleanup phase.

Returning a loser to the new primary is local reconciliation after the global
failover has finished. For now it may remain in role-based logic; later it
moves to the top-level `handle_local_rewind()` stage. It must not run while a
global failover phase still exists.

Consequently, direct failover handling and failover-specific guards are
removed from `primary_iter()`, `replica_iter()`, and `dead_iter()`.

The handler consumes the `db_state` and `zk_state` snapshots already read by
`run_iteration()`. It must not call a role iteration recursively. Operation
handlers do not sleep or finalize the iteration; common iteration cleanup and
sleep remain owned by `run_iteration()`.

## Ordering invariant

For this change, the effective structure is:

```python
refresh_common_state()
write_iteration_state()
if maintenance:
    finish_iteration()
    return
if handle_failover(db_state, zk_state):
    finalize_iteration()
    return
handle_existing_maintenance_and_role_logic()
finalize_iteration()
```

Later changes may add `handle_switchover()` and `handle_local_rewind()` between
failover and role-based logic without changing the failover contract.

# Alternatives

## Keep failover guards in every role iteration

Rejected because ownership remains implicit and new role paths can
accidentally run during an active failover.

## Return the command execution result

Rejected because `False` would ambiguously mean both "no failover" and
"failover command failed or is waiting". Only operation ownership controls
whether the iteration may continue.

## Move all cluster operations at once

Rejected for this change. Extracting failover first keeps the review and test
surface bounded. Switchover and local rewind will adopt the same interface in
separate changes.

## Dispatch failover only for replicas

Rejected because the winner may already report role `primary` while its local
post-promotion group or global cleanup is incomplete. Persistent operation
state, not the current PostgreSQL role, determines dispatch.

# Consequences

- Active failover becomes blocking with respect to all ordinary iterations.
- Maintenance mode pauses failover progress, including terminal cleanup.
- `finished` and `failed` become explicit, retryable cleanup phases; `None` is
  the only idle failover state.
- Failover progress no longer depends on whether PostgreSQL currently reports
  `primary`, `replica`, or no live role.
- Role-based methods become unaware of failover except for data they expose in
  the common observation snapshot.
- The boolean handler contract can be reused by switchover and local rewind.
- Tests must assert that every persistent failover phase, including waiting
  and command failure, prevents role-based dispatch.

# Links

- [ADR-0002](ADR-0002-exception-propagation-to-run-iteration.md)
- [ADR-0005](ADR-0005-idempotent-iterations.md)
- [ADR-0007](ADR-0007-failover-state-machine.md)
- [ADR-0008](ADR-0008-host-local-command-group-progress.md)
- [ADR-0010](ADR-0010-top-level-blocking-switchover-handler.md)
- [`src/main.py`](../src/main.py)
- [`src/failover/`](../src/failover/)
