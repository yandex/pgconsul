# ADR-0010: Top-level blocking switchover handler

**Status:** Accepted
**Date:** 2026-08-24
**Deciders:** munakoiso

# Context

Switchover is currently dispatched from `primary_iter()`, `replica_iter()`,
`non_ha_replica_iter()`, and `dead_iter()`. Each role therefore knows which
switchover phases it may handle and which ordinary actions it must suppress.
Missing a guard lets role-based reconciliation interfere with the active state
machine.

Failover is already a top-level blocking operation. Switchover needs the same
ownership boundary.

# Decision

Add `handle_switchover(db_state, zk_state) -> bool` to `run_iteration()` after
active failover handling and before automatic failover initialization and
role-based dispatch.

The return value means that switchover owns the iteration:

- `True` when a persistent switchover phase exists, including an empty plan,
  a waiting host, a failed command, fallback failover, and cleanup;
- `False` only when no persistent switchover phase exists.

The effective order is:

```python
if maintenance:
    return
if handle_failover(db_state, zk_state):
    return
if handle_switchover(db_state, zk_state):
    return
if start_failover(db_state, zk_state):
    return
handle_role_based_logic()
```

`handle_switchover()` routes one step by persistent state and host identity:

- the recorded old primary runs `PrimarySwitchoverMachine`;
- the selected candidate runs `CandidateSwitchoverMachine`;
- replicas move to the selected candidate only in phases where its slots are
  being prepared;
- unrelated hosts wait without entering ordinary reconciliation.

The handler also owns terminal cleanup and recovery after the old primary has
lost its lock. Switchover role-specific guards and stale cleanup are removed
from ordinary iterations.

## Fallback failover

If the primary lock disappears before the planned handoff, the switchover
machine persists phase `fallback` and requests failover initialization. From
the next iteration, the higher-priority failover handler owns the iteration.

The failover handler does not inspect switchover ZK metadata. Once failover is
finished and a primary holds the lock, the switchover machine resumes and
cleans only switchover metadata. Thus the two machines have separate state and
an explicit handoff.

Switchover cleanup deletes its persistent state marker last. Absence of that
marker is the only idle state.

## Maintenance precedence

Maintenance remains above both operation handlers and pauses switchover
progress, including cleanup and fallback initialization.

# Alternatives

## Keep role-specific switchover guards

Rejected because operation ownership stays distributed across unrelated role
logic and is easy to violate when a new branch is added.

## Let failover detect a running switchover

Rejected because it couples failover to switchover metadata. The machine that
needs fallback must explicitly initialize it.

## Run switchover before failover

Rejected because a fallback switchover deliberately waits while the failover
machine elects and promotes a new primary.

# Consequences

- Every persistent switchover phase blocks ordinary role reconciliation.
- Role changes and PostgreSQL restarts do not change which operation owns the
  iteration.
- Failover and switchover state remain independent.
- Side-replica movement becomes part of the switchover handler.
- Cleanup and partial-command failures are retried on later iterations.

# Links

- [ADR-0005](ADR-0005-idempotent-iterations.md)
- [ADR-0006](ADR-0006-switchover-machine-command-plan.md)
- [ADR-0008](ADR-0008-host-local-command-group-progress.md)
- [ADR-0009](ADR-0009-top-level-blocking-failover-handler.md)
- [`src/main.py`](../src/main.py)
- [`src/switchover/`](../src/switchover/)
