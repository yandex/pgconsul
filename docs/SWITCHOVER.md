# Switchover protocol

pgconsul uses the manager-owned switchover protocol described by ADR-0014.
There are no separate primary-side and candidate-side planners. `main.py`
builds one immutable observation, the pure `SwitchoverMachine` returns a command
plan, and the shared `CommandExecutor` executes its effects.

Only the current switchover manager changes the versioned global record. The
old primary manages preparation. After the committed handoff, the candidate
takes over management and promotion. Other replicas only publish operation-ID
scoped acknowledgements.

## Phases

| Phase | Meaning |
|---|---|
| `scheduled` | The operator request exists. |
| `preparing_durability` | The old primary prepares the candidate durability requirement and commits a table-backed WAL barrier. |
| `preparing_candidate` | The candidate creates slots, installs its pre-promote SSN and checkpoints. |
| `turning_sides` | Side replicas turn to the candidate and acknowledge the operation. |
| `handoff_committed` | The old primary has committed the irreversible handoff; the candidate must promote or fenced failover must finish it. |
| `waiting_archive` | Promotion is acknowledged; history and the final old-timeline WAL are awaited. |
| `fallback` | Pre-handoff recovery is delegated to ordinary failover. |
| `failed` | The operation failed and is waiting for manager-owned cleanup. |
| `cleanup` | Terminal record cleanup is scheduled. |

## Preparation and handoff

1. The old primary acquires the switchover-manager lock and freezes the stable
   durability members in the operation record.
2. The record publishes a durability policy. With PostgreSQL patches enabled,
   the candidate is mandatory without shrinking the quorum. Without the
   patches, the policy contracts durability to the old-primary/candidate pair.
3. The common durability reconciliation function applies that policy and commits the
   service-table WAL barrier under the prepared SSN. A barrier attempt has a
   deadline; an ambiguous timeout is retried with the same operation ID. The
   switchover advances only after the machine publishes its readiness ACK.
4. The candidate creates physical slots, installs the full pre-promote SSN,
   checkpoints, and publishes its expected timeline.
5. Side replicas turn to the candidate. A two-HA-host cluster requires no side
   replica. Larger clusters must collect the recorded number; timeout before
   that point fails and rolls the operation back. It never permits handoff with
   an insufficient side set.
6. The desired primary is materialized as the candidate. The old primary sends
   the pooler stop command, releases/stops ownership as defined by the handoff,
   and CAS-writes `handoff_committed`.
7. The candidate acquires the leader lock, promotes with its reserved timeline,
   and acknowledges that exact timeline. Promotion is bounded by
   `promote_timeout`; rejection records `promote_failed` and immediately starts
   fenced failover. The candidate then observes the failover-owned desired
   primary state and fences itself. A failed or ambiguous promotion is never
   followed by an automatic return to the old primary after committed handoff.

## Completion and timeout

Before `handoff_committed`, `switchover_timeout` means rollback to the old
primary. After `handoff_committed`, the old primary cannot be reopened: timeout
marks the switchover failed and starts ordinary fenced failover using the
recorded expected timeline. Once the candidate has acknowledged successful
promotion, the operation no longer fails on the global timeout; it completes
the archive wait and cleanup.

After promotion the record switches its durability policy to monotonic
expansion, which common durability reconciliation applies. Existing members are
never removed by expansion. The record is cleaned only after the
new timeline history and one of the possible final old-timeline WAL names are
available in the archive. Replica checkpoints after completion are best effort;
they reduce unnecessary rewinds after a later restart but are not a safety
predicate.

## Idempotency

Every global write is a CAS against the record version and requires the
switchover-manager lock. Host-local promotion state and replica acknowledgements
are keyed by `operation_id`. Restarting any daemon therefore repeats the current
phase instead of replaying an action from another operation.

See also:

- [ADR-0014](../adr/ADR-0014-switchover-durability.md)
- [ADR-0012](../adr/ADR-0012-safe-durability-membership-change.md)
- [ADR-0015](../adr/ADR-0015-persistent-return-to-cluster-machine.md)
