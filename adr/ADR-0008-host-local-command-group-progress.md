# ADR-0008: Host-local persistence for command-group progress

**Status:** Accepted
**Date:** 2026-08-24

# Context

Switchover reused `_do_failover`, which wrote `failover_state` and
`current_promoting_host`. If switchover stopped mid-promotion, its metadata
could be mistaken for an independent failover. Several switchover phases also
described work performed only by the old primary and were unnecessarily
visible to every host.

The existing command groups are already retryable. Introducing a phase for
every individual side effect would make both machines larger without adding a
coordination benefit.

# Decision

Persist a phase in ZK only when another host needs it to make a decision.
Persist host-bound command groups as JSON files in `/var/cache/pgconsul`:

- `switchover_primary_state.json`: `sync_set`, `pooler_stopped`, `pg_stopped`;
- `switchover_candidate_state.json`: `creating_slots`, `promoting`, `checkpointing`;
- `failover_participant_state.json`: `creating_slots`, `promoting`, `checkpointing`.

The file contains the current group, is written with `flush` and `fsync`
before the group is executed, and is cleared after completion. A malformed or
unknown value is logged, removed, and fails the current iteration.

Failover election and cross-host handoff phases remain in ZK. Promotion itself
does not write operation-specific ZK metadata. Its caller owns the global
transition: failover uses `promoting` → `finished`; switchover uses
`candidate_acquired` → `promoted`.

Switchover cleanup deletes only `switchover/*` nodes and never failover nodes.

# Alternatives

- Keep every phase in ZK: rejected because switchover and failover then share
  unrelated promotion metadata.
- Persist every individual side effect: rejected because current command
  groups are retryable and provide sufficient recovery points.
- Repeat the full promotion pipeline: rejected because retrying `promote` on
  an already-primary PostgreSQL can repeat role-specific side effects.

# Consequences

- A host restart resumes its current command group without exposing internal
  progress to other hosts.
- A coordinator phase that may move between hosts must remain in ZK.
- Local state is meaningful only under its enclosing global ZK phase; entry to
  a new promotion clears stale local progress.
- Runtime packaging must create `/var/cache/pgconsul` writable by `postgres`.
- Switchover no longer writes or deletes failover state or election metadata.

# Links

- [ADR-0005](ADR-0005-idempotent-iterations.md)
- [ADR-0006](ADR-0006-switchover-machine-command-plan.md)
- [ADR-0007](ADR-0007-failover-state-machine.md)
- [`src/local_state.py`](../src/local_state.py)
