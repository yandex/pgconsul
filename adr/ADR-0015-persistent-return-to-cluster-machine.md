# Persistent Host-Local Return-to-Cluster Machine

# Context

Returning a host to the cluster can require several PostgreSQL starts, timeline
and archive checks, and `pg_rewind`. Performing those actions in one call blocks
the main loop, loses progress on daemon restart, and may let a half-repaired
host participate in failover or switchover.

Failover and switchover also need to reserve their old or new primary while a
handoff is in progress, without teaching return-to-cluster every cluster
operation phase.

# Decision

Return-to-cluster is a persistent host-local state machine stored atomically in
`return_to_cluster_state.json`.

An active repair state exclusively claims the host-local iteration. Each
iteration performs one bounded action and persists its result. PostgreSQL start
is asynchronous and observed on later iterations. `pg_rewind` remains a
blocking command, but it is invoked only after SQL is unavailable, PostgreSQL
is in a terminal state, and `pg_status` confirms that its process is stopped.

Cluster operations reserve a host by writing the generic `blocked` return
phase. They clear or replace that state when the host may return. The return
machine does not inspect failover or switchover phases.

If automatic repair is exhausted, the machine creates the existing rewind-fail
flag and enters `resetup_required`. It resumes only after an external process
removes that flag.

Return-to-cluster is not part of the election proof and cannot authorize a
primary. It preserves the selected branch as follows:

1. A successfully turned replica follows the new primary directly and may
   resume archive restore only after streaming is established.
2. A divergent or uncertain replica reads the target timeline history and the
   forkpoint.
3. If its durable LSN is past the forkpoint, it runs `pg_rewind`; otherwise it
   first performs archive-only catch-up to the forkpoint, then attaches to the
   new primary.
4. Before rewind or archive-only catch-up, it waits for the target history and
   the old-timeline WAL segment containing the forkpoint in the archive.
5. Missing, malformed, unrelated, or incomplete history or WAL causes a safe
   wait, never a guess.

The archive barrier ensures that all older WAL needed by recovery is already
immutable and available before restore sources are enabled.

Directly reloading `primary_conninfo` on an already streaming replica is not a
safe substitute for archive-only catch-up. PostgreSQL restarts walreceiver and
can request old-timeline WAL from the new primary before trying the archive.
The new primary cannot provide WAL after its forkpoint. Clearing
`primary_conninfo` first makes the recovery loop consume the verified archive
prefix; the connection to the new primary is restored only after replay reaches
the forkpoint.

If required archive history or fork WAL is unavailable, pgconsul waits instead
of claiming the safety guarantee.

# Alternatives

- Keep return-to-cluster as one blocking call. This hides progress, blocks the
  local loop during PostgreSQL startup, and is not restart-safe.
- Derive whether return is allowed from failover and switchover state. This
  couples the repair machine to every cluster-operation protocol.
- Run `pg_rewind` asynchronously. This would require process identity,
  supervision, output ownership, and crash recovery that are not currently
  needed.

# Consequences

- A repairing host cannot vote, coordinate an operation, or report readiness.
- Daemon restarts preserve repair progress and retry counters.
- Other healthy hosts must perform global manager duties while this host is
  repairing.
- PostgreSQL startup no longer blocks `run_iteration`, while `pg_rewind` still
  does.
- A stopped and terminal PostgreSQL state is a mandatory precondition for every
  rewind attempt.

# Links

- `docs/RETURN_TO_CLUSTER.md`
- `src/return_to_cluster/state.py`
- `src/main.py`
- ADR-0005: Idempotent Iterations
- ADR-0006: Switchover Machine Command Plan
