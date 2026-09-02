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
