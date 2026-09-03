# ADR-0006: Cluster-operation state machines use command plans

- Status: Accepted
- Date: 2026-08-10
- Deciders: kopylov74
- Ticket: MDB-41951

# Context

Failover and switchover are persistent, multi-iteration operations. Mixing
state routing, infrastructure reads and side effects in `main.py` makes their
ordering difficult to review and makes restart safety dependent on incidental
control flow.

# Decision

Cluster operations use a Functional Core / Imperative Shell architecture:

1. `main.py` reads PostgreSQL and ZooKeeper and builds one typed observation.
2. A state machine consumes only that observation and returns a `Decision`:
   an ordered `Plan` of immutable command values plus `owns_iteration`. It
   performs no I/O.
3. The shared `CommandExecutor` interprets the plan and stops at the first
   failed command. The next iteration rebuilds the observation and replans.

An empty plan does not imply iteration ownership. `owns_iteration=true` means
the operation is waiting and ordinary role reconciliation must remain
suppressed; `owns_iteration=false` yields the iteration to other machines.
Persistence of an operation phase, rather than a Python stack frame, is the
retry boundary.

The command vocabulary is shared by failover and switchover. Small common
effects such as acquiring a lock, updating a timer or publishing a vote are
explicit commands. Existing resumable procedures, including promotion,
return-to-cluster and a complete idempotent switchover phase action, may remain
opaque commands. Their command value still fixes the decision and its inputs;
their implementation belongs to the imperative shell.

There is exactly one current switchover protocol. Its pure
`SwitchoverMachine` owns phase and host routing. The global switchover record is
manager-owned; candidates and side replicas publish only operation-scoped
acknowledgements. `main.py` contains infrastructure implementations for the
opaque actions, but no second state router.

The persistent host-local return-to-cluster protocol follows the same rule.
Its machine chooses the bounded local action and iteration ownership; the
orchestrator only builds observations and implements the selected effects.

Plans are ordered and fail-fast. If an effect must be persisted before an
external action, the persistence command precedes that action in the plan (or
the opaque action performs the same CAS fence internally before continuing).

# Alternatives

Inline state routing and effects in `main.py` was rejected because it
duplicates routing across role handlers and obscures retry boundaries.

Separate executors per operation were rejected because lock, timing,
PostgreSQL and ZooKeeper error handling would be duplicated.

Decomposing every resumable procedure into primitive commands was rejected as
unnecessary churn. Opaque commands are allowed only when the delegated
procedure is independently idempotent and tested.

# Consequences

- Decisions can be unit-tested without infrastructure mocks.
- All infrastructure errors cross one executor boundary.
- A restart repeats a phase from persisted state instead of resuming an
  in-memory call chain.
- The executor remains intentionally mechanical; domain routing belongs only
  to state machines.

# Links

- [ADR-0005](ADR-0005-idempotent-iterations.md)
- [ADR-0007](ADR-0007-failover-state-machine.md)
- [ADR-0014](ADR-0014-switchover-durability.md)
- [`src/commands.py`](../src/commands.py)
- [`src/command_executor.py`](../src/command_executor.py)
