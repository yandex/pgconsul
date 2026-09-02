# Safe durability membership change

- Status: Accepted
- Deciders: munakoiso

# Context

PostgreSQL and ZooKeeper cannot atomically change the durability configuration.
PostgreSQL uses `synchronous_standby_names` (SSN) to choose replicas that may
acknowledge a commit. PgConsul uses the membership stored in ZooKeeper to choose
replicas whose durable WAL may participate in failover.

For a membership `D` containing primary `P`, let:

```text
R(D,P) = D - {P}
W(D)   = ceil(|R| / 2) = floor(|D| / 2)
Q(D)   = |R| - W(D) + 1
```

PostgreSQL uses `ANY W(D)(R(D,P))`. Any possible write ACK set of `W(D)`
replicas intersects every failover read quorum of `Q(D)` replicas.

The previous protocol selected either SSN-first or ZooKeeper-first according
to the membership-size parity. That proof was correct only while failover used
one selected intermediate membership and required two recovery mechanisms.
It also made direct multi-host changes hard to reason about.

# Decision

Every durability change uses one protocol:

1. CAS-write a ZooKeeper transition containing `source`, `target`, and a fresh
   `operation_id`. The stable membership remains `source`.
2. Apply the SSN derived from `target` on the current primary and wait until
   PostgreSQL reports it effective.
3. On a non-blocking connection, replace the single row in PgConsul's service
   table and commit with `synchronous_commit = on`:

   ```sql
   BEGIN;
   SET LOCAL synchronous_commit = on;
   CREATE TABLE IF NOT EXISTS public.pgconsul_durability_barrier (
       singleton boolean PRIMARY KEY CHECK (singleton),
       operation_id text NOT NULL
   );
   TRUNCATE TABLE public.pgconsul_durability_barrier;
   INSERT INTO public.pgconsul_durability_barrier
       (singleton, operation_id) VALUES (true, '<operation_id>');
   COMMIT;
   ```

4. Only after that commit succeeds, CAS-write `stable = target` and clear the
   transition.

The service-table write forces ordinary WAL under every PostgreSQL
configuration supported by PgConsul. `TRUNCATE` prevents repeated barriers
from accumulating dead rows; the primary-key/check pair admits only the single
`true` row. A successful commit proves that the complete primary WAL prefix
through the write was flushed by a target ACK set. Every target read quorum
intersects that set. Transactions after the write are governed by target SSN
as well.

The barrier runs on an asynchronous PostgreSQL connection. A pending commit
must not block the main loop. A timeout or lost connection is never treated as
success because the local commit may have completed before the synchronous ACK
was observed. Repeating the truncate-and-insert with the same operation ID is
safe.

ZooKeeper stores neither the barrier LSN nor replica acknowledgements. They are
unnecessary: the successful synchronous commit is the barrier result. After a
restart, the primary reapplies the target SSN, repeats the service-table write
for the same `operation_id`, and retries the final CAS.

## Crash recovery

| Crash point | Persisted fact | Recovery |
|---|---|---|
| before transition CAS | old membership only | start again |
| after intent, before target SSN | source remains stable | apply target SSN |
| after target SSN, before barrier completion | source and target recorded | reapply target SSN and repeat table write |
| after barrier commit, before final CAS | source and target recorded | repeat table write and CAS target |
| after final CAS, before observing success | target without transition | treat operation as complete |

Reapplying target SSN, replacing the operation-id row, and retrying a CAS are
idempotent while the same primary remains active.

## Failover during a transition

During `source -> target`, either SSN may have governed an acknowledged
transaction: the primary may have failed before or after applying target.
Failover therefore freezes voters from the union of both memberships and
checks safety independently for both configurations.

For each `D` in `(source, target)` it must:

1. obtain `Q(D)` valid, fenced votes from `R(D,P)`;
2. choose a winner whose durable LSN is not behind at least `Q(D)` of those
   votes.

The same winner must pass both checks. Merely obtaining the two read quorums is
insufficient: their newest WAL may reside on different hosts. The winner is
chosen only from the stable/source electorate. After promotion, PgConsul keeps
`source`, discards the failed primary's unfinished transition with CAS, and
starts a fresh transition later if `target` is still desired.

This double check means every possible source ACK set and every possible
target ACK set intersects a set whose WAL is contained by the winner.

If `source` does not yet exist during initial cluster configuration, there is
no prior SSN against which failover can be proved. Failover remains disabled
until the target barrier succeeds and the first stable membership is written.

## Scope and idempotence

Normal reconciliation changes one host per transition. Replacement is an
expansion followed by a contraction. One-host changes usually make one of the
two failover checks imply the other. A direct multi-host replacement can
require otherwise unnecessary hosts during an unfinished transition. For
example:

```text
source = ANY 2(a,b,c)
target = ANY 2(a,b,d)
```

With only `a,c` available, source has two votes but target has only one.
Failover waits. Decomposing changes into adjacent transitions bounds this
availability loss and is enforced by the implementation.

All state changes use ZooKeeper CAS. Only the primary lock holder advances the
membership transition. Reapplying SSN, replacing the service-table row, and
retrying the final CAS are idempotent. A CAS conflict causes a fresh read and
reconciliation; it is not interpreted as completion.

## Fail-closed conditions

Pgconsul must wait instead of claiming the safety guarantee when there is no
failover-visible synchronous membership, a membership transition violates its
one-host scope, the target-SSN service-table WAL barrier cannot be confirmed,
or an unfinished transition lacks a read quorum or one candidate safe for both
source and target.

# Alternatives

Keep parity-dependent SSN-first and ZooKeeper-first transitions. This avoids a
barrier for some changes but requires distinct crash and failover reasoning.

Use `pg_logical_emit_message()` instead of a service table. It avoids a table,
but is not available with every PostgreSQL configuration supported by
PgConsul.

Store a barrier LSN and replica flush acknowledgements in ZooKeeper. This
duplicates PostgreSQL's synchronous-commit protocol and introduces stale
intermediate facts after a primary change.

Stop writes while changing both systems. This simplifies the proof but adds
write downtime to routine membership changes.

Permit arbitrary direct membership replacement. It remains safe with the
double failover check, but can reduce failover availability for the entire
unfinished transition.

# Consequences

Every membership change performs one synchronous WAL commit after applying
target SSN. The normal main loop remains responsive while that commit waits.

Failover observations and health probes carry both endpoint memberships while
a transition exists. Promotion waits unless one candidate is proven safe for
both.

The ZK transition format is smaller and has one execution path, but an old
primary failure may discard completed target work and repeat it on the new
primary. This costs time, not acknowledged data.

# Links

- [Data-safety contract](../docs/DATA_SAFETY.md)
- ADR-0003: ZK client and domain layering
- ADR-0005: Idempotent iterations
- ADR-0007: Failover state machine
- ADR-0013: Single-coordinator failover safety
