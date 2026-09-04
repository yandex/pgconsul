# Durability membership changes

`durability_members` is the ZooKeeper source of truth for the PostgreSQL
durability group. It contains the primary and the replicas eligible to
acknowledge a synchronous commit. PostgreSQL receives the corresponding
`synchronous_standby_names` (SSN) configuration.

For a primary `P` and membership `D`, PgConsul configures the replicas
`D - {P}` as `ANY floor(|D| / 2)(...)`. The failover read quorum is chosen so
that every possible commit ACK set intersects it. The full proof is in
[ADR-0012](../adr/ADR-0012-safe-durability-membership-change.md).

## State graph

```text
                         policy chooses desired D1
                                    |
                                    v
 +------------------+     D1 == D0? +------------------+
 | STABLE(D0)       |<--------------| no change        |
 | no transition    |               +------------------+
 +--------+---------+
          |
          | CAS: write transition { source=D0, target=D1, operation_id }
          v
 +------------------+
 | PREPARED         |
 | stable=D0        |
 | transition=D0->D1|
 +--------+---------+
          |
          | apply SSN derived from D1
          | (failure/restart: remain PREPARED and retry)
          v
 +------------------+
 | TARGET SSN       |
 | active on P      |
 +--------+---------+
          |
          | commit the service-table WAL barrier under target SSN
          | (timeout/unknown outcome: retry same operation_id)
          v
 +------------------+
 | BARRIER          |
 | CONFIRMED        |
 +--------+---------+
          |
          | CAS: stable=D1; clear transition
          v
 +------------------+
 | STABLE(D1)       |
 | no transition    |
 +------------------+
```

`PREPARED`, `TARGET SSN`, and `BARRIER CONFIRMED` are logical recovery points:
ZooKeeper persists `PREPARED`; later work is safely repeated from the same
transition and operation ID. The barrier itself need not be stored separately.

## Why the barrier is required

ZooKeeper and PostgreSQL cannot atomically change membership and SSN. During a
transition either source or target SSN may have acknowledged a client commit.
The WAL barrier is a synchronous commit made after target SSN is active. Its
success proves that a target ACK set flushed the primary WAL prefix through the
barrier. Only then may ZooKeeper advertise `D1` as stable.

If the barrier result is unknown, PgConsul treats it as incomplete, reapplies
target SSN, and retries the same service-table operation. It never treats an
expired client-side deadline as a successful barrier.

## Transition scope

```text
ordinary expansion:     [P,A] -> [P,A,B] -> [P,A,B,C]
                         one added host per transition

replacement:             [P,A,B] -> [P,A,B,C] -> [P,A,C]
                         expansion first, then contraction

pure contraction:        [P,A,B,C] -> [P,A]
                         any number of removals is permitted
```

The restriction avoids an unnecessarily broad pair of simultaneous failover
requirements during an unfinished replacement. A pure contraction introduces
no new replica, so it may remove several unavailable members in one step to
restore a surviving primary's write availability.

## Preconditions and transition causes

Only the host that is simultaneously all of the following may mutate
PostgreSQL durability:

- PostgreSQL primary;
- holder of the primary lock;
- materialized `desired_primary`;
- on the ZooKeeper timeline.

It also serializes with failover through `epoch_manager`, then strictly checks
that no failover is active before it changes durability state. This prevents a
membership change from racing the failover electorate freeze.

The desired membership is selected in this order:

```text
active failover       -> freeze transition; do not start ordinary change
maintenance           -> local primary only (async)
active switchover     -> persisted pin or monotonic expansion policy
single-node primary   -> local primary only
ordinary operation    -> configured, alive, streaming HA replicas
```

An existing transition always has priority over a newer policy. The reconciler
first resumes it; only a stable state can start the next transition.

## Failover during a transition

```text
                 failover begins while D0 -> D1 is PREPARED
                                   |
                                   v
 +--------------------------------------------------------------+
 | Freeze durability state. For both D0 and D1 require:         |
 |   * a fenced read quorum of votes;                            |
 |   * one candidate whose durable LSN dominates that quorum.    |
 +-----------------------------+--------------------------------+
                               |
                  no common safe candidate / quorum
                               |
                               v
                         wait safely
                               |
                    common safe winner exists
                               |
                               v
                     promote winner and finish failover
                               |
                               v
             resume ordinary durability reconciliation afterwards
```

The winner is never selected merely because each endpoint has a quorum: the
same candidate must be safe for every SSN that could have acknowledged a
commit. While failover remains capable of re-election, it freezes the
transition instead of materializing a new endpoint.

## Relation to switchover

Switchover supplies a persisted durability policy rather than bypassing this
protocol:

- with PostgreSQL patches, C is made mandatory while preserving D0;
- without patches, the preparation pin contracts to `{P, C}`;
- after promotion, the new primary monotonically expands membership only with
  replicas that are actually streaming from it.

The switchover WAL barrier is committed under this prepared durability policy.
See [Switchover](SWITCHOVER.md) for the complete cross-host protocol.
