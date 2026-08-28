# Safe durability membership change

- Status: Accepted
- Deciders: munakoiso

# Context

PostgreSQL and ZooKeeper cannot change the durability configuration atomically.
PostgreSQL uses `synchronous_standby_names` to decide which replicas may
acknowledge a commit. Failover uses the stable durability membership stored in
ZooKeeper to decide how many durable LSN observations are sufficient.

Writing either side first creates an intermediate state in which the SSN and
the ZooKeeper membership differ. A fixed write order is unsafe. For example,
when expanding `ANY 2(A,B,C)` to `ANY 2(A,B,C,D)`, changing SSN first permits a
commit acknowledged by `C,D`, while failover using the old membership may
inspect only `A,B`.

The durability membership contains the primary. For a membership `D` and its
primary `p`, the replica set is `R = D - {p}`. The SSN threshold is uniquely
defined as:

```
W(D) = ceil(|R| / 2) = floor(|D| / 2)
```

For synchronous configurations, failover must obtain durable LSNs from:

```
Q(D) = |R| - W(D) + 1
```

members of `R`. Every set of `Q(D)` replicas intersects every possible SSN ACK
set of `W(D)` replicas.

The argument assumes `synchronous_commit=on`, an ACK represents WAL flushed on
the replica, all observations belong to one fenced primary timeline, and
failover selects the greatest durable LSN from its read-quorum. WAL prefix
ordering then ensures that the selected replica contains every acknowledged
transaction represented in that read-quorum.

# Decision

The theoretical data-safety contract for durability membership changes is
based on the following invariants. The field named `stable` is the membership
committed for failover; during a ZK-first transition it may intentionally
precede application of the target SSN:

1. The ZooKeeper `stable` membership is the only durability configuration used
   by failover.
2. The actual SSN write-quorum must intersect every failover read-quorum
   allowed by `stable`.
3. One transition adds or removes exactly one host. Replacement is expansion
   followed by contraction.
4. `members` and their derived `ANY` threshold change as one configuration.
   The threshold is not persisted in ZooKeeper.
5. Membership writes use ZooKeeper compare-and-set. Only the current primary
   lock holder may advance a transition.
6. An unfinished transition is completed idempotently before another
   membership change starts.
7. Before an SSN-first transition publishes its target as stable, the target
   SSN write-quorum has durably flushed a WAL barrier that covers the source
   history.

For adjacent memberships, one of the two non-atomic write orders preserves the
cross-quorum intersection invariant:

| Change | Sequence |
|--------|----------|
| Derived `W` increases | Apply target SSN, pass LSN barrier, publish target as `stable` |
| Derived `W` decreases | Publish target as `stable`, apply target SSN |
| Expansion, `W` unchanged | Publish target as `stable`, apply target SSN |
| Contraction, `W` unchanged | Apply target SSN, pass LSN barrier, publish target as `stable` |

More formally, SSN-first is allowed only when every `Q(source)` read-quorum
intersects every `W(target)` write-quorum. ZooKeeper-first is allowed only when
every `Q(target)` read-quorum intersects every `W(source)` write-quorum. With
one added or removed replica and `W = ceil(|R| / 2)`, the four cases above are
exhaustive:

| Change | Old replica count | Derived threshold | Safe order |
|--------|-------------------|-------------------|------------|
| Expansion | even | increases by one | SSN-first |
| Expansion | odd | unchanged | ZooKeeper-first |
| Contraction | even | unchanged | SSN-first |
| Contraction | odd | decreases by one | ZooKeeper-first |

The opposite order is unsafe in every row. In the counterexamples below, `P`
is the primary. The ACK set lists replicas that may be the only surviving
holders of an acknowledged commit after `P` fails.

1. **Expansion where `W` increases.** Consider
   `[P,A,B] -> [P,A,B,C]`, or `ANY 1(A,B) -> ANY 2(A,B,C)`. With
   ZooKeeper-first, the intermediate stable membership is `[P,A,B,C]` while
   SSN is still `ANY 1(A,B)`. A commit may be acknowledged only by `A`, while
   failover may read the valid target quorum `{B,C}`. The sets are disjoint.

2. **Contraction where `W` decreases.** Consider
   `[P,A,B,C] -> [P,A,B]`, or `ANY 2(A,B,C) -> ANY 1(A,B)`. With SSN-first,
   the intermediate stable membership remains `[P,A,B,C]`, but a commit may
   already be acknowledged only by `A`. Failover may read the valid source
   quorum `{B,C}`, which does not contain the commit.

3. **Expansion where `W` is unchanged.** Consider
   `[P,A] -> [P,A,B]`, or `ANY 1(A) -> ANY 1(A,B)`. With SSN-first, a commit
   may be acknowledged only by the new replica `B`, while failover still uses
   `[P,A]` and reads only `{A}`.

4. **Contraction where `W` is unchanged.** Consider
   `[P,A,B] -> [P,A]`, or `ANY 1(A,B) -> ANY 1(A)`. With ZooKeeper-first, a
   commit may still be acknowledged only by the removed replica `B`, while
   failover already uses `[P,A]` and reads only `{A}`.

Write order alone is not sufficient for SSN-first. For example, before
`[P,A,B] -> [P,A,B,C]`, an acknowledged commit may exist only on `A`. Merely
applying `ANY 2(A,B,C)` does not copy that existing commit to `B` or `C`. If
the target were published immediately, the target read-quorum `{B,C}` could
still miss it.

Before the first operation, one JSON record is CAS-written with the source,
target, and selected order. For an SSN-first transition, `stable` remains the
source until SSN has changed and the LSN barrier has passed. For a
ZooKeeper-first transition, the same CAS both records the transition and
publishes the target as `stable`.

After applying a target SSN in an SSN-first transition, the primary reads its
current flush LSN `L` and CAS-persists `L` in the transition. It then waits
until at least `W(target)` target replicas report `flush_lsn >= L`. WAL prefix
ordering means those replicas also contain every source commit preceding
`L`. Every target failover read-quorum intersects this flushed set, so the
target may then be published as stable. Waiting is level-triggered and
non-blocking: an unsuccessful check leaves the transition for the next
iteration.

ZooKeeper-first transitions do not need an LSN barrier. While ZooKeeper already
contains the target and PostgreSQL still uses the source SSN, every target
failover read-quorum intersects every possible source ACK set by the safe-order
condition above. Therefore each target read-quorum already contains a replica
with every commit acknowledged under the source SSN. Applying the target SSN
then restores the ordinary target write/read-quorum intersection. A commit
racing with the SSN reload is covered by either the source or target
intersection, depending on which SSN PostgreSQL used for that commit.

This argument depends on changing exactly one host. For a multi-host jump, a
target read-quorum can be disjoint from a source ACK set, so ZooKeeper-first
would require another proof or a barrier. Large changes are instead decomposed
into adjacent transitions. During expansion or contraction, ZooKeeper-first
and SSN-first steps alternate because `W(D) = floor(|D| / 2)` changes only on
every second membership-size change. Thus multiple ZooKeeper-first steps in a
large contraction are separated by SSN-first steps and their LSN barriers.

After the SSN and `stable` membership both describe the target, the transition
metadata is cleared with CAS. A crash at any point leaves either the original
state or a recorded, cross-quorum-safe intermediate state. Reapplying the
target SSN and CAS-finalizing the record are idempotent.

Initialization without a stable membership is represented as an SSN-first
transition without a source. It applies SSN and passes the same LSN barrier
before publishing the first stable membership. Failover remains disabled while
no stable membership exists.

The theorem covers synchronous configurations where `W(D) > 0`. A membership
containing only the primary derives `W(D) = 0` and represents intentionally
asynchronous operation; failover cannot claim synchronous durability from it.

Failover is proved against these invariants in ADR-0013. Switchover remains a
separate extension.

# Alternatives

Make failover inspect both source and target configurations. This is safe but
can require observations from a larger union of hosts and makes failover
unavailable during otherwise survivable partial failures.

Use one fixed write order. Neither SSN-first nor ZooKeeper-first is safe for all
expansions and contractions.

Temporarily stop writes. This makes the metadata switch simpler but introduces
write downtime for routine membership changes.

Use a joint SSN that intersects both configurations. This keeps failover based
on one stable membership but may temporarily require more synchronous replicas
and block writes.

# Consequences

Failover remains based only on the stable membership. Routine reconciliation
must progress through one-host steps and may need several iterations for a
large change. The ZK value becomes richer because it also carries recoverable
transition metadata, but it no longer stores a redundant threshold.

The selected order is part of the safety proof and must not be reordered by
callers or command plans. Tests must cover every write-order row and crashes
between its two operations.

# Links

- [Data-safety contract](../docs/DATA_SAFETY.md)
- ADR-0003: ZK client and domain layering
- ADR-0005: Idempotent iterations
- ADR-0007: Failover state machine
- ADR-0011: Versioned atomic switchover record
