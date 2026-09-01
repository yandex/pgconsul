# Switchover durability and branch recovery

- Status: Proposed
- Deciders: munakoiso

# Context

A switchover moves writes from the old primary `P` to candidate `C`. PostgreSQL
SSN, the primary leader lock, timeline state, and the global operation record
cannot be changed atomically. The protocol must therefore remain safe after a
process crash or host failure between any two writes.

`D0` is the stable durability membership before switchover. For a primary in
`D0`, `W(D0) = floor(|D0| / 2)` is the number in `ANY W(...)` after excluding
that primary from SSN.

# Decision

One ephemeral switchover-manager lock has a single holder. Only that manager
may CAS-update the versioned `switchover/record`. Other hosts publish
operation-id-scoped acknowledgements. Local promotion progress is also keyed
by the operation id.

## Preparation without the PostgreSQL patches

The compatibility protocol uses no bridge replica and never turns a non-HA
replica into an HA member.

1. The manager freezes `P`, `C`, `D0`, and the operation id.
2. Normal ADR-0012 transitions contract durability to `{P,C}`, one host at a
   time. `C` therefore remains the synchronous replica required by `P`.
3. `P` commits `advance_wal_barrier(operation_id)` with
   `synchronous_commit=on`. Success proves that `C` flushed every commit of `P`
   through the barrier.
4. `C` creates slots for the side replicas, disables archive restore, and
   calculates the timeline PostgreSQL will choose: the first timeline after
   the consecutive local history files above `P`'s timeline. It configures its
   pre-promotion SSN from `D0`, completes a restartpoint, and acknowledges all
   of this to the manager.
5. Side HA replicas disable archive restore and turn to `C`. A turned replica
   is never sent back to `P` by the active operation.

Before the handoff, `C` must have at least `W(D0)` side replicas streaming from
it. `P` is not yet a replica of `C` and cannot satisfy `C`'s SSN. In a two-host
cluster there are no side replicas, so this condition is empty; after promote,
writes wait until `P` returns as a replica.

All preparation happens while `P` remains open for writes.

## Early leader-lock transfer

After `C` has installed SSN and the expected timeline, the manager CAS-writes
`desired_primary=C`. The common reconciler makes `P` release the leader lock
without stopping its pooler or PostgreSQL. `C` acquires the lock while it is
still a replica.

During this phase the leader lock denotes the planned owner, not the current
PostgreSQL primary. Active switchover handling therefore owns reconciliation;
ordinary primary/failover logic must not interpret `C`'s early lock as a
completed promotion. `last_primary` is not changed merely by this acquisition.

This transfer is outside the hot path. Once `C` holds the lock and enough side
replicas are streaming from it, `P` sends a non-blocking pooler-stop request,
starts asynchronous PostgreSQL shutdown, and the manager CAS-writes
`handoff_committed`. `C` may promote only from that phase, with the matching
operation id, desired-primary record, and leader lock. No additional leader
lock wait or WAL barrier remains after handoff.

The target timeline is stored in the switchover record but is not written to
the cluster timeline node before promotion. Successful promotion writes the
actual current timeline by the normal promotion completion path. There is no
post-promote equality check: with archive restore disabled, the locally
predicted first unused timeline is the one PostgreSQL selects.

## Failure after handoff

`handoff_committed` means that `C` was allowed to promote, not that a commit on
its branch necessarily exists. A failover vote is published only after archive
restore and walreceiver input are fenced. It contains the operation version,
the voter's actual timeline, durable WAL endpoint, and priority. A stopped `P`
publishes its control-file timeline without an LSN; its LSN is irrelevant when
the protocol returns directly to `P`.

Let `Tn` be `C`'s planned timeline and let `R(C)` be the replicas named by
`C`'s prepared SSN. A host that voted on another timeline can no longer have
acknowledged a later commit on `Tn`. A host without a vote is conservatively
assumed able to contain such a commit.

The target branch remains possible exactly while:

```
|votes_on_Tn(R(C)) union non_voters(R(C))| >= W(D0)
```

If the inequality becomes false, `C` could not have acknowledged a commit and
the unique safe continuation is `P` on the source timeline. This decision is
monotonic because votes never disappear within a failover version.

Otherwise failover remains on `Tn`. Only a `Tn` host may win, and LSNs are
compared only between `Tn` votes. Votes from other timelines still count
towards the read quorum: after fencing they prove that those hosts cannot hide
a `Tn` commit. If no safe target-branch winner is available, the cluster waits
for another voter or for `C`; it does not guess that rollback is safe.

## Completion and archive barrier

After promotion, the deadline no longer aborts cluster recovery. The operation
completes SSN expansion, a best-effort checkpoint on the new primary, and
waits until S3 contains the new history file and either the complete or partial
old-timeline WAL file containing the fork point. Once the candidate has
acknowledged promotion and the operation enters `waiting_archive`, every
already turned side replica also attempts one local checkpoint. Replica
checkpoint failure is recorded but never gates completion; it may only cause
an unnecessary later rewind. The new primary and already turned replicas
remain available during this work.

The archive is assumed append-only and ordered per timeline: once a WAL segment
is visible, every preceding segment on that timeline is visible and immutable.
Return-to-cluster is outside switchover. It derives remaster versus rewind from
the local durable endpoint and complete target history after the archive
barrier.

Before `handoff_committed`, timeout changes `desired_primary` back to `P`, marks
the operation failed, and lets ordinary durability reconciliation restore
`D0`. After handoff but before successful promotion, timeout starts the fenced
failover described above. After successful promotion, cleanup continues
without applying the user-operation deadline.

## Optional patched protocol

The compatibility contraction exists only because stock PostgreSQL cannot
combine its ordinary quorum with one separately mandatory synchronous replica.
With `use_pg_patches`, `P` keeps `D0` and applies
`ALWAYS(C), ANY W(D0)(R(D0,P))`. The service-table barrier therefore proves
that `C` has every preceding commit without contracting durability.

The manager scans the current primary's consecutive local history files when
the high-water mark is absent, then CAS-reserves a never-before-used timeline
for the operation. `C` keeps archive restore enabled and promotes with
`pg_ctl promote --timeline N`. Before handoff it configures the same ordinary
`D0` quorum it would use as primary. The manager/ACK, early-lock, handoff, and
mixed-timeline failover rules remain the same. A pre-handoff rollback explicitly
restores ordinary `ANY` SSN because the ZK durability membership did not change.
Ordinary failover winners reserve and use timelines from the same high-water
mark.

# Alternatives

Keep an `S1` bridge member. Rejected: choosing and pinning a temporary bridge
adds another failure-sensitive SSN transition. Preparing `C` with `D0` and
waiting for the required number of actual side connections is simpler.

Write the target timeline to ZK before promotion and forbid rollback after
that write. Rejected: a timeline number alone does not prove that `C` could
acknowledge a commit. Fenced votes provide the stronger and exact predicate.

Transfer the leader lock on the hot path. Rejected: it adds an avoidable ZK
round trip after `P` starts shutting down.

# Consequences

The leader lock temporarily names a replica during an active switchover, so
all generic ownership reconciliation must explicitly respect the operation.
The unpatched protocol temporarily contracts durability to two hosts and can
therefore reduce write availability, but not data safety. Mixed-timeline votes
make rollback after handoff possible only when the absence of a target-branch
write quorum has been proved.

# Links

- [Data-safety contract](../docs/DATA_SAFETY.md)
- ADR-0006: Cluster operation command plans
- ADR-0010: Top-level switchover ownership
- ADR-0011: Versioned switchover record
- ADR-0012: Safe durability membership changes
- ADR-0013: Single-coordinator failover safety
