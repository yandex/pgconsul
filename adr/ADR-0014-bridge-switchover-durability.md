# Bridge switchover durability

- Status: Proposed
- Deciders: munakoiso

# Context

A planned switchover changes the primary while PostgreSQL and ZooKeeper have
separate, non-atomic durability state.  The normal durability transition
protocol in ADR-0012 safely changes one member at a time, but it cannot by
itself ensure that the promotion candidate remains a synchronous target until
the handoff.

The existing switchover implementation lets several hosts advance its global
phase.  It also waits for every side replica and blocks the old primary before
the candidate is ready.  Both make the operation unnecessarily fragile or
long.

# Decision

One host holds an ephemeral switchover-manager lock.  Only that manager may
CAS-update the versioned `switchover/record`.  Host-local workers execute
manager-issued commands and publish operation-id-scoped acknowledgements; an
ack never advances a global phase by itself.

The record contains `P` (old primary), `C` (candidate), the source durability
configuration `D0`, a durability pin, and an optional bridge replica `S1`.
The pin has two modes.

## Contracting mode

`P` owns the contracting pin.  Its only normal durability target is `{P, C}`.
The regular ADR-0012 transition mechanism performs the one-member changes and
their LSN barriers.  While the pin is active, ordinary quorum reconciliation
must not remove `C` from `P`'s actual SSN.

After `{P, C}` is stable, `P` records a handoff LSN and the manager waits until
`C` has flushed it.  This is a readiness barrier and occurs while `P` remains
available for writes.

For a cluster with side replicas, `C` continuously publishes the turned HA
replicas currently streaming from it and their flush LSNs.  Immediately before
the bridge SSN change, the manager CAS-selects the freshest such replica as
`S1` (hostname breaks equal-LSN ties).  `S1` is pinned to stream only from `C`:
its restore command is disabled and normal return-to-cluster logic may not send
it back to `P`.
Before releasing the primary lock, `P` performs the single bridge expansion
`{P, C} -> {P, C, S1}` through ADR-0012.  The manager waits for all of:

```
stable durability_members == {P, C, S1}
no durability transition
SSN(P) requires C
C pre-promotion SSN requires P and S1
S1 streams from C with restore_command disabled
```

`C` applies its pre-promotion SSN before it waits for the primary lock.  For
the bridge configuration this is `ANY 1(P,S1)`.

The number of live turned side replicas required before handoff is
`W(D0) = ceil((|D0|-1)/2)`.  `S1` is sufficient for the immediate post-promote
write path; the complete set avoids a later availability wait while the
original quorum strength is restored.  The manager may wait a bounded grace
period for additional side replicas but does not wait for all of them.  If no
side replica is live for the whole catch-up timeout, it proceeds without a
bridge, with the same safe write unavailability as a two-host cluster.

The manager does not recheck `S1` after its CAS selection: only the small SSN
preparation remains.  If `S1` fails in that window, it stays in the pinned
durability set.  Removing or replacing it would require a separate durability
transition.  Commits may wait for `S1` or `P` to return, but cannot lose data.

The handoff predicate is:

```
handoff_ready(P, C, S1) if
  C remains in P's actual SSN,
  C has flushed handoff_lsn,
  C has persisted its pre-promotion SSN,
  C has completed a restartpoint,
  and S1 remains pinned to C.
```

## Handoff commit, promotion, and recovery

Before asking `P` to release the primary lock, the manager CAS-writes
`handoff_committed` with the operation id, `C`, and
`expected_timeline = T_old + 1`.  `C` has no right to promote from any earlier
phase.  `P` writes `expected_timeline` to the ZK timeline node and only then
releases the primary lock.  `C` requires both records before it promotes.
Thus the ZK timeline is deliberately a *committed branch fence* during the
short interval before PostgreSQL has created that timeline; it is not merely a
report of an already-observed PostgreSQL timeline.

The manager then asks `P` to request asynchronous pooler shutdown, release the
primary lock, and stop PostgreSQL.  It does not wait for pooler or PostgreSQL
shutdown before `C` promotes.  `C` already has slots and SSN configured, and
does not wait for manager acknowledgement after it gets the primary lock.
`synchronous_commit=on` and the unchanged SSN on `P` ensure that `P` cannot
acknowledge a transaction missing from `C`.  After releasing the lock, `P` is
forbidden from changing SSN or durability and must eventually stop; this
prevents a stale primary from later resetting SSN.

The ZK timeline write is the irreversible branch decision.  If `C` fails
after it, failover first stops WAL receivers and disables archive restore, then
accepts votes only from `expected_timeline`; old-timeline votes are ignored.
If this election has no winner, it waits for the designated `C` to return and
resume its already-authorized promotion.  It must not restore `P` merely
because no new-timeline host replied.

Before that timeline write, promotion by `C` is impossible.  A failure there
may use ordinary old-timeline failover, including selecting `P` if it is still
eligible.  The switchover manager performs the failover entry checks and
persists the initial failover state first, then CAS-transitions the switchover
record to `fallback`.  If the CAS fails, top-level failover handling still has
priority over switchover handling.  If failover initialization fails, the
switchover record is unchanged.  After failover cleanup, a manager CAS-clears
the stale fallback record.  A manager failure at any point is retried by a new
lease holder.  Cancellation before the branch decision is also safe, but must
restore `D0` using ADR-0012 transitions before clearing the record; it is not a
hot-path operation.

## Expanding mode

After `C` holds the primary lock and promotes, the pin changes owner to `C`.
Until the switchover finishes, `C` may use the regular durability mechanism
only to add turned HA replicas.  It may not remove `P` or any other member.
The eventual removal of `P` is normal reconciliation after the pin is cleared.

This monotonic phase prevents a restarted `P` from resuming the old
reconciliation and makes the post-handoff state unambiguous for failover.

For a truly two-host cluster there is no `S1`.  `C` promotes with an SSN that
requires `P`; writes remain blocked until `P` returns as a replica.  The
operation must never silently become asynchronous.  A non-HA replica is not a
valid bridge member: an SSN acknowledgement from a host ignored by failover
would break the durability proof.

## Archive and return

After promotion, the switchover waits for the last old-timeline WAL segment
before the new timeline and the new timeline's history file in S3.  This wait
does not block the new primary or already turned replicas.  It only prevents
return-to-cluster for old and unturned replicas.  If the archive is unavailable
the durable cluster continues running in `waiting_archive`; return is retried
later and another switchover is not started.

The archive is an append-only ordered log per timeline: visibility of a WAL
segment implies visibility and immutability of every preceding segment on that
timeline.  History-file visibility alone is insufficient because PostgreSQL
may archive history ahead of the preceding `.partial` WAL segment, hence the
separate fork-WAL barrier.

Return-to-cluster runs from one top-level place, outside both failover and
switchover.  Once the archive barrier is open it chooses remaster or rewind
from timeline history for only the replicas that did not turn successfully.
The vote fences (`restore_command=/bin/false` and `primary_conninfo=''`) remain
in place through that decision.  After the barrier and any rewind, pgconsul
removes both persistent overrides while PostgreSQL is stopped, writes recovery
configuration for the current primary, and only then starts PostgreSQL.  This
allows old WAL to come from S3 and the current open segment to stream from the
primary; waiting for recovery before enabling `primary_conninfo` would
deadlock when that segment has not yet been archived.

TODO: add a fault-injection Behave scenario which restarts `pgconsul` on `P`
after handoff and before its return.  It must verify that the post-restart
choice between remaster and rewind is derived from local flush LSN and target
timeline history, not process memory.

## Future direction

The bridge and temporary quorum contraction are compatibility machinery.  A
planned PostgreSQL capability to require one concrete, guaranteed-synchronous
replica independently of the ordinary quorum will remove this need.  With that
capability, switchover will keep its normal durability quorum throughout the
handoff and use the concrete guarantee only for `P -> C`; no bridge member or
quorum contraction is required.

# Alternatives

Keep all side replicas as a hard gate.  This unnecessarily delays a healthy
handoff while `P` is still serving writes.

Allow a non-HA replica to be `S1` without changing failover.  Rejected because
the replica could be the sole holder of an acknowledged transaction while no
eligible failover winner contains it.

Allow `C` to remove members during the pinned phase.  Rejected because a
partial expansion could remove the only old-primary guard and silently make
the new primary asynchronous.

# Consequences

Switchover gains manager ownership, explicit CAS fencing, and a small bridge
state.  The common two-host case is safe but may have write unavailability
until the old primary returns.  The implementation must test pin ownership,
no reverse routing of `S1`, no SSN change by `P` after release, and the
two-host no-async path.  It must also test that `P` is untouched before the
branch fence, that `P` cannot be restored after it, that `C` cannot promote
before the ZK timeline write, and that a failed pre-promotion `C` leaves the
committed handoff available for retry rather than falling back to `P`.

# Links

- ADR-0006: Cluster operation command plans
- ADR-0010: Top-level switchover ownership
- ADR-0011: Versioned switchover record
- ADR-0012: Safe durability membership changes
- ADR-0013: Single-coordinator failover safety
