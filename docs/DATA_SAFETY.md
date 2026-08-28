# PgConsul data-safety contract

This document states the conditions under which pgconsul preserves commits
acknowledged to clients. It covers normal operation, durability membership
changes, failover, bridge switchover, and return to cluster.

It is a safety contract, not an availability promise. If a required predicate
cannot be established, pgconsul must wait instead of promoting a host or
weakening the write quorum.

## Safety theorem

Let `k` be a transaction whose successful commit was reported by a managed
primary while the assumptions below held. If pgconsul subsequently authorizes
another primary, that primary's WAL contains the commit record of `k` and all
WAL preceding it on the selected branch.

The theorem applies only when `allow_potential_data_loss=false`.

## Terms

- A **durability member** is an HA PostgreSQL host whose durable WAL may be
  used either to acknowledge a commit or to elect a primary. Non-HA and
  cascading-only hosts are not durability members.
- `P` is the current or failed primary.
- `D` is the failover-visible durability membership stored in ZooKeeper. The
  current field name is `stable`. During a ZK-first transition this means
  "committed for failover", not "already applied to both ZK and PostgreSQL".
- `R(D,P) = D - {P}` is the replica set for primary `P`.
- `W(D) = ceil(|R| / 2) = floor(|D| / 2)` is the PostgreSQL write threshold.
  It is the number in `ANY W(...)` in `synchronous_standby_names` (SSN).
- `Q(D) = |R| - W(D) + 1` is the failover read threshold.
- An **ACK set** is any set of `W(D)` distinct, directly connected replicas
  whose durable flush acknowledgements may release a commit on `P`.
- The **effective SSN** is the configuration PostgreSQL actually uses. It can
  temporarily differ from `D` during a recorded membership transition.
- A **durable LSN** is the end of the last valid WAL record present in the
  replica's local `pg_wal`. For a safe election pgconsul reads it with
  `lwaldump()` after future archive restores are disabled and walreceiver is
  stopped.
- A **failover version** identifies one immutable electorate and its votes.
- A **timeline fence** is the timeline to which the protocol has irrevocably
  committed. During bridge switchover it can be reserved before PostgreSQL
  creates the timeline.
- The **fencing cut** of a failover is the point at which `Q(D)` valid voters
  have stopped archive restore and walreceiver and published versioned votes.
- In bridge switchover, `C` is the candidate and `S1` is an optional HA bridge
  replica that has already changed its upstream from `P` to `C`.

For a fixed `D`, every ACK set intersects every set of `Q(D)` replicas because

```text
W(D) + Q(D) = |R(D,P)| + 1.
```

## Failure model and external assumptions

The proof relies on all of the following:

1. Processes and hosts may crash, restart, become partitioned, and repeat an
   operation after an unknown result. They are not Byzantine.
2. ZooKeeper provides linearizable writes, CAS versions, and exclusive
   ephemeral locks. A process that has lost a lock cannot successfully perform
   a lock-guarded write.
3. PostgreSQL has `fsync=on`, storage honours flushes, and an SSN `flush_lsn`
   acknowledgement survives the storage failures covered by the deployment.
4. Every protected transaction commits with `synchronous_commit=on` or
   `remote_apply`. `remote_write`, `local`, and `off` are outside the theorem.
5. SSN member names uniquely identify trusted physical replicas of this
   PostgreSQL cluster. A different process cannot connect using another
   member's `application_name`.
6. Hosts do not change SSN, recovery sources, timelines, or pgconsul's ZK
   records outside the protocol. In particular, a stale primary cannot weaken
   SSN or switch protected transactions to asynchronous commit.
7. WAL on one timeline is a totally ordered prefix. Comparing LSNs from
   different timelines is forbidden unless timeline history first establishes
   the ancestor relationship and forkpoint.
8. Applying SSN is complete only after PostgreSQL reloads it and reports the
   new value. A racing commit is governed wholly by either the old or the new
   effective SSN.
9. The archive publishes WAL objects atomically and immutably. Visibility of a
   WAL segment implies visibility of every preceding segment on that timeline.
   History and WAL belong to the same PostgreSQL system identifier and pass
   PostgreSQL integrity checks.
10. `lwaldump()` returns the end of the valid WAL prefix durably present in
    local `pg_wal`, including WAL flushed by walreceiver before a PostgreSQL
    restart.

An archive or history failure can block return to cluster, but it is not used
to justify a promotion.

`|D| = 1` derives `W(D) = 0` and is asynchronous operation. Safe automatic
failover cannot be proved from an empty electorate and is therefore forbidden.

## Normal operation

The following invariant holds whenever no membership transition is active:

1. `P` belongs to `D` and owns the primary lock.
2. PostgreSQL on `P` uses `ANY W(D)(R(D,P))`.
3. Failover uses exactly `R(D,P)` and requires `Q(D)` valid votes.
4. The ZK timeline fence equals the primary's active timeline.

Therefore every protected commit is durably present on `P` and on an ACK set
that intersects every legal failover read quorum.

The primary lock prevents two protocol participants from being authorized at
once, but the lock alone is not the data fence. Network-partition fencing is
proved by the frozen failover voters below.

## Changing durability membership

Let `source` and `target` both contain the same primary and differ by exactly
one replica. A replacement is an expansion followed by a contraction.

The transition record contains `source`, `target`, order, optional barrier LSN,
and a ZK version. While the same primary remains active, no second membership
change starts until this record is finished. If that primary fails, failover
uses `stable` and supersedes the unfinished transition as described below.

### SSN-first

SSN-first means:

1. CAS-create the transition while `D = source` remains visible to failover.
2. Apply the target SSN on `P`.
3. Read primary flush LSN `L` only after the target SSN is effective.
4. CAS-store `L` in the transition.
5. Wait until `W(target)` distinct target replicas report `flush_lsn >= L`.
6. CAS-publish `D = target` and clear the transition.

The barrier copies the complete source prefix through `L` to a target ACK set.
Every target read quorum intersects that set. Commits after `L` are already
protected by the target SSN.

SSN-first is used when every source read quorum intersects every target ACK
set:

```text
for all A ⊆ R(source,P), |A| = Q(source),
for all B ⊆ R(target,P), |B| = W(target): A ∩ B != ∅.
```

### ZK-first

ZK-first means:

1. CAS-create the transition and publish `D = target` in the same write.
2. Apply the target SSN on `P`.
3. CAS-clear the transition.

No LSN barrier is needed. While PostgreSQL still uses the source SSN, every
target read quorum intersects every source ACK set. After reload, the ordinary
target invariant holds.

ZK-first is used when:

```text
for all A ⊆ R(target,P), |A| = Q(target),
for all B ⊆ R(source,P), |B| = W(source): A ∩ B != ∅.
```

For one-host changes the safe order is exhaustive:

| Change | `W` change | Order |
|---|---:|---|
| expansion | increases | SSN-first |
| expansion | unchanged | ZK-first |
| contraction | unchanged | SSN-first |
| contraction | decreases | ZK-first |

Large changes are safe only as a sequence of these adjacent transitions.

The first membership is initialized as SSN-first without a source. Failover is
disabled until the SSN is effective, its LSN barrier has passed, and the first
`D` has been published.

### Membership crash recovery

| Crash point | Persisted fact | Recovery |
|---|---|---|
| before transition CAS | old membership only | start again |
| after target SSN, before storing `L` | SSN-first record | reapply target SSN, read a new `L` |
| after storing `L`, before barrier | record with `L` | recheck the same barrier |
| after barrier, before target CAS | record with `L` | recheck barrier, CAS target |
| after ZK-first CAS, before target SSN | target is failover-visible and source is recorded | apply target SSN |
| after final CAS, before observing success | target without transition | treat operation as complete |

Every persisted intermediate state satisfies one of the two cross-quorum
conditions. Reapplying SSN and repeating a CAS are idempotent while the primary
does not change.

If the primary fails during either transition order, the frozen electorate is
always derived from `stable`; `target` is never used as an alternative voting
set. Before promotion the winner applies SSN derived from that same `stable`.
After publishing its new timeline, but before publishing `promoted`, it
CAS-clears the old transition while preserving `stable`. In particular, it
never resumes an SSN-first barrier LSN created by the old primary. Normal
reconciliation may then start a new transition from `stable`, with a new SSN
application and a new barrier LSN on the new primary.

## Failover

### Freezing the electorate

One coordinator owns the failover-manager lock. It freezes in ZK:

- a new failover version;
- `P`'s timeline fence;
- electorate `E = R(D,P)` from the failover-visible membership.

Current liveness and later HA membership changes cannot add voters. Votes from
another version, timeline, or host outside `E` are ignored.

Before voting, a participant performs this ordered sequence:

1. set `restore_command` to a disabled command and wait for reload;
2. clear `primary_conninfo`, reload, and wait until walreceiver has stopped;
3. verify its local timeline against the frozen timeline;
4. read its durable LSN;
5. write a vote containing host, version, timeline, LSN, and priority.

`pg_last_wal_receive_lsn()` and `pg_last_wal_replay_lsn()` cannot replace
`lwaldump()` here. Walreceiver's receive position is process state and is lost
when PostgreSQL restarts. Hot standby may accept SQL after reaching consistency
while replay is still behind WAL that walreceiver had already flushed before
the restart. Voting with either SQL position can therefore omit a synchronous
commit that is still present in local `pg_wal`, let an older replica win, and
discard that commit. `lwaldump()` reconstructs the durable endpoint from the
WAL files themselves. If it is absent or fails, safe failover stops; there is
no fallback to the receive or replay position.

After step 2 no new streaming WAL can enter the voter and no new archive
restore command can be started. An already obtained WAL file or an in-flight
restore may still advance local recovery, but that cannot lower the voted
durable prefix or provide an acknowledgement to `P`. A voter does not
reconnect to `P` after voting.

### Why the old primary is fenced

Failover continues only after valid votes from a set `V ⊆ E` with
`|V| = Q(D)` exist. Every possible ACK set of the effective SSN intersects
`V`, including the cross-quorum SSN used by an unfinished membership change.

Consequently:

- every commit already acknowledged by `P` exists on at least one voter;
- after the fencing cut, `P` cannot obtain all acknowledgements required for a
  new protected commit;
- an in-flight commit released using an acknowledgement produced before the
  cut is also present on that voter.

This is the split-brain data fence. Loss of the primary lock is necessary for
authorization, but is not used as a substitute for this intersection.

### Selecting and activating the winner

All accepted votes are on one timeline, so their WAL is prefix-ordered. The
coordinator selects the greatest `(durable LSN, priority)`; priority only
breaks equal-LSN choices. For every acknowledged commit, at least one voter
contains it, and the greatest-LSN voter cannot be behind that voter.

Before promotion the winner applies SSN derived from the same `D`, with itself
removed from the replica list. It then acquires the primary lock and promotes.
Promotion is not reported as successful until the winner has written its
actual new timeline to ZK and CAS-discarded any unfinished membership
transition left by the failed primary. The CAS preserves `D`; a conflict is
retried rather than allowing the stale transition to continue on the new
timeline.

The winner's pooler can already be running while it is a replica. This does not
create an acknowledgement window in safe mode: every other member named in
its pre-promotion SSN remains fenced and does not reconnect until the winner
publishes `promoted`, which happens after the timeline write. Thus no protected
commit can complete on the new primary before its timeline is published.

### Failover crash recovery

| Crash point | Recovery |
|---|---|
| electorate/version written, phase absent | a later initialization replaces the inactive metadata |
| active phase, votes incomplete | a new coordinator resumes the same version and electorate |
| quorum voted, winner absent | deterministically select the greatest valid vote |
| winner written, phase not advanced | recompute from all valid votes, overwrite the winner if needed, then advance the phase |
| winner has lock, promote not finished | resume the host-local promotion state |
| PostgreSQL promoted, timeline not written | retry post-promote finalization; voters remain fenced |
| timeline written, old membership transition remains | CAS-clear the transition while preserving `stable`; never reuse its barrier LSN |
| timeline written, transition cleared, participant result absent | publish the same versioned `promoted` result |
| participant result written, global phase unfinished | a new coordinator observes it and finishes |

Only the coordinator changes global failover phases. A winner publishes only
its versioned local result.

## Bridge switchover

Let `D0` be the original membership, `P` the old primary, and `C ∈ D0` the
candidate. One manager CAS-updates the operation-id-scoped switchover record;
workers only publish operation-id-scoped acknowledgements.

### Transferring every old-primary commit to `C`

1. Using the normal membership protocol, contract `D0` to `{P,C}`.
2. The effective SSN on `P` is now `ANY 1(C)`.
3. Read `handoff_lsn` and wait until `C` has durably flushed it.
4. Turn eligible side replicas to stream from `C` with archive restore
   disabled. Select the freshest live HA side replica as optional `S1`.
5. If `S1` exists, expand through the normal membership protocol to
   `{P,C,S1}`. Otherwise keep `{P,C}`.

The bridge topology is part of the proof. Before promotion, `S1` streams from
`C`, not directly from `P`. Therefore with `ANY 1(C,S1)` on `P`, only `C` can
produce an acknowledgement for a new LSN on `P`; a stale old connection from
`S1` cannot advance. Every commit acknowledged by `P` up to shutdown is thus
durably present on `C`, not merely on either named host.

`C` prepares its future SSN before promotion:

- with a bridge: `ANY 1(P,S1)` for membership `{C,P,S1}`;
- without a bridge: `ANY 1(P)` for membership `{C,P}`.

### Committing the branch and promoting

After all preparation acknowledgements, the manager CAS-writes
`handoff_committed` with operation id, `C`, and
`expected_timeline = old_timeline + 1`.

`P` then writes `expected_timeline` to the ZK timeline node before releasing
the primary lock. At this point the ZK value is a committed branch fence, not
a claim that PostgreSQL has already created the timeline. Rollback to the old
branch is forbidden.

`P` requests asynchronous pooler shutdown, releases the lock, and requests
PostgreSQL shutdown. Waiting for shutdown is unnecessary for safety: the SSN
on `P` remains unchanged, and every protected commit it can still acknowledge
must be present on `C`.

`C` requires both committed records, acquires the primary lock, and promotes.
It verifies that PostgreSQL's current insertion timeline equals the committed
timeline and confirms that value in ZK before reporting `promoted` and
explicitly ensuring the pooler is running. The branch fence already exists, so
even an acknowledged commit during this short interval belongs to the new
branch and cannot be discarded by a later failover.

After promotion:

- if `S1` exists, `P` is stopped and `S1` is directly connected to `C`, so
  `ANY 1(P,S1)` requires `S1` until `P` returns;
- if `S1` does not exist, `ANY 1(P)` blocks protected writes until `P` returns;
- for `{C,P,S1}`, failover requires both other members to vote, so it
  intersects a commit acknowledged by either `P` or `S1`;
- adding further turned HA replicas uses the normal one-host membership
  protocol and raises `W` before the larger membership becomes visible when a
  barrier is required.

This is why an intermediate `ANY 1(P,S1)` is safe. There is no state in which
the failover-visible membership is `{C,P,S1,X}` while the effective SSN is
still `ANY 1(P,S1)`.

### Switchover failure and crash recovery

Before the branch fence is written, `C` has no right to promote. If `P` or the
manager disappears, the switchover manager initializes ordinary old-timeline
failover first and only then CAS-marks the switchover as `fallback`. Top-level
failover handling has priority, so a failed fallback CAS cannot reopen the
switchover path.

After the branch fence is written, old-timeline promotion is forbidden. A
failover fences all participants but counts only votes from the committed new
timeline. If no such voter exists, it waits for `C` to return and resume the
authorized promotion. If `C` had already created the timeline and another
durability member received it, ordinary greatest-LSN election proceeds using
only that timeline.

| Crash point | Recovery |
|---|---|
| before switchover record CAS | no operation exists |
| during contraction or bridge expansion | resume the recorded membership transition |
| after side acknowledgements, before selecting `S1` | recompute and CAS-select the freshest eligible host |
| after selecting `S1`, before prepared ACKs | keep the selected member and retry preparation |
| after `handoff_committed`, before timeline fence | `C` cannot promote; a live `P` retries the fence, otherwise the manager starts old-timeline failover |
| after timeline fence, before lock release | branch is committed; retry release and shutdown |
| after lock release, before promote | only `C` may acquire the lock and resume promotion |
| after promote, before timeline verification | resume local post-promote finalization; old branch remains fenced |
| after verification, before promoted ACK | primary is safe; rewrite the operation-scoped ACK |
| after promoted ACK, before global phase | a new manager observes the ACK and enters expansion |

The failover-manager lock and switchover-manager lock are separate, but they do
not create two concurrent decisions: active failover is handled first in every
iteration; membership changes require the primary lock; and the only
switchover-to-failover edge persists failover state before marking fallback.

## Return to cluster

Return is not part of the election proof and cannot authorize a primary. It
must preserve the selected branch:

1. A successfully turned replica follows the new primary directly and may
   resume archive restore only after streaming is established.
2. A divergent or uncertain replica reads the target timeline history and the
   forkpoint.
3. If its durable LSN is past the forkpoint, it runs `pg_rewind`; otherwise a
   direct remaster is allowed.
4. Before rewind/return, it waits for the target history and the old-timeline
   WAL segment containing the forkpoint in the archive.
5. Missing, malformed, unrelated, or incomplete history/WAL causes a safe
   wait, never a guess.

The archive barrier ensures that all older WAL needed by recovery is already
immutable and available before restore sources are enabled.

## Conditions that require fail-closed behaviour

Pgconsul must not claim this safety guarantee when any of these is true:

- no failover-visible synchronous membership exists;
- the electorate is empty or fewer than `Q(D)` valid votes can be frozen;
- `lwaldump()` is unavailable or cannot establish a voter's local WAL end;
- a vote has the wrong version or timeline;
- the winner cannot apply its pre-promotion SSN;
- the promotion timeline cannot be published or verified;
- a membership transition violates its recorded order or one-host scope;
- the required SSN-first LSN barrier cannot be observed;
- a committed switchover branch has no eligible new-timeline continuation;
- required archive history or fork WAL is unavailable for return to cluster.

Waiting in these states preserves safety even when it reduces availability.

## Related decisions

- [ADR-0012: safe durability membership change](../adr/ADR-0012-safe-durability-membership-change.md)
- [ADR-0013: single-coordinator failover](../adr/ADR-0013-single-coordinator-failover-safety.md)
- [ADR-0014: bridge switchover durability](../adr/ADR-0014-bridge-switchover-durability.md)
- [PostgreSQL 14 synchronous replication](https://www.postgresql.org/docs/14/warm-standby.html#SYNCHRONOUS-REPLICATION)
- [PostgreSQL 14 recovery information functions](https://www.postgresql.org/docs/14/functions-admin.html#FUNCTIONS-RECOVERY-INFO-TABLE)
