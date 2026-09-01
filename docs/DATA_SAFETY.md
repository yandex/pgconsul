# PgConsul data-safety contract

This document states the conditions under which pgconsul preserves commits
acknowledged to clients. It covers normal operation, durability membership
changes, failover, switchover, and return to cluster.

It is a safety contract, not an availability promise. If a required predicate
cannot be established, pgconsul must wait instead of promoting a host or
weakening the write quorum.

## Safety theorem

Let `k` be a transaction whose successful commit was reported by a managed
primary while the assumptions below held. If pgconsul subsequently authorizes
another primary, that primary's WAL contains the commit record of `k` and all
WAL preceding it on the selected branch.

The theorem applies to every automatic failover and ordinary operator-initiated
failover. An explicit `pgconsul-util failover --with-data-loss` request is
outside this theorem.

## Terms

- A **durability member** is an HA PostgreSQL host whose durable WAL may be
  used either to acknowledge a commit or to elect a primary. Non-HA and
  cascading-only hosts are not durability members.
- `P` is the current or failed primary.
- `D` is a durability membership containing its primary. When no transition is
  active, the ZooKeeper field `stable` contains the single current `D`.
- `R(D,P) = D - {P}` is the replica set for primary `P`.
- `W(D) = ceil(|R| / 2) = floor(|D| / 2)` is the PostgreSQL write threshold.
  It is the number in `ANY W(...)` in `synchronous_standby_names` (SSN).
- `Q(D) = |R| - W(D) + 1` is the failover read threshold.
- An **ACK set** is any set of `W(D)` distinct, directly connected replicas
  whose durable flush acknowledgements may release a commit on `P`.
- The **effective SSN** is the configuration PostgreSQL actually uses. It can
  temporarily differ from `D` during a recorded membership transition.
- A **durability transition** is the operation-id-scoped ZooKeeper intent
  `source -> target`. Until its WAL barrier succeeds, both configurations are
  treated as potentially effective by failover.
- A **durable LSN** is the end of the last valid WAL record present in the
  replica's local `pg_wal`. For a safe election pgconsul reads it with
  `lwaldump()` after future archive restores are disabled and walreceiver is
  stopped.
- A **failover version** identifies one immutable electorate and its votes.
- A **branch vote** records the voter's actual timeline after its external WAL
  sources have been fenced.
- The **fencing cut** of a failover is the point at which every relevant
  configuration has its `Q(D)` valid voters with archive restore and
  walreceiver stopped and versioned votes published.
- In switchover, `C` is the selected candidate and `D0` is the durability
  membership frozen before preparation starts.

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

1. `P` belongs to `D` and owns the primary lock, except during an active
   switchover that has transferred the lock early to its still-replica `C`.
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

The transition record contains `source`, `target`, `operation_id`, and a ZK
version. `stable` remains `source` until the operation finishes. No second
membership change starts while the transition exists.

Every transition uses this order:

1. CAS-create the `source -> target` intent in ZK.
2. Apply `ANY W(target)(R(target,P))` on `P` and wait until it is effective.
3. On a non-blocking PostgreSQL connection, replace the singleton row in the
   PgConsul service table and commit it synchronously:

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

4. After successful commit, CAS-set `stable = target` and clear the transition.

The table write creates ordinary WAL and its commit waits for the
already-effective target SSN. Success therefore proves that a target ACK set
contains the complete WAL prefix through the write. Every target read quorum
intersects that set, and later commits are also governed by target SSN.

`TRUNCATE` prevents repeated barriers from accumulating dead rows. The table's
constraints allow only the one `true` row. A statement timeout or lost
connection is not proof of the barrier: local commit can finish before the
client receives the synchronous acknowledgement. PgConsul instead leaves the
transition active and safely repeats the truncate-and-insert on a later
iteration.

The barrier LSN and acknowledgements are not stored in ZK. PostgreSQL's
successful synchronous commit is the only barrier result needed.

The first membership has `source = null`. Failover is disabled until target
SSN is effective, the service-table commit succeeds, and target becomes stable.

### Membership crash recovery

| Crash point | Persisted fact | Recovery |
|---|---|---|
| before transition CAS | old membership only | start again |
| after intent, before target SSN | source remains stable | apply target SSN |
| after target SSN, before barrier completion | source and target recorded | reapply target SSN and repeat table write |
| after barrier commit, before final CAS | source and target recorded | repeat table write and CAS target |
| after final CAS, before observing success | target without transition | treat operation as complete |

Reapplying target SSN, replacing the operation-id row, and retrying a CAS are
idempotent while the same primary remains active.

If `P` fails during the transition, failover cannot know whether source or
target SSN governed the last acknowledged commit. It therefore obtains a read
quorum for both configurations and requires one candidate to dominate both:

```text
safe(source, candidate, votes)
AND safe(target, candidate, votes)
```

For each configuration `D`, `safe` means that the candidate's durable LSN is
not behind at least `Q(D)` valid votes from `R(D,P)`. Collecting both quorums is
not enough if no one candidate contains the WAL represented by both. The
winner is selected from stable/source members. Before promotion it applies SSN
derived from source. After publishing its new timeline it CAS-clears the old
transition while preserving source. Reconciliation can later start a new
transition on the new primary.

One-host changes normally make one of the two checks imply the other. Direct
multi-host replacement can make failover wait for the union of otherwise
independent hosts. PgConsul therefore decomposes changes into one-host steps.

## Failover

### Starting failover

Each HA replica continuously tracks two intervals for the primary named by ZK:
PostgreSQL port 5432 has been unreachable, and the replica's replay position
has not moved. A contender first checks `min_failover_timeout`, acquires the
failover-manager lock, and CAS-increments the persistent `probe_id`. Replicas
from the union of every currently relevant `R(D,P)` answer only that exact
probe with the two interval results. Normally there is one `D`; an unfinished
transition supplies both source and target.

The contender may create failover state only after every relevant `D` has
`Q(D)` replies saying both intervals are at least
`primary_unavailability_timeout`. These fresh replies also prove that all
required read quorums are currently available. If the bounded probe does not
collect them, the contender releases the manager lock; a later contender
starts another probe ID. Thus observations from different times cannot be
accumulated. Timeline, `replics_info`, and a primary-written availability
timestamp are not entry predicates.

### Freezing the electorate

One coordinator owns the failover-manager lock. It freezes in ZK:

- a new failover version;
- `P`'s timeline fence;
- electorate `E`, the union of relevant replica sets. During a transition this
  is `R(source,P) ∪ R(target,P)`.

Current liveness and later HA membership changes cannot add voters. Votes from
another version or a host outside `E` are ignored. Ordinary failover accepts
only its active timeline; post-handoff switchover failover records every actual
timeline for the branch predicate below.

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

The only exception is an explicit
`pgconsul-util failover --with-data-loss --no-wal-fencing`. In that mode the
operator asks participants to publish moving, unfenced positions and explicitly
chooses a host. Pgconsul labels the result unsafe; none of the vote-intersection
or immutable-prefix conclusions in this section apply.

After step 2 no new streaming WAL can enter the voter and no new archive
restore command can be started. An already obtained WAL file or an in-flight
restore may still advance local recovery, but that cannot lower the voted
durable prefix or provide an acknowledgement to `P`. A voter does not
reconnect to `P` after voting.

### Why the old primary is fenced

After persisting failover state, the coordinator CAS-clears the materialized
`desired_primary` under the failover operation ID. `P` then closes its pooler,
stops WAL archiving, and releases the leader lock. If `P` cannot observe ZK,
its ZK session would normally expire and remove the lock.

Failover continues only after every relevant configuration `D` has valid votes
from at least `Q(D)` members of `R(D,P)`. Every possible ACK set of either
possibly effective SSN intersects its corresponding voted set.

Consequently:

- every commit already acknowledged by `P` exists on at least one voter;
- after the fencing cut, `P` cannot obtain all acknowledgements required for a
  new protected commit;
- an in-flight commit released using an acknowledgement produced before the
  cut is also present on that voter.

This is the split-brain data fence. Loss of the primary lock is necessary for
authorization, but is not used as a substitute for this intersection.

Only after establishing this fence—or after an explicit operator data-loss
override—may the coordinator override a live ZK session. It first gives `P`
`primary_unavailability_timeout` to release the lock. After that timeout it
reads the lowest lock contender, verifies that its identifier is still the
observed `P`, and version-deletes exactly that child node. It never recursively
deletes the lock directory.

The persistent null `desired_primary` prevents every host from acquiring the
leader lock during this interval. Primary-lock acquisition validates the
materialized owner both before and after the Kazoo acquire. A host for which
the owner changed during acquire immediately releases the lock and reports
failure. Thus a stale local Kazoo `is_acquired` value cannot authorize another
promotion or a later reacquisition by `P`.

### Selecting and activating the winner

After the old lock is absent, the coordinator persists `election_winner`,
CAS-writes that host into the same `desired_primary` operation, and only then
advances from the voting phase. If the selected winner already owns the lock
(the safe old-branch rollback case), deletion is unnecessary. The winner's
lock command validates both the hostname and operation ID, acquires the leader
lock, and only then permits promotion.

In an ordinary failover all accepted winner votes are on one timeline, so
their WAL is prefix-ordered. Without a transition, the coordinator selects the
greatest `(durable LSN, priority)`;
priority only breaks equal-LSN choices. During a transition, it considers
stable/source candidates in that order and selects the first candidate whose
LSN is not behind a read quorum of both source and target. Thus the winner
contains every commit represented by each possible effective SSN.

Before promotion the winner applies SSN derived from the same `D`, with itself
removed from the replica list. It then acquires the primary lock and promotes.
With `use_target_promote`, the live primary keeps the timeline high-water mark at
least as high as its local history, and the failover winner CAS-reserves the
next value before running `pg_ctl promote --timeline N`.
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
| timeline written, old membership transition remains | CAS-clear the transition while preserving `stable` |
| timeline written, transition cleared, participant result absent | publish the same versioned `promoted` result |
| participant result written, global phase unfinished | a new coordinator observes it and finishes |

Only the coordinator changes global failover phases. A winner publishes only
its versioned local result.

## Switchover

Let `D0` be the original membership, `P` the old primary, and `C ∈ D0` the
candidate. One manager CAS-updates the operation-id-scoped switchover record;
workers only publish operation-id-scoped acknowledgements.

### Transferring every old-primary commit to `C`

1. Without the PostgreSQL patches, contract `D0` to `{P,C}` through the normal
   one-host transition protocol. With the patches, keep `D0` and set
   `ALWAYS(C), ANY W(D0)(R(D0,P))` on `P`.
2. Commit the operation-scoped service-table barrier on `P` with
   `synchronous_commit=on`. In either mode it cannot complete without `C`.
3. On `C`, create side-replica slots and configure the future ordinary SSN
   from `D0`. Without the patches, disable archive restore and predict the
   first unused local timeline. With the patches, use the timeline reserved
   by CAS above the persistent high-water mark and leave restore enabled.
   Finish a restartpoint in both modes.
4. Turn side HA replicas to `C`, with archive restore disabled, until at least
   `W(D0)` of them are actually streaming from `C`.

The barrier cannot succeed until `C` has flushed every preceding commit of
`P`. Because `C` remains the only acknowledgement source in `P`'s contracted
SSN, every later protected commit accepted before shutdown is also present on
`C`.

### Lock transfer and handoff

After candidate preparation, the manager CAS-writes `desired_primary=C`.
`P` releases the leader lock but keeps its pooler and PostgreSQL running; `C`
acquires the lock while still a replica. During this explicit state, the lock
names the planned owner and does not claim that `C` is already primary.

When the required side replicas are streaming from `C`, `P` sends a
non-blocking pooler-stop request and starts asynchronous PostgreSQL shutdown.
The manager then CAS-writes `handoff_committed`. Only then may `C` promote.
Because lock transfer is already complete, the post-handoff hot path contains
no ZK lock wait and no extra WAL barrier.

The expected timeline is recorded before handoff but is not written to the
cluster timeline node before PostgreSQL creates it. Without the patches,
archive restore is disabled and PostgreSQL chooses the first timeline after
the consecutive history files in local `pg_wal`. With the patches, the
candidate uses `pg_ctl promote --timeline N` with the CAS-reserved timeline.
Successful promotion publishes the actual timeline by the ordinary promotion
completion path.

### Failure after handoff

`handoff_committed` proves that `C` could promote, but not that it acknowledged
a commit. Failover therefore fences all electorate members and records every
voter's actual timeline.

Let `Tn` be the planned timeline, and let `R(D0,C)` be the replicas in `C`'s
prepared SSN. Hosts already voting on another timeline cannot acknowledge a
later `Tn` commit; nonvoters conservatively may do so. The target branch is
still possible while:

```text
|votes_on_Tn(R(D0,C)) union non_voters(R(D0,C))| >= W(D0)
```

If this becomes false, a commit on `C` was impossible and failover returns to
`P`. Otherwise only `Tn` hosts are candidate winners and their LSNs are
compared only with one another. Fenced votes from other timelines still count
towards the read quorum because they prove those hosts cannot hide a `Tn`
commit. If no safe `Tn` winner exists, failover waits for another voter or for
`C`.

| Crash point | Recovery |
|---|---|
| before operation CAS | no operation exists |
| during contraction | resume ADR-0012 transition |
| after candidate preparation, before desired-primary CAS | retry CAS |
| after `P` releases the lock, before `C` acquires it | `C` retries acquisition; pre-handoff timeout restores desired `P` |
| after `C` acquires the lock, before handoff | manager continues preparation; `C` cannot promote |
| after handoff, before promote | mixed-timeline failover applies the commit-possibility predicate |
| after promote, before timeline publication | resume local promotion finalization |
| after promoted ACK | a new manager observes it and completes cleanup |

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
- a vote has the wrong version, or no authorized branch can collect its read quorum;
- the winner cannot apply its pre-promotion SSN;
- the promotion timeline cannot be published or verified;
- a membership transition violates its one-host scope;
- the target-SSN service-table WAL barrier cannot be confirmed;
- an unfinished transition lacks a read quorum or one candidate safe for both
  source and target;
- a committed switchover branch has no eligible new-timeline continuation;
- required archive history or fork WAL is unavailable for return to cluster.

Waiting in these states preserves safety even when it reduces availability.

## Related decisions

- [ADR-0012: safe durability membership change](../adr/ADR-0012-safe-durability-membership-change.md)
- [ADR-0013: single-coordinator failover](../adr/ADR-0013-single-coordinator-failover-safety.md)
- [ADR-0014: switchover durability and branch recovery](../adr/ADR-0014-bridge-switchover-durability.md)
- [PostgreSQL 14 synchronous replication](https://www.postgresql.org/docs/14/warm-standby.html#SYNCHRONOUS-REPLICATION)
- [PostgreSQL 14 recovery information functions](https://www.postgresql.org/docs/14/functions-admin.html#FUNCTIONS-RECOVERY-INFO-TABLE)
