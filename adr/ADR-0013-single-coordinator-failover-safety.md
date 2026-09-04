# Single-coordinator failover and its data-safety proof

- Status: Accepted
- Deciders: munakoiso

# Context

Failover previously allowed both its coordinator and its winner to change the
global failover phase. Votes were collected from the current HA host list, and
the WAL position could be read before every participant had stopped receiving
WAL. This made the protocol difficult to fence and its durability argument
dependent on mutable liveness data.

The stable durability membership `D` contains the failed primary `p`. Its
replica set is `R = D - {p}`. As defined by ADR-0012, the old primary's SSN
write threshold is:

```
W(D) = ceil(|R| / 2)
```

The failover read threshold is:

```
Q(D) = |R| - W(D) + 1
```

Every set of `Q(D)` replicas intersects every possible SSN ACK set of `W(D)`
replicas.

## Safety contract and shared assumptions

Let `k` be a transaction whose successful commit was reported by a managed
primary while the assumptions below held. If pgconsul subsequently authorizes
another primary, that primary's WAL contains the commit record of `k` and all
WAL preceding it on the selected branch.

The theorem applies to every automatic failover and ordinary operator-initiated
failover. An explicit `pgconsul-util failover --with-data-loss` request is
outside this theorem.

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

Whenever no membership transition is active, `P` belongs to `D` and owns the
primary lock, except during an active switchover that has transferred the lock
early to its still-replica `C`. PostgreSQL on `P` uses
`ANY W(D)(R(D,P))`, failover uses exactly `R(D,P)` and requires `Q(D)` valid
votes, and the ZK timeline fence equals the primary's active timeline.

Therefore every protected commit is durably present on `P` and on an ACK set
that intersects every legal failover read quorum. The primary lock prevents
two protocol participants from being authorized at once, but the lock alone
is not the data fence. Network-partition fencing is proved by the frozen
failover voters.

# Decision

Before creating failover state, contenders run bounded health probes. A probe
has a monotonically increasing `probe_id`, the observed primary, the exact
durability-state ZK version, and every membership that may currently be
effective. Normally this is only stable `D`; during `source -> target` it is
both endpoints. The contender holds the failover coordinator lock for one
probe only. Every member of their union answers that exact `probe_id` with two
observations accumulated locally across normal iterations:

- the primary has been unreachable through PostgreSQL for at least
  `primary_unavailability_timeout`;
- the local replay position has not moved for at least the same interval.

Failover starts only after every relevant `D` has `Q(D)` replicas reporting
both conditions. The same fresh responses prove that every required read
quorum is currently available. A probe that does not collect all quorums
within its bounded round releases the manager lock. A later contender
increments `probe_id`, so responses from different moments cannot accumulate
across attempts. Therefore per-report TTL is unnecessary.

`min_failover_timeout` is checked before a probe is created. Timeline equality
is not an entry condition: votes carry their timeline and the election applies
the timeline fence. `last_primary_availability` and global `replics_info` are
not inputs to failover initialization.

One host holds the failover coordinator lock for the whole operation. Only
that coordinator may change the global phase, select the winner, finish the
operation, or clean its global metadata. If the coordinator dies, another host
may acquire the lock and resume the same persisted failover.

At initialization the coordinator freezes:

- an immutable `failover_version`, unique for this operation;
- the old primary timeline;
- the electorate copied from the union of relevant durability memberships
  with the failed primary removed.

Current HA membership and liveness changes never add voters to this electorate.
Required vote sets are derived separately for every frozen membership using
`Q(D)`.

If no stable durability membership exists, automatic and ordinary manual
failover are forbidden. Only an explicit operator request created with
`pgconsul-util failover --with-data-loss` freezes the current HA membership
instead. The data-safety argument below does not cover that operation.

Before publishing a vote, every participant performs the following ordered
operation:

1. disable archive restore and reload PostgreSQL;
2. clear `primary_conninfo`, reload PostgreSQL, and wait until
   `pg_stat_wal_receiver` confirms that walreceiver has stopped;
3. verify that the local timeline equals the frozen failover timeline;
4. use `lwaldump()` to scan local `pg_wal` from the replay position and read
   the end of its last valid WAL record;
5. atomically write one JSON vote containing `failover_version`, timeline,
   and flush LSN.

A vote from another failover version, another timeline, or a host outside the
frozen electorate is ignored. Equal LSNs are ordered by hostname only to make
the choice deterministic.

The winner acquires the primary lock and publishes only its versioned local
promotion result. The coordinator observes the lock and local result and is
the sole writer of `winner_selected -> promoting -> finished/failed`.

Leader ownership is materialized in the persistent `desired_primary` record.
After persisting the initial failover state, the coordinator CAS-writes
`desired_primary.hostname = null` with the failover operation ID. The old
primary observes this at the top of its iteration, closes the pooler, stops WAL
archiving, and releases the leader lock. It must not reacquire it. After the
election, the coordinator persists `election_winner`, CAS-writes that winner
into the same operation record, and only then advances the global phase. An
`AcquireLock` command validates both the desired hostname and operation ID;
promotion remains impossible until the winner actually owns the leader lock.

Participants keep archive restore disabled after voting. A losing replica first
tries to stream directly from the winner. Because restore remains fenced, this
attempt can receive WAL only from the winner. On success the replica starts
working immediately and then resumes archive restore.

After a failed direct switch, generic return-to-cluster fetches the winner's
timeline history and validates the complete ancestor chain. It also waits for
the old-timeline `.partial` WAL file containing the winner's forkpoint. The WAL check
is required because PostgreSQL gives history files priority in the archiver
queue, so history presence alone is not an archive-order barrier. If the local
timeline is absent from the chain, or its durable LSN is past its switchpoint,
the replica runs `pg_rewind`; otherwise it retries the direct switch. Missing
or invalid archive artifacts cause an indefinite safe wait.

This relies on the archive being append-only and ordered per timeline: once a
WAL segment is visible in S3, every preceding segment on that timeline is also
visible and immutable.  The history file does not establish this property,
because PostgreSQL may archive it ahead of the preceding `.partial` segment;
therefore the explicit fork-WAL check is the archive barrier.

## Data-safety argument

Assume `synchronous_commit=on`, an SSN ACK means that WAL was flushed on the
replica, all accepted votes have the frozen timeline and version, and the
operator did not explicitly request `--with-data-loss`.

For every commit acknowledged by the failed primary, an ACK set `A` under the
effective configuration `D` durably contains that commit. Without a
transition there is one such `D`. During a transition, failover conservatively
treats both source and target as possibly effective.

For every relevant `D`, failover collects at least `Q(D)` votes from
`R(D,p)`. The coordinator selects a stable/source candidate whose durable LSN
is not behind a read quorum of every relevant configuration. The ACK set for
the actually effective SSN intersects its corresponding dominated read quorum.
WAL on one timeline is prefix-ordered, so the winner contains the intersecting
voter's commit and therefore every acknowledged commit.

Collecting the endpoint quorums without testing the same candidate would not
be sufficient: their newest WAL could reside on different hosts. This is why
the candidate dominance predicate is evaluated independently for source and
target.

Before promotion the winner applies SSN derived from stable/source membership.
After writing its new timeline and before reporting `promoted`, it
CAS-discards the failed primary's unfinished transition while preserving that
membership. Any still desired membership change starts again under ordinary
reconciliation and performs a new service-table WAL barrier on the new primary.

Disabling archive restore and waiting for walreceiver to stop prevents new WAL
from arriving from any external source after fencing. Reading PostgreSQL's
durable position after fencing gives a lower bound on the WAL that promotion
can retain; it cannot hide an earlier acknowledged prefix.

The election cannot use `pg_last_wal_receive_lsn()` or
`pg_last_wal_replay_lsn()` instead. The receive position is lost with
walreceiver's shared-memory state on a PostgreSQL restart, while hot standby
can become queryable before replay reaches all WAL flushed before that restart.
Both values may therefore be behind an acknowledged commit that is still in
local `pg_wal`. `lwaldump()` recovers the endpoint from the valid on-disk WAL
records. Its failure is fatal to the vote; falling back to either SQL position
would invalidate the quorum-intersection proof.

Keeping restore disabled until streaming from the winner is established closes
the post-election fence for the fast path. The fallback path additionally waits
for the target history and the old-timeline WAL segment containing the fork.
Thus a loser cannot consume archived WAL from the old primary past the winner's
fork before it either follows the new timeline or is rewound.

Finally, promotion requires both the materialized authorization and the primary
lock. The failed primary is fenced when `desired_primary` is cleared; if it
cannot observe ZK, its session expires and its lock disappears. Thus two
primaries cannot legitimately accept writes under the same coordinator decision.

## Crash recovery

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

## Fail-closed conditions

Pgconsul must wait instead of claiming the safety guarantee when the electorate
is empty or fewer than `Q(D)` valid votes can be frozen, `lwaldump()` is
unavailable or cannot establish a voter's local WAL end, a vote has the wrong
version or no authorized branch can collect its read quorum, the winner cannot
apply its pre-promotion SSN, or the promotion timeline cannot be published or
verified.

# Alternatives

Let every participant update the global phase. This makes winner progress
direct but permits competing decisions and stale commands to advance a newer
failover.

Use the current alive-host list as the electorate. This can lower or change the
read quorum after the old primary has already acknowledged commits and breaks
the intersection proof.

Use PostgreSQL's receive or replay position instead of `lwaldump()`. This
avoids an extension dependency, but can understate local durable WAL after a
PostgreSQL restart and allow an older replica to win. This is unsafe.

# Consequences

Failover may sacrifice availability rather than omit a read quorum for either
endpoint of an unfinished durability transition or let a failed return proceed
before its archive barrier. Stale votes and participant results remain harmless
because they carry a different failover version. The protocol gains extra ZK
metadata for the frozen electorate, endpoint memberships, version, and
participant status.

Safe quorum failover also requires the `lwaldump` extension on every voter. A
missing extension or failed scan blocks failover instead of using a weaker LSN.

The protocol gains extra ZK metadata for the health-probe counter and reports,
the materialized desired primary, frozen electorate, version, and participant
status.

The proof does not apply when potential data loss is explicitly allowed,
`synchronous_commit` does not wait for replica flush, timeline fencing is
bypassed, or the old primary continues accepting writes after losing its ZK
lock.

# Links

- [Data-safety contract](../docs/DATA_SAFETY.md)
- ADR-0007: Failover state machine
- ADR-0009: Top-level blocking failover handler
- ADR-0012: Safe durability membership change
