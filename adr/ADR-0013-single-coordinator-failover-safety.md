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

# Decision

One host holds the failover coordinator lock for the whole operation. Only
that coordinator may change the global phase, select the winner, finish the
operation, or clean its global metadata. If the coordinator dies, another host
may acquire the lock and resume the same persisted failover.

At initialization the coordinator freezes:

- an immutable `failover_version`, unique for this operation;
- the old primary timeline;
- the electorate `R`, copied from stable `durability_members` with the failed
  primary removed.

Current HA membership and liveness changes never add voters to this electorate.
The required vote count is derived only from its frozen size using `Q(D)`.

If no stable durability membership exists, failover is forbidden unless
`allow_potential_data_loss=true`. In that explicitly unsafe mode, the
coordinator freezes the current HA membership instead. The data-safety argument
below does not cover this fallback.

Before publishing a vote, every participant performs the following ordered
operation:

1. disable archive restore and reload PostgreSQL;
2. clear `primary_conninfo`, reload PostgreSQL, and wait until
   `pg_stat_wal_receiver` confirms that walreceiver has stopped;
3. verify that the local timeline equals the frozen failover timeline;
4. read the greatest of PostgreSQL's last durably received and last replayed
   WAL positions;
5. atomically write one JSON vote containing `failover_version`, timeline,
   flush LSN, and priority.

A vote from another failover version, another timeline, or a host outside the
frozen electorate is ignored. Priority is considered only after LSN.

The winner acquires the primary lock and publishes only its versioned local
promotion result. The coordinator observes the lock and local result and is
the sole writer of `winner_selected -> promoting -> finished/failed`.

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
replica, all accepted votes have the frozen timeline and version, and
`allow_potential_data_loss=false`.

For every commit acknowledged by the failed primary, some SSN ACK set `A` of
size `W(D)` durably contains that commit. Failover proceeds only after a vote
set `V` of size `Q(D)` has been collected from the unchanged electorate.
Because `A` and `V` intersect, at least one voter contains the commit.

The coordinator selects the voter with the greatest flush LSN. WAL on one
timeline is prefix-ordered, so the winner is not behind the intersecting voter
and therefore contains every acknowledged commit.

ADR-0012 also guarantees the required cross-quorum intersection while an SSN
membership transition is incomplete. Consequently the stable ZK membership
used by failover is never weaker than the ACK guarantees of the actual SSN on
the failed primary.

Disabling archive restore and waiting for walreceiver to stop prevents new WAL
from arriving from any external source after fencing. Reading PostgreSQL's
durable position after fencing gives a lower bound on the WAL that promotion
can retain; it cannot hide an earlier acknowledged prefix.

Keeping restore disabled until streaming from the winner is established closes
the post-election fence for the fast path. The fallback path additionally waits
for the target history and the old-timeline WAL segment containing the fork.
Thus a loser cannot consume archived WAL from the old primary past the winner's
fork before it either follows the new timeline or is rewound.

Finally, promotion requires the primary lock. The failed primary has lost that
lock and must fence itself when its ZK session is unavailable, so two primaries
cannot legitimately accept writes under the same coordinator decision.

# Alternatives

Let every participant update the global phase. This makes winner progress
direct but permits competing decisions and stale commands to advance a newer
failover.

Use the current alive-host list as the electorate. This can lower or change the
read quorum after the old primary has already acknowledged commits and breaks
the intersection proof.

Keep `lwaldump`. The extension is unnecessary once external WAL sources are
fenced before PostgreSQL reports its durable receive/replay positions, and it
adds a crash-prone database dependency.

# Consequences

Failover may sacrifice availability rather than admit a voter outside the
stable durability membership or let a failed return proceed before its archive
barrier. Stale votes and participant results remain harmless because they carry
a different failover version. The protocol gains extra ZK metadata for the
frozen electorate, version, and participant status.

The proof does not apply when potential data loss is explicitly allowed,
`synchronous_commit` does not wait for replica flush, timeline fencing is
bypassed, or the old primary continues accepting writes after losing its ZK
lock.

# Links

- ADR-0007: Failover state machine
- ADR-0009: Top-level blocking failover handler
- ADR-0012: Safe durability membership change
