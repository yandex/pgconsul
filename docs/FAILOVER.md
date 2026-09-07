# Failover

Failover is coordinated through ZooKeeper and resumed one iteration at a time.
Its safety contract is defined by ADR-0013.

## Roles

- The host holding `epoch_manager` is the sole coordinator. It writes the
  global phase, freezes the electorate, selects the winner, and cleans up.
- Durability participants only publish versioned votes and local progress.
- The top-level primary-ownership reconciler grants the winner the primary
  lock from `desired_primary`; the participant then promotes PostgreSQL but
  does not change the global failover phase.

If the coordinator dies, another host acquires `epoch_manager` and resumes the
same `failover_version`.

## Frozen metadata

Before publishing `voting`, the coordinator stores:

- `failover_version`: immutable ID of this failover;
- the old primary timeline, already stored in the cluster timeline node.

For a safe failover, the coordinator CAS-writes the unchanged
`durability_members` state before publishing the phase. That version bump
fences a stale primary writer. The electorate is then derived from that frozen
state: every member of every active durability endpoint except the failed
primary. No separate electorate node is stored.

An asynchronous cluster may have no durability state. Automatic and ordinary
operator-initiated failover stop in that case. Only an explicit
`pgconsul-util failover --with-data-loss` stores its frozen electorate in the
versioned request itself, because there may be no durability state. This one
operation is outside the durability proof below.

The electorate never follows changes in alive or HA membership. Votes from
other hosts are ignored.

For the durable-membership transition that defines all possible voting
quorums, see [Durability membership changes](DURABILITY.md).

## Voting

Every electorate member repeatedly executes one idempotent command:

1. set `restore_command` to the disabled command and reload;
2. clear `primary_conninfo`, reload, and wait for walreceiver to disappear;
3. verify the failover timeline;
4. use `lwaldump()` to read the durable endpoint from local `pg_wal`;
5. atomically publish:

```json
{
  "failover_version": "d7c...",
  "timeline": 42,
  "flush_lsn": 123456
}
```

The coordinator waits for the durability read-quorum, not for all alive hosts.
It selects the greatest `flush_lsn` vote; hostname breaks an LSN tie.

## Operator-initiated failover

`pgconsul-util failover` writes a versioned request. A replica that acquires
`epoch_manager` turns it into the normal failover state; the CLI never writes
the global phase or election winner directly.

`pgconsul-util failover --with-data-loss` collects the votes available before
`--timeout`, prints them in descending timeline/LSN order, and asks for a
winner. Empty input selects the freshest LSN on the highest voted timeline;
`--yes` selects that default without prompting. The diagnostic marks the
selection safe only if the ordinary timeline, membership, read-quorum, and LSN
dominance checks all pass. The explicit selection is honored even when they do
not pass, so an `UNSAFE` result is outside pgconsul's data-safety guarantee.
If the CLI exits before storing the winner, repeating the same command resumes
the existing request and vote collection.

By default data-loss voting still disables `restore_command` and walreceiver.
`--no-wal-fencing` leaves both sources enabled. The CLI marks every vote as
unfenced, prints a warning, and always reports the selected host as unsafe
because the displayed positions can continue to move.

## Phases

```text
 [IDLE]
   |
   | coordinator acquires epoch_manager, freezes version and electorate
   v
 [VOTING] -- participants fence restore/WAL receiver and publish votes
   |  |       coordinator finds a candidate safe for every required quorum
   |  | failover_timeout
   |  v
   | [CLEANUP] (records failed-failover cooldown)
   v
 [PROMOTING] -- winner waits for primary ownership and promotes
   |  |           local promotion failure or promote_timeout
   |  v
   | [RESOLVING_WINNER] -- winner resolves primary ownership
   |  |                         | cannot complete promotion
   |  | promotion completed    v
   |  +------------------> [CLEANUP]
   v
 [CLEANUP] --> [IDLE]

 Any `wait` condition is a self-loop: no phase changes and the next iteration
 retries from the persisted state.
```

- `voting`: participants fence WAL sources and publish versioned votes. The
  coordinator waits up to `failover_timeout`, validates the read quorums and
  writes the winner.
- An election timeout or a winner that cannot promote records a separate
  failed-failover timestamp. Automatic retry waits for
  `failed_failover_cooldown`; a manual failover request can proceed at once.
- `promoting`: the top-level reconciler grants the winner the primary lock.
  The winner resumes its local promotion group and publishes a versioned
  `promoted` or `failed` result.
- `resolving_winner`: a timed-out or failed winner must either complete
  promotion or release primary ownership before cleanup.
- `cleanup`: the coordinator removes timing records, then failover metadata and
  releases its lock. A failed
  failover also removes its unmaterialized `desired_primary`, allowing the old
  primary to rejoin.

A switchover candidate that confirms its committed promotion concurrently
with recovery failover may move failover directly to `cleanup`. At that point
the candidate already owns the primary lock, so another promotion must stop.

Promotion substeps remain persisted locally in `failover_participant` state.

## Returning participants

Archive restore remains disabled after voting. A losing replica first tries to
stream directly from the winner. On success it can work immediately and then
resume archive restore.

Only a failed direct switch waits for the archive. It fetches the winner's
timeline history and the old-timeline `.partial` WAL file containing the
forkpoint. The WAL file is a necessary second barrier because PostgreSQL gives
history files priority in the archiver queue. The replica then retries a direct
switch if its durable LSN is not past the forkpoint, or runs `pg_rewind`
otherwise. Missing archive files cause an indefinite safe wait.

## ZooKeeper nodes

| Node | Purpose |
|---|---|
| `failover_state` | Global phase, written only by the coordinator |
| `epoch_manager` | Coordinator lock |
| `failover_version` | Immutable operation ID |
| `election_vote/<host>` | Atomic versioned vote JSON |
| `election_winner` | Selected host |
| `failover_participant/<host>` | Atomic versioned local progress |
| `failover_request` | Versioned operator request, optional selected winner, and electorate for `--with-data-loss` |

Cleanup keeps the `cleanup` phase until all ordinary metadata is removed, then
deletes the phase and releases `epoch_manager`. If the latter release is
interrupted, its local holder releases the orphaned lock on the next iteration.

## Implementation

| File | Responsibility |
|---|---|
| `src/failover/coordinator.py` | Global decisions and phase transitions |
| `src/failover/participant.py` | Vote and promotion |
| `src/failover/types.py` | Immutable observation and phase types |
| `src/main.py` | Coordinator/participant dispatch and primary-lock reconciliation |
| `src/command_executor.py` | PostgreSQL and ZooKeeper effects |
