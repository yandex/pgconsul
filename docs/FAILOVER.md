# Failover

Failover is coordinated through ZooKeeper and resumed one iteration at a time.
Its safety contract is defined by ADR-0013.

## Roles

- The host holding `epoch_manager` is the sole coordinator. It writes the
  global phase, freezes the electorate, selects the winner, and cleans up.
- Durability participants only publish versioned votes and local progress.
- The winner acquires the primary lock and promotes PostgreSQL, but does not
  change the global failover phase.

If the coordinator dies, another host acquires `epoch_manager` and resumes the
same `failover_version`.

## Frozen metadata

Before publishing `walreceiver_disabling`, the coordinator stores:

- `failover_version`: immutable ID of this failover;
- `failover_members`: stable `durability_members` without the failed primary;
- the old primary timeline, already stored in the cluster timeline node.

An asynchronous cluster may have no durability state. Failover is then allowed
only with `allow_potential_data_loss=yes`; its electorate is frozen from the HA
membership instead. This mode is outside the durability proof below.

The electorate never follows changes in alive or HA membership. Votes from
other hosts are ignored.

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
  "flush_lsn": 123456,
  "priority": 100
}
```

The coordinator waits for the durability read-quorum, not for all alive hosts.
It selects the greatest `(flush_lsn, priority)` vote.

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
walreceiver_disabling
  -> gates_passed
  -> registration
  -> voting
  -> winner_selected
  -> promoting
  -> finished | failed
```

- `walreceiver_disabling`: participants fence WAL sources and vote; the
  coordinator waits for the read-quorum.
- `gates_passed` / `registration`: persistent boundaries before selection.
- `voting`: the coordinator validates versioned votes and writes the winner.
- `winner_selected`: the winner acquires the primary lock; the coordinator
  observes it and advances the phase.
- `promoting`: the winner publishes a versioned `promoted` or `failed` local
  result; the coordinator advances the global phase.
- `finished` / `failed`: the coordinator stops timers and removes metadata.

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
| `failover_members` | Frozen durability electorate |
| `election_vote/<host>` | Atomic versioned vote JSON |
| `election_winner` | Selected host |
| `failover_participant/<host>` | Atomic versioned local progress |
| `failover_request` | Versioned operator request and optional selected winner |

Cleanup deletes the global phase last. Therefore an absent `failover_state` is
the only idle state.

## Implementation

| File | Responsibility |
|---|---|
| `src/failover/coordinator.py` | Global decisions and phase transitions |
| `src/failover/participant.py` | Vote, lock acquisition, promotion |
| `src/failover/types.py` | Immutable observation and phase types |
| `src/failover/machine.py` | Coordinator/participant routing |
| `src/command_executor.py` | PostgreSQL and ZooKeeper effects |
