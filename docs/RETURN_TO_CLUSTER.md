# RETURN TO CLUSTER

Return-to-cluster re-attaches a host as a replica after failover, switchover,
source replacement, or a local PostgreSQL failure.

## Persistent local machine

`_return_to_cluster()` does not execute PostgreSQL commands. It writes
`return_to_cluster_state.json` under `local_state_directory`. On following
iterations an active return state claims the whole host-local iteration, so
the host cannot vote, coordinate another operation, acquire the primary lock,
or report itself ready before it is streaming again.

The state is local because only the local daemon executes it. Cluster
operations issue requests and publish their own acknowledgements through ZK.

| Phase | Meaning |
|-------|---------|
| `blocked` | Failover/switchover has reserved this host for handoff or promotion; normal iteration continues, but generic return is disabled |
| `requested` | A return to the versioned target was requested |
| `starting` | PostgreSQL was configured or restarted without rewind |
| `rewinding` | The next bounded step is a blocking `pg_rewind` |
| `starting_after_rewind` | Rewind completed and PostgreSQL start is monitored asynchronously |
| `resetup_required` | Automatic repair is exhausted; wait for external resetup |

Every state contains the target host, timeline, desired-primary operation ID,
start/rewind counters, and the last observed progress signature. State files
are atomically replaced and fsynced. A malformed file fails closed by creating
the rewind-failed flag and entering `resetup_required`.

The operation ID prevents an old failover or switchover from clearing a newer
state. If desired primary or timeline changes, the machine discards the old
attempt and starts a new `requested` epoch.

A versioned return request is accepted only after its target is materialized
as `desired_primary`. While failover has no winner, or while the requested host
belongs to an older primary epoch, no local return state is created.

## Blocking semantics

The machine is blocking only at the iteration-routing level. It performs one
bounded step and returns from `run_iteration()`:

- `pg_start` is launched asynchronously;
- recovery and streaming are observed on later iterations;
- `pg_rewind` remains a blocking external command;
- before `pg_rewind`, PostgreSQL must be confirmed stopped and in a terminal
  state, and `pg_status` must report that the server process is not running. A
  successful `pg_stop` exit code or unavailable SQL alone is insufficient.

A host releases failover and switchover manager locks when it accepts a return
request. Any healthy host can resume the idempotent cluster operation from ZK.

## Choosing direct attach or rewind

`ReturnObservation.build()` and the pure `decide_return_action()` function
still decide the data-safe action:

| Action | Meaning |
|--------|---------|
| `WAIT_HISTORY` | Wait for the target timeline history in the archive |
| `WAIT_ARCHIVE` | Wait for one fork WAL filename in the archive |
| `SIMPLE_SWITCH` | Point the replica at the target without rewind |
| `REWIND` | Rewind a former primary, a destructive local operation, or a divergent replica |

Former primaries are always rewound. For another replica, timeline history,
the local durable LSN, and the fork point determine whether a direct attach is
safe. Archive unavailability is not converted into resetup; the machine waits.

## Progress and retry policy

### SQL is available

The machine reads `pg_last_wal_replay_lsn()` through `get_replay_diff()`.
Every changed value refreshes the progress deadline. If the LSN does not move
for `return_lsn_stall_timeout` (default 60 seconds), the machine stops
PostgreSQL and proceeds to rewind.

### PostgreSQL is starting up

SQL may be unavailable before consistent recovery. Progress is the combination
of:

- recovery fields from `pg_controldata`;
- WAL filenames and offsets opened by the PostgreSQL startup process;
- startup-process `/proc/<pid>/io` read counters.

Any change refreshes the deadline. If the complete signature is unchanged for
`return_startup_stall_timeout` (default 300 seconds), the machine enters
`resetup_required`. This avoids both an infinite wait on impossible recovery
and false rewind while recovery is actively reading WAL.

### PostgreSQL is stopped

- If the cached replication source is still the target, start PostgreSQL up to
  `primary_switch_checks` times before rewind.
- If the target changed or the former role was primary, try rewind immediately.
- After a successful rewind, try PostgreSQL start several times. If it still
  cannot return, another rewind attempt is allowed.
- After `max_rewind_retries`, create `.pgconsul_rewind_fail.flag` and enter
  `resetup_required`.

Source/archive/ZK unavailability does not consume a rewind attempt. Only a
completed failed rewind does.

## External resetup

In `resetup_required`, pgconsul performs no repair or HA work while
`.pgconsul_rewind_fail.flag` exists. The external resetup process stops
pgconsul, replaces PGDATA, and removes that existing flag.

After the flag disappears, the machine resets its counters, reloads the
current desired primary, returns to `requested`, and validates that the rebuilt
instance becomes a streaming replica. The local state is removed only after
that validation. Removing the flag without fixing PGDATA merely causes repair
to fail again and recreate the flag.

## Failover and switchover integration

- A failover winner writes `blocked` before acquiring the leader lock and
  promoting. It clears the block only after becoming primary.
- Switchover P and C write `blocked` before committed handoff. C clears its
  block after promotion. P replaces its block with a return request only after
  the handoff and archive barrier permit return.
- Failover losers and replicas turned during switchover receive `requested`.
  They do not process the cluster operation while returning. Once streaming,
  local state is cleared; on the next iteration the operation handler writes
  its normal ACK.

## Source files

| File | Purpose |
|------|---------|
| `src/return_to_cluster/state.py` | Persistent state and atomic FS store |
| `src/return_to_cluster/types.py` | Timeline/archive observation |
| `src/return_to_cluster/machine.py` | Pure attach-versus-rewind decision |
| `src/main.py` | Iteration routing and imperative state transitions |
