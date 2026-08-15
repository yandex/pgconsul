# FAILOVER

Failover is the process of **unplanned** primary switching due to hardware
failure, networking loss on the current primary, or a PostgreSQL process
crash. Unlike [switchover](SWITCHOVER.md) (scheduled), failover is triggered
by the disappearance of the `leader` lock in ZK.

## Architecture overview (MDB-41951, ADR-0007, ADR-0006)

Failover is implemented as **two cooperating state machines** — a
**coordinator** and a **participant**. Every HA replica runs the participant
machine; the node that holds `ELECTION_MANAGER_LOCK_PATH` additionally runs
the coordinator machine. Both machines follow the "pure planner +
imperative shell" pattern (same as switchover, ADR-0006):

* All I/O (ZK reads, PostgreSQL queries) is concentrated in a single point —
  `FailoverObservation.build()` in `src/failover/types.py`.
* Handler methods (`plan_*`) are **pure functions**: they receive an immutable
  observation snapshot and return a **Plan** — an ordered list of commands.
* `CommandExecutor.run()` in `src/command_executor.py` executes the commands
  one by one, stopping on the first failure (fail-fast).

```
          +---------------------------------------------------+
          |  Pgconsul._run_failover_step()  (imperative shell)|
          |                                                   |
          |  1. db_state = db.get_state()                     |
          |  2. zk_state  = zk.get_state()                    |
          |  3. record    = FailoverRecord.from_zk_state()   |
          |  4. obs       = FailoverObservation.build(...)    |  <- ALL I/O HERE
          |  5. executor.run(coord_or_part_machine, obs)     |
          +---------------------------------------------------+
                                |
                                v
          +---------------------------------------------------+
          |  CommandExecutor.run(machine, obs)                |
          |                                                   |
          |  plan = machine.plan(obs)    <- pure function     |
          |  for cmd in plan:                                 |
          |      if not dispatch(cmd):  <- fail-fast          |
          |          return                                   |
          +---------------------------------------------------+
```

### Source files

| File | Purpose |
|------|---------|
| `src/failover/types.py` | Domain types: phases, `FailoverRecord`, `FailoverObservation`, `FailoverMachineConfig`, `_check_last_failover_time` |
| `src/failover/coordinator.py` | `FailoverCoordinatorMachine` — state machine on the coordinator side (holds `ELECTION_MANAGER_LOCK_PATH`) |
| `src/failover/participant.py` | `FailoverParticipantMachine` — state machine on every HA replica |
| `src/commands.py` | Command vocabulary (frozen dataclasses) and `Plan` type alias |
| `src/command_executor.py` | `CommandExecutor` — imperative shell that interprets Plans |

## Phases (`FailoverPhase`)

Phases are persisted in the ZK node `failover_state`:

| Phase | Value | Written by | Meaning |
|-------|-------|------------|---------|
| `DETECTED` | `detected` | `_run_failover_step` | Coordinator acquired election lock, starting failover |
| `WALRECEIVER_DISABLING` | `walreceiver_disabling` | Coordinator | Sleep + disable walreceiver (no gate recheck) |
| `GATES_PASSED` | `gates_passed` | Coordinator | Walreceiver disabled, ready to open voting |
| `REGISTRATION` | `registration` | Coordinator | Voting opened — participants write their votes |
| `VOTING` | `voting` | Coordinator | All alive hosts voted — tallying votes |
| `WINNER_SELECTED` | `winner_selected` | Coordinator | Winner written to ZK |
| `PROMOTING` | `promoting` | `_do_failover` / `_promote` | Winner is promoting (legacy phase) |
| `CHECKPOINTING` | `checkpointing` | `_promote` | Post-promote checkpoint (legacy phase) |
| `CREATING_SLOTS` | `creating_slots` | `_promote_handle_slots` | Creating replication slots (legacy phase) |
| `FINISHED` | `finished` | `_promote` / `ResetFailoverNode` | Failover complete |
| `FAILED` | `failed` | Coordinator | Gates/quorum/lock failed — reset + return-to-cluster |

> New phase values (`detected`, `walreceiver_disabling`, `gates_passed`,
> `registration`, `voting`, `winner_selected`, `failed`) are unrecognized by
> old pgconsul versions — this is an intentional fence against parallel
> failovers (ADR-0007 §5 — two-phase rollout, same technique as ADR-0005 §5).
> Existing values (`promoting`, `checkpointing`, `creating_slots`,
> `finished`) are written by the legacy `_do_failover`/`_promote` path and
> read by old pgconsul versions.

## How phase transitions work

### FailoverTransitionTo — the idempotency fence

A phase transition is a **command** (`FailoverTransitionTo`), not a direct
call. A handler includes it in the returned Plan:

```python
plan.append(FailoverTransitionTo(phase=FailoverPhase.WALRECEIVER_DISABLING))
```

`CommandExecutor` executes it via `_exec_failover_transition_to()`:

```python
def _exec_failover_transition_to(self, phase: FailoverPhase) -> bool:
    if not self._zk.write_failover_state(phase):
        return False
    log_event(f'FAILOVER PHASE -> {phase}', level='warning')
    return True
```

The phase is written to ZK **before** the actual action. This is the
"idempotency fence" (ADR-0007 §2): if pgconsul crashes (kill -9, OOM,
restart) mid-Plan, on the next startup the machine reads the already-updated
phase from ZK and resumes from it, rather than repeating the action from the
previous phase.

### Empty Plan = "wait"

If a handler returns an empty Plan (`[]`), it means "nothing to do yet" —
not all hosts have voted, the winner hasn't acquired the lock, etc. The
iteration is not "consumed" and the machine retries on the next cycle.

### Fail-fast

If any command fails, `CommandExecutor` stops executing the remaining
commands. The iteration is "consumed" and the machine retries on the next
cycle with a fresh observation. Because the phase is already in ZK, the
handler re-derives the same Plan idempotently.

## The failover process

Failover starts when the `leader` lock (`PRIMARY_LOCK_PATH`) in ZK
disappears. This can happen in the following cases:

* Real network loss / disconnection from ZK
* PgConsul releases the lock itself if the local Postgres is inoperable
  (see `release_lock`, `release_lock_and_return_to_cluster`)

One (or more) replicas detect the loss of the lock holder and the failover
process begins via `_run_failover_step()` in `src/main.py`.

### Startup: becoming the coordinator

`_run_failover_step()` checks the current `failover_state`:

* If the phase is `None` or `FINISHED` — no active failover. The node tries
  to become the coordinator via `_try_become_failover_coordinator()`:
  acquires `ELECTION_ENTER_LOCK_PATH`, checks that `PRIMARY_LOCK_PATH` is
  still unheld, writes `cleanup` election status, and acquires
  `ELECTION_MANAGER_LOCK_PATH`. On success, writes `detected` to
  `failover_state` to start the process.
* If the phase is active but no coordinator holds the lock (e.g. after
  restart) — tries to re-acquire `ELECTION_MANAGER_LOCK_PATH` to resume.
* Otherwise — builds an observation and delegates one step to the
  coordinator or participant machine.

### Machine selection

> Simplified pseudocode — see `_run_failover_step` in `src/main.py` for the
> exact implementation.

```python
_winner_is_coord = obs.is_coordinator and obs.election_winner == obs.my_hostname
_cleanup_phase = obs.record.phase in (FINISHED, FAILED)
if obs.is_coordinator and not (_winner_is_coord and not _cleanup_phase):
    return executor.run(coordinator_machine, obs)
return executor.run(participant_machine, obs)
```

The coordinator normally drives the phases. But if the coordinator **is**
the winner, it must run the participant machine (to acquire the lock and
promote) — otherwise the winner never acquires the lock and failover stalls.
The exception is `FINISHED`/`FAILED`: the coordinator must release the
election manager lock and reset the failover node.

### Phase 1: DETECTED → WALRECEIVER_DISABLING (Coordinator)

`plan_detected()` runs the gate checks (`_gates_pass`). On failure → empty
Plan (retry next iteration). If no alive hosts at all →
`TransitionTo(FAILED)`. On success:

```python
[StartTimer('failover'), StartTimer('downtime'),
 FailoverTransitionTo(WALRECEIVER_DISABLING)]
```

**Gates** (`_gates_pass`, pure predicates over the observation):

* `autofailover` is enabled (or `switchover_in_progress`)
* Timeline sync: `zk_timeline == local_timeline`
* Last failover was long enough ago (`min_failover_timeout`)
* Primary is unreachable via libpq (skipped on `switchover_in_progress`)
* Primary unavailability timeout elapsed (`primary_unavailability_timeout`)
* Not replaying WAL
* `replics_info` is available
* Alive hosts are present
* Promote-safe: enough alive hosts for quorum (unless `allow_data_loss`)

> Gates are checked **once** in `plan_detected`. `plan_walreceiver_disabling`
> runs unconditionally without re-checking gates — this prevents the
> "primary returned" deadlock where `is_primary_unreachable=False` caused
> `plan_detected` to return `[]` forever.

### Phase 2: WALRECEIVER_DISABLING → GATES_PASSED (Coordinator)

`plan_walreceiver_disabling()`:

```python
[Sleep(sleep_before_disable_walreceiver),   # debug-only, optional
 DisableWalReceiver(timeout=walreceiver_disable_timeout),
 FailoverTransitionTo(GATES_PASSED)]
```

Disabling walreceiver **before** voting ensures the old primary can no longer
get a synchronous write acknowledged (MDB-41951). The LSN for voting is read
via `get_wal_receive_lsn()` which falls back to `pg_last_wal_receive_lsn()`
when `lwaldump()` crashes after disable (see `src/pg.py`, `get_wal_receive_lsn`:
on `use_lwaldump=True` it catches `PostgresConnectionError` from `lwaldump()`,
reconnects, and reads `pg_last_wal_receive_lsn()`).

**Participant** (`plan_walreceiver_disabling`): same `Sleep` +
`DisableWalReceiver`, but **without** `FailoverTransitionTo` — the
coordinator owns phase transitions.

### Phase 3: GATES_PASSED → REGISTRATION (Coordinator)

`plan_gates_passed()`:

```python
[CleanupVotes(),
 WriteElectionStatus(status='registration'),
 FailoverTransitionTo(REGISTRATION),
 WriteElectionVote(lsn=host_lsn, priority=host_priority)]  # coordinator votes too
```

### Phase 4: REGISTRATION → VOTING (Coordinator)

`plan_registration()` waits for all alive HA hosts to vote
(`_all_alive_voted`). Empty Plan if not all voted (retry next iteration).
When all voted:

```python
[WriteElectionStatus(status='selection'),
 FailoverTransitionTo(VOTING)]
```

**Participant** (`plan_vote`): writes its vote (idempotent). Empty Plan if
`host_lsn` is unavailable (PG dead — retry next iteration):

```python
[WriteElectionVote(lsn=host_lsn, priority=host_priority)]
```

### Phase 5: VOTING → WINNER_SELECTED (Coordinator)

`plan_voting()` checks quorum (`_is_election_valid` — actual votes, not
just alive hosts). `TransitionTo(FAILED)` if quorum not met or no winner
determined. Otherwise:

```python
[WriteElectionWinner(winner=winner),
 WriteElectionStatus(status='done'),
 FailoverTransitionTo(WINNER_SELECTED)]
```

The winner is determined by `_determine_winner()`: highest `(lsn, priority)`
tuple among all votes.

### Phase 6: WINNER_SELECTED → PROMOTING

**Coordinator** (`plan_winner_selected`): waits for the winner to acquire
the primary lock. Empty Plan until `lock_holder` is not None, then:

```python
[FailoverTransitionTo(PROMOTING)]
```

**Participant — winner** (`plan_winner_selected`): acquires the lock
non-blocking and transitions to `PROMOTING`:

```python
[AcquireLock(timeout=0),
 FailoverTransitionTo(PROMOTING)]
```

`AcquireLock(timeout=0)` is non-blocking. If the lock is already held by us
(previous attempt failed mid-way), it succeeds immediately and
`plan_promoting` retries `DoFailover` (idempotent via `delete_failover_state`).

Safety: if `is_replaying_wal` — empty Plan (wait).

**Participant — loser** (`_plan_loser`): emit event log; the shell delegates
to `ReturnToClusterMachine`:

```python
[Log('FAILOVER: winner is {winner}, returning to cluster', event=True)]
```

### Phase 7: PROMOTING / CHECKPOINTING / CREATING_SLOTS (Participant — winner)

`plan_promoting()` (winner): `DoFailover` is **opaque** — it delegates to
`_do_failover()` which starts with `delete_failover_state`, making it safe to
retry. The executor releases the lock on failure (fail-fast → retry next
iteration).

```python
[DoFailover(old_primary=None),
 WriteLastFailoverTime(),
 StopTimer('failover')]
```

`_do_failover()` / `_promote()` writes the legacy phases in sequence:

| Sub-phase | Written by | Action |
|-----------|-----------|--------|
| `creating_slots` | `_promote_handle_slots` | Create replication slots for HA hosts |
| `promoting` | `_promote` | `pg_ctl promote` |
| `checkpointing` | `_promote` | Post-promote checkpoint |
| `finished` | `_promote` | Failover complete |

**Coordinator** (`plan_promoting` / `plan_checkpointing` /
`plan_creating_slots`): empty Plan — waits for the winner to finish.

### Phase 8: FINISHED — coordinator cleanup

`plan_finished()` (coordinator): releases the election manager lock and
resets the failover node:

```python
[Log('FAILOVER: finished, coordinator releasing election lock', event=True),
 ReleaseLock(),
 ResetFailoverNode()]
```

**Participant — winner** (`plan_finished`): empty Plan (already promoted).

**Participant — loser** (`plan_finished`): delegates to
`ReturnToClusterMachine`.

### Phase FAILED — abort

`plan_failed()` (coordinator): releases the election lock and resets:

```python
[Log('FAILOVER: coordinator failed, resetting', event=True),
 ReleaseLock(),
 ResetFailoverNode()]
```

**Participant** (`plan_failed`): emit event log; the shell handles reset +
return-to-cluster:

```python
[Log('FAILOVER: election failed, returning to cluster', event=True)]
```

## Transition diagram

```
                +-------------------------------------------------+
                |  leader lock disappears (PRIMARY_LOCK_PATH)     |
                +-----------------------+-------------------------+
                                        |
                                        v
                +-------------------------------------------+
                |  _try_become_failover_coordinator()       |
                |  acquire ELECTION_MANAGER_LOCK_PATH       |
                |  write 'detected' to failover_state       |
                +-----------------------+-------------------+
                                        |
                                        v
                  +-----------+   plan_detected (Coordinator)
                  |  DETECTED |   gates pass? -> WALRECEIVER_DISABLING
                  +-----+-----+   gates fail? -> [] (retry)
                        |         no alive hosts? -> FAILED
                        v
              +----------------------+
              | WALRECEIVER_DISABLING|  Coordinator + Participant:
              +----------+-----------+  Sleep + DisableWalReceiver
                         |               (coordinator -> GATES_PASSED)
                         v
                  +--------------+
                  | GATES_PASSED |  Coordinator: CleanupVotes +
                  +------+-------+  WriteElectionStatus(registration)
                         |          + vote -> REGISTRATION
                         v
                  +---------------+
                  |  REGISTRATION |  Coordinator: wait for all alive
                  +------+--------+  hosts to vote -> VOTING
                         |
                         v
                  +----------+        Coordinator: tally votes,
                  |  VOTING  |-------> check quorum, write winner
                  +-----+----+        -> WINNER_SELECTED (or FAILED)
                        |
                        v
                +----------------+    Coordinator: wait for lock_holder
                | WINNER_SELECTED|-----> -> PROMOTING
                +-------+--------+
                        |
             +----------+----------+
             |                     |
        Winner (Participant)   Loser (Participant)
        AcquireLock(timeout=0)  Log -> ReturnToClusterMachine
        -> PROMOTING
             |                     |
             v                     v
        +-----------+        (return to cluster as replica)
        | PROMOTING |    Coordinator: wait for winner
        +-----+-----+
              |  DoFailover (opaque -> _do_failover)
              v
        +----------------+   _promote_handle_slots
        | CREATING_SLOTS |-------> creating slots
        +-------+--------+
                |  _promote
                v
        +--------------+   pg_ctl promote
        |  PROMOTING   |-------->
        +------+-------+
               |  _promote
               v
        +---------------+  checkpoint
        | CHECKPOINTING |-+
        +-------+-------+ |
                |         |  _promote
                v         v
           +-----------+
           |  FINISHED |  Coordinator: ReleaseLock + ResetFailoverNode
           +-----------+  Loser: ReturnToClusterMachine

           Any phase ---> FAILED (quorum not met / no winner / no alive hosts)
                         Coordinator: ReleaseLock + ResetFailoverNode
                         Participant: Log -> ReturnToClusterMachine
```

## Idempotency guarantees

| Mechanism | Where | What it provides |
|-----------|-------|-----------------|
| Phase in ZK before action (fence) | `FailoverTransitionTo` at the start of Plan | Restart -> resume from recorded phase, not repeat action |
| Empty Plan = "wait" | `plan_*` return `[]` | Doesn't consume iteration, retries next cycle |
| Fail-fast | `CommandExecutor.run` | Command failure -> stop, retry; doesn't execute half a Plan |
| Idempotent commands | `StartTimer` (skip if started), `WriteElectionVote`, `CleanupVotes` | Safe repeat on restart |
| Non-blocking lock | `AcquireLock(timeout=0)` | Lock held -> fail-fast -> retry, no hang |
| `DoFailover` idempotent | `_do_failover` starts with `delete_failover_state` | Safe to retry promote on restart |
| Coordinator resume | `_run_failover_step` re-acquires `ELECTION_MANAGER_LOCK_PATH` | Coordinator crash -> another node resumes coordination |
| Winner-is-coordinator routing | `_run_failover_step` machine selection | Coordinator that is also the winner runs participant plan (acquire lock + promote) |
| New phase values | `detected`, `gates_passed`, `voting`, `winner_selected`, `failed` | Old pgconsul versions don't recognize them -> no parallel failovers |

## Entry points from `main.py`

The failover machines are driven from `_run_failover_step()` in `src/main.py`,
which is called from:

1. **`replica_iter()`** — the main entry point. When `holder is None` (no
   primary lock holder) → `_run_failover_step()`. Also when failover is
   active (`promoting`/`checkpointing`/`creating_slots`) and this node holds
   the lock → drives the participant machine to finish the promote. Fallback
   from failed switchover → `_run_failover_step(switchover_in_progress=True)`.

2. **`dead_iter()`** — when PostgreSQL is dead. Releases the primary lock
   if held, then delegates to `_return_to_cluster()` (which uses
   `ReturnToClusterMachine`). If a switchover is active and this host is the
   old primary, runs the switchover state machine instead (to advance
   `pg_stopped` → `primary_shut`).

3. **Switchover fallback** — `replica_iter()` falls back to failover when a
   switchover has failed (`is_failed()`) and there is no primary lock holder,
   so the cluster recovers a primary instead of waiting forever (MDB-41951).

## Scenarios

### Scenario 1: Normal failover (replica wins)

1. Primary dies → `leader` lock disappears
2. Replica detects `holder is None` → `_run_failover_step()`
3. No active failover → `_try_become_failover_coordinator()` → acquires
   `ELECTION_MANAGER_LOCK_PATH`, writes `detected`
4. `plan_detected` → gates pass → `WALRECEIVER_DISABLING`
5. `plan_walreceiver_disabling` → `DisableWalReceiver` → `GATES_PASSED`
6. `plan_gates_passed` → `CleanupVotes` + open registration → `REGISTRATION`
7. All replicas vote (participant `plan_vote`) → coordinator `plan_registration`
   → `VOTING`
8. `plan_voting` → quorum met, winner selected → `WINNER_SELECTED`
9. Winner (participant) → `AcquireLock(timeout=0)` → `PROMOTING`
10. Winner (participant) → `DoFailover` → `creating_slots` → `promoting` →
    `checkpointing` → `finished`
11. Coordinator `plan_finished` → `ReleaseLock` + `ResetFailoverNode`
12. Losers → `ReturnToClusterMachine` (re-attach as replicas)

### Scenario 2: Coordinator is the winner

1. Steps 1–8 as above
2. The coordinator is also the election winner
3. `_run_failover_step` detects `winner_is_coord` → runs **participant**
   machine (not coordinator) for `WINNER_SELECTED`
4. Participant `plan_winner_selected` → `AcquireLock` → `PROMOTING` →
   `DoFailover` → `FINISHED`
5. On `FINISHED` → coordinator machine runs `plan_finished` (release lock +
   reset), because `_cleanup_phase` is True

### Scenario 3: Failover fails (quorum not met)

1. Primary dies → `detected` → `WALRECEIVER_DISABLING` → `GATES_PASSED` →
   `REGISTRATION` → `VOTING`
2. `plan_voting` → `_is_election_valid` returns False (not enough votes)
3. `TransitionTo(FAILED)`
4. Coordinator `plan_failed` → `ReleaseLock` + `ResetFailoverNode`
5. Participant `plan_failed` → Log → shell delegates to
   `ReturnToClusterMachine`

### Scenario 4: Coordinator crash mid-failover

1. Coordinator crashes after writing `REGISTRATION` to ZK
2. On restart (or another node): `_run_failover_step` sees active phase but
   no coordinator holds `ELECTION_MANAGER_LOCK_PATH`
3. `_try_become_failover_coordinator()` → re-acquires the lock, resumes
   coordination from `REGISTRATION`
4. Process continues from the recorded phase — no action repeated
