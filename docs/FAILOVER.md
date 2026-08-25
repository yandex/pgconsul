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
* `handle_failover()` runs before role-based dispatch. Any persistent
  failover phase claims the whole iteration (ADR-0009).

```
          +---------------------------------------------------+
          |  Pgconsul._run_failover_step()  (imperative shell)|
          |                                                   |
          |  1. db_state = db.get_state()                     |
          |  2. zk_state  = zk.get_state()                    |
          |  3. phase     = FailoverPhase.from_str()         |
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
| `src/failover/types.py` | Domain types: phases, record, observation and machine config |
| `src/failover/machine.py` | `FailoverMachine` — state-machine entry point and side routing |
| `src/failover/coordinator.py` | `FailoverCoordinatorMachine` — state machine on the coordinator side (holds `ELECTION_MANAGER_LOCK_PATH`) |
| `src/failover/participant.py` | `FailoverParticipantMachine` — state machine on every HA replica |
| `src/commands.py` | Command vocabulary (frozen dataclasses) and `Plan` type alias |
| `src/command_executor.py` | `CommandExecutor` — imperative shell that interprets Plans |

## Phases (`FailoverPhase`)

Phases are persisted in the ZK node `failover_state`:

| Phase | Value | Written by | Meaning |
|-------|-------|------------|---------|
| `WALRECEIVER_DISABLING` | `walreceiver_disabling` | Coordinator | Sleep + disable walreceiver (no gate recheck) |
| `GATES_PASSED` | `gates_passed` | Coordinator | Walreceiver disabled, ready to open voting |
| `REGISTRATION` | `registration` | Coordinator | Voting opened — participants write their votes |
| `VOTING` | `voting` | Coordinator | All alive hosts voted — tallying votes |
| `WINNER_SELECTED` | `winner_selected` | Coordinator | Winner written to ZK |
| `PROMOTING` | `promoting` | Participant | Winner is running its local promotion groups |
| `FINISHED` | `finished` | Participant | Promotion complete; coordinator cleanup is pending |
| `FAILED` | `failed` | Coordinator | Failed operation; coordinator cleanup is pending |

`None` is the only idle state. `FINISHED` and `FAILED` remain blocking until
`CleanupFailover` removes the failover metadata.

Winner-only command groups are persisted in
`<local_state_directory>/failover_participant_state.json`.

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
failover handler still consumes the iteration and retries on the next cycle.

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

Failover initialization:

* An HA replica without a primary lock holder tries to acquire
  `ELECTION_MANAGER_LOCK_PATH`, rechecks that `PRIMARY_LOCK_PATH` is still
  unheld, evaluates the entry gates and writes `walreceiver_disabling`.
* If the phase is active but no coordinator holds the lock (e.g. after
  restart) — tries to re-acquire `ELECTION_MANAGER_LOCK_PATH` to resume.
* Switchover fallback calls the same initializer explicitly; failover never
  reads switchover metadata.

### Machine selection

`FailoverMachine` owns coordinator/participant routing:

```python
cleanup = obs.must_reset or obs.phase in (FINISHED, FAILED)
if obs.is_coordinator and (cleanup or obs.election_winner != obs.my_hostname):
    return coordinator.plan(obs)
return participant.plan(obs)
```

The coordinator normally drives the phases. But if the coordinator **is**
the winner, it must run the participant machine (to acquire the lock and
promote) — otherwise the winner never acquires the lock and failover stalls.
The exception is `FINISHED`/`FAILED`: only the coordinator runs terminal
cleanup; all participants wait.

### Entry gates and WALRECEIVER_DISABLING

Entry gates run before the first persistent phase is written. Once
`walreceiver_disabling` exists, the operation is committed and the phase runs
without rechecking them.

**Gates** (`_gates_pass`, pure predicates over the observation):

* `autofailover` is enabled
* Timeline sync: `zk_timeline == local_timeline`
* Last failover was long enough ago (`min_failover_timeout`)
* Primary is unreachable via libpq
* Primary unavailability timeout elapsed (`primary_unavailability_timeout`)
* Not replaying WAL
* `replics_info` is available
* Alive hosts are present
* Promote-safe: enough alive hosts for quorum (unless `allow_data_loss`)

> The `autofailover` and libpq gates are disabled only when switchover explicitly
> requests fallback initialization. `plan_walreceiver_disabling`
> runs unconditionally without re-checking gates — this prevents the
> operation from backing out after it crossed its persistent entry boundary.

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
 FailoverTransitionTo(REGISTRATION),
 WriteElectionVote(lsn=host_lsn, priority=host_priority)]  # coordinator votes too
```

### Phase 4: REGISTRATION → VOTING (Coordinator)

`plan_registration()` waits for all alive HA hosts to vote
(`_all_alive_voted`). Empty Plan if not all voted (retry next iteration).
When all voted:

```python
[FailoverTransitionTo(VOTING)]
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
[ClearLocalState('failover_participant'), AcquireLock(timeout=0),
 FailoverTransitionTo(PROMOTING)]
```

`AcquireLock(timeout=0)` is non-blocking. `plan_promoting` reacquires it after
a restart and resumes the local command group.

Safety: if `is_replaying_wal` — empty Plan (wait).

**Participant — loser** (`_plan_loser`): emit an event and wait while the
global failover is active:

```python
[Log('FAILOVER: winner is {winner}, returning to cluster', event=True)]
```

### Phase 7: PROMOTING (Participant — winner)

`plan_promoting()` keeps one aggregate phase in ZK and delegates detailed
progress to the winner's local filesystem.

```python
[AcquireLock(timeout=0), Promote(scope='failover_participant'),
 WriteLastFailoverTime(),
 StopTimer('failover'), FailoverTransitionTo(FINISHED)]
```

`_do_failover()` persists the existing command groups locally:

| Local group | Action |
|-------------|--------|
| `creating_slots` | Resume WAL, create slots, configure SSN |
| `promoting` | Run `pg_ctl promote` if PostgreSQL is not already primary |
| `checkpointing` | Checkpoint, write timeline, leave sync group, update quorum |

**Coordinator** (`plan_promoting`): empty Plan — waits for the winner.

### Phase 8: FINISHED — coordinator cleanup

`plan_finished()` stops remaining timers and runs terminal cleanup:

```python
[Log('FAILOVER: finished, cleaning up', event=True),
 StopTimer(...), CleanupFailover()]
```

**Participant — winner** (`plan_finished`): empty Plan (already promoted).

**Participant — loser** (`plan_finished`): waits for cleanup.

`CleanupFailover` deletes votes, the election winner, and
`failover_state`, then releases `ELECTION_MANAGER_LOCK_PATH`. It never
releases the winner's primary leader lock.

If cleanup was interrupted, `failover_must_be_reset` is included in the next
`FailoverObservation`. The coordinator machine emits `CleanupFailover`
again; `main.py` does not perform cleanup outside the machine.

### Phase FAILED — abort

`plan_failed()` (coordinator) runs the same terminal cleanup:

```python
[Log('FAILOVER: coordinator failed, cleaning up', event=True),
 StopTimer(...), CleanupFailover()]
```

**Participant** (`plan_failed`): emit an event and wait for coordinator
cleanup:

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
                |  _try_acquire_failover_coordinator()      |
                |  acquire ELECTION_MANAGER_LOCK_PATH       |
                |  entry gates; write WALRECEIVER_DISABLING |
                +-----------------------+-------------------+
                                        |
                                        v
              +----------------------+
              | WALRECEIVER_DISABLING|  Coordinator + Participant:
              +----------+-----------+  Sleep + DisableWalReceiver
                         |               (coordinator -> GATES_PASSED)
                         v
                  +--------------+
                  | GATES_PASSED |  Coordinator: CleanupVotes +
                  +------+-------+  vote -> REGISTRATION
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
        AcquireLock(timeout=0)  Log + wait
        -> PROMOTING
             |                     |
             v
        +-----------+
        | PROMOTING |    Coordinator: wait for winner
        +-----+-----+
              |  Promote
              v
        +----------------+   local filesystem
        | creating_slots |-------> slots + SSN
        +-------+--------+
                |  _promote
                v
        +--------------+   pg_ctl promote
        |  promoting   |-------->
        +------+-------+
               |  _promote
               v
        +---------------+  checkpoint
        | checkpointing |-+
        +-------+-------+ |
                |         |  _promote
                v         v
           +-----------+
           |  FINISHED |  Coordinator: CleanupFailover
           +-----------+  -> failover_state is absent

           Any phase ---> FAILED (quorum not met / no winner / no alive hosts)
                         Coordinator: CleanupFailover
                         Participant: Log + wait
```

## Idempotency guarantees

| Mechanism | Where | What it provides |
|-----------|-------|-----------------|
| Phase in ZK before action (fence) | `FailoverTransitionTo` at the start of Plan | Restart -> resume from recorded phase, not repeat action |
| Empty Plan = "wait" | `plan_*` return `[]` | Consumes the iteration without commands, retries next cycle |
| Fail-fast | `CommandExecutor.run` | Command failure -> stop, retry; doesn't execute half a Plan |
| Idempotent commands | `StartTimer` (skip if started), `WriteElectionVote`, `CleanupVotes` | Safe repeat on restart |
| Non-blocking lock | `AcquireLock(timeout=0)` | Lock held -> fail-fast -> retry, no hang |
| Local promotion groups | `<local_state_directory>/failover_participant_state.json` | Retry the current group after restart |
| Coordinator resume | `_run_failover_step` re-acquires `ELECTION_MANAGER_LOCK_PATH` | Coordinator crash -> another node resumes coordination |
| Winner-is-coordinator routing | `FailoverMachine` | Coordinator that is also the winner runs participant plan (acquire lock + promote) |

## Entry point from `main.py`

`run_iteration()` calls `handle_failover()` after collecting the common DB/ZK
snapshot, writing service nodes, and checking maintenance, but before
role-based dispatch. Maintenance blocks failover initiation, resumption, and
cleanup; persistent progress resumes after maintenance is disabled. The
handler:

1. resumes any persistent phase regardless of the current PostgreSQL role;
2. performs terminal cleanup for `FINISHED`, `FAILED`, or the reset marker;
3. detects a missing primary lock on an HA replica and starts failover;
4. accepts explicit fallback initialization from the switchover machine.

Returning `True` means failover owns the iteration, including empty plans and
failed commands. `primary_iter()`, `replica_iter()`, and `dead_iter()` do not
drive failover.

## Scenarios

### Scenario 1: Normal failover (replica wins)

1. Primary dies → `leader` lock disappears
2. Top-level `handle_failover()` detects `holder is None`
3. No active failover → `_try_acquire_failover_coordinator()` → entry gates pass
   → writes `WALRECEIVER_DISABLING`
4. `plan_walreceiver_disabling` → `DisableWalReceiver` → `GATES_PASSED`
5. `plan_gates_passed` → `CleanupVotes` + open registration → `REGISTRATION`
6. All replicas vote (participant `plan_vote`) → coordinator `plan_registration`
   → `VOTING`
7. `plan_voting` → quorum met, winner selected → `WINNER_SELECTED`
8. Winner (participant) → `AcquireLock(timeout=0)` → `PROMOTING`
9. Winner → global `PROMOTING`; local `creating_slots` → `promoting` →
    `checkpointing`; then global `FINISHED`
10. Coordinator `plan_finished` → `CleanupFailover` → state becomes `None`
11. Losers re-attach during subsequent local reconciliation

### Scenario 2: Coordinator is the winner

1. Steps 1–8 as above
2. The coordinator is also the election winner
3. `FailoverMachine` detects `winner_is_coord` → runs **participant**
   machine (not coordinator) for `WINNER_SELECTED`
4. Participant `plan_winner_selected` → `AcquireLock` → `PROMOTING` →
   `Promote` → `FINISHED`
5. On `FINISHED` → coordinator machine runs terminal cleanup

### Scenario 3: Failover fails (quorum not met)

1. Primary dies → `WALRECEIVER_DISABLING` → `GATES_PASSED` →
   `REGISTRATION` → `VOTING`
2. `plan_voting` → `_is_election_valid` returns False (not enough votes)
3. `TransitionTo(FAILED)`
4. Coordinator `plan_failed` → `CleanupFailover` → state becomes `None`
5. Participants wait until cleanup completes

### Scenario 4: Coordinator crash mid-failover

1. Coordinator crashes after writing `REGISTRATION` to ZK
2. On restart (or another node): `_run_failover_step` sees active phase but
   no coordinator holds `ELECTION_MANAGER_LOCK_PATH`
3. `_try_become_failover_coordinator()` → re-acquires the lock, resumes
   coordination from `REGISTRATION`
4. Process continues from the recorded phase — no action repeated
