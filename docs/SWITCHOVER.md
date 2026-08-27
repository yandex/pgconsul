# SWITCHOVER

Switchover is the process of scheduled primary switching to another host.

There are two possible options:
* "failover to" — when the new primary is known in advance and it is important to switch to it
* "failover from" — when any replica is suitable, the main thing is to release the current primary

## Architecture overview (MDB-41951, ADR-0005 §3, ADR-0006)

Switchover is implemented as **two cooperating state machines** — one on each
side (old primary and candidate). Each machine follows the "pure planner +
imperative shell" pattern:

* All I/O (ZK reads, PostgreSQL queries) is concentrated in a single point —
  `SwitchoverObservation.build()` in `src/switchover/types.py`.
* Handler methods (`plan_*`) are **pure functions**: they receive an immutable
  observation snapshot and return a **Plan** — an ordered list of commands.
* `CommandExecutor.run()` in `src/command_executor.py` executes the commands
  one by one, stopping on the first failure (fail-fast).

```
          +---------------------------------------------------+
          |  Pgconsul.run_iteration()  (imperative shell)     |
          |                                                   |
          |  1. db_state = db.get_state()                     |
          |  2. zk_state  = zk.get_state()                    |
          |  3. sw_record = SwitchoverRecord.from_zk_state()  |
          |  4. obs = SwitchoverObservation.build(...)        |  <- ALL I/O HERE
          |  5. executor.run(machine, obs)                    |
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
| `src/switchover/types.py` | Domain types: phases, `SwitchoverRecord`, `SwitchoverObservation`, `SwitchoverMachineConfig` |
| `src/switchover/primary.py` | `PrimarySwitchoverMachine` — state machine on the old primary side |
| `src/switchover/candidate.py` | `CandidateSwitchoverMachine` — state machine on the candidate side |
| `src/commands.py` | Command vocabulary (frozen dataclasses) and `Plan` type alias |
| `src/command_executor.py` | `CommandExecutor` — imperative shell that interprets Plans |

## Phases (`SwitchoverPhase`)

Cross-host state is persisted as one versioned JSON value in
`switchover/record`. Phase changes and metadata updates use CAS; a stale plan
cannot overwrite or clean up a newer switchover.

| Phase | Value | Written by | Meaning |
|-------|-------|------------|---------|
| `SCHEDULED` | `scheduled` | dbaas_worker / pgconsul-util | Switchover scheduled by external system |
| `INITIATED` | `initiated` | PrimarySwitchoverMachine | Primary fixed candidate + side replicas |
| `CANDIDATE_FOUND` | `candidate_found` | CandidateSwitchoverMachine | Candidate ready (slots created, side replicas turned) |
| `PRIMARY_SHUT` | `primary_shut` | PrimarySwitchoverMachine | Old primary released the leader lock |
| `CANDIDATE_ACQUIRED` | `candidate_acquired` | CandidateSwitchoverMachine | Candidate holds the lock but hasn't promoted (MDB-41951 race fix) |
| `PROMOTED` | `promoted` | CandidateSwitchoverMachine | Candidate promoted itself |
| `FAILED` | `failed` | either side | Rollback / cleanup needed |
| `FALLBACK` | `fallback` | PrimarySwitchoverMachine | Explicit handoff to failover initialization |

Primary-only command groups are persisted in
`<local_state_directory>/switchover_primary_state.json`: `sync_set`,
`pooler_stopped`, and `pg_stopped`.

## How phase transitions work

### TransitionTo — the idempotency fence

A phase transition is a **command** (`TransitionTo`), not a direct call. A
handler includes it in the returned Plan:

```python
plan.append(WriteLocalState('switchover_primary', SwitchoverPhase.SYNC_SET))
```

`CommandExecutor` executes it via `_exec_transition_to()`:

```python
def _exec_transition_to(self, phase: SwitchoverPhase) -> bool:
    if not self._write_switchover_record(phase=phase):
        return False
    log_event(f'SWITCHOVER PHASE -> {phase}', level='warning')
    return True
```

The phase is written to ZK **before** the actual action (or at the start of a
command block). This is the "idempotency fence" (ADR-0005 §3): if pgconsul
crashes (kill -9, OOM, restart) mid-Plan, on the next startup the machine reads
the already-updated phase from ZK and resumes from it, rather than repeating
the action from the previous phase.

### Empty Plan = "wait"

If a handler returns an empty Plan (`[]`), it means "nothing to do yet" — the
candidate hasn't caught up, side replicas haven't turned, etc. Switchover still
owns the iteration and the machine retries on the next cycle.

### Fail-fast

If any command fails, `CommandExecutor` stops executing the remaining
commands. The iteration is "consumed" and the machine retries on the next
cycle with a fresh observation. Because the phase is already in ZK, the
handler re-derives the same Plan idempotently.

## The switchover process

Switchover starts when the CLI or worker atomically publishes a complete
`scheduled` record containing the old primary, timeline and optional destination.

The process is performed simultaneously by the old primary and the candidate
replica, which synchronize through setting and waiting for phase values in ZK.

### Phase 1: SCHEDULED -> SYNC_SET (PrimarySwitchoverMachine)

`plan_scheduled()` runs sanity gates (any failure -> empty Plan, retry):

* Current role is `primary`
* `hostname` in the switchover record matches the current host
* ZK timeline matches the switchover timeline
* HA replicas are present
* Last role transition was long enough ago (or enough replicas are alive)
* A candidate is selected
* The candidate is in sync (replay lag <= `max_allowed_switchover_lag_ms`)

Action:
```python
[StartTimer('switchover'), WriteCandidate(candidate),
 SetSyncReplication(candidate), WriteLocalState(SYNC_SET)]
```

### Phase 2: SYNC_SET -> INITIATED (PrimarySwitchoverMachine)

`plan_sync_set()` fixes the candidate and side replicas:

```python
[WriteCandidate(candidate), WriteSideReplicas(side_replicas),
 ClearLocalState('switchover_primary'), TransitionTo(INITIATED)]
```

### Phase 3: INITIATED — handoff to candidate

Both machines are active simultaneously:

**Old primary** (`plan_initiated`): waits for the candidate to write
`CANDIDATE_FOUND`. If the candidate is dead -> `TransitionTo(FAILED)`.
The next iteration routes the new phase to `plan_candidate_found()`.

**Candidate** (`plan_initiated`):
```python
[Log('SWITCHOVER STARTED', event=True), CreateSlots(side_replicas)]
# when all side replicas turned ->
+ [TransitionTo(CANDIDATE_FOUND)]
# otherwise -> CreateSlots only (retry next iteration)
```

`CreateSlots` is idempotent — emitted every iteration while waiting. The
executor re-creates only missing slots, so repeating the command is safe
(see `src/command_executor.py`, `_exec_create_slots`).

### Phase 4: CANDIDATE_FOUND -> POOLER_STOPPED (PrimarySwitchoverMachine)

`plan_candidate_found()`:
```python
[StoreReplicsInfo(), Checkpoint(), StartTimer('downtime'),
 StopPooler(), Log('Cluster closed'),
 WriteLocalState('switchover_primary', POOLER_STOPPED)]
```

This is a kill-9 recovery point (ADR-0006 §4): if the primary crashes here, on
restart it sees `POOLER_STOPPED` and proceeds to `plan_pooler_stopped`.

### Phase 5: POOLER_STOPPED -> PG_STOPPED (PrimarySwitchoverMachine)

`plan_pooler_stopped()` does a non-blocking sync check: if the candidate is
not yet in sync -> empty Plan (wait). Otherwise:
```python
[StopPostgresql(wait=False), WriteLocalState('switchover_primary', PG_STOPPED)]
```

### Phase 6: PG_STOPPED -> PRIMARY_SHUT (PrimarySwitchoverMachine)

`plan_pg_stopped()`:
```python
[TransitionTo(PRIMARY_SHUT), ClearLocalState('switchover_primary'),
 ReleaseLock(wait=5),
 StopPostgresql(wait=True), SetSimplePrimarySwitchTry()]
```

`Sleep(wal_drain_delay)` is a **local fixed delay** to let the sync replica
drain the last WAL — it is *not* a cluster-event wait (ADR-0005 §1 prohibits
waiting for cluster events inside an iteration). It is a one-shot delay per
phase, not a level-triggered retry.

`TransitionTo(PRIMARY_SHUT)` is placed **before** `ReleaseLock` — the phase is
fenced before the lock is released. After `ReleaseLock` the candidate can
acquire the leader lock.

### Phase 7: PRIMARY_SHUT -> CANDIDATE_ACQUIRED -> PROMOTED (CandidateSwitchoverMachine)

`plan_candidate_found()` attempts non-blocking lock acquisition
(`timeout=0`). If the lock is still held by the primary -> fail-fast, retry
next iteration. When the lock is acquired:

```python
[ClearLocalState('switchover_candidate'),
 AcquireLock(allow_queue=True, timeout=0),
 TransitionTo(CANDIDATE_ACQUIRED),   # <- MDB-41951 race fix
 StartTimer('downtime'),              # (if primary didn't start it)
 Promote(scope='switchover_candidate', old_primary=old_primary),
 TransitionTo(PROMOTED),
 WriteLastSwitchoverTime(), StopTimer('switchover'), CleanupSwitchover()]
```

> **Race fix (MDB-41951):** `CANDIDATE_ACQUIRED` is inserted **before**
> `Promote`. The old primary in `plan_primary_shut` checks
> `phase == PROMOTED` before rewinding — this guarantees that rewind starts
> only after a successful promote. Without this intermediate phase, the
> primary could start rewinding at `CANDIDATE_ACQUIRED`/`PRIMARY_SHUT`, and if
> the promote fails, the cluster gets stuck (two "primaries", one without a
> lock).

`Promote` persists `creating_slots`, `promoting`, and `checkpointing` in
`<local_state_directory>/switchover_candidate_state.json`. It does not write or
delete failover metadata.

### Phase 8: PROMOTED — old primary returns to cluster

`plan_primary_shut()` (same handler for `PRIMARY_SHUT` and `PROMOTED`):

```python
if lock_holder == my_hostname:       # unexpectedly holding lock -> release
    [StopPooler(), ReleaseLock(wait=5)]
elif new_primary is not None and phase == PROMOTED:
    [Log('new primary found'), DeleteHostOp(), SetSimplePrimarySwitchTry(),
     RewindFromSource(new_primary, is_postgresql_dead=True)]
else:
    []   # wait for candidate to promote
```

## Transition diagram

```
                    +------------+
                    |  SCHEDULED |  (external system)
                    +-----+------+
                          | plan_scheduled (Primary)
                          v
                    +------------+
                    |  SYNC_SET  |  (local primary state)
                    +-----+------+
                          | plan_sync_set (Primary)
                          v
                    +------------+      +----------------------+
                    |  INITIATED | <--> | Candidate:           |
                    +-----+------+      | CreateSlots, wait    |
                          |             | side replicas        |
                          |             +----------------------+
            +-------------+             |
            |             |      Candidate:
   Primary: |        TransitionTo(CANDIDATE_FOUND)
   waits    |                         |
   CANDIDATE_FOUND                    v
            v              +----------------------+
       +-----------------+ |   CANDIDATE_FOUND    |
       | CANDIDATE_FOUND | +----------+-----------+
       +-------+---------+            |
               | plan_candidate_found (Primary)
               v
          +----------------+
          | POOLER_STOPPED |  (local primary state)
          +-------+--------+
                  | plan_pooler_stopped (Primary)
                  v
          +------------+
          | PG_STOPPED |  (local primary state)
          +-----+------+
                | plan_pg_stopped (Primary)
                v
          +--------------+         +-------------------------+
          | PRIMARY_SHUT | ----->  | Candidate:              |
          +-------+------+  lock   | AcquireLock             |
                  | released       | TransitionTo(           |
                  |                |   CANDIDATE_ACQUIRED)   |
                  |                +-----------+-------------+
                  |                            | Promote
                  |                            v
                  |                +----------------+
                  |                |    PROMOTED    |
                  |                +-------+--------+
                  |                        |
                  v                        v
          Primary: plan_primary_shut (phase == PROMOTED)
          -> RewindFromSource -> return to cluster as replica

          Any phase ---> FAILED (TransitionTo(FAILED) on error/dead candidate)
```

## Idempotency guarantees

| Mechanism | Where | What it provides |
|-----------|-------|-----------------|
| Phase in ZK before action (fence) | `TransitionTo` at the start of Plan | Restart -> resume from recorded phase, not repeat action |
| Empty Plan = "wait" | `plan_*` return `[]` | Claims the iteration and retries next cycle |
| Fail-fast | `CommandExecutor.run` | Command failure -> stop, retry; doesn't execute half a Plan |
| Idempotent commands | `StartTimer` (skip if started), `CreateSlots`, `WriteCandidate` | Safe repeat on restart |
| Non-blocking lock | `AcquireLock(timeout=0)` | Lock held -> fail-fast -> retry, no hang |
| Local command groups | `<local_state_directory>/switchover_*_state.json` | Host restarts resume without exposing internal progress to other hosts |

## Entry point from `main.py`

`run_iteration()` calls the blocking `handle_switchover()` before role-based
logic. It routes the old primary, candidate, side replicas and unrelated hosts
from persistent switchover metadata. Ordinary role iterations never handle an
active switchover.

## Scenarios

### Scenario 1: Normal switchover

1. `scheduled` → primary `plan_scheduled` → local `SYNC_SET`
2. local `plan_sync_set` → fix candidate + side replicas → global `INITIATED`
3. Candidate `plan_initiated` → `CreateSlots` (idempotent) → side replicas
   turn → `CANDIDATE_FOUND`
4. Next primary iteration sees `CANDIDATE_FOUND` → `plan_candidate_found` →
   local `POOLER_STOPPED` → local `PG_STOPPED` → global `PRIMARY_SHUT`
   (release lock)
5. Candidate `plan_candidate_found` → `AcquireLock` → `CANDIDATE_ACQUIRED` →
   `Promote` → `PROMOTED`
6. Primary `plan_primary_shut` (phase == `PROMOTED`) → `RewindFromSource` →
   return to cluster as replica

### Scenario 2: Switchover with dead candidate

1. `scheduled` → `SYNC_SET` → `INITIATED` — candidate starts creating slots
2. Candidate dies (PG crash) before writing `CANDIDATE_FOUND`
3. Primary `plan_initiated` checks `obs.candidate_alive` → `False`
4. Primary emits `TransitionTo(FAILED)` — switchover aborts
5. `plan_failed` cleans switchover metadata if a primary still holds the lock;
   otherwise it enters `FALLBACK` and initializes failover
