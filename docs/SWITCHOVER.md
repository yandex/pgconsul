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

Phases are persisted in the ZK node `switchover/state`:

| Phase | Value | Written by | Meaning |
|-------|-------|------------|---------|
| `SCHEDULED` | `scheduled` | dbaas_worker / pgconsul-util | Switchover scheduled by external system |
| `SYNC_SET` | `sync_set` | PrimarySwitchoverMachine | Primary enabled sync replication on candidate |
| `INITIATED` | `initiated` | PrimarySwitchoverMachine | Primary fixed candidate + side replicas |
| `CANDIDATE_FOUND` | `candidate_found` | CandidateSwitchoverMachine | Candidate ready (slots created, side replicas turned) |
| `POOLER_STOPPED` | `pooler_stopped` | PrimarySwitchoverMachine | Primary stopped pooler (kill-9 recovery point) |
| `PG_STOPPED` | `pg_stopped` | PrimarySwitchoverMachine | Primary stopped PG (non-blocking) |
| `PRIMARY_SHUT` | `primary_shut` | PrimarySwitchoverMachine | Old primary released the leader lock |
| `CANDIDATE_ACQUIRED` | `candidate_acquired` | CandidateSwitchoverMachine | Candidate holds the lock but hasn't promoted (MDB-41951 race fix) |
| `PROMOTED` | `promoted` | CandidateSwitchoverMachine | Candidate promoted itself |
| `FAILED` | `failed` | either side | Rollback / cleanup needed |

> New phase values (`sync_set`, `primary_shut`, `promoted`) are unrecognized by
> old pgconsul versions — this is an intentional fence against parallel
> switchovers (ADR-0005 §5, two-phase rollout).

## How phase transitions work

### TransitionTo — the idempotency fence

A phase transition is a **command** (`TransitionTo`), not a direct call. A
handler includes it in the returned Plan:

```python
plan.append(TransitionTo(SwitchoverPhase.SYNC_SET))
```

`CommandExecutor` executes it via `_exec_transition_to()`:

```python
def _exec_transition_to(self, phase: SwitchoverPhase) -> bool:
    if not self._zk.write_switchover_state(phase):
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
candidate hasn't caught up, side replicas haven't turned, etc. The iteration is
not "consumed" and the machine retries on the next cycle.

### Fail-fast

If any command fails, `CommandExecutor` stops executing the remaining
commands. The iteration is "consumed" and the machine retries on the next
cycle with a fresh observation. Because the phase is already in ZK, the
handler re-derives the same Plan idempotently.

## The switchover process

Switchover starts when the CLI or worker writes the `scheduled` value to
`SWITCHOVER_STATE_PATH` and information to `SWITCHOVER_PRIMARY_PATH` (from
where and where to switch the primary).

The process is performed simultaneously by the old primary and the candidate
replica, which synchronize through setting and waiting for phase values in ZK.

### Phase 1: SCHEDULED -> SYNC_SET (PrimarySwitchoverMachine)

`plan_scheduled()` runs sanity gates (any failure -> empty Plan, retry):

* Current role is `primary`
* `hostname` in the switchover record matches the current host
* ZK timeline matches the switchover timeline
* Cluster is not in failover process
* HA replicas are present
* Last role transition was long enough ago (or enough replicas are alive)
* A candidate is selected
* The candidate is in sync (replay lag <= `max_allowed_switchover_lag_ms`)

Action:
```python
[StartTimer('switchover'), WriteCandidate(candidate),
 SetSyncReplication(candidate), TransitionTo(SYNC_SET)]
```

### Phase 2: SYNC_SET -> INITIATED (PrimarySwitchoverMachine)

`plan_sync_set()` fixes the candidate and side replicas:

```python
[WriteCandidate(candidate), WriteSideReplicas(side_replicas),
 TransitionTo(INITIATED), WriteFailoverState('switchover_initiated')]
```

### Phase 3: INITIATED — handoff to candidate

Both machines are active simultaneously:

**Old primary** (`plan_initiated`): waits for the candidate to write
`CANDIDATE_FOUND`. Detects it via `obs.live_switchover_state`. When detected,
inlines pre-shutdown prep (`StoreReplicsInfo`, `Checkpoint`) and delegates to
`plan_candidate_found()`. If the candidate is dead -> `TransitionTo(FAILED)`.

**Candidate** (`plan_initiated`):
```python
[Log('SWITCHOVER STARTED', event=True), CreateSlots(side_replicas)]
# when all side replicas turned ->
+ [TransitionTo(CANDIDATE_FOUND)]
# otherwise -> CreateSlots only (retry next iteration)
```

`CreateSlots` is idempotent — emitted every iteration while waiting.

### Phase 4: CANDIDATE_FOUND -> POOLER_STOPPED (PrimarySwitchoverMachine)

`plan_candidate_found()`:
```python
[StartTimer('downtime'), StopPooler(), Log('Cluster closed'),
 TransitionTo(POOLER_STOPPED)]
```

This is a kill-9 recovery point (ADR-0006 §4): if the primary crashes here, on
restart it sees `POOLER_STOPPED` and proceeds to `plan_pooler_stopped`.

### Phase 5: POOLER_STOPPED -> PG_STOPPED (PrimarySwitchoverMachine)

`plan_pooler_stopped()` does a non-blocking sync check: if the candidate is
not yet in sync -> empty Plan (wait). Otherwise:
```python
[StopPostgresql(wait=False), TransitionTo(PG_STOPPED)]
```

### Phase 6: PG_STOPPED -> PRIMARY_SHUT (PrimarySwitchoverMachine)

`plan_pg_stopped()`:
```python
[Sleep(wal_drain_delay), WriteFailoverState('switchover_master_shut'),
 TransitionTo(PRIMARY_SHUT), ReleaseLock(wait=5),
 StopPostgresql(wait=True), SetSimplePrimarySwitchTry()]
```

`TransitionTo(PRIMARY_SHUT)` is placed **before** `ReleaseLock` — the phase is
fenced before the lock is released. After `ReleaseLock` the candidate can
acquire the leader lock.

### Phase 7: PRIMARY_SHUT -> CANDIDATE_ACQUIRED -> PROMOTED (CandidateSwitchoverMachine)

`plan_candidate_found()` attempts non-blocking lock acquisition
(`timeout=0`). If the lock is still held by the primary -> fail-fast, retry
next iteration. When the lock is acquired:

```python
[AcquireLock(allow_queue=True, timeout=0),
 TransitionTo(CANDIDATE_ACQUIRED),   # <- MDB-41951 race fix
 StartTimer('downtime'),              # (if primary didn't start it)
 DoFailover(old_primary),
 TransitionTo(PROMOTED),
 CleanupSwitchover(), WriteLastSwitchoverTime(), StopTimer('switchover')]
```

> **Race fix (MDB-41951):** `CANDIDATE_ACQUIRED` is inserted **before**
> `DoFailover`. The old primary in `plan_primary_shut` checks
> `phase == PROMOTED` before rewinding — this guarantees that rewind starts
> only after a successful promote. Without this intermediate phase, the
> primary could start rewinding at `CANDIDATE_ACQUIRED`/`PRIMARY_SHUT`, and if
> the promote fails, the cluster gets stuck (two "primaries", one without a
> lock).

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
                    |  SYNC_SET  |
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
          | POOLER_STOPPED |
          +-------+--------+
                  | plan_pooler_stopped (Primary)
                  v
          +------------+
          | PG_STOPPED |
          +-----+------+
                | plan_pg_stopped (Primary)
                v
          +--------------+         +-------------------------+
          | PRIMARY_SHUT | ----->  | Candidate:              |
          +-------+------+  lock   | AcquireLock             |
                  | released       | TransitionTo(           |
                  |                |   CANDIDATE_ACQUIRED)   |
                  |                +-----------+-------------+
                  |                            | DoFailover
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
| Empty Plan = "wait" | `plan_*` return `[]` | Doesn't consume iteration, retries next cycle |
| Fail-fast | `CommandExecutor.run` | Command failure -> stop, retry; doesn't execute half a Plan |
| Idempotent commands | `StartTimer` (skip if started), `CreateSlots`, `WriteCandidate` | Safe repeat on restart |
| Non-blocking lock | `AcquireLock(timeout=0)` | Lock held -> fail-fast -> retry, no hang |
| Failed-promote guard | `plan_candidate_found`: `lock_holder == my_hostname` -> `ReleaseLock + FAILED` | Prevents infinite retry on failed promote |
| New phase values | `sync_set`, `primary_shut`, `promoted` | Old pgconsul versions don't recognize them -> no parallel switchovers |

## Entry points from `main.py`

The machines are driven from three places in `src/main.py`:

1. **`primary_iter()`** — if `sw_record.is_active()` and
   `sw_record.belongs_to(my_hostname)` -> runs `PrimarySwitchoverMachine`.

2. **`replica_iter()`** — if `sw_record.candidate == my_hostname` -> runs
   `CandidateSwitchoverMachine`. Non-candidate replicas return to cluster
   (stream from the candidate) when phase >= `INITIATED`.

3. **`dead_iter()`** — if the old primary died between `PG_STOPPED` and
   `PRIMARY_SHUT`, runs `PrimarySwitchoverMachine` so it can advance to
   `PRIMARY_SHUT` (release lock, final PG stop). Without this guard, the old
   primary gets stuck in an infinite loop: `dead_iter -> return None ->
   dead_iter -> ...` (MDB-41951).
