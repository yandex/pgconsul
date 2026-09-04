## Basic entries in ZK

* `TIMELINE_INFO_PATH` = `timeline`
Contains the timeline of the cluster, those of the primary at the time when there were no problems in the cluster.
It is updated by the primary during the iteration of normal operation.

* `FAILOVER_INFO_PATH` = `failover_state`
Contains the cluster-wide failover phase. Only the `epoch_manager` holder may
write it. `finished` and `failed` are cleanup phases; cleanup deletes this node
last.

* `FAILOVER_VERSION_PATH` = `failover_version`
Immutable ID of the active failover. Votes and participant results with another
version are ignored.

For a safe failover, the electorate is derived from the `durability_members`
state CAS-fenced by the coordinator at failover start: all members of source
and target transition endpoints except the failed primary. A manual
`--with-data-loss` failover instead stores its electorate in
`failover_request`.

* `ELECTION_VOTE_PATH` = `election_vote/%fqdn%`
One atomic JSON vote containing `failover_version`, timeline, `flush_lsn`, and
priority.

* `FAILOVER_PARTICIPANT_PATH` = `failover_participant/%fqdn%`
One atomic JSON value containing versioned winner-local promotion progress.

* `DURABILITY_MEMBERS_PATH` = `durability_members`
Contains the stable durability group, including the current primary, and an
optional in-progress membership transition:

```json
{
  "members": ["primary", "replica1", "replica2"],
  "transition": {
    "from_members": ["primary", "replica1"],
    "to_members": ["primary", "replica1", "replica2"],
    "operation_id": "e149f768d5d34c3c8f5b6520eb917bb1"
  }
}
```

The primary derives SSN by removing itself and uses
`ANY ceil(replica_count / 2)`. During a transition, `members` remains the
source membership until target SSN has accepted a synchronous WAL write to the
PgConsul service table. Failover checks both `from_members` and `to_members`;
the transition is also sufficient to resume the change after restart. The
barrier LSN and replica acknowledgements are not stored in ZK. Ordinary
membership reconciliation uses each replica's `alive` lock to detect its
failure; losing replication streaming alone does not evict it.

* `REPLICS_INFO_PATH` = `replics_info`
Contains information from the `pg_stat_replication` on the current primary.
It is used during normal reconciliation and switchover. Failover uses its
frozen versioned votes.

* `SWITCHOVER_RECORD_PATH` = `switchover/record`
Contains the complete switchover state in one JSON value. Every update uses
ZooKeeper compare-and-set with the version returned by the preceding read.
An empty object means that no switchover is active; cleanup does not delete the
node, so its version remains monotonic.

```
{
    'hostname': primary, # current primary
    'timeline': timeline, # the last known timeline of the cluster before switchover
    'destination': new_primary, # optional requested primary
    'phase': phase,
    'candidate': candidate,
    'side_replicas': side_replicas,
}
```

* `MAINTENANCE_PATH` = `maintenance`
It is used to enable and disable maintenance mode.

* `MAINTENANCE_TIME_PATH` = `maintenance/ts`
The time when maintenance was enabled

* `MAINTENANCE_PRIMARY_PATH` = `maintenance/primary`
The current primary at the time maintenance is enabled

## Basic locks in ZK

* `HOST_ALIVE_LOCK_PATH` = `alive/%fqdn%`
It is held by each host if the local Postgres is alive. Ordinary durability
reconciliation uses it to identify a failed replica: a member is removed only
after its own alive lock has been absent for `quorum_removal_delay`. A broken
or partitioned primary-to-replica replication connection alone does not remove
the replica. This deliberately accepts the residual risk that a complex
network failure affecting the replica and every ZooKeeper server expires the
replica session.

* `PRIMARY_LOCK_PATH` = `leader`
The main lock in PgConsul is held by the primary.
The disappearance of this lock is the reason to start failover.
The lock disappears when the network primary loses contact with ZK, or is released voluntarily when Postgres is inoperable, and in some other cases.

* `ELECTION_MANAGER_LOCK_PATH` = `epoch_manager`
It identifies the sole failover coordinator. The lock is held for the complete
operation, including voting, promotion observation, terminal transition, and
cleanup.

This lock is taken by the replica (or former primary) when switching to a new primary. The lock is taken for the duration of the switch, so that no more than 1 replica is switched at a time.

* `SWITCHOVER_LOCK_PATH` = `switchover/lock`
This lock is taken by the CLI at the time of creating/clearing information about switchover in ZK. This lock is not involved in the primary switching process itself.

## Two-layer architecture

The ZooKeeper integration is split into two layers:

### Transport layer — `ZkClient` (`src/zk_client.py`)

Wraps `KazooClient` and provides raw data operations with path-prefix support.
All methods raise **domain exceptions** instead of raw Kazoo exceptions:

| Exception | Meaning |
|-----------|---------|
| `ZkClientError` | General ZK connection or command error |
| `ZkNoNodeError` | Node does not exist (subclass of `ZkClientError`) |
| `ZkSessionExpiredError` | ZK session expired (subclass of `ZkClientError`) |
| `ZkLockTimeout` | Lock acquire timed out |
| `ZkConnectionClosedError` | Connection was explicitly closed |

`ZkClient` handles reconnection with exponential backoff and jitter via `reconnect()`.
Lock lifecycle (drop stale locks, re-init primary lock) is owned by the layer above.

### Business layer — `Zookeeper` (`src/zk.py`)

Wraps `ZkClient` and provides domain-oriented methods.

**Error-handling contract:**

* `get()` — raises `ZookeeperException` on ZK errors (callers decide whether to propagate or swallow).
* `noexcept_get()` — swallows all exceptions, returns `None`.
* `write()` — raises `ZookeeperException` on ZK errors.
* `noexcept_write()` — swallows all exceptions, returns `False`.
* `delete()` — catches `ZkClientError`, logs it, returns `False`; returns `True` on success or when node is absent.
* `delete_*()` methods — delegate to `delete()` and therefore always return `bool` (never raise `ZkClientError`).
* High-level `write_*()` methods — catch `Exception`, log it, return `False`.

This means callers of `Zookeeper` methods **never receive raw Kazoo or `ZkClientError` exceptions** — all errors are either converted to `ZookeeperException` (for `get`/`write`) or absorbed and logged (for `noexcept_*` and `delete`/`write_*` variants).
