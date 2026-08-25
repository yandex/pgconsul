## Basic entries in ZK

* `TIMELINE_INFO_PATH` = `timeline`
Contains the timeline of the cluster, those of the primary at the time when there were no problems in the cluster.
It is updated by the primary during the iteration of normal operation.

* `FAILOVER_INFO_PATH` = `failover_state`
It contains only cluster-wide failover coordination phases. Winner-local
promotion progress is stored under `local_state_directory`. `finished` and
`failed` are cleanup phases; successful cleanup deletes this node.

* `QUORUM_PATH` = `quorum`
The list of replicas that held `QUORUM_MEMBER_LOCK_PATH` in the previous iteration. Only those replicas that are part of the quorum participate in the failover process. It is updated by the primary at each trouble-free iteration.

* `REPLICS_INFO_PATH` = `replics_info`
Contains information from the `pg_stat_replication` on the current primary.
It is used to select the most relevant replica during switchover/failover.

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
It is held by each host if the local Postgres is alive. It is used in various places to get a list of live (but not necessarily replicating) hosts.

* `PRIMARY_LOCK_PATH` = `leader`
The main lock in PgConsul is held by the primary.
The disappearance of this lock is the reason to start failover.
The lock disappears when the network primary loses contact with ZK, or is released voluntarily when Postgres is inoperable, and in some other cases.

* `QUORUM_MEMBER_LOCK_PATH` = `quorum/members/%fqdn%`
It is used in quorum replication mode. It is held by a replica that is part of the quorum, which is HA and replicates. It is released if the replica finds that replication is not working, Postgres is broken, or the primary has changed.

* `ELECTION_MANAGER_LOCK_PATH` = `epoch_manager`
It is used for selecting the most relevant replica during the failover process. One of the quorum members captures this lock and selects a replica with the maximum LSN. The rest of the participants simply provide their LSN. The lock is held throughout the selection.

* `PRIMARY_SWITCH_LOCK_PATH` = `reprimary`
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
