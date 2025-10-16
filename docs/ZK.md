## Basic entries in ZK

* `TIMELINE_INFO_PATH` = `timeline`
Contains the timeline of the cluster, those of the primary at the time when there were no problems in the cluster.
It is updated by the primary during the iteration of normal operation.

* `FAILOVER_INFO_PATH` = `failover_state`
It contains information about the promotion process of the new primary.

* `QUORUM_PATH` = `quorum`
The list of replicas that held `QUORUM_MEMBER_LOCK_PATH` in the previous iteration. Only those replicas that are part of the quorum participate in the failover process. It is updated by the primary at each trouble-free iteration.

* `REPLICS_INFO_PATH` = `replics_info`
Contains information from the `pg_stat_replication` on the current primary.
It is used to select the most relevant replica during switchover/failover.

* `SWITCHOVER_STATE_PATH' = `switchover/state`
switchover starts with the fact that the CLI or worker writes `scheduled` here.
Later on, the old and new primary coordinate their actions in the switchover process through this entry.

* `SWITCHOVER_PRIMARY_PATH` = `switchover/master`
Details of the switchover execution.

```
{
    'hostname': primary, # current primary
    'timeline': timeline, # the last known timeline of the cluster before switchover
    'destination': new_primary, # new primary if switchover goes to a specific host
}
```

* `CURRENT_PROMOTING_HOST` = `current_promoting_host`
Before executing pg_ctl promote, the FQDN of the new primary is written here. This entry is used if the failover/switchover procedure is interrupted for any reason. Then, at the next iteration, the primary will be able to determine whether it needs to complete the procedure (if the FQDN matches) or let go of the lock and become a replica.

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

* `SYNC_REPLICA_LOCK_PATH` = `sync_replica`
It is used in synchronous replication mode when only one of the replicas is synchronous. It is held by the actual synchronous replica. Legacy.

* `ELECTION_MANAGER_LOCK_PATH` = `epoch_manager`
It is used for selecting the most relevant replica during the failover process. One of the quorum members captures this lock and selects a replica with the maximum LSN. The rest of the participants simply provide their LSN. The lock is held throughout the selection.

* `PRIMARY_SWITCH_LOCK_PATH` = `reprimary`
This lock is taken by the replica (or former primary) when switching to a new primary. The lock is taken for the duration of the switch, so that no more than 1 replica is switched at a time.

* `SWITCHOVER_LOCK_PATH` = `switchover/lock`
This lock is taken by the CLI at the time of creating/clearing information about switchover in ZK. This lock is not involved in the primary switching process itself.
