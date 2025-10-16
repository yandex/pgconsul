# SWITCHOVER

switchover is the process of scheduled primary switching to another host.

There are two possible options:
* "failover to" - when the new primary is known in advance and it is important to switch to him
* "failover from" - when any replica is suitable, the main thing is to release the current primary

## The switchover process

switchover starts with the fact that the CLI or worker writes the `scheduled` value
to `SWITCHOVER_STATE_PATH`, and information to `SWITCHOVER_PRIMARY_PATH` from where and where to switch the primary.

The switchover process is performed simultaneously by the old primary and the candidate replica, which are synchronized through setting and waiting for values in ZK.

### Verification of necessity

The current primary verifies (`_check_primary_switchover`) that:

* His current role is indeed a primary
* the command on switchover is relevant:
* its status is `scheduled`
    * the `hostname` from where to switch is the same as the current primary
    * the timeline recorded in the command matches the timeline of the current primary
* if little time has passed since the last switchover/failover, it checks that all HA replicas have returned to the cluster and are alive
* the cluster is not in the failover process
* the candidate to be switched to is a synchronous/quorum replica and does not lag behind
* passes the selected candidate to `_do_primary_switchover`

All replicas perform approximately the same checks, but in a separate function `_detect_replica_switchover`.

### Switching the primary

Old primary (`_do_primary_switchover`):
* captures the ZK candidate selected in `_check_primary_switchover`. Candida can't change from now on.
* makes the selected candidate the only synchronous replica. This guarantees that all transactions of the old primary will be on it.
* signals the replica by writing the value `initiated` to `SWITCHOVER_STATE_PATH`

All replicas (`_accept_switchover`):
* are waiting for a signal from the primary to start the switchover procedure
* get the candidate selected by the old primary from ZK.
* if the replica is not a candidate, it turns to the candidate.
* if the replica is a candidate, it continues the switching procedure as a new primary

The new primary (`_accept_switchover`):
* signals readiness to the primary by writing the value `candidate_found` to `SWITCHOVER_STATE_PATH`

The old primary (`_do_primary_switchover`):
* is waiting for a signal from the candidate's readiness
* is waiting for all other replicas to disconnect (and thus won't get extra wal from old primary)
* makes a `CHECKPOINT`
* closes against load (stops the Pooler)
* waits for the new primary to catch up with replication to lag < `max_allowed_switchover_lag_ms`
* starts Postgres shutdown in the background
* waits for 5 seconds
* releases the `leader_lock`

New primary (`_accept_switchover`):
* waits for release and captures the `leader_lock`
* creates replication slots on the new primary
* actually does pg_ctl promote
* clears the switchover information in ZK

The old primary
* waits for Postgres to stop completely
* waits until the new primary captures the `leader_lock` and returns to the cluster as a replica
