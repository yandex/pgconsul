# PgConsul's algorithm of operation

Further in the text, the terms primary and replica refer to PgConsul processes running on hosts with a Postgres primary and, accordingly, a replica.

## General work scenario

PgConsul performs the main work in a loop by calling the 'run_iteration` function.
At the beginning of the iteration, PgConsul takes the following steps:

* determines the current `role` of the Postgres instance on its host
* loads the cluster status from the database (`db_state')
* loads the cluster state from ZK (`zk_state')
* captures `alive_lock` in ZK for its host (`_zk_alive_refresh')
* checks the `maintenance` status

Next, depending on the current Postgres role and settings, one of the functions is performed:

* `primary_iter` is the primary in a HA cluster (it is the state of Postgres, not the possession of a `leader_lock`)
* `single_node_primary_iter` is the primary in a non-HA (usually 1-node) cluster
* `replica_iter' - replica in the HA cluster
* `non_ha_replica_iter' - cascading replica
* `dead_iter` - failed to get the current role, usually if Postgres is not available

### The primary's work scenario (`primary_iter')

#### Capturing the `leader_lock`

In a normally running cluster, the primary must hold the `leader_lock'.

Therefore, first of all, PgConsul finds out whether it is necessary to release the lock (and return as a replica), this may be the case:

* the primary should become a cascading replica (`stream_from` appeared in the settings)
* the primary tried to make a `rewind` and failed (it's strange that it's in `primary_iter')
* another host is already being promoted (`current_promoting_host` is set to ZK)

Either capture the lock if no one is holding it and the Postgres timeline matches the one recorded in ZK, or if there is no more recent primary. This may be the case, for example, with PgConsul reconnections/restarts.

Captures the `leader_lock', if this fails, it returns to the cluster as a replica.

Creates/deletes the necessary replication slots (is it strange that it's so early?).

Saves the current state of `db_state` to ZK (there will be `zk_state` on the trace. iterations)

Handles an incomplete failover/switchover: if the current host was supposed to become the primary, it simply cleans the data in ZK, otherwise it returns as a replica to the cluster.

#### Fixing problems

By this point, it is clear that the current host is the legitimate primary.
Therefore, PgConsul fixes the remaining problems, bringing the cluster to the "correct" state for this primary.

Starts the Pooler if it is not running.

Enables WAL archiving if it has been disabled.

#### Changing the replication type

PgConsul controls the type of replication used (`async`/`sync`/`quorum`), downgrading it to `async` if necessary. This is necessary so that the primary remains available in case of replica failure (degradation 2=>1 host).

PgConsul supports two work options implemented as classes `QuorumReplicationManager` (preferred) and `SingleSyncReplicationManager` (deprecated). Next, the logic is described for the `QuorumReplicationManager`.

PgConsul calculates a list of live HA replicas:
* those that hold `alive_lock`
* do replicate, those are visible in `pg_stat_replication` as `sync/quorum` (`_get_needed_replication_type`)

If it is a number:
* `> 0` - only those who actually replicate are recorded in `synchronous_standby_names`.
* `= 0` - switches Postgres to `async` replication

#### Checking the need for switchover

Checks that the scheduled switch flag has been set in ZK and makes a switchover (`_do_primary_switchover`)

### The HA replica's operation scenario (`replica_iter')

If there is no connection to ZK, it does not do anything.

Records the current status in ZK:
* is added/removed from the list of HA hosts
* updates information about wal_receiver
* updates information about his remarks

Checks that the scheduled switch flag has been set in ZK and makes a switchover (`_accept_switchover`)

If no one holds the `leader_lock`, it initiates the failover procedure (`_accept_failover`)

If the current replication source differs from the current one, the replica is rotated to a new primary (the one who holds the `leader_lock')

If replication does not work for some reason, the replica leaves the quorum.
This is important for 2-legged clusters, so that the lagging replica does not turn out to be the only candidate (and winner) in the primary elections. Next, the procedure for returning to the cluster is started (`replica_return`/`_return_to_cluster`)

If everything is OK:
* the replica opens access to the host for the client
* returns to the quorum
* configures slots for cascading replication

### The script of the broken (dead_iter)

In this situation, PgConsul is first and foremost:
* closes against load (stops the Pooler)
* withdraws from the quorum
* releases the `leader_lock` if it was held before
