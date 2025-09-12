# FAILOVER

failover is the process of unplanned primary switching due to hardware failure./networking on the current primary or Postgres process crash

## The failover process

The failover process starts with the fact that the `leader_lock' in ZK disappears.

This can happen in the following cases::

* Real network loss / disconnection from ZK
* PgConsul can release the lock itself if the local Postgres is inoperable (see the calls `release_lock`, `release_lock_and_return_to_cluster`)

Next, one (or more) replicas detect the loss of the `leader_lock' and the failover process begins.

The process consists of 3 parts
* Opportunity check (`_can_do_failover`)
* Selecting the next wizard (unexpectedly also inside `_can_do_failover')
* Launch (promote) a new wizard

### Opportunity check

The replica verifies that:
* failover is allowed in the settings
* The timeline of the replica coincides with the timeline of the cluster before the failover
* enough time has passed since the last failover so as not to switch "in a circle"
* since the disappearance of the "leader_lock"-and enough time has passed to exclude the primary's collapses
* the wizard is indeed unavailable via the SQL protocol, which would exclude ZK collapses
* the replica has finished applying the WAL, and is ready for failover
* the number of live replicas, more than half of those that made up the quorum.
  This ensures that at least one of them has the latest LSN from the primary.

### Choosing a new primary

* all replicas involved in the selection stop using WAL
* participants try to capture the lock `ELECTION_MANAGER_LOCK_PATH`, one of them captures and becomes the election manager
* All participants record their current LSN in ZK
* the manager compares them, determines the winner, records the result in ZK
* the winner gets True when returning from `_can_do_failover', the rest are False

### Launch (promote) a new wizard

The winner of the election
* captures the `leader_lock`
* creates replication slots on the new primary
* actually does pg_ctl promote
