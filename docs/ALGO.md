# PgConsul iteration algorithm

PgConsul reconciles PostgreSQL and ZooKeeper one bounded iteration at a time.
It does not keep an in-memory operation workflow: cluster-operation state is
persisted in ZooKeeper, and return-to-cluster progress is persisted locally on
the affected host.

## Iteration order

```text
+-------------------+
| Read DB and ZK    |
+---------+---------+
          |
          v
+-------------------+
| Refresh liveness  |
| and maintenance   |
+---------+---------+
          |
          v
+-------------------+  only authoritative primary; one bounded step
| Reconcile          |
| durability         |
+---------+---------+
          |
          v
+-------------------+--- active return state ----------> [END]
| Resume local       |
| return-to-cluster |
+---------+---------+
          |
          v
+-------------------+--- local host must return -------> [END]
| Reconcile desired |
| primary epoch     |
+---------+---------+
          |
          v
+-------------------+--- active failover owns ---------> [END]
| Resume active     |
| failover          |
+---------+---------+
          |
          v
+-------------------+--- owning switchover step -------> [END]
| Resume active     |
| switchover        |
+---------+---------+
          |
          v
+-------------------+--- failover started -------------> [END]
| Start failover if |
| health probe says |
| primary failed    |
+---------+---------+
          |
          v
+-------------------+
| Role reconciliation: primary / replica / dead |
+-------------------+
          |
          v
        [END]
```

An operation normally makes one idempotent, bounded step and is retried from
its persisted state on the next iteration. An exception or unavailable
ZooKeeper does not create a synthetic successful state.

## Ownership and precedence

- A local return-to-cluster state owns the affected host. The host cannot
  become a coordinator, vote, or report readiness until it streams from the
  current primary.
- An active failover owns cluster-operation routing. The coordinator is the
  holder of `epoch_manager`; other electorate members only fence WAL sources,
  vote, promote if elected, or return to the winner.
- A switchover record owns its protocol phases. `WAITING_ARCHIVE` is
  deliberately non-blocking for the new primary and side replicas; the old
  primary remains fenced.
- Durability reconciliation never owns an iteration. It may make one safe
  primary-side step before the operation routers run, so an operation can
  still observe and react to a failure in the same iteration.

## Persistent state and sources of truth

| Concern | Persistent state | Owner |
|---|---|---|
| Primary epoch | ZooKeeper primary lock, timeline and `desired_primary` | current primary / operation coordinator |
| Durability membership | ZooKeeper `durability_members` state | authoritative primary through the durability reconciler |
| Failover | ZooKeeper failover phase, version, CAS-fenced durability state, votes and winner | `epoch_manager` coordinator |
| Switchover | Versioned ZooKeeper `switchover/record` and operation ACKs | switchover manager |
| Return to cluster | Host-local `return_to_cluster_state.json` | local daemon |

Every cross-host mutation is versioned or fenced by a ZooKeeper lock. A daemon
restart therefore resumes the recorded phase rather than replaying a guessed
action.

## Detailed protocols

- [Durability membership changes](DURABILITY.md)
- [Failover](FAILOVER.md)
- [Switchover](SWITCHOVER.md)
- [Return to cluster](RETURN_TO_CLUSTER.md)
- [Data-safety contract](DATA_SAFETY.md)
