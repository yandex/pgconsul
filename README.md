# PgConsul

## Purpose

PgConsul is a tool for maintaining High-Availability Postgresql cluster configurations. It is responsible for cluster recovery in case of emergencies.

## Scope

* Adjusting replication mode depending on cluster nodes connectivity.
* Switching the primary role across cluster members, if needed.
* Isolating a node from the load in the event of an abnormal situation.

## How it works

### Overview

Once started, pgconsul enters the processing loop where it performs the following once a second:

1. Checks for Zookeeper lock.
2. Collects information about the replicas and primary status.
3. Writes the collected information to ZK.
4. Decides whether to interfere with the cluster operation.

Step 4 currently depends on the following factors:

* Interaction with ZK is running smoothly.
* Whether the current instance holds a primary or synchronous replica lock.
* Whether there is a primary lock in ZK.
* Whether there are active replicas in the cluster.
* Replication type meets the requirements from the config.

More details:

* [Deailed algorithm description](./docs/en/ALGO.md)
* [Switchover process](./docs/en/SWITCHOVER.md)
* [Failover process](./docs/en/FAILOVER.md)
* [Zookeeper data structures](./docs/en/ZK.md)

### Usage

The delivery kit includes pgconsul-util that enables you to switch to another primary or initialize a cluster if it is run from a backup or the ZK address changes.

Switch primary to another (most recent) replica with 300s timeout.
```
pgconsul-util switchover -y -b -t 300
```

Switch primary to the particular replica `host2`
```
pgconsul-util switchover -y -b -t 300 -d host2
```

Reset swtichover information in ZK. Useful for interrupting a stuck switchover.
```
pgconsul-util switchover -r
```

Enable maintenance mode (pause pgconsul activity)
```
pgconsul-util maintenance -m enable
```

Disable maintenance mode (resume pgconsul activity)
```
pgconsul-util maintenance -m disable
```

Show maintenance status
```
pgconsul-util maintenance -m show
```

For a detailed key description, see `pgconsul-util --help` and `pgconsul-util <command> --help`.

### Configuration

Here is configuration [example](./docs/en/CONFIG.md) with some explanations.

### External components

* [Kazoo](https://github.com/python-zk/kazoo) is used to interact with Zookeeper
* [Psycopg2](https://github.com/psycopg/psycopg2) is used to interact with Postgresql

#### Migration to a different prefix or address in ZK

PgConsul has protection set up against running a working cluster in "empty" ZK. This is done to avoid the consequences of the configuration error (see 1.4.)
If the current instance's timeline exceeds 1 at startup (meaning that primary promote was performed at least once), while `<prefix>/all_hosts@ZK` contains no child node, pgconsul crashes.

At the same time, startup like this may be required, for example, under a managed change of the ZK address or prefix.

To do this, you can use the utility's `initzk` mode, e.g.:

```
pgconsul-init --config pgconsul.conf --zk new.zk.addr:port --prefix /new_prefix pg01a.fq.dn pg01b.fq.dn pg01c.fq.dn
```

Unless otherwise specified, the ZK prefixes and addresses are used from the configuration (by default, `/etc/pgconsul.conf`).
The only required parameter is a list of space-separated hostnames.
