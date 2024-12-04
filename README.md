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

### Sequence of actions

**1. Initialization. Loading plugins**

* 1.1. Making sure there is no stop flag. If there is, pgconsul won't start.
* 1.2. Checking connectivity between PG and ZK. If ZK can't be reached and the current role is primary, connection pooler stops. If PG is not operable, while ZK is up and running, then primary lock is released.
* 1.3. Checking whether pg_rewind can be run. If not, pgconsul terminates.
* 1.4. Making sure the active cluster is not run on "empty" ZK, for this is most likely caused by an error in the configuration. If both of these conditions are met, pgconsul terminates:
  * 1.4.1. Checking if there are child nodes in ZK at `<prefix>/all_hosts`.
  * 1.4.2. Checking the timeline of the current instance: it must exceed 1.
* 1.5. In blocking mode, creating a child node with the current instance's hostname at `<prefix>/all_hosts/<hostname>`.

**2. Main loop**

* 2.1. Waiting during `global:iteration_timeout`
* 2.2. Identifying the current local role and status of PG.
* 2.3. Identifying the status of ZK (if there is a connect).
* 2.4. Writing a status file with information from 2.2. and 2.3.
* 2.5. Depending on the role (primary, replica, or malfunction), different checks and actions are performed. For details, see steps 3, 4, and 5 below.
* 2.6. Reinitializing a connection to PG and ZK if it is lost.

**3. Actions and checks performed if the local role is "primary"**

* 3.1. Trying to get a primary lock. If the attempt fails:
  * 3.1.1. Local connection pooler stops.
  * 3.1.2. If the lock holder is not determined, a connection is reinitialized. Return to (step 2)
  * 3.1.3. If the holder is determined and this is not the current instance, the host actually turns into a replica. The role transfer procedure is described in 4.3.
* 3.2. Writing replica information to ZK if the ZK information about the timeline matches that received from PG.
* 3.3. If ZK timeline matches the local timeline but step 3.2. failed, stop connection pooler and return to step 2.
* 3.4. If ZK has no timeline information, the local one is written.
* 3.5. If the local and ZK timelines are different:
  * 3.5.1. Make a checkpoint.
  * 3.5.2. If the ZK timeline exceeds the local one, stop connection pooler and go to step 2.
  * 3.5.3. If the local timeline exceeds the ZK timeline, information in ZK is overwritten.
* 3.6. Starting connection pooler.
* 3.7. If `primary:change_replication_type` is set:
  * 3.7.1. Compare the current and desired replication type: sync or async.
  * 3.7.2. Set the appropriate replication type. If sync, also set the name of the host holding a sync_replica lock.

**4. Actions and checks performed if the local role is "replica"**

* 4.1. Checking if there is connectivity to ZK. If not, return to step 2.
* 4.2. Checking if there is a primary lock. If not, do a failover: (any exception is captured and pgconsul is aborted).
  * 4.2.1. Making some checks. If any of them fails, return to step 2:
    * 4.2.1.1. Checking if the current instance is a sync replica.
    * 4.2.1.2. Checking if enough time has passed since the last failover attempt (set by the `replica:min_failover_timeout` option).
    * 4.2.1.3. If the ZK timeline is determined, compare it with the current one. If not, skip this check.
    * 4.2.1.4. Making sure a sufficient number of loops completed (see `replica:failover_checks`).
    * 4.2.1.5. Making sure the primary is actually dead by making `SELECT 42` from the host specified in the recovery.conf file (make an attempt each time; if no response is received, increment the counter until it exceeds the `replica:dead_primary_checks` value).
  * 4.2.2. Getting a ZK primary lock. If the attempt fails, return to step 2.
  * 4.2.3. Trying to delete information about the status of the previous failover. If the attempt fails, release the primary lock and return to step 2.
  * 4.2.4. If replication_slots is used:
    * 4.2.4.1. Marking in ZK that the failover status is "creating_slots".
    * 4.2.4.2. Reading the list of cluster members in the shard and excluding the current instance from the list. If the attempt fails, release the lock and return to step 2.
    * 4.2.1.3. Creating replication_slots.
  * 4.2.5. Marking in ZK that the failover status is "promoting". Trying to run `pg_ctl promote`. If the attempt fails, release the lock and return to step 2.
  * 4.2.6. Waiting until PG is up and running (during `global:iteration_timeout`)
  * 4.2.7. Marking in ZK that the failover status is "checkpointing" and making a checkpoint.
  * 4.2.8. Writing the current timeline in ZK, updating the failover status to "finished", and setting the current time to last_failover_time.
* 4.3. Checking if the local information about the primary role location matches the address of the host holding the primary lock. Otherwise:
  * 4.3.1. Stop connection pooler.
  * 4.3.2. If the number of primary_switch attempts does not exceed the `<primary|replica>:primary_switch_checks` value or the instance is under transition, go to step 2. (`self._return_to_cluster()`)
  * 4.3.3. If PG is already in the failover state, go to step 2.
  * 4.3.4. If PG is being restored from an archive, do a failover:
    * 4.3.4.1. Creating and filling out a `recovery.conf` file pointing to the new primary.
    * 4.3.4.2. Waiting until PG gets consistent.
    * 4.3.4.3. Waiting until the primary starts sending WALs.
    * 4.3.4.4. Return to step 2.
  * 4.3.5. If the rewind retries counter exceeds the `global:max_rewind_retries` value, set a regular flag (see 1.1.) and abort pgconsul with an error message returned.
  * 4.3.6. Making a rewind retry, as step 3.1.3.4. failed. If the attempt does not succeed, go to step 2.
    * 4.3.6.1. Stopping PG if it does not change to the normal state (in terms of pg_control).
    * 4.3.6.2. Deleting the `recovery.conf` file and disabling archiving for a while.
    * 4.3.6.3. Running PG and resetting `postgresql.auto.conf`.
    * 4.3.6.4. Setting a rewind lock in ZK, running a rewind, and releasing the lock.
    * 4.3.6.5. Repeat the actions similar to 3.1.3.4.
  * 4.3.7. If the replication slots are enabled, add them.
* 4.4. Checking that ZK contains information about the current replica and it is marked as "streaming". Otherwise:
  * 4.4.1. If the current replica's type is "sync", release the sync replica lock.
  * 4.4.2. Making a checkpoint.
  * 4.4.3. If the current timeline is less than ZK timeline by 1:
    * 4.4.3.1. Wait for logs from the primary during `replica:recovery_timeout`. Otherwise, make a failover retry (step 4.3).
* 4.5. If `replica:start_pooler` is set to "yes", start connection pooler.
* 4.6. If the current replica is marked as "streaming", try to get a sync replica lock.

**5. Actions and checks performed if the local role can't be identified**

* 5.1. Stopping connection pooler.
* 5.2. Releasing the primary and sync replica locks in ZK if the current instance is holding them.
* 5.3. Based on the previously saved state (see 2.2 and 2.3), trying to determine the role, primary, timeline, PG version, and pgdata directory location. In the event of a failure or if this information is unavailable, assign the "replica" role and set the last primary to None.
* 5.4. Checking if there is an active primary lock:
  * 5.4.1. Comparing the hostname of its holder with information from 5.3. If the previous local role was "replica" and the primary has not changed, try to run PG and return to step 2.
  * 5.4.2. If the primary has changed or the previous local role was different from "replica", switch the local instance to "replica" mode and then follow step 4.3.
* 5.5. If there are no active locks (the cluster is inactive):
  * 5.5.1. If the previous role was "primary" and the timeline information from ZK does not match the latest local timeline, return to step 2.

### pgconsul-util

The delivery kit includes pgconsul-util that enables you to switch to another primary or initialize a cluster if it is run from a backup or the ZK address changes.
For a detailed key description, see `pgconsul-util --help` and `pgconsul-util <command> --help`.

#### Scheduled primary switch

PgConsul supports scheduled switching over to a different primary. This functionality assumes that the primary role switches over to the current synchronous replica.
To initiate this, use `switchover` mode, e.g.:

```
pgconsul-util -c pgconsul.conf switchover
2017-01-19 15:50:32,583 DEBUG: lock holders: {u'sync_replica': u'pgtest01i.some.net', u'primary': u'pgtest01h.some.net', u'timeline': 38}
2017-01-19 15:50:32,583 INFO: switchover pgtest01h.some.net (timeline: 38) to pgtest01i.some.net
type "yes" to continue: yes
2017-01-19 15:50:35,157 INFO: initiating switchover with {u'timeline': 38, u'hostname': u'pgtest01h.some.net'}
2017-01-19 15:50:35,173 DEBUG: No lock instance for switchover/primary. Creating one.
2017-01-19 15:50:35,531 DEBUG: state: {u'info': {u'timeline': 38, u'hostname': u'pgtest01h.some.net'}, u'progress': u'scheduled', u'failover': u'finished', u'replicas': [{u'replay_location_diff': 128, u'write_location_diff': 0, u'sync_state': u'sync', u'sent_location_diff': 0, u'primary_location': u'5/760B6700', u'client_hostname': u'pgtest01i.some.net', u'state': u'streaming'}, {u'replay_location_diff': 128, u'write_location_diff': 0, u'sync_state': u'async', u'sent_location_diff': 0, u'primary_location': u'5/760B6700', u'client_hostname': u'pgtest01f.some.net', u'state': u'streaming'}]}
2017-01-19 15:50:35,673 DEBUG: current switchover status: scheduled, failover: finished
2017-01-19 15:50:36,832 DEBUG: current switchover status: initiated, failover: switchover_initiated
2017-01-19 15:50:38,258 DEBUG: current switchover status: initiated, failover: switchover_primary_shut
2017-01-19 15:50:39,401 DEBUG: current switchover status: promoting_replica, failover: promoting
2017-01-19 15:50:40,559 DEBUG: current switchover status: promoting_replica, failover: promoting
2017-01-19 15:50:41,689 DEBUG: current switchover status: promoting_replica, failover: promoting
2017-01-19 15:50:42,897 DEBUG: current switchover status: promoting_replica, failover: promoting
2017-01-19 15:50:45,079 INFO: primary is now pgtest01i.some.net
2017-01-19 15:50:45,142 DEBUG: full state: {u'info': {u'timeline': 38, u'hostname': u'pgtest01h.some.net'}, u'progress': u'finished', u'failover': u'finished', u'replicas': [{u'replay_location_diff': 128, u'write_location_diff': 0, u'sync_state': u'sync', u'sent_location_diff': 0, u'primary_location': u'5/760B6780', u'client_hostname': u'pgtest01i.some.net', u'state': u'streaming'}, {u'replay_location_diff': 128, u'write_location_diff': 0, u'sync_state': u'async', u'sent_location_diff': 0, u'primary_location': u'5/760B6780', u'client_hostname': u'pgtest01f.some.net', u'state': u'streaming'}]}
2017-01-19 15:50:45,142 DEBUG: waiting for replicas to appear...
2017-01-19 15:50:46,206 DEBUG: replicas up: pgtest01h.some.net@5/77002098
2017-01-19 15:50:47,270 DEBUG: replicas up: pgtest01h.some.net@5/77002198
2017-01-19 15:50:48,335 DEBUG: replicas up: pgtest01h.some.net@5/77002198
2017-01-19 15:50:49,416 DEBUG: replicas up: pgtest01h.some.net@5/770024F8
2017-01-19 15:50:50,497 DEBUG: replicas up: pgtest01h.some.net@5/770024F8
2017-01-19 15:50:51,561 DEBUG: replicas up: pgtest01f.some.net@5/77002580, pgtest01h.some.net@5/77002580
2017-01-19 15:50:51,561 INFO: switchover finished, status "finished"
```

By default, the 60s timeout is set for switching over, starting the primary, and having replicas appear in `streaming` status (for each stage). You can override the parameter value with the`--timeout` option. The expected amount of replicas is set using the `--replicas` option and defaults to 2.
If the switchover fails for some reason and/or it is required to reset the switchover status (for example, there is a typo when explicitly setting the primary or timeline), use the `--reset` option. However, as this functionality involves intervening in the distributed algorithm, you should only do this if there is a guarantee that no switchover will occur. Otherwise, there is a risk of failover for the cluster.
In addition, you can explicitly set the primary and timeline to switch over. Please keep in mind that, if they differ from the actual ones, the pgconsul logic will ignore them.

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

### Configuration

Pay special attention to the following:

1. You can set up the `change_replication_type` and `change_replication_metric` parameters so that pgconsul does not change the replication type at all. Or, in the event of issues, it only degrades to asynchronous replication at daytime, while always performs synchronous replication at nighttime and weekends when the load is lower.

2. The `allow_potential_data_loss` parameter assumes switching the primary even if none of the replicas is synchronous (i.e., with data loss). In this case, the replica with the older xlog position becomes a new primary.

#### Sample configuration with a description

```ini
[global]
# Username the daemon will run under.
daemon_user = postgres

# Log file path. If the path is relative, the parent directory will be working_dir (below)
log_file = /var/log/pgconsul/pgconsul.log

# Startup without going to background mode
foreground = no

# Log details. Possible values: debug, info, warning, error, and critical.
log_level = debug

# Path to the pid file.
pid_file = /var/run/pgconsul/pgconsul.pid

# Daemon working directory (cwd)
working_dir = /tmp

# Local PG instance connection string.
local_conn_string = dbname=postgres user=postgres connect_timeout=1

# Additional parameters in case of connecting to the primary.
# Used to invoke pg_rewind.
append_rewind_conn_string = port=5432 dbname=postgres user=xxx password=xxx connect_timeout=10 sslmode=verify-full

# Connection string used to verify if PG is available.
append_primary_conn_string = port=6432 dbname=postgres user=xxx password=xxx connect_timeout=1 sslmode=verify-full

# Timeout in seconds between main loop iterations (see above).
iteration_timeout = 1

# Zookeeper connection string
zk_hosts = zk02d.some.net:2181,zk02e.some.net:2181,zk02g.some.net:2181

# Path to the directory with executable files from the PG delivery kit (pg_rewind, pg_controldata, pg_ctl)
bin_path = /usr/lib/postgresql/9.6/bin

# Whether to use replication_slots if the roles change
use_replication_slots = yes

# Command to generate the recovery.conf file. The following arguments are passed to the command:
# # %m is the primary hostname
# # %p is the full path to the recovery.conf file
generate_recovery_conf = /usr/local/yandex/populate_recovery_conf.py -s -r -p %p %m

# Maximum number pg_rewind retries. Once this number is reached, pgysnc sets a flag and aborts (see)
max_rewind_retries = 3

# Whether connection pooler is used as a standalone instance
standalone_pooler = yes

# Address at which the connection pooler check is running if standalone_pooler = yes
pooler_addr = localhost

# Port at which the connection pooler check is running if standalone_pooler = yes
pooler_port = 6432

# Timeout of the connection pooler check at address:port in seconds
pooler_conn_timeout = 1

[primary]
# Whether to change the replication type to synchronous (or asynchronous)
# Only done if there is a lock in ZK.
change_replication_type = yes

# Criterion for changing the replication type:
# 'count' means that replication becomes asynchronous if all replicas are down
#           and synchronous if at least one replica is available.
# 'load' means that replication becomes asynchronous if the number of sessions exceeds overload_sessions_ratio.
#           If this parameter returns to the normal value, replication becomes synchronous again.
# 'time' indicates that the replication type will only change at the specified time. Requires that the count or load is present (see above)
change_replication_metric = count,load,time

# Session number threshold (including inactive ones), after reaching which the replication type should be changed (if the respective argument is set above)
overload_sessions_ratio = 75

# Schedule for disabling synchronous replication: if the current time falls within the set interval, pgconsul may disable synchronous replication.
# In the example below, the weekday change hours are specified and weekend ones are set to "never".
weekday_change_hours = 10-22
weekend_change_hours = 0-0

# Number of checks after which the old primary becomes a replica of the new primary.
primary_switch_checks = 3

[replica]
# Number of checks after which a synchronous replica becomes the primary.
failover_checks = 3

# Whether to start connection pooler on the replica if no anomalies are detected.
start_pooler = yes

# Number of checks after which the replica will change the primary (replication source).
primary_switch_checks = 5

# Interval (sec) during which new failover attempts are not allowed. The counter is started after the last failover.
min_failover_timeout = 3600

# Allow a failover if a cluster has no synchronous replicas.
allow_potential_data_loss = no

# Cluster instance recovery timeout. Once the set threshold is reached, pg_rewind is started.
recovery_timeout = 60

# Number of primary availability check retries via the PG protocol before a failover is run.
# Relevant if there is no connectivity between ZK and the current primary.
dead_primary_checks = 86400
```

### External components

* [Kazoo](https://github.com/python-zk/kazoo) is used to interact with Zookeeper
* [Psycopg2](https://github.com/psycopg/psycopg2) is used to interact with Postgresql
