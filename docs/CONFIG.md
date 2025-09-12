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
