### Configuration

Automatic replication-mode changes use only the availability of streaming HA
replicas. Automatic failover always enforces the durability contract. Data loss
can be authorized only for one explicit operator request with
`pgconsul-util failover --with-data-loss`.

At startup pgconsul checks `fsync` and `synchronous_commit`. Unsafe values do
not prevent startup, but emit `DATA SAFETY IS NOT GUARANTEED` at CRITICAL level.
If PostgreSQL is unavailable, the check is deferred until the first iteration
with SQL access. The check is diagnostic because session-level settings can
still be changed by clients.

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

# Directory for host-local failover and switchover progress files.
# The daemon user must be able to create and remove files here.
local_state_directory = /var/cache/pgconsul

# Local PG instance connection string.
# This role must own or be able to create and truncate
# public.pgconsul_durability_barrier in this database.
local_conn_string = dbname=postgres user=postgres connect_timeout=1

# Additional parameters in case of connecting to the primary.
# Used to invoke pg_rewind.
append_rewind_conn_string = port=5432 dbname=postgres user=xxx password=xxx connect_timeout=10 sslmode=verify-full

# Connection string used to verify if PG is available.
append_primary_conn_string = port=6432 dbname=postgres user=xxx password=xxx connect_timeout=1 sslmode=verify-full

# Timeout in seconds between main loop iterations (see above).
iteration_timeout = 1

# Deadline for blocking external commands except promote and pg_rewind.
# pg_rewind is intentionally unbounded; promote has its own deadline below.
external_command_timeout = 60

# Deadline for the promote command and its PostgreSQL role transition.
promote_timeout = 300

# Client deadline for one WAL-barrier attempt. An expired attempt has an
# unknown outcome and is safely retried with the same operation ID.
wal_barrier_timeout = 60

# Overall deadline for switchover preparation and promotion. Before the
# committed handoff, expiry rolls the operation back. After the handoff and
# before the candidate promotion ACK, expiry starts fenced failover recovery.
# The deadline no longer applies after the promotion ACK.
switchover_timeout = 180

# Use a PostgreSQL build that supports EVERY(...), ANY ... in
# synchronous_standby_names.
use_pg_patches = no

# Use a PostgreSQL build that supports pg_ctl promote --timeline N.
# target_promote must also be configured in [commands].
use_target_promote = no

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

# Fetch a timeline history file from the WAL archive.
# %f is the history filename, %p is a temporary destination path.
fetch_timeline_history = wal-g wal-fetch %f %p

# Required when use_target_promote=yes. %a is the reserved timeline.
target_promote = pg_ctl promote --timeline %a -w -t %t -D %p

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

# Async logging configuration
# Maximum number of log records in queue before dropping new ones
async_log_queue_size = 5000

# Optional welcome message to display on pgconsul startup
# If empty, no message is displayed
welcome_message =

# Number of WAL files to upload before promoting a replica to primary.
wals_to_upload = 20

# Read a failover vote's durable LSN by scanning local pg_wal with the
# lwaldump extension. Required for quorum_commit. PostgreSQL's receive position
# is lost on restart and its replay
# position may still lag WAL already flushed before that restart, so neither is
# a safe fallback. A missing or failing extension blocks failover.
use_lwaldump = yes

[primary]
# Whether to change the replication type to synchronous (or asynchronous)
# Only done if there is a lock in ZK.
change_replication_type = yes

# If no HA replica is streaming for this many seconds, switch to async.
# Seeing a streaming HA replica resets the timer and requires sync again.
before_async_unavailability_timeout = 15

# Number of checks after which the old primary becomes a replica of the new primary.
primary_switch_checks = 3

# Delay in seconds before removing a replica from quorum after it loses the quorum lock in ZooKeeper.
# Values: 0 (immediate removal, default), 1-120 (delayed removal).
# Recommended: 30-60 seconds for protection against transient network issues.
# Note: In a 2-node cluster, this may cause write downtime up to the configured value if a replica actually fails.
quorum_removal_delay = 0

[replica]
# A durability replica reports the primary unavailable only after both the
# PostgreSQL endpoint and its local WAL replay position have remained still for
# this many seconds. Failover requires Q(D) responses to one fresh probe ID.
# After WAL fencing, the same timeout is the grace period for the old primary
# to release its leader lock before the coordinator version-deletes its holder node.
primary_unavailability_timeout = 5

# Whether to start connection pooler on the replica if no anomalies are detected.
start_pooler = yes

# Number of checks after which the replica will change the primary (replication source).
primary_switch_checks = 5

# Interval (sec) during which new failover attempts are not allowed. The counter is started after the last failover.
min_failover_timeout = 3600

# Timeout for individual external recovery commands such as stop and start.
recovery_timeout = 60

# Maximum time with SQL available but without WAL replay LSN progress before
# return-to-cluster stops PostgreSQL and tries pg_rewind.
return_lsn_stall_timeout = 60

# Maximum time in PostgreSQL "starting up" state without changes in startup
# process WAL descriptors, I/O counters, or pg_controldata recovery fields.
# On expiry the host is marked RESETUP_REQUIRED.
return_startup_stall_timeout = 300

### Command safety

The `[commands] pg_stop` command must not request PostgreSQL's `smart`
shutdown mode, because it can wait indefinitely for clients to disconnect.
Startup rejects `-m smart`, `--mode smart`, `--mode=smart`, and `-msmart`.
`fast`, `immediate`, and commands without an explicit shutdown mode are allowed.

```
