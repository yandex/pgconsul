# pgconsul-util command-line reference

`pgconsul-util` is the administrative CLI for reading cluster state and
starting operations coordinated through ZooKeeper.

## Global options

Global options must precede the subcommand:

```text
pgconsul-util [-c <path>] [--zk <hosts>] [--zk-prefix <path>] <command>
```

| Option | Default | Description |
|---|---|---|
| `-c`, `--config <path>` | `/etc/pgconsul.conf` | Pgconsul configuration file |
| `--zk <fqdn:port,...>` | configuration value | Override the ZooKeeper connection string |
| `--zk-prefix <path>` | configuration value | Override the ZooKeeper cluster prefix |

Run `pgconsul-util --help` or `pgconsul-util <command> --help` for the
effective help of the installed version.

## `info`

Print the combined PostgreSQL and ZooKeeper cluster state.

```console
pgconsul-util info
pgconsul-util info --short
pgconsul-util info --json
```

| Option | Description |
|---|---|
| `-s`, `--short` | Print a reduced ZooKeeper view |
| `-j`, `--json` | Emit JSON instead of YAML |

## `switchover`

Start a planned primary switch. The primary, timeline, and destination are
normally detected from ZooKeeper.

```console
pgconsul-util switchover -y --block --timeout 300
pgconsul-util switchover -y --block --destination pg2.example.net
pgconsul-util switchover --reset
```

| Option | Description |
|---|---|
| `-d`, `--destination <fqdn>` | Select the candidate explicitly |
| `-b`, `--block` | Wait for completion or failure |
| `-t`, `--timeout <sec>` | Operation wait timeout; default `60` |
| `-y`, `--yes` | Do not ask for confirmation |
| `-r`, `--reset` | Reset switchover state in ZooKeeper |
| `--replicas <count>` | In blocking mode, wait for this many replicas |
| `--primary <fqdn>` | Override the detected old primary |
| `--timeline <number>` | Override the detected primary timeline |

`--reset` is disruptive and should only be used after the operation state has
been inspected. See [SWITCHOVER.md](./SWITCHOVER.md) for the protocol.

## `failover`

Start an operator-requested failover. Without data-loss override, pgconsul
uses the ordinary fenced voting and quorum-safety checks.

```console
pgconsul-util failover
pgconsul-util failover --reset
```

### Explicit data-loss mode

`--with-data-loss` allows the operator to choose a winner without a complete
durability read-quorum:

```console
pgconsul-util failover --with-data-loss --timeout 30
```

The CLI prints every available versioned vote in descending timeline, LSN,
priority order and prompts for an FQDN. Empty input chooses the freshest LSN on
the highest voted timeline. The result is labelled `SAFE` or `UNSAFE` and
explains failed safety predicates. The explicit selection is honored even when
it is unsafe.

```text
timeline  lsn                 priority  wal-fenced  host
12        0/5A001F20          100       yes         pg3.example.net
12        0/59FFE810          200       yes         pg2.example.net
12        0/58001000          300       yes         pg4.example.net

Host to promote [pg3.example.net]: pg2.example.net
pg2.example.net: SAFE
  - host does not have the maximum LSN on its timeline
```

By default, participants disable `restore_command`, clear `primary_conninfo`,
and wait for walreceiver to stop before voting. `--no-wal-fencing` skips this
fence. The CLI warns that vote positions may move and always reports the
selection as unsafe:

```console
pgconsul-util failover --with-data-loss --no-wal-fencing
```

If the CLI exits before storing a winner, repeating the same command resumes
the request. The WAL-fencing mode must match the existing request.

| Option | Description |
|---|---|
| `--with-data-loss` | Permit an operator-selected winner without enough votes |
| `--no-wal-fencing` | Keep archive restore and walreceiver enabled; requires `--with-data-loss` |
| `-t`, `--timeout <sec>` | Time to collect votes; default `60` |
| `-y`, `--yes` | Select the default winner without prompting |
| `-r`, `--reset` | Reset failover state in ZooKeeper |

`UNSAFE` means that pgconsul cannot prove preservation of acknowledged data.
See [FAILOVER.md](./FAILOVER.md) and [DATA_SAFETY.md](./DATA_SAFETY.md).

## `maintenance` (`maint`)

Control maintenance mode:

```console
pgconsul-util maintenance --mode enable --wait_all
pgconsul-util maintenance --mode show
pgconsul-util maintenance --mode disable --wait_all
```

| Option | Description |
|---|---|
| `-m`, `--mode enable\|disable\|show` | Requested mode; default `enable` |
| `-w`, `--wait_all` | Wait for all alive HA hosts to acknowledge the mode |
| `-t`, `--timeout <sec>` | `--wait_all` timeout; default `300` |

## `initzk`

Create membership nodes for a cluster or check that they already exist:

```console
pgconsul-util initzk pg1.example.net pg2.example.net pg3.example.net
pgconsul-util initzk --test pg1.example.net pg2.example.net pg3.example.net
```

`--test` does not create nodes. It exits successfully only when every supplied
member is already initialized.

## `reset-all`

Delete all cluster nodes under the configured ZooKeeper prefix except the
membership list, while coordinating maintenance mode:

```console
pgconsul-util reset-all
pgconsul-util reset-all --force --timeout 300
```

| Option | Description |
|---|---|
| `-f`, `--force` | Skip the destructive confirmation prompt |
| `-t`, `--timeout <sec>` | Maintenance coordination timeout; default `300` |

This command is destructive. Verify `--zk`, `--zk-prefix`, and the member list
before confirming it.
