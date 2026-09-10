# scripts/

Helper scripts for debugging pgconsul integration tests.

This directory contains standalone tools that automate the diagnostic workflow
described in the [Debugging Tests](../AGENTS.md#debugging-tests) section of
`AGENTS.md`. Each script is self-contained and can be copied into a running
test container.

---

## Contents

| Script / Package | Purpose |
|------------------|---------|
| [`analyze_failed_scenario.py`](analyze_failed_scenario.py) | Analyze failed behave test logs and pinpoint the root cause |
| [`failure_analyzer/`](failure_analyzer/) | Modular package behind `analyze_failed_scenario.py` (see [`failure_analyzer/README.md`](failure_analyzer/README.md)) |
| [`dump_zk.py`](dump_zk.py) | Dump all ZooKeeper records under a path prefix |

---

## analyze_failed_scenario.py

Automates steps 1–2 of the debugging workflow: scans `test_execution.log` for
failed steps, locates the corresponding container logs, and searches them for
known failure patterns (WAL divergence, pg_rewind failure, connection errors,
timeline mismatch, stuck loops, etc.). It ranks findings by likelihood and
prints a concise root-cause summary.

The script is a thin entry point that delegates to the
[`failure_analyzer`](failure_analyzer/) package — a modular implementation
with pluggable log sources, pre-compiled regex patterns, and separate
text/JSON reporters. See [`failure_analyzer/README.md`](failure_analyzer/README.md)
for architecture details and extension guide.

### Usage

```bash
# Auto-discover logs (logs/)
python scripts/analyze_failed_scenario.py

# Point at a specific log directory
python scripts/analyze_failed_scenario.py logs

# Show all findings (not just top 15)
python scripts/analyze_failed_scenario.py -v logs

# Machine-readable JSON for CI pipelines (parses cleanly from stdout)
python scripts/analyze_failed_scenario.py --format json logs

# Force pure-Python reader (no external grep dependency)
python scripts/analyze_failed_scenario.py --no-grep logs
```

### CLI reference

```
usage: analyze_failed_scenario.py [-h] [--verbose] [--docker] [--no-docker]
                                  [--format {text,json}] [--no-grep]
                                  [log_dirs ...]
```

| Option | Description |
| --- | --- |
| `log_dirs` | Log directories to search (default: auto-discover `logs/` and `logs.local/`). |
| `--verbose`, `-v` | Show all findings, not just the top 15. |
| `--docker` | Also scan live docker containers. Enabled automatically when no saved container logs are found. |
| `--no-docker` | Disable docker container scanning (overrides auto-detect). |
| `--format {text,json}` | Output format (default: `text`). `json` is intended for CI. |
| `--no-grep` | Use a pure-Python reader instead of the external `grep` binary (portable, slower). |

### Output streams

The report goes to **stdout**, while progress messages and diagnostic logs go
to **stderr**. This separation lets CI pipelines capture JSON from stdout
without contamination:

```bash
# Capture JSON report only
python scripts/analyze_failed_scenario.py --format json logs > report.json
```

The script handles large `postgresql.log` files (180+ MB) by using `grep` to
pre-filter DEBUG lines, keeping analysis time under ~15 seconds. Use
`--no-grep` to fall back to a pure-Python reader when `grep` is unavailable or
for debugging.

---

## dump_zk.py

A standalone script that dumps every ZK node (path + value) under the
configured `zk_lockpath_prefix`. It reads `/etc/pgconsul.conf` and connects
exactly the way pgconsul does — SSL (port 2281) + digest auth — so it works
out of the box inside a postgresql container, which already has
`python3-kazoo` and the SSL certs in `/etc/zk-ssl/`.

The script has **zero external dependencies** beyond the standard library and
`kazoo` (already installed in postgresql containers). It does not require the
pgconsul venv — the system `python3` is sufficient.

### Usage (inside a postgresql container)

```bash
# Copy the script into a running postgresql container
docker cp scripts/dump_zk.py pgconsul_postgresql1_1:/tmp/dump_zk.py

# Dump everything under the configured prefix (reads /etc/pgconsul.conf)
docker exec pgconsul_postgresql1_1 python3 /tmp/dump_zk.py

# JSON output (pipe to jq / diff against a known-good snapshot)
docker exec pgconsul_postgresql1_1 python3 /tmp/dump_zk.py --format json

# Tree structure only (paths, no values)
docker exec pgconsul_postgresql1_1 python3 /tmp/dump_zk.py --tree

# Override the path prefix
docker exec pgconsul_postgresql1_1 python3 /tmp/dump_zk.py --path /pgconsul/postgresql/

# Connect without a config file — pass everything explicitly
docker exec pgconsul_postgresql1_1 python3 /tmp/dump_zk.py \
    --hosts pgconsul_zookeeper1_1.pgconsul_pgconsul_net:2281 \
    --prefix /pgconsul/postgresql/ --ssl \
    --cert /etc/zk-ssl/server.crt --key /etc/zk-ssl/server.key \
    --ca /etc/zk-ssl/ca.cert.pem --auth --user user1 --password testpassword123
```

### CLI reference

```
usage: dump_zk.py [-h] [--config CONFIG] [--path PATH]
                  [--format {text,json}] [--tree] [--verbose]
                  [--hosts HOSTS] [--prefix PREFIX] [--ssl]
                  [--cert CERT] [--key KEY] [--ca CA] [--no-verify]
                  [--auth] [--user USER] [--password PASSWORD]
```

| Option | Description |
| --- | --- |
| `--config` | Path to pgconsul config (default: `/etc/pgconsul.conf`). |
| `--path` | ZK path prefix to dump (default: from config, e.g. `/pgconsul/postgresql/`). |
| `--format {text,json}` | Output format (default: `text`). |
| `--tree` | Show only the tree structure (paths, no values). |
| `--verbose`, `-v` | Enable debug logging on stderr. |
| `--hosts` | ZK hosts (e.g. `host:2281,...`) — skips config parsing. |
| `--prefix` | ZK path prefix (required with `--hosts`). |
| `--ssl` | Use SSL for ZK connection. |
| `--cert` / `--key` / `--ca` | SSL certificate / key / CA files. |
| `--no-verify` | Disable SSL cert verification. |
| `--auth` | Use digest auth for ZK. |
| `--user` / `--password` | ZK auth credentials. |

### Output streams

As with `analyze_failed_scenario.py`, the dump goes to **stdout** and
diagnostic logs go to **stderr**, so the output can be piped to a file or
`jq` without contamination:

```bash
docker exec pgconsul_postgresql1_1 python3 /tmp/dump_zk.py --format json > zk_state.json
```

### Example output (text)

```
pgconsul/postgresql/  =  <empty>
  leader  =  pgconsul_postgresql1_1
  last_leader  =  pgconsul_postgresql1_1
  failover_state  =  {
    "state": "idle",
    "host": null
  }
  all_hosts  =  <empty>
    pgconsul_postgresql1_1  =  <empty>
      prio  =  100
      ha  =  1
      op  =  primary
    pgconsul_postgresql2_1  =  <empty>
      prio  =  50
      ha  =  1
      op  =  replica
  ...
```
