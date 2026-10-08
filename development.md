# Local development

## Setup

### Linux
```shell
sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo apt install tox python3 python3-venv
```

### Mac OS
```shell
brew install colima qemu tox docker docker-compose docker-buildx
colima status && colima start
```
Make Docker find the Homebrew plugins — add into `~/.docker/config.json`:
```json
{
    "cliPluginsExtraDirs": [
        "/opt/homebrew/lib/docker/cli-plugins"
    ]
}
```
(Alternatively, symlink each plugin into `~/.docker/cli-plugins/`.)

### Verify the Docker plugins resolve
```shell
docker compose version
docker buildx version
```

## Test types
- [Unit](./tests/unit/) — `make unit_test`.
- [Behave](./tests/features/) with [steps](./tests/steps/) — BDD integration on a real ZooKeeper + PostgreSQL + pgconsul cluster in Docker — `make check_test`.
- [Jepsen](https://github.com/jepsen-io/jepsen) consistency testing — `make jepsen`.
- **Lint / types** — `make lint`

Find detailed explanations/commands below.

## Unit tests (no Docker)
Fast feedback loop, no containers — runs via a dedicated tox env that sets up the venv and
installs pytest plus the runtime deps:
```shell
make unit_test
make unit_test_coverage
```

## Build
Run this once before the feature-test commands below — it builds the container images the
behave harness runs against (requires the Docker daemon running, e.g. `colima start`).
```shell
make build
```

### PostgreSQL version
`PG_MAJOR` selects the Postgres major version (default `14`). Set it for `build` **and** the
test commands — keep them consistent, since `build` bakes the images for that version:
```shell
PG_MAJOR=16 make build        # same as: make build PG_MAJOR=16
PG_MAJOR=16 make check_test
```

## Test all features
```shell
make check_test
```

## Test specific feature
```shell
TEST_ARGS='-i archive.feature' make check_test
```

## Test with debug
```shell
export DEBUG=true

TEST_ARGS='-i cascade.feature -t @fail_replication_source' make check_test
```

## Manual test
```shell
TEST_ARGS='-i manual_test.feature' make check_test
```
After launch this command you have 10 hours for manual test with setup:
- 3 zookeeper
- 3 postgresql + pgconsul + pgbouncer
