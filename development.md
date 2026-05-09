# Local development
## Build
```shell
make build
```

## Test all features
```shell
make check_test
```

## Test specific feature
```shell
make check_test TEST_ARGS="tests/features/archive.feature"
```

## Test specific scenario by line number
```shell
make check_test TEST_ARGS="tests/features/kill_primary.feature:108"
```

## Test specific Scenario Outline by line number
```shell
make check_test TEST_ARGS="tests/features/kill_primary.feature:175"
```

## Test specific tag
```shell
make check_test TEST_ARGS="--tags @fail_replication_source tests/features/cascade.feature"
```

## Test with debug
```shell
DEBUG=1 make check_test TEST_ARGS="--tags @fail_replication_source tests/features/cascade.feature"
```
Flags:
- `DEBUG` Save logs all steps (not only failed).

## Debug logs
- `logs/debug/test_execution.log` — test execution details, timing, retries
- Per-scenario logs: `test_execution_<scenario_name>.log`
- Failed step logs: `logs/<feature_file>/<line_number>/<hostname>/` — container logs (pgconsul, postgresql, pgbouncer, zookeeper)

## Manual test
```shell
make check_test TEST_ARGS='tests/features/manual_test.feature' 
```
After launch this command you have 10 hours for manual test with setup:
- 3 zookeeper
- 3 postgresql + pgconsul + pgbouncer
- woodpecker for test load

## Run unstoppable tests (continue on failure)
```shell
tox -e behave_unstoppable -- tests/features/cascade.feature
```

## Run local on Linux
```shell
sudo apt install tox python3 python3-venv
```

## Run local on Mac OS
```shell
brew install colima qemu tox docker docker-compose
sudo ln -s ~/.colima/docker.sock /var/run/docker.sock
colima status && colima start
```

### Add into ~/.docker/config.json
```json
{
    "cliPluginsExtraDirs": [
        "/opt/homebrew/lib/docker/cli-plugins"
    ]
}
```
