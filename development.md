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
TEST_ARGS='-i archive.feature' make check_test
```

## Test specific scenario by line number
```shell
TEST_ARGS="-i kill_primary.feature:108" make check_test
```

## Test specific Scenario Outline by line number
```shell
TEST_ARGS="-i kill_primary.feature:175" make check_test
```

## Test specific tag
```shell
TEST_ARGS="--tags @fail_replication_source -i cascade.feature" make check_test
```

## Test with debug
```shell
DEBUG=1 TEST_ARGS="--tags @fail_replication_source -i cascade.feature" make check_test
```
Flags:
- `DEBUG` Save logs all steps (not only failed).

## Debug logs
- `logs/debug/test_execution.log` — test execution details, timing, retries
- Per-scenario logs: `test_execution_<scenario_name>.log`
- Failed step logs: `logs/<feature_file>/<line_number>/<hostname>/` — container logs (pgconsul, postgresql, pgbouncer, zookeeper)

## Manual test
```shell
TEST_ARGS='-i manual_test.feature' make check_test
```
After launch this command you have 10 hours for manual test with setup:
- 3 zookeeper
- 3 postgresql + pgconsul + pgbouncer
- woodpecker for test load

## Run unstoppable tests (continue on failure)
```shell
tox -e behave_unstoppable -- tests/features cascade.feature
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
