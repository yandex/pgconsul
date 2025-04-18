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
