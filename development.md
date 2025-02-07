# Local development
## Install dependencies
```shell
sudo apt install tox python3 python3-venv
```

## Test all features
```shell
make check
```

## Test specific feature
```shell
TEST_ARGS='-i archive.feature' make check
```
