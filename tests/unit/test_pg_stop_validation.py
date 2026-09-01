import pytest

from src.command_manager import validate_pg_stop_command


@pytest.mark.parametrize('command', [
    'pg_ctl stop -m smart -D %p',
    'pg_ctl stop --mode smart -D %p',
    'pg_ctl stop --mode=smart -D %p',
    'pg_ctl stop -msmart -D %p',
])
def test_pg_stop_rejects_smart_shutdown(command):
    with pytest.raises(ValueError, match='smart'):
        validate_pg_stop_command(command)


@pytest.mark.parametrize('command', [
    'pg_ctl stop -m fast -D %p',
    'pg_ctl stop --mode immediate -D %p',
    'pg_ctl stop --mode=fast -D %p',
    'pg_ctl stop -mimmediate -D %p',
    'service postgresql stop',
])
def test_pg_stop_allows_non_smart_shutdown(command):
    validate_pg_stop_command(command)
