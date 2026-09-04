from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src import cli
from src.types import DurabilityConfig, DurabilityState


def _zk_context(zk):
    context = MagicMock()
    context.__enter__.return_value = zk
    return context


def _options(hostname='replica', wait=None, absent=False):
    return SimpleNamespace(hostname=hostname, wait=wait, absent=absent)


def test_durability_exclude_records_start_time_and_waits_for_stable_removal():
    zk = MagicMock()
    zk.exists_path.return_value = True
    zk.set_durability_exclusion.return_value = True

    with patch('src.cli.create_zk', return_value=_zk_context(zk)), \
         patch('src.cli.time.time', return_value=123.0), \
         patch('src.cli._wait_durability_membership', return_value=True) as wait:
        cli.durability_exclude(_options(wait=30.0), MagicMock())

    zk.set_durability_exclusion.assert_called_once_with('replica', 123.0)
    wait.assert_called_once_with(zk, 'replica', False, 30.0)


def test_durability_include_wait_timeout_is_nonzero_exit():
    zk = MagicMock()
    zk.clear_durability_exclusion.return_value = True

    with patch('src.cli.create_zk', return_value=_zk_context(zk)), \
         patch('src.cli._wait_durability_membership', return_value=False):
        with pytest.raises(SystemExit) as error:
            cli.durability_include(_options(wait=1.0), MagicMock())

    assert error.value.code == 2


def test_durability_check_accepts_absent_stable_member():
    zk = MagicMock()
    zk.get_durability_state.return_value = (
        DurabilityState(DurabilityConfig.build(['primary'])), 1,
    )

    with patch('src.cli.create_zk', return_value=_zk_context(zk)):
        cli.durability_check(_options(absent=True), MagicMock())
