from unittest.mock import MagicMock, call, patch

import pytest
from kazoo.exceptions import NotEmptyError

from src.exceptions import ResetException
from src.zk import ZookeeperException

with patch('src.read_config', create=True), \
     patch('src.init_logging', create=True):
    from src.cli import reset_all


def test_reset_all_uses_maintenance_deletion(zk):
    zk.get_root_children = MagicMock(return_value=[zk.MEMBERS_PATH, 'other', zk.MAINTENANCE_PATH])
    zk.delete = MagicMock(return_value=True)
    zk.write_maintenance_status = MagicMock(return_value=True)
    zk.delete_maintenance = MagicMock(return_value=True)
    zk.get_maintenance_status = MagicMock(return_value=None)
    calls = MagicMock()
    calls.attach_mock(zk.delete, 'delete')
    calls.attach_mock(zk.write_maintenance_status, 'write_maintenance_status')
    calls.attach_mock(zk.delete_maintenance, 'delete_maintenance')
    context = MagicMock()
    context.__enter__.return_value = zk
    opts = MagicMock(force=True, timeout=5)

    with patch('src.cli.create_zk', return_value=context), patch('src.cli.enable_maintenance'):
        reset_all(opts, MagicMock())

    assert calls.mock_calls == [
        call.delete('other', recursive=True),
        call.write_maintenance_status('disable'),
        call.delete_maintenance(),
    ]


def test_reset_all_reports_maintenance_cleanup_timeout(zk):
    zk.get_root_children = MagicMock(return_value=['maintenance'])
    zk.write_maintenance_status = MagicMock(return_value=True)
    zk.delete_maintenance = MagicMock(return_value=False)
    context = MagicMock()
    context.__enter__.return_value = zk
    opts = MagicMock(force=True, timeout=5)

    with patch('src.cli.create_zk', return_value=context), \
         patch('src.cli.enable_maintenance'), \
         patch('src.cli.helpers.await_for', return_value=False):
        with pytest.raises(ResetException, match='Could not reset node "maintenance"'):
            reset_all(opts, MagicMock())
    zk.write_maintenance_status.assert_called_once_with('disable')
    zk.delete_maintenance.assert_called_once_with()


def test_reset_all_reports_maintenance_disable_failure(zk):
    zk.get_root_children = MagicMock(return_value=[zk.MAINTENANCE_PATH])
    zk.write_maintenance_status = MagicMock(side_effect=ZookeeperException('ZK unavailable'))
    zk.delete_maintenance = MagicMock()
    context = MagicMock()
    context.__enter__.return_value = zk
    opts = MagicMock(force=True, timeout=5)

    with patch('src.cli.create_zk', return_value=context), patch('src.cli.enable_maintenance'):
        with pytest.raises(ResetException, match='Could not reset node "maintenance"') as exc_info:
            reset_all(opts, MagicMock())

    assert isinstance(exc_info.value.__cause__, ZookeeperException)
    zk.delete_maintenance.assert_not_called()


def test_reset_all_keeps_maintenance_enabled_when_other_deletion_fails(zk):
    zk.get_root_children = MagicMock(return_value=['other'])
    zk.delete = MagicMock(return_value=False)
    zk.write_maintenance_status = MagicMock()
    zk.delete_maintenance = MagicMock()
    context = MagicMock()
    context.__enter__.return_value = zk
    opts = MagicMock(force=True, timeout=5)

    with patch('src.cli.create_zk', return_value=context), patch('src.cli.enable_maintenance'):
        with pytest.raises(ResetException, match='Could not reset node "other"'):
            reset_all(opts, MagicMock())

    zk.write_maintenance_status.assert_not_called()
    zk.delete_maintenance.assert_not_called()


def test_reset_all_waits_for_daemon_after_recreated_child(zk):
    zk.get_root_children = MagicMock(return_value=[zk.MAINTENANCE_PATH])
    zk._zk_client._kazoo.delete.side_effect = NotEmptyError('new child')
    zk.get_maintenance_status = MagicMock(side_effect=['disable', None])
    context = MagicMock()
    context.__enter__.return_value = zk
    opts = MagicMock(force=True, timeout=5)

    def wait_for_absence(predicate, timeout, event_name):
        assert predicate() is False
        return predicate()

    with patch('src.cli.create_zk', return_value=context), \
         patch('src.cli.enable_maintenance'), \
         patch('src.cli.helpers.await_for', side_effect=wait_for_absence):
        reset_all(opts, MagicMock())

    zk._zk_client._kazoo.delete.assert_called_once_with('/pgconsul/maintenance', recursive=True)
    assert zk.get_maintenance_status.call_count == 2
