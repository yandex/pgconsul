from unittest.mock import MagicMock, patch

import pytest
from kazoo.exceptions import NotEmptyError

from src.exceptions import ResetException
from src.zk import ZookeeperException

with patch.dict('sys.modules', {'yaml': MagicMock()}), \
     patch('src.read_config', create=True), \
     patch('src.init_logging', create=True):
    from src.cli import reset_all


def test_reset_all_uses_maintenance_deletion(zk):
    zk.get_root_children = MagicMock(return_value=[zk.MEMBERS_PATH, 'other', zk.MAINTENANCE_PATH])
    zk.delete = MagicMock(return_value=True)
    zk.delete_maintenance = MagicMock(return_value=True)
    context = MagicMock()
    context.__enter__.return_value = zk
    opts = MagicMock(force=True, timeout=5)

    with patch('src.cli.create_zk', return_value=context), patch('src.cli.enable_maintenance'):
        reset_all(opts, MagicMock())

    zk.delete.assert_called_once_with('other', recursive=True)
    zk.delete_maintenance.assert_called_once_with()


def test_reset_all_reports_maintenance_deletion_failure(zk):
    zk.get_root_children = MagicMock(return_value=['maintenance'])
    zk.delete_maintenance = MagicMock(side_effect=ZookeeperException('not empty'))
    context = MagicMock()
    context.__enter__.return_value = zk
    opts = MagicMock(force=True, timeout=5)

    with patch('src.cli.create_zk', return_value=context), patch('src.cli.enable_maintenance'):
        with pytest.raises(ResetException, match='Could not reset node "maintenance"'):
            reset_all(opts, MagicMock())


def test_reset_all_retries_maintenance_with_recreated_child(zk):
    zk.get_root_children = MagicMock(return_value=[zk.MAINTENANCE_PATH])
    zk._zk_client._kazoo.delete.side_effect = [NotEmptyError('new child'), None]
    context = MagicMock()
    context.__enter__.return_value = zk
    opts = MagicMock(force=True, timeout=5)

    with patch('src.cli.create_zk', return_value=context), \
         patch('src.cli.enable_maintenance'), \
         patch('src.zk.time.sleep'):
        reset_all(opts, MagicMock())

    assert zk._zk_client._kazoo.delete.call_count == 2
    zk._zk_client._kazoo.delete.assert_called_with('/pgconsul/maintenance', recursive=True)
