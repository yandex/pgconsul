import json
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.main import Pgconsul
from src.types import SsnInfo, ZkState
from src.zk import ZookeeperException


@pytest.mark.parametrize('zk_available', [True, False])
def test_rewind_fail_flag_releases_primary_lock_and_skips_iteration(tmp_path, zk_available):
    status_file = tmp_path / 'pgconsul.status'
    standby_names = {'primary': {}, 'replica': {}}
    status_file.write_text(json.dumps({
        'zk_state': {'lock_holder': 'primary', 'synchronous_standby_names': standby_names},
    }))
    instance = Pgconsul.__new__(Pgconsul)
    instance.config = SimpleNamespace(working_dir=str(tmp_path))
    instance.db = MagicMock()
    instance.notifier = MagicMock()
    instance.zk = MagicMock()
    instance.zk.PRIMARY_LOCK_PATH = 'master'
    instance.finish_iteration = MagicMock()
    if zk_available:
        instance.zk.get_state.return_value = ZkState(
            alive=True,
            lock_holder='new-primary',
            timeline=7,
            synchronous_standby_names={'new-primary': SsnInfo(value=None, last_update=None)},
        )
    else:
        instance.zk.get_state.side_effect = ZookeeperException('ZooKeeper unavailable')

    started_at = time.time()
    with patch.object(instance, 'is_rewind_flag_set', return_value=True):
        instance.run_iteration('100')

    instance.zk.release_if_hold.assert_called_once_with(instance.zk.PRIMARY_LOCK_PATH)
    instance.db.is_alive_and_in_terminal_state.assert_not_called()
    instance.notifier.notify.assert_called_once_with()
    instance.finish_iteration.assert_called_once()
    status = json.loads(status_file.read_text())
    if zk_available:
        assert started_at <= status['ts'] <= time.time()
        assert status['status'] == 'waiting'
        assert status['db_state'] is None
        assert status['zk_state']['lock_holder'] == 'new-primary'
        assert status['zk_state']['timeline'] == 7
        assert status['zk_state']['synchronous_standby_names'] == {'new-primary': [None, None]}
    else:
        assert status == {'zk_state': {'lock_holder': 'primary', 'synchronous_standby_names': standby_names}}
