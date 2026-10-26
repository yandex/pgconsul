from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.main import Pgconsul


def test_rewind_fail_flag_releases_primary_lock_and_skips_iteration():
    instance = Pgconsul.__new__(Pgconsul)
    instance.config = SimpleNamespace(working_dir='/tmp')
    instance.db = MagicMock()
    instance.zk = MagicMock()
    instance.zk.PRIMARY_LOCK_PATH = 'master'
    instance.finish_iteration = MagicMock()

    with patch.object(instance, 'is_rewind_flag_set', return_value=True):
        instance.run_iteration('100')

    instance.zk.release_if_hold.assert_called_once_with(instance.zk.PRIMARY_LOCK_PATH)
    instance.db.is_alive_and_in_terminal_state.assert_not_called()
    instance.finish_iteration.assert_called_once()
