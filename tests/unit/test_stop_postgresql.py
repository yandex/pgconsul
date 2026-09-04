from unittest.mock import MagicMock, patch


def test_stop_postgresql_does_not_change_replication_mode():
    from src.main import Pgconsul

    with patch('src.main.pgconsul.__init__', return_value=None):
        instance = Pgconsul.__new__(Pgconsul)
    instance.db = MagicMock()
    instance._durability_manager = MagicMock()

    instance.stop_postgresql(timeout=17, wait=False)

    instance._durability_manager.change_replication_to_async.assert_not_called()
    instance.db.stop_postgresql.assert_called_once_with(timeout=17, wait=False)
