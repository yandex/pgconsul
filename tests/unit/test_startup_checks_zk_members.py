from unittest.mock import MagicMock, patch

import pytest


def test_existing_timeline_one_cluster_refuses_to_bootstrap_empty_zk():
    """pgconsul_util.feature:616: prev_state distinguishes restart from bootstrap."""
    from src.main import Pgconsul

    with patch('src.main.pgconsul.__init__', return_value=None):
        inst = Pgconsul.__new__(Pgconsul)
    inst.db = MagicMock()
    inst.zk = MagicMock()
    inst.config = MagicMock(iteration_timeout=1.0)
    inst.db.get_prev_state.return_value = {
        'pgdata': '/var/lib/postgresql/data',
    }
    inst.db.is_alive.return_value = True
    inst.db.is_ready_for_pg_rewind.return_value = True
    inst.db.get_timeline.return_value = 1
    inst.zk.get_members_retry.return_value = []
    inst.db.ensure_archive_mode.return_value = True

    with pytest.raises(SystemExit):
        inst.startup_checks()

    inst.db.pgpooler.assert_called_once_with('stop')


def _make_safe_quorum_instance():
    from src.main import Pgconsul

    with patch('src.main.pgconsul.__init__', return_value=None):
        inst = Pgconsul.__new__(Pgconsul)
    inst.db = MagicMock()
    inst.zk = MagicMock()
    inst.config = MagicMock(
        iteration_timeout=1.0,
        quorum_commit=True,
        use_lwaldump=True,

    )
    inst.db.get_prev_state.return_value = None
    inst.db.get_timeline.return_value = 1
    inst.zk.get_members_retry.return_value = ['replica']
    inst.db.is_alive.return_value = True
    inst.db.ensure_archive_mode.return_value = True
    inst.db.check_extension_installed.return_value = True
    return inst


def test_safe_quorum_requires_lwaldump_configuration():
    inst = _make_safe_quorum_instance()
    inst.config.use_lwaldump = False

    with pytest.raises(SystemExit):
        inst.startup_checks()


def test_safe_quorum_requires_installed_lwaldump_extension():
    inst = _make_safe_quorum_instance()
    inst.db.check_extension_installed.return_value = False

    with pytest.raises(SystemExit):
        inst.startup_checks()


def test_startup_warns_when_postgres_data_safety_settings_are_unsafe(caplog):
    inst = _make_safe_quorum_instance()
    inst.db.get_data_safety_settings.return_value = {
        'fsync': 'off',
        'synchronous_commit': 'local',
    }

    inst.startup_checks()

    assert 'DATA SAFETY IS NOT GUARANTEED: fsync is "off"' in caplog.text
    assert 'DATA SAFETY IS NOT GUARANTEED: synchronous_commit is "local"' in caplog.text


def test_startup_defers_data_safety_check_while_postgres_is_down(caplog):
    inst = _make_safe_quorum_instance()
    inst.db.is_alive.return_value = False

    inst.startup_checks()

    inst.db.get_data_safety_settings.assert_not_called()
    assert 'Data-safety checks are deferred' in caplog.text
