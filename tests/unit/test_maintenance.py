# encoding: utf-8
"""
Unit tests for MaintenanceHandler (step 12c, ADR-0004).
"""
from configparser import RawConfigParser
from unittest.mock import MagicMock, patch

import pytest

from src.maintenance import (
    MaintenanceHandler,
    MaintenanceHandlerConfig,
    build_maintenance_handler_config,
    create_maintenance_handler,
)


def _make_config(
    stream_from: str | None = None,
    change_replication_type: bool = True,
    sync_replication_in_maintenance: bool = False,
) -> MaintenanceHandlerConfig:
    return MaintenanceHandlerConfig(
        stream_from=stream_from,
        change_replication_type=change_replication_type,
        sync_replication_in_maintenance=sync_replication_in_maintenance,
    )


def _make_handler(config: MaintenanceHandlerConfig | None = None) -> MaintenanceHandler:
    zk = MagicMock()
    db = MagicMock()
    rm = MagicMock()
    return MaintenanceHandler(zk, db, config or _make_config(), rm)


class TestUpdateStatusEnable:
    """update_status with maintenance_status == 'enable'."""

    def test_sets_in_maintenance_true(self):
        handler = _make_handler()
        handler._zk.get_maintenance_status.return_value = 'enable'
        handler._config = _make_config(stream_from=None)

        handler.update_status({}, {}, is_single_node=False)

        assert handler.is_in_maintenance is True

    def test_non_ha_skips_zk_changes(self):
        handler = _make_handler(_make_config(stream_from='somehost'))
        handler._zk.get_maintenance_status.return_value = 'enable'

        handler.update_status({}, {}, is_single_node=False)

        assert handler.is_in_maintenance is True
        handler._zk.write_maintenance_ts.assert_not_called()
        handler._zk.write_maintenance_primary.assert_not_called()

    def test_writes_ts_and_primary_when_missing(self):
        handler = _make_handler(_make_config(stream_from=None))
        handler._zk.get_maintenance_status.return_value = 'enable'
        handler._zk.get_maintenance_ts.return_value = None
        handler._zk.get_maintenance_primary.return_value = None
        db_state = {'role': 'replica', 'primary_fqdn': 'host1'}

        handler.update_status(db_state, {}, is_single_node=False)

        handler._zk.write_maintenance_ts.assert_called_once()
        handler._zk.write_maintenance_primary.assert_called_once_with('host1')

    def test_does_not_overwrite_existing_ts_and_primary(self):
        handler = _make_handler(_make_config(stream_from=None))
        handler._zk.get_maintenance_status.return_value = 'enable'
        handler._zk.get_maintenance_ts.return_value = '123'
        handler._zk.get_maintenance_primary.return_value = 'existing'
        db_state = {'role': 'replica', 'primary_fqdn': 'host1'}

        handler.update_status(db_state, {}, is_single_node=False)

        handler._zk.write_maintenance_ts.assert_not_called()
        handler._zk.write_maintenance_primary.assert_not_called()

    def test_timeline_mismatch_stops_pooler_and_archiving(self):
        handler = _make_handler(_make_config(stream_from=None))
        handler._zk.get_maintenance_status.return_value = 'enable'
        handler._zk.TIMELINE_INFO_PATH = '/timeline'
        db_state = {'role': 'primary', 'alive': True, 'timeline': 1}
        zk_state = {'/timeline': 2}

        handler.update_status(db_state, zk_state, is_single_node=False)

        handler._db.pgpooler.assert_called_once_with('stop')
        handler._db.stop_archiving_wal.assert_called_once()

    def test_primary_replication_switch_returns_when_not_single_node(self):
        handler = _make_handler(_make_config(stream_from=None, change_replication_type=True))
        handler._zk.get_maintenance_status.return_value = 'enable'
        handler._db.get_replication_state.return_value = ('sync', None)
        handler._replication_manager.change_replication_to_async.return_value = True
        db_state = {'role': 'primary', 'alive': True, 'timeline': 5}
        zk_state = {handler._zk.TIMELINE_INFO_PATH: 5}

        handler.update_status(db_state, zk_state, is_single_node=False)

        handler._replication_manager.change_replication_to_async.assert_called_once()


class TestUpdateStatusDisable:
    """update_status with maintenance_status == 'disable' or ''."""

    def test_disable_sets_in_maintenance_false_and_deletes(self):
        handler = _make_handler(_make_config(stream_from=None))
        handler._is_in_maintenance = True
        handler._zk.get_maintenance_status.return_value = 'disable'

        handler.update_status({}, {}, is_single_node=False)

        assert handler.is_in_maintenance is False
        handler._zk.delete_maintenance.assert_called_once()

    def test_empty_string_also_disables(self):
        handler = _make_handler(_make_config(stream_from=None))
        handler._zk.get_maintenance_status.return_value = ''

        handler.update_status({}, {}, is_single_node=False)

        assert handler.is_in_maintenance is False
        handler._zk.delete_maintenance.assert_called_once()

    def test_non_ha_does_not_delete_on_disable(self):
        handler = _make_handler(_make_config(stream_from='somehost'))
        handler._zk.get_maintenance_status.return_value = 'disable'

        handler.update_status({}, {}, is_single_node=False)

        handler._zk.delete_maintenance.assert_not_called()


class TestUpdateStatusNone:
    """update_status with maintenance_status == None."""

    def test_none_sets_in_maintenance_false(self):
        handler = _make_handler()
        handler._zk.get_maintenance_status.return_value = None

        handler.update_status({}, {}, is_single_node=False)

        assert handler.is_in_maintenance is False


class TestUpdateStatusUnexpected:
    """update_status with unexpected maintenance_status."""

    def test_unexpected_logs_error(self):
        handler = _make_handler()
        handler._zk.get_maintenance_status.return_value = 'bogus'

        with patch('src.maintenance.logging') as mock_logging:
            handler.update_status({}, {}, is_single_node=False)

        mock_logging.error.assert_called_once()


class TestUpdateReplicationOnMaintenanceEnter:
    """_update_replication_on_maintenance_enter logic."""

    def test_change_replication_type_disabled_returns_true(self):
        handler = _make_handler(_make_config(change_replication_type=False))
        assert handler._update_replication_on_maintenance_enter() is True

    def test_sync_allowed_returns_true(self):
        handler = _make_handler(
            _make_config(change_replication_type=True, sync_replication_in_maintenance=True)
        )
        assert handler._update_replication_on_maintenance_enter() is True

    def test_already_async_returns_true(self):
        handler = _make_handler(_make_config(change_replication_type=True))
        handler._db.get_replication_state.return_value = ('async', None)
        assert handler._update_replication_on_maintenance_enter() is True

    def test_sync_switches_to_async(self):
        handler = _make_handler(_make_config(change_replication_type=True))
        handler._db.get_replication_state.return_value = ('sync', None)
        handler._replication_manager.change_replication_to_async.return_value = True
        assert handler._update_replication_on_maintenance_enter() is True
        handler._replication_manager.change_replication_to_async.assert_called_once()


class TestBuildMaintenanceHandlerConfig:
    """build_maintenance_handler_config parses INI sections."""

    def test_parses_all_fields(self):
        cp = RawConfigParser()
        cp.read_string("""
[global]
stream_from = somehost

[primary]
change_replication_type = yes
sync_replication_in_maintenance = no
""")
        cfg = build_maintenance_handler_config(cp)
        assert cfg.stream_from == 'somehost'
        assert cfg.change_replication_type is True
        assert cfg.sync_replication_in_maintenance is False

    def test_stream_from_defaults_to_none(self):
        cp = RawConfigParser()
        cp.read_string("""
[primary]
change_replication_type = no
sync_replication_in_maintenance = no
""")
        cfg = build_maintenance_handler_config(cp)
        assert cfg.stream_from is None


class TestCreateMaintenanceHandler:
    """create_maintenance_handler builds handler with injected deps."""

    def test_creates_handler_with_config(self):
        cp = RawConfigParser()
        cp.read_string("""
[global]

[primary]
change_replication_type = yes
sync_replication_in_maintenance = no
""")
        db = MagicMock()
        zk = MagicMock()
        rm = MagicMock()

        handler = create_maintenance_handler(cp, db, zk, rm)

        assert isinstance(handler, MaintenanceHandler)
        assert handler._config.change_replication_type is True
        assert handler._db is db
        assert handler._zk is zk
        assert handler._replication_manager is rm


class TestIsInMaintenanceProperty:
    """is_in_maintenance is a read-only property backed by _is_in_maintenance."""

    def test_defaults_to_false(self):
        handler = _make_handler()
        assert handler.is_in_maintenance is False

    def test_reflects_internal_flag(self):
        handler = _make_handler()
        handler._is_in_maintenance = True
        assert handler.is_in_maintenance is True

    def test_is_read_only(self):
        handler = _make_handler()
        try:
            handler.is_in_maintenance = True
            raise AssertionError('Expected AttributeError (read-only property)')
        except AttributeError:
            pass
