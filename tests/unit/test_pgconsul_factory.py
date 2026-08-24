# encoding: utf-8
"""
Unit tests for build_pgconsul_config and create_pgconsul (ADR-0004).
"""
from configparser import RawConfigParser
from unittest.mock import MagicMock, patch

import pytest

from src.main import PgconsulConfig, build_pgconsul_config, create_pgconsul


def _full_config(**section_overrides) -> RawConfigParser:
    """Return a RawConfigParser with all sections/keys consumed by build_pgconsul_config."""
    global_defaults = {
        'welcome_message': 'hello',
        'working_dir': '/var/lib/pgconsul',
        'local_state_directory': '/custom/pgconsul-state',
        'iteration_timeout': '5.0',
        'quorum_commit': 'no',
        'use_lwaldump': 'no',
        'update_prio_in_zk': 'yes',
        'use_replication_slots': 'no',
        'replication_slots_polling': 'no',
        'priority': '100',
        'autofailover': 'yes',
        'switchover_replica_turn_timeout': '30.0',
        'switchover_rollback_timeout': '60.0',
        'switchover_catchup_timeout': '120.0',
        'max_rewind_retries': '3',
        'election_timeout': '10',
        'do_consecutive_primary_switch': 'no',
        'max_allowed_switchover_lag_ms': '1000',
    }
    global_defaults.update(section_overrides.pop('global', {}))
    replica_defaults = {
        'allow_potential_data_loss': 'no',
        'close_detached_after': '0.0',
        'start_pooler': 'yes',
        'recovery_timeout': '30.0',
        'can_delayed': 'no',
        'primary_switch_disable_archive_restore': 'no',
        'primary_switch_checks': '3',
        'primary_switch_restart': 'no',
        'primary_unavailability_timeout': '60.0',
        'walreceiver_disable_timeout': '10.0',
        'min_failover_timeout': '3600.0',
    }
    replica_defaults.update(section_overrides.pop('replica', {}))
    primary_defaults = {
        'change_replication_type': 'no',
        'sync_replication_in_maintenance': 'no',
    }
    primary_defaults.update(section_overrides.pop('primary', {}))
    debug_defaults = {
        'promote_checkpoint_sql': '',
        'failure_name': '',
        'failure_count': '100000000',
        'sleep_before_disable_walreceiver': '0',
        'election_lsn_read_sleep': '0',
        'election_loser_timeout': '0',
    }
    debug_defaults.update(section_overrides.pop('debug', {}))

    config = RawConfigParser()
    config['global'] = global_defaults
    config['replica'] = replica_defaults
    config['primary'] = primary_defaults
    config['debug'] = debug_defaults
    config['commands'] = {'log_timing': ''}
    return config


class TestBuildPgconsulConfig:
    """build_pgconsul_config parses all INI sections into PgconsulConfig."""

    def test_builds_all_fields(self):
        config = _full_config()
        cfg = build_pgconsul_config(config)

        assert cfg.welcome_message == 'hello'
        assert cfg.working_dir == '/var/lib/pgconsul'
        assert cfg.local_state_directory == '/custom/pgconsul-state'
        assert cfg.iteration_timeout == 5.0
        assert cfg.quorum_commit is False
        assert cfg.use_lwaldump is False
        assert cfg.update_prio_in_zk is True
        assert cfg.use_replication_slots is False
        assert cfg.replication_slots_polling is False
        assert cfg.priority == '100'
        assert cfg.stream_from is None
        assert cfg.autofailover is True
        assert cfg.switchover_replica_turn_timeout == 30.0
        assert cfg.switchover_rollback_timeout == 60.0
        assert cfg.switchover_catchup_timeout == 120.0
        assert cfg.max_rewind_retries == 3
        assert cfg.election_timeout == 10
        assert cfg.do_consecutive_primary_switch is False
        assert cfg.max_allowed_switchover_lag_ms == 1000
        assert cfg.allow_potential_data_loss is False
        assert cfg.close_detached_after == 0.0
        assert cfg.start_pooler is True
        assert cfg.recovery_timeout == 30.0
        assert cfg.can_delayed is False
        assert cfg.primary_switch_disable_archive_restore is False
        assert cfg.primary_switch_checks == 3
        assert cfg.primary_switch_restart is False
        assert cfg.primary_unavailability_timeout == 60.0
        assert cfg.walreceiver_disable_timeout == 10.0
        assert cfg.min_failover_timeout == 3600.0
        assert cfg.change_replication_type is False
        assert cfg.sync_replication_in_maintenance is False
        assert cfg.promote_checkpoint_sql == ''
        assert cfg.failure_name == ''
        assert cfg.failure_count == 100000000
        assert cfg.sleep_before_disable_walreceiver == 0.0
        assert cfg.election_lsn_read_sleep == 0.0
        assert cfg.election_loser_timeout == 0

    def test_stream_from_set(self):
        config = _full_config(**{'global': {'stream_from': 'upstream.example.com'}})
        cfg = build_pgconsul_config(config)
        assert cfg.stream_from == 'upstream.example.com'

    def test_local_state_directory_defaults_to_var_cache(self):
        config = _full_config()
        config.remove_option('global', 'local_state_directory')

        cfg = build_pgconsul_config(config)

        assert cfg.local_state_directory == '/var/cache/pgconsul'

    def test_returns_pgconsul_config_instance(self):
        config = _full_config()
        cfg = build_pgconsul_config(config)
        assert isinstance(cfg, PgconsulConfig)


class TestCreatePgconsul:
    """create_pgconsul builds all components and injects them into Pgconsul."""

    def test_returns_pgconsul_with_injected_deps(self):
        config = _full_config()
        with patch('src.main.create_command_manager') as mock_cmd, \
             patch('src.main.create_postgres') as mock_pg, \
             patch('src.main.create_zk') as mock_zk, \
             patch('src.main.create_replication_manager') as mock_repl, \
             patch('src.main.create_replication_slot_manager') as mock_slot, \
             patch('src.main.TimingTracker') as mock_timings, \
             patch('src.main.Pgconsul.startup_checks'), \
             patch('src.main.register_sigterm_handler'):
            inst = create_pgconsul(config)

        assert inst is not None
        assert {
            str(store.path.parent) for store in inst._local_states.values()
        } == {'/custom/pgconsul-state'}
        mock_cmd.assert_called_once_with(config)
        mock_pg.assert_called_once_with(config=config, cmd_manager=mock_cmd.return_value)
        mock_zk.assert_called_once_with(config=config)
        mock_repl.assert_called_once_with(config, mock_pg.return_value, mock_zk.return_value)
        mock_slot.assert_called_once_with(config, mock_pg.return_value, mock_zk.return_value)
        mock_timings.assert_called_once()
