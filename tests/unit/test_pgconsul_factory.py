# encoding: utf-8
"""
Unit tests for build_pgconsul_config and create_pgconsul (ADR-0004).
"""
from configparser import RawConfigParser
from unittest.mock import MagicMock, patch

import pytest

from src.main import PgconsulConfig, build_pgconsul_config, create_pgconsul
from src.zk import create_zk
from src.zk_client import KazooState, ZkClient, ZkClientConfig


def _full_config(**section_overrides) -> RawConfigParser:
    """Return a RawConfigParser with all sections/keys consumed by build_pgconsul_config."""
    global_defaults = {
        'welcome_message': 'hello',
        'working_dir': '/var/lib/pgconsul',
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

    def test_returns_pgconsul_config_instance(self):
        config = _full_config()
        cfg = build_pgconsul_config(config)
        assert isinstance(cfg, PgconsulConfig)


class TestCreatePgconsul:
    """create_pgconsul builds all components and injects them into Pgconsul."""

    @pytest.mark.parametrize('failed_attempts, expected_delays', [
        (0, []),
        (1, []),
        (3, [6, 12]),
        (8, [6, 12, 24, 30, 30, 30, 30]),
    ])
    def test_startup_recovers_after_zookeeper_outage(self, failed_attempts, expected_delays):
        config = _full_config(**{'global': {
            'zk_lockpath_prefix': '/pgconsul',
            'release_lock_after_acquire_failed': 'yes',
        }})
        client = ZkClient(ZkClientConfig(
            hosts='localhost:2181', timeout=1, connect_max_delay=10,
            max_delay_on_reinit=30, path_prefix='/pgconsul',
        ))
        sessions = [MagicMock() for _ in range(failed_attempts + 1)]
        for session in sessions[:-1]:
            session.connected = False
        sessions[-1].connected = True
        sessions[-1].state = KazooState.CONNECTED

        with patch('src.main.create_command_manager'), \
             patch('src.main.create_postgres'), \
             patch('src.zk.create_zk_client', return_value=client), \
             patch('src.zk_client.KazooClient', side_effect=sessions), \
             patch('src.zk_client.uniform', side_effect=lambda low, high: high), \
             patch('src.zk_client.time.sleep') as sleep, \
             patch('src.main.create_replication_manager') as replication, \
             patch('src.main.create_replication_slot_manager'), \
             patch('src.main.TimingTracker'), \
             patch('src.main.Pgconsul.startup_checks'), \
             patch('src.main.register_sigterm_handler'):
            inst = create_pgconsul(config)

        assert inst.zk.is_alive()
        assert replication.call_args.args[2] is inst.zk
        assert [call.args[0] for call in sleep.call_args_list] == expected_delays
        for session in sessions:
            session.start_async.assert_called_once_with()
            session.start_async.return_value.wait.assert_called_once_with(1)
        for session in sessions[:-1]:
            session.remove_listener.assert_called_once_with(client._listener)
            session.stop.assert_called_once_with()
            session.close.assert_called_once_with()
        sessions[-1].stop.assert_not_called()
        sessions[-1].close.assert_not_called()

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
        mock_cmd.assert_called_once_with(config)
        mock_pg.assert_called_once_with(config=config, cmd_manager=mock_cmd.return_value)
        mock_zk.assert_called_once_with(config=config, retry_connection=True)
        mock_repl.assert_called_once_with(config, mock_pg.return_value, mock_zk.return_value)
        mock_slot.assert_called_once_with(config, mock_pg.return_value, mock_zk.return_value)
        mock_timings.assert_called_once()


def test_cli_zookeeper_connection_failure_does_not_retry():
    config = _full_config(**{'global': {
        'zk_lockpath_prefix': '/pgconsul',
        'release_lock_after_acquire_failed': 'yes',
    }})
    client = MagicMock()
    client.init.return_value = False

    with patch('src.zk.create_zk_client', return_value=client):
        with pytest.raises(Exception, match='Could not connect to ZK'):
            create_zk(config)

    client.reconnect.assert_not_called()
