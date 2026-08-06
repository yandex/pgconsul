# encoding: utf-8
# noqa: E501
"""
Unit tests for factory functions: create_postgres (src/pg.py) and
create_command_manager (src/command_manager.py).

Both factories build a dataclass config from a RawConfigParser-like object.
We use a real configparser.RawConfigParser populated with the sections/keys
the factories read, so the tests mirror production config shape closely.
"""

from configparser import RawConfigParser
from unittest.mock import MagicMock, patch

import pytest

from src.command_manager import (
    CommandManager,
    Commands,
    build_command_manager_config,
    create_command_manager,
)
from src.pg import Postgres, PostgresConfig, build_postgres_config, create_postgres


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _global_config(**overrides) -> RawConfigParser:
    """Return a RawConfigParser with a 'global' section suitable for build_postgres_config."""
    defaults = {
        'local_conn_string': 'host=localhost port=5432 dbname=postgres user=postgres',
        'use_lwaldump': 'no',
        'quorum_commit': 'no',
        'working_dir': '/tmp',
        'recovery_conf_rel_path': 'recovery.conf',
        'use_replication_slots': 'no',
        'standalone_pooler': 'no',
        'pooler_addr': 'localhost',
        'pooler_port': '6432',
        'pooler_conn_timeout': '1.0',
        'postgres_timeout': '5.0',
        'iteration_timeout': '5.0',
        'wals_to_upload': '20',
    }
    defaults.update(overrides)
    config = RawConfigParser()
    config['global'] = defaults
    return config


def _commands_config(**overrides) -> RawConfigParser:
    """Return a RawConfigParser with a 'commands' section suitable for build_command_manager_config."""
    defaults = {
        'promote': 'pg_ctl promote -D %p',
        'rewind': 'pg_rewind -D %p --source-server=%m',
        'get_control_parameter': 'pg_controldata %p',
        'pg_start': 'pg_ctl start -D %p -t %t',
        'pg_stop': 'pg_ctl stop -D %p -t %t %w',
        'pg_status': 'pg_ctl status -D %p',
        'pg_reload': 'pg_ctl reload -D %p',
        'pooler_start': 'systemctl start pgbouncer',
        'pooler_stop': 'systemctl stop pgbouncer',
        'pooler_status': 'systemctl status pgbouncer',
        'list_clusters': 'pg_lsclusters',
        'generate_recovery_conf': 'pg_basebackup -R -D %p -h %m',
    }
    defaults.update(overrides)
    config = RawConfigParser()
    config['commands'] = defaults
    return config


# ---------------------------------------------------------------------------
# Tests: build_command_manager_config / create_command_manager
# ---------------------------------------------------------------------------

class TestBuildCommandManagerConfig:
    """build_command_manager_config builds Commands from a RawConfigParser."""

    def test_builds_all_fields(self):
        config = _commands_config()
        cmds = build_command_manager_config(config)
        assert isinstance(cmds, Commands)
        assert cmds.promote == 'pg_ctl promote -D %p'
        assert cmds.rewind == 'pg_rewind -D %p --source-server=%m'
        assert cmds.get_control_parameter == 'pg_controldata %p'
        assert cmds.pg_start == 'pg_ctl start -D %p -t %t'
        assert cmds.pg_stop == 'pg_ctl stop -D %p -t %t %w'
        assert cmds.pg_status == 'pg_ctl status -D %p'
        assert cmds.pg_reload == 'pg_ctl reload -D %p'
        assert cmds.pooler_start == 'systemctl start pgbouncer'
        assert cmds.pooler_stop == 'systemctl stop pgbouncer'
        assert cmds.pooler_status == 'systemctl status pgbouncer'
        assert cmds.list_clusters == 'pg_lsclusters'
        assert cmds.generate_recovery_conf == 'pg_basebackup -R -D %p -h %m'

    def test_missing_section_raises(self):
        config = RawConfigParser()
        with pytest.raises(ValueError, match='No commands section in config'):
            build_command_manager_config(config)

    def test_custom_values(self):
        config = _commands_config(promote='custom-promote-cmd')
        cmds = build_command_manager_config(config)
        assert cmds.promote == 'custom-promote-cmd'


class TestCreateCommandManager:
    """create_command_manager wraps build_command_manager_config + CommandManager."""

    def test_returns_command_manager(self):
        config = _commands_config()
        cm = create_command_manager(config)
        assert isinstance(cm, CommandManager)
        assert isinstance(cm._commands, Commands)
        assert cm._commands.promote == 'pg_ctl promote -D %p'


# ---------------------------------------------------------------------------
# Tests: build_postgres_config / create_postgres
# ---------------------------------------------------------------------------

class TestBuildPostgresConfig:
    """build_postgres_config builds PostgresConfig from a RawConfigParser."""

    def test_builds_all_fields(self):
        config = _global_config()
        cfg = build_postgres_config(config)
        assert isinstance(cfg, PostgresConfig)
        assert cfg.conn_string == 'host=localhost port=5432 dbname=postgres user=postgres'
        assert cfg.use_lwaldump is False
        assert cfg.working_dir == '/tmp'
        assert cfg.recovery_filepath == 'recovery.conf'
        assert cfg.use_replication_slots is False
        assert cfg.standalone_pooler is False
        assert cfg.pooler_addr == 'localhost'
        assert cfg.pooler_port == 6432
        assert cfg.pooler_conn_timeout == 1.0
        assert cfg.postgres_timeout == 5.0
        assert cfg.iteration_timeout == 5.0
        assert cfg.wals_to_upload == 20

    def test_use_lwaldump_from_quorum_commit(self):
        """use_lwaldump is True when quorum_commit is True even if use_lwaldump is False."""
        config = _global_config(use_lwaldump='no', quorum_commit='yes')
        cfg = build_postgres_config(config)
        assert cfg.use_lwaldump is True

    def test_use_lwaldump_explicit(self):
        """use_lwaldump is True when explicitly set, regardless of quorum_commit."""
        config = _global_config(use_lwaldump='yes', quorum_commit='no')
        cfg = build_postgres_config(config)
        assert cfg.use_lwaldump is True

    def test_db_state_path_property(self):
        """db_state_path is derived from working_dir."""
        config = _global_config(working_dir='/var/lib/pgconsul')
        cfg = build_postgres_config(config)
        assert cfg.db_state_path == '/var/lib/pgconsul/.pgconsul_db_state.cache'

    def test_custom_wals_to_upload(self):
        config = _global_config(wals_to_upload='50')
        cfg = build_postgres_config(config)
        assert cfg.wals_to_upload == 50


class TestCreatePostgres:
    """create_postgres wraps build_postgres_config + Postgres."""

    def test_returns_postgres(self):
        config = _global_config()
        mock_cmd = MagicMock()
        mock_cmd.list_clusters.return_value = []

        with patch('src.pg.psycopg2.connect') as mock_connect, \
             patch.object(Postgres, 'get_role', return_value='primary'), \
             patch.object(Postgres, '_get_pgdata_path', return_value='/data/pg'):
            mock_connect.return_value = MagicMock()
            pg = create_postgres(config, mock_cmd)

        assert isinstance(pg, Postgres)
        assert pg.config.conn_string == 'host=localhost port=5432 dbname=postgres user=postgres'
        assert pg.config.use_lwaldump is False
