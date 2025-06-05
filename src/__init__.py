"""
Automatic failover of PostgreSQL with help of ZK
"""

# encoding: utf-8

import logging
import os
import sys

from configparser import RawConfigParser
from argparse import ArgumentParser
from pwd import getpwnam

from lockfile import AlreadyLocked
from lockfile.pidlockfile import PIDLockFile
import daemon

from .command_manager import CommandManager, Commands
from .main import PgconsulConfig, pgconsul
from .pg import Postgres, PostgresConfig
from .plugin import PluginRunner, Plugins, load_plugins
from .replication_manager import (
    QuorumReplicationManager,
    ReplicationManager,
    ReplicationManagerConfig,
    SingleSyncReplicationManager,
)
from .types import PluginsConfig
from .zk import Zookeeper, ZookeeperConfig


def parse_cmd_args():
    """
    Parse args and return result
    """
    usage = "Usage: %prog [options]"
    parser = ArgumentParser(usage=usage)
    parser.add_argument("-c", "--config", dest="config_file", default='/etc/pgconsul.conf')
    parser.add_argument("-p", "--pid-file", dest="pid_file", default=None)
    parser.add_argument("-l", "--log-file", dest="log_file", default=None)
    parser.add_argument("-f", "--foreground", dest="foreground", default="no")
    parser.add_argument("--log-level", dest="log_level", default=None)
    parser.add_argument("-w", "--working-dir", dest="working_dir", default=None)
    return parser.parse_args()


def read_config(filename=None, options=None):
    """
    Merge config with default values and cmd options
    """
    defaults: dict[str, dict] = {
        'global': {
            'log_file': '/var/log/pgconsul/pgconsul.log',
            'log_level': 'debug',
            'pid_file': '/var/run/pgconsul/pgconsul.pid',
            'working_dir': '.',
            'foreground': 'no',
            'local_conn_string': 'dbname=postgres ' + 'user=postgres connect_timeout=1',
            'append_primary_conn_string': 'connect_timeout=1',
            'iteration_timeout': 1.0,
            'zk_hosts': 'localhost:2181',
            'zk_lockpath_prefix': None,
            'plugins_path': '/etc/pgconsul/plugins',
            'recovery_conf_rel_path': 'recovery.conf',
            'use_replication_slots': 'no',
            'max_rewind_retries': 3,
            'postgres_timeout': 60,
            'election_timeout': 5,
            'priority': 0,
            'update_prio_in_zk': 'yes',
            'standalone_pooler': 'yes',
            'pooler_port': 6432,
            'pooler_addr': 'localhost',
            'pooler_conn_timeout': 1,
            'stream_from': None,
            'autofailover': 'yes',
            'do_consecutive_primary_switch': 'no',
            'quorum_commit': 'no',
            'use_lwaldump': 'no',
            'zk_connect_max_delay': 60,
            'zk_auth': 'no',
            'zk_username': None,
            'zk_password': None,
            'zk_ssl': 'no',
            'keyfile': None,
            'certfile': None,
            'ca_cert': None,
            'verify_certs': 'no',
            'drop_slot_countdown': 300,
            'replication_slots_polling': None,
            'max_allowed_switchover_lag_ms': 60000,
            'release_lock_after_acquire_failed': 'yes',
            'election_loser_timeout': 0,  # Timeout for election losers. For test purposes only.
        },
        'primary': {
            'change_replication_type': 'yes',
            'change_replication_metric': 'count,load',
            'overload_sessions_ratio': 75,
            'weekday_change_hours': '10-22',
            'weekend_change_hours': '0-0',
            'primary_switch_checks': 3,
            'sync_replication_in_maintenance': 'yes',
            'before_async_unavailability_timeout': 15,
        },
        'replica': {
            'primary_unavailability_timeout': 5,
            'start_pooler': 'yes',
            'primary_switch_checks': 3,
            'min_failover_timeout': 3600,
            'allow_potential_data_loss': 'no',
            'recovery_timeout': 60,
            'can_delayed': 'no',
            'primary_switch_restart': 'yes',
            'close_detached_after': 300,
        },
        'commands': {
            'promote': '/usr/lib/postgresql/10/bin/pg_ctl promote -D %p',
            'rewind': "/usr/lib/postgresql/10/bin/pg_rewind"
            " --target-pgdata=%p --source-server='host=%m connect_timeout=10'",
            'get_control_parameter': "/usr/lib/postgresql/10/bin/pg_controldata %p | grep '%a:'",
            'pg_start': 'sudo service postgresql-10 start',
            'pg_stop': '/usr/lib/postgresql/10/bin/pg_ctl stop -s -m fast -w -t %t -D %p',
            'pg_status': 'sudo service postgresql-10 status',
            'pg_reload': '/usr/lib/postgresql/10/bin/pg_ctl reload -s -D %p',
            'pooler_start': 'sudo service pgbouncer start',
            'pooler_stop': 'sudo service pgbouncer stop',
            'pooler_status': 'sudo service pgbouncer status >/dev/null 2>&1',
            'list_clusters': 'pg_lsclusters --no-header',
            'generate_recovery_conf': '/usr/local/yandex/populate_recovery_conf.py -s -r -p %p %m',
        },
        'debug': {},
        'plugins': {'wals_to_upload': 20},
    }

    config = RawConfigParser()
    if not filename:
        filename = options.config_file

    config.read(filename)

    #
    # Appending default config with default values.
    #
    for section in defaults:
        if not config.has_section(section):
            config.add_section(section)
        for key, value in defaults[section].items():
            if not config.has_option(section, key):
                config.set(section, key, value)
    if config.get('global', 'replication_slots_polling') is None:
        config.set('global', 'replication_slots_polling', config.get('global', 'use_replication_slots'))

    #
    # Rewriting global config with parameters from command line.
    #
    if options:
        for key, value in vars(options).items():
            if value is not None:
                config.set('global', key, value)

    return config


def init_logging(config: RawConfigParser):
    """
    Set log level and format
    """
    level = getattr(logging, config.get('global', 'log_level').upper())
    logging.getLogger('kazoo').setLevel(logging.WARN)
    logging.basicConfig(level=level, format='%(asctime)s %(levelname)-7s:\t%(message)s')


def start(config: RawConfigParser):
    """
    Start daemon
    """
    init_logging(config)

    pidfile = PIDLockFile(config.get('global', 'pid_file'), timeout=-1)

    try:
        pidfile.acquire()
    except AlreadyLocked:
        try:
            os.kill(pidfile.read_pid(), 0)
            print('Already running!')
            sys.exit(1)
        except OSError:
            pass

    pidfile.break_lock()

    if not config.has_section('commands'):
        raise ValueError('No commands section in config')

    if not config.has_section('plugins'):
        raise ValueError('No plugins section in config')

    plugins_config = dict(config.items('plugins'))

    command_manager = _get_command_manager(config)
    plugins = load_plugins(config.get('global', 'plugins_path'))

    db = Postgres(
        config=_postgres_config(config, plugins_config),
        plugins=PluginRunner(plugins['Postgres']),
        cmd_manager=command_manager,
    )
    zk = get_zookeeper(config, plugins['Zookeeper'])

    working_dir = config.get('global', 'working_dir')
    with _daemon_context(config, working_dir, pidfile):
        pgconsul(
            db=db,
            zk=zk,
            command_manager=_get_command_manager(config),
            replication_manager=_get_replication_manager(config, db, zk),
            config=_pgconsul_config(config),
        ).start()


def get_zookeeper(config: RawConfigParser, plugins: Plugins | None = None) -> Zookeeper:
    zk_config = ZookeeperConfig(
        ca_cert=config.get('global', 'ca_cert'),
        certfile=config.get('global', 'certfile'),
        iteration_timeout=config.getfloat('global', 'iteration_timeout'),
        keyfile=config.get('global', 'keyfile'),
        release_lock_after_acquire_failed=config.getboolean('global', 'release_lock_after_acquire_failed'),
        verify_certs=config.getboolean('global', 'verify_certs'),
        zk_auth=config.getboolean('global', 'zk_auth'),
        zk_connect_max_delay=config.getfloat('global', 'zk_connect_max_delay'),
        zk_hosts=config.get('global', 'zk_hosts'),
        zk_lockpath_prefix=config.get('global', 'zk_lockpath_prefix'),
        zk_password=config.get('global', 'zk_password'),
        zk_ssl=config.getboolean('global', 'zk_ssl'),
        zk_username=config.get('global', 'zk_username'),
    )
    if not plugins:
        return Zookeeper(config=zk_config)
    return Zookeeper(config=zk_config, plugins=PluginRunner(plugins))


def _daemon_context(config: RawConfigParser, working_dir: str, pidfile: PIDLockFile) -> daemon.DaemonContext:
    if config.getboolean('global', 'foreground'):
        usr = getpwnam(config.get('global', 'daemon_user'))
        return daemon.DaemonContext(
            working_directory=working_dir,
            uid=usr.pw_uid,
            gid=usr.pw_gid,
            detach_process=False,
            stdout=sys.stdout,
            stderr=sys.stderr,
            pidfile=pidfile,
        )

    logfile = open(config.get('global', 'log_file'), 'a')
    return daemon.DaemonContext(working_directory=working_dir, stdout=logfile, stderr=logfile, pidfile=pidfile)


def _get_command_manager(config: RawConfigParser) -> CommandManager:
    return CommandManager(
        Commands(
            promote=config.get('commands', 'promote'),
            rewind=config.get('commands', 'rewind'),
            get_control_parameter=config.get('commands', 'get_control_parameter'),
            pg_start=config.get('commands', 'pg_start'),
            pg_stop=config.get('commands', 'pg_stop'),
            pg_status=config.get('commands', 'pg_status'),
            pg_reload=config.get('commands', 'pg_reload'),
            pooler_start=config.get('commands', 'pooler_start'),
            pooler_stop=config.get('commands', 'pooler_stop'),
            pooler_status=config.get('commands', 'pooler_status'),
            list_clusters=config.get('commands', 'list_clusters'),
            generate_recovery_conf=config.get('commands', 'generate_recovery_conf'),
        )
    )


def _postgres_config(config: RawConfigParser, plugins_config: PluginsConfig) -> PostgresConfig:
    return PostgresConfig(
        conn_string=config.get('global', 'local_conn_string'),
        use_lwaldump=config.getboolean('global', 'use_lwaldump') or config.getboolean('global', 'quorum_commit'),
        working_dir=config.get('global', 'working_dir'),
        recovery_filepath=config.get('global', 'recovery_conf_rel_path'),
        use_replication_slots=config.getboolean('global', 'use_replication_slots'),
        standalone_pooler=config.getboolean('global', 'standalone_pooler'),
        pooler_addr=config.get('global', 'pooler_addr'),
        pooler_port=config.getint('global', 'pooler_port'),
        pooler_conn_timeout=config.getfloat('global', 'pooler_conn_timeout'),
        postgres_timeout=config.getfloat('global', 'postgres_timeout'),
        iteration_timeout=config.getfloat('global', 'iteration_timeout'),
        plugins=plugins_config,
    )


def _get_replication_manager(config: RawConfigParser, db: Postgres, zk: Zookeeper) -> ReplicationManager:
    if config.getboolean('global', 'quorum_commit'):
        return QuorumReplicationManager(
            _replication_manager_config(config),
            db,
            zk,
        )

    return SingleSyncReplicationManager(
        _replication_manager_config(config),
        db,
        zk,
    )


def _replication_manager_config(config: RawConfigParser) -> ReplicationManagerConfig:
    return ReplicationManagerConfig(
        priority=config.getint('global', 'priority'),
        primary_unavailability_timeout=config.getfloat('replica', 'primary_unavailability_timeout'),
        change_replication_metric=config.get('primary', 'change_replication_metric'),
        weekday_change_hours=config.get('primary', 'weekday_change_hours'),
        weekend_change_hours=config.get('primary', 'weekend_change_hours'),
        overload_sessions_ratio=config.getfloat('primary', 'overload_sessions_ratio'),
        before_async_unavailability_timeout=config.getfloat('primary', 'before_async_unavailability_timeout'),
    )


def _pgconsul_config(config: RawConfigParser) -> PgconsulConfig:
    return PgconsulConfig(
        allow_potential_data_loss=config.getboolean('replica', 'allow_potential_data_loss'),
        append_primary_conn_string=config.get('global', 'append_primary_conn_string'),
        autofailover=config.getboolean('global', 'autofailover'),
        can_delayed=config.getboolean('replica', 'can_delayed'),
        change_replication_type=config.getboolean('primary', 'change_replication_type'),
        close_detached_after=config.getfloat('replica', 'close_detached_after'),
        do_consecutive_primary_switch=config.getboolean('global', 'do_consecutive_primary_switch'),
        drop_slot_countdown=config.getint('global', 'drop_slot_countdown'),
        election_loser_timeout=config.getint('global', 'election_loser_timeout'),
        election_timeout=config.getint('global', 'election_timeout'),
        iteration_timeout=config.getfloat('global', 'iteration_timeout'),
        min_failover_timeout=config.getfloat('replica', 'min_failover_timeout'),
        max_allowed_switchover_lag_ms=config.getint('global', 'max_allowed_switchover_lag_ms'),
        max_rewind_retries=config.getint('global', 'max_rewind_retries'),
        postgres_timeout=config.getfloat('global', 'postgres_timeout'),
        primary_switch_checks=config.getint('replica', 'primary_switch_checks'),
        primary_switch_restart=config.getboolean('replica', 'primary_switch_restart'),
        primary_unavailability_timeout=config.getfloat('replica', 'primary_unavailability_timeout'),
        priority=config.getint('global', 'priority'),
        promote_checkpoint_sql=config.get('debug', 'promote_checkpoint_sql', fallback=None),
        quorum_commit=config.getboolean('global', 'quorum_commit'),
        recovery_timeout=config.getfloat('replica', 'recovery_timeout'),
        replication_slots_polling=config.getboolean('global', 'replication_slots_polling'),
        start_pooler=config.getboolean('replica', 'start_pooler'),
        stream_from=config.get('global', 'stream_from'),
        sync_replication_in_maintenance=config.getboolean('primary', 'sync_replication_in_maintenance'),
        update_prio_in_zk=config.getboolean('global', 'update_prio_in_zk'),
        use_lwaldump=config.getboolean('global', 'use_lwaldump'),
        use_replication_slots=config.getboolean('global', 'use_replication_slots'),
        working_dir=config.get('global', 'working_dir'),
    )


def main():
    """
    Main function. All magic is done here
    """

    options = parse_cmd_args()
    config = read_config(filename=options.config_file, options=options)
    start(config)


if __name__ == '__main__':
    main()
