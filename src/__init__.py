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
from .main import pgconsul


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
            'switchover_catchup_timeout': 60,
            'switchover_replica_turn_timeout': 180,
            'switchover_rollback_timeout': 180,
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
            'max_delay_on_zk_reinit': 60,
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
            'pg_stop': '/usr/lib/postgresql/10/bin/pg_ctl stop -s -m fast %w -t %t -D %p',
            'pg_status': 'sudo service postgresql-10 status',
            'pg_reload': '/usr/lib/postgresql/10/bin/pg_ctl reload -s -D %p',
            'pooler_start': 'sudo service pgbouncer start',
            'pooler_stop': 'sudo service pgbouncer stop',
            'pooler_status': 'sudo service pgbouncer status >/dev/null 2>&1',
            'list_clusters': 'pg_lsclusters --no-header',
            'generate_recovery_conf': '/usr/local/yandex/populate_recovery_conf.py -s -r -p %p %m',
        },
        'debug': {
            'election_loser_timeout': 0,  # Timeout for election losers. For test purposes only.
        },
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


def init_logging(config):
    """
    Set log level and format
    """
    level = getattr(logging, config.get('global', 'log_level').upper())
    logging.getLogger('kazoo').setLevel(logging.WARN)
    format = '{asctime} {levelname:<8}: {message}'
    if config.get('debug', 'log_func_name', fallback=False):
        format = '{asctime} {levelname:<8}: {funcName:<30}: {message}'
    logging.basicConfig(level=level, format=format, style='{')


def config_back_compatibility(config):
    pg_stop = config.get('commands', 'pg_stop').split()
    if '%w' not in pg_stop:
        logging.error('pg_stop command should contain %w placeholder. trying to make it from existing pg_stop')
        pg_stop = [a for a in pg_stop if a not in ('-w', '-W')] + ['%w']
        pg_stop = ' '.join(pg_stop)
        logging.error('new pg_stop command is: %s', pg_stop)
        config.set('commands', 'pg_stop', pg_stop)


def start(config):
    """
    Start daemon
    """
    usr = getpwnam(config.get('global', 'daemon_user'))

    init_logging(config)

    config_back_compatibility(config)

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

    if config.getboolean('global', 'foreground'):
        working_dir = config.get('global', 'working_dir')
        with daemon.DaemonContext(
            working_directory=working_dir,
            uid=usr.pw_uid,
            gid=usr.pw_gid,
            detach_process=False,
            stdout=sys.stdout,
            stderr=sys.stderr,
            pidfile=pidfile,
        ):
            pgconsul(config=config).start()
    else:
        working_dir = config.get('global', 'working_dir')
        logfile = open(config.get('global', 'log_file'), 'a')
        with daemon.DaemonContext(working_directory=working_dir, stdout=logfile, stderr=logfile, pidfile=pidfile):
            pgconsul(config=config).start()


def main():
    """
    Main function. All magic is done here
    """

    options = parse_cmd_args()
    config = read_config(filename=options.config_file, options=options)
    start(config)


if __name__ == '__main__':
    main()
