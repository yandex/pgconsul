# coding: utf-8
"""
Various utility fucntions:
    - Zookeeper structures init
    - Scheduled switchover
"""
import argparse
import functools
import json
import yaml
import socket
import sys
import logging

from . import read_config, init_logging, zk as zookeeper
from . import helpers
from . import utils
from .exceptions import SwitchoverException, FailoverException, ResetException


class ParseHosts(argparse.Action):
    """
    Check validity of provided hostnames
    """

    def __call__(self, parser, namespace, values, option_string=None):
        for value in values:
            try:
                socket.getaddrinfo(value, 0)
            except Exception as exc:
                raise ValueError('invalid hostname: %s: %s' % (value, exc))
            namespace.members.append(value)


def entry():
    """
    Entry point.
    """
    opts = parse_args()
    conf = read_config(
        filename=opts.config_file,
        options=opts,
    )
    init_logging(conf)
    try:
        opts.action(opts, conf)
    except (KeyboardInterrupt, EOFError):
        logging.error('abort')
        sys.exit(1)
    except RuntimeError as err:
        logging.error(err)
        sys.exit(1)
    except Exception as exc:
        logging.exception(exc)
        sys.exit(1)


def maintenance_enabled(zk):
    """
    Returns True if all hosts confirmed that maintenance is enabled.
    """
    for host in zk.get_alive_hosts():
        if zk.get(zk.get_host_maintenance_path(host)) != 'enable':
            return False
    return True


def maintenance_disabled(zk):
    """
    Common maintenance node should be deleted
    """
    return zk.get(zk.MAINTENANCE_PATH) is None


def _wait_maintenance_enabled(zk, timeout):
    is_maintenance_enabled = functools.partial(maintenance_enabled, zk)
    if not helpers.await_for(is_maintenance_enabled, timeout, 'enabling maintenance mode'):
        # Return cluster to last state, i.e. disable maintenance.
        zk.write(zk.MAINTENANCE_PATH, 'disable')
        raise TimeoutError
    logging.info('Success')


def _wait_maintenance_disabled(zk, timeout):
    is_maintenance_disabled = functools.partial(maintenance_disabled, zk)
    if not helpers.await_for(is_maintenance_disabled, timeout, 'disabling maintenance mode'):
        # Return cluster to the last state, i.e. enable maintenance.
        # There is obvious race condition between time when primary deletes this node
        # and we write value here. We assume that big timeout will help us here.
        zk.write(zk.MAINTENANCE_PATH, 'enable')
        raise TimeoutError
    logging.info('Success')


def maintenance(opts, conf):
    """
    Enable or disable maintenance mode.
    """
    zk = zookeeper.Zookeeper(config=conf, plugins=None)
    if opts.mode == 'enable':
        zk.ensure_path(zk.MAINTENANCE_PATH)
        zk.noexcept_write(zk.MAINTENANCE_PATH, 'enable', need_lock=False)
        if opts.wait_all:
            _wait_maintenance_enabled(zk, opts.timeout)
    elif opts.mode == 'disable':
        zk.write(zk.MAINTENANCE_PATH, 'disable', need_lock=False)
        if opts.wait_all:
            _wait_maintenance_disabled(zk, opts.timeout)
    elif opts.mode == 'show':
        val = zk.get(zk.MAINTENANCE_PATH) or 'disable'
        print('{val}d'.format(val=val))


def initzk(opts, conf):
    """
    Creates structures in zk.MEMBERS_PATH corresponding
    to members` names or checks if it has been done earlier.
    ! We override iteration_timeout here because it's timeout for ZK operations,
    for initzk is not important how fast zk response, but it's use in cluster restore
    and can fail if zk didn't response for 1 second
    """
    conf.set('global', 'iteration_timeout', 5)
    zk = zookeeper.Zookeeper(config=conf, plugins=None)
    for host in opts.members:
        path = '{members}/{host}'.format(members=zk.MEMBERS_PATH, host=host)
        if opts.test:
            logging.debug(f'Fetching path "{path}"...')
            if not zk.exists_path(path):
                logging.debug(f'Path "{path}" not found in ZK, initialization has not been performed earlier')
                sys.exit(2)
        else:
            logging.debug('creating "%s"...', path)
            if not zk.ensure_path(path):
                raise RuntimeError(f'Could not create path "{path}" in ZK')
    if opts.test:
        logging.debug('Initialization for all fqdns has been performed earlier')
    else:
        logging.debug('ZK structures are initialized')


def switchover(opts, conf):
    """
    Perform planned switchover.
    """
    try:
        switch = utils.Switchover(
            conf=conf, primary=opts.primary, timeline=opts.timeline, new_primary=opts.destination, timeout=opts.timeout
        )
        if opts.reset:
            return switch.reset(force=True)
        logging.info('switchover %(primary)s (timeline: %(timeline)s) to %(sync_replica)s', switch.plan())
        # ask user confirmation if necessary.
        if not opts.yes:
            helpers.confirm()
        # perform returns False on soft-fail.
        # right now it happens when an unexpected host has become
        # the new primary instead of intended sync replica.
        if not switch.is_possible():
            logging.error('Switchover is impossible now.')
            sys.exit(1)
        if not switch.perform(opts.replicas, block=opts.block):
            sys.exit(2)
    except SwitchoverException as exc:
        logging.error('unable to switchover: %s', exc)
        sys.exit(1)


def failover(opts, conf):
    """
    Operations during failover.
    """
    try:
        fail = utils.Failover(conf=conf)
        if opts.reset:
            return fail.reset()
    except FailoverException as exc:
        logging.error('unable to reset failover state: %s', exc)
        sys.exit(1)


def reset_all(opts, conf):
    """
    Resets all nodes in ZK, except for zk.MEMBERS_PATH
    """
    conf.set('global', 'iteration_timeout', 5)
    zk = zookeeper.Zookeeper(config=conf, plugins=None)
    logging.debug("resetting all ZK nodes")
    for node in [x for x in zk.get_children("") if x != zk.MEMBERS_PATH]:
        logging.debug(f'resetting path "{node}"')
        if not zk.delete(node, recursive=True):
            raise ResetException(f'Could not reset node "{node}" in ZK')
    logging.debug("ZK structures are reset")


def show_info(opts, conf):
    """
    Show cluster's information
    """
    info = _show_info(opts, conf)
    style = {'sort_keys': True, 'indent': 4}
    if info is not None:
        if opts.json:
            print(json.dumps(info, **style))
        else:
            print(yaml.dump(info, **style))


def _show_info(opts, conf):
    zk = zookeeper.Zookeeper(config=conf, plugins=None)
    zk_state = zk.get_state()
    zk_state['primary'] = zk_state.pop('lock_holder')  # rename field name to avoid misunderstunding
    if zk_state[zk.MAINTENANCE_PATH]['status'] is None:
        zk_state[zk.MAINTENANCE_PATH] = None

    if opts.short:
        return {
            'alive': zk_state['alive'],
            'primary': zk_state['primary'],
            'last_failover_time': zk_state[zk.LAST_FAILOVER_TIME_PATH],
            'maintenance': zk_state[zk.MAINTENANCE_PATH],
            'replics_info': _short_replica_infos(zk_state['replics_info']),
        }

    db_state = _get_db_state(conf)
    return {**db_state, **zk_state}


def _get_db_state(conf):
    fname = '%s/.pgconsul_db_state.cache' % conf.get('global', 'working_dir')
    try:
        with open(fname, 'r') as fobj:
            return json.loads(fobj.read())
    except Exception:
        logging.info("Can't load pgconsul status from %s, skipping", fname)
        return dict()


def _short_replica_infos(replics):
    ret = {}
    if replics is None:
        return ret
    for replica in replics:
        ret[replica['client_hostname']] = ', '.join(
            [
                replica['state'],
                'sync_state {0}'.format(replica['sync_state']),
                'replay_lag_msec {0}'.format(replica['replay_lag_msec']),
            ]
        )
    return ret


def parse_args():
    """
    Parse multiple commands.
    """
    arg = argparse.ArgumentParser(
        description="""
        pgconsul utility
        """
    )
    arg.add_argument(
        '-c',
        '--config',
        dest='config_file',
        type=str,
        metavar='<path>',
        default='/etc/pgconsul.conf',
        help='path to pgconsul main config file',
    )
    arg.add_argument(
        '--zk',
        type=str,
        dest='zk_hosts',
        metavar='<fqdn:port>,[<fqdn:port>,...]',
        help='override config zookeeper connection string',
    )
    arg.add_argument(
        '--zk-prefix',
        metavar='<path>',
        type=str,
        dest='zk_lockpath_prefix',
        help='override config zookeeper path prefix',
    )
    arg.set_defaults(action=lambda *_: arg.print_help())

    subarg = arg.add_subparsers(
        help='possible actions', title='subcommands', description='for more info, see <subcommand> -h'
    )

    # Init ZK command
    initzk_arg = subarg.add_parser('initzk', help='define zookeeper structures')
    initzk_arg.add_argument(
        'members',
        metavar='<fqdn> [<fqdn> ...]',
        action=ParseHosts,
        default=[],
        nargs='+',
        help='Space-separated list of cluster members hostnames',
    )
    initzk_arg.add_argument(
        '-t',
        '--test',
        action='store_true',
        default=False,
        help='Check if zookeeper intialization had already been performed for given hosts. Returns 0 if it had.',
    )
    initzk_arg.set_defaults(action=initzk)

    maintenance_arg = subarg.add_parser('maintenance', help='maintenance mode')
    maintenance_arg.add_argument(
        '-m', '--mode', metavar='[enable, disable, show]', default='enable', help='Enable or disable maintenance mode'
    )
    maintenance_arg.add_argument(
        '-w',
        '--wait_all',
        help='Wait for all alive high-availability hosts finish entering/exiting maintenance mode',
        action='store_true',
        default=False,
    )
    maintenance_arg.add_argument(
        '-t', '--timeout', help='Set timeout for maintenance command with --wait_all option', type=int, default=5 * 60
    )
    maintenance_arg.set_defaults(action=maintenance)

    # Info command
    info_arg = subarg.add_parser('info', help='info about cluster')
    info_arg.add_argument(
        '-s',
        '--short',
        help='short output from zookeeper',
        action='store_true',
        default=False,
    )
    info_arg.add_argument(
        '-j',
        '--json',
        help='show output in json format',
        action='store_true',
        default=False,
    )
    info_arg.set_defaults(action=show_info)

    # Scheduled switchover command
    switch_arg = subarg.add_parser(
        'switchover',
        help='perform graceful switchover',
        description="""
        Perform graceful switchover of the current primary.
        The default is to auto-detect its hostname and
        timeline in ZK.
        This behaviour can be overridden with options below.
        """,
    )
    switch_arg.add_argument('-d', '--destination', help='sets host where to switch', default=None, metavar='<fqdn>')
    switch_arg.add_argument(
        '-b', '--block', help='block until switchover completes or fails', default=False, action='store_true'
    )
    switch_arg.add_argument(
        '-t',
        '--timeout',
        help='limit each step to this amount of seconds',
        type=int,
        default=60,
        metavar='<sec>',
    )
    switch_arg.add_argument(
        '-y', '--yes', help='do not ask confirmation before proceeding', default=False, action='store_true'
    )
    switch_arg.add_argument(
        '-r',
        '--reset',
        help='reset switchover state in ZK (potentially disruptive)',
        default=False,
        action='store_true',
    )
    switch_arg.add_argument(
        '--replicas',
        help='if in blocking mode, wait until this number of replicas become online',
        type=int,
        default=2,
        metavar='<int>',
    )
    switch_arg.add_argument('--primary', help='override current primary hostname', default=None, metavar='<fqdn>')
    switch_arg.add_argument('--timeline', help='override current primary timeline', default=None, metavar='<fqdn>')
    switch_arg.set_defaults(action=switchover)

    fail_arg = subarg.add_parser(
        'failover',
        help='operations on failover state',
        description="""
           Change state of current failover.
           """,
    )
    fail_arg.add_argument(
        '-r',
        '--reset',
        help='reset failover state in ZK (potentially disruptive)',
        default=False,
        action='store_true',
    )
    fail_arg.set_defaults(action=failover)

    reset_all_arg = subarg.add_parser('reset-all', help='reset all nodes except members')
    reset_all_arg.set_defaults(action=reset_all)

    try:
        return arg.parse_args()
    except ValueError as err:
        arg.exit(message='%s\n' % err)
        exit(1)
