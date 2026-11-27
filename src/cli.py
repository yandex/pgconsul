# coding: utf-8
"""
Various utility functions:
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
import time

from . import read_config, init_logging
from .zk import create_zk, Zookeeper, ZookeeperException
from . import helpers
from .types import ReplicaInfos
from .exceptions import ResetException
from .switchover import create_switchover_request


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
    init_logging(conf, is_foreground=True)
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


def maintenance_enabled(zk: Zookeeper):
    """
    Returns True if all hosts confirmed that maintenance is enabled.
    """
    for host in zk.get_alive_hosts():
        if zk.get_host_maintenance_status(host) != 'enable':
            return False
    return True


def maintenance_disabled(zk: Zookeeper):
    """
    Common maintenance node should be deleted
    """
    return zk.get_maintenance_status() is None


def _wait_maintenance_enabled(zk: Zookeeper, timeout):
    is_maintenance_enabled = functools.partial(maintenance_enabled, zk)
    if not helpers.await_for(is_maintenance_enabled, timeout, 'enabling maintenance mode'):
        # Return cluster to last state, i.e. disable maintenance.
        zk.write_maintenance_status('disable')
        raise TimeoutError
    logging.info('Success')


def _wait_maintenance_disabled(zk: Zookeeper, timeout):
    is_maintenance_disabled = functools.partial(maintenance_disabled, zk)
    if not helpers.await_for(is_maintenance_disabled, timeout, 'disabling maintenance mode'):
        # Return cluster to the last state, i.e. enable maintenance.
        # There is obvious race condition between time when primary deletes this node
        # and we write value here. We assume that big timeout will help us here.
        zk.write_maintenance_status('enable')
        raise TimeoutError
    logging.info('Success')


def enable_maintenance(zk: Zookeeper, timeout, wait_all):
    zk.write_maintenance_status('enable')
    if wait_all:
        _wait_maintenance_enabled(zk, timeout)


def maintenance(opts, conf):
    """
    Enable or disable maintenance mode.
    """
    with create_zk(config=conf) as zk:
        if opts.mode == 'enable':
            enable_maintenance(zk, opts.timeout, opts.wait_all)
        elif opts.mode == 'disable':
            zk.write_maintenance_status('disable')
            if opts.wait_all:
                _wait_maintenance_disabled(zk, opts.timeout)
        elif opts.mode == 'show':
            val = zk.get_maintenance_status() or 'disable'
            print('{val}d'.format(val=val))


def initzk(opts, conf):
    """
    Creates structures in zk.MEMBERS_PATH corresponding
    to members` names or checks if it has been done earlier.
    """
    try:
        zk: Zookeeper = create_zk(config=conf,)
    except Exception as exc:
        # ZK is unreachable: connection failure is indistinguishable from "not initialized".
        if opts.test:
            logging.exception('Could not connect to ZK (KazooTimeoutError)')
            sys.exit(2)
        path = f'{Zookeeper.MEMBERS_PATH}/{opts.members[0]}'
        raise RuntimeError(f'Could not create path "{path}" in ZK') from exc
    with zk:
        for host in opts.members:
            if opts.test:
                path = zk.get_member_path(host)
                logging.debug(f'Fetching path "{path}"...')
                if not zk.member_exists(host):
                    logging.debug(f'Path "{path}" not found in ZK, initialization has not been performed earlier')
                    sys.exit(2)
            else:
                path = zk.get_member_path(host)
                logging.debug('creating "%s"...', path)
                if not zk.ensure_member(host):
                    raise RuntimeError(f'Could not create path "{path}" in ZK')
        if opts.test:
            logging.debug('Initialization for all fqdns has been performed earlier')
        else:
            logging.debug('ZK structures are initialized')


def switchover(opts, conf):
    """
    Perform planned switchover.
    """
    if opts.yes:
        logging.warning('The -y/--yes option is deprecated and will be removed soon')
    with create_zk(config=conf) as zk:
        state = zk.get_cluster_state()
        if state is None:
            logging.error('Cluster state is not initialized')
            sys.exit(1)
        if opts.reset:
            request = zk.get_switchover_request()
            if request is None or request[0].get('status') != 'requested':
                logging.error('Only a request that has not started can be cancelled')
                sys.exit(1)
            zk.set_switchover_request(dict(request[0], status='failed'), request[1])
            return
        if opts.timeline is not None and int(opts.timeline) != state.possible_write_configurations[-1].timeline:
            logging.error('Requested timeline does not match cluster state')
            sys.exit(1)
        request_id = create_switchover_request(zk, opts.primary, opts.destination, opts.timeout)
        if request_id is None:
            logging.error('Another switchover request is active')
            sys.exit(1)
        logging.info('Switchover request %s created', request_id)
        if not opts.block:
            return
        deadline = time.monotonic() + opts.timeout + 5
        while time.monotonic() < deadline:
            request = zk.get_switchover_request()
            if request is None or request[0].get('request_id') != request_id:
                logging.error('Switchover request was replaced')
                sys.exit(2)
            status = request[0].get('status')
            if status == 'failed':
                sys.exit(2)
            if status == 'done':
                if opts.replicas is None or len(zk.get_alive_hosts() or []) - 1 >= opts.replicas:
                    return
            time.sleep(1)
        logging.error('Timed out waiting for switchover request %s', request_id)
        sys.exit(2)


def failover(opts, conf):
    """
    Operations during failover.
    """
    if not opts.reset:
        return
    with create_zk(config=conf) as zk:
        state = zk.get_cluster_state()
        if state is None or state.operation['type'] != 'failover' or state.operation['phase'] != 'voting':
            logging.error('Only an unfinished voting phase can be reset')
            sys.exit(1)
        if time.time() < state.operation['deadline']:
            logging.error('Voting deadline has not expired')
            sys.exit(1)
        if not zk.cas_cluster_state(state, state.evolve(operation=state.operation['source_operation'])):
            logging.error('Failover state changed concurrently')
            sys.exit(1)


def reset_all(opts, conf):
    """
    Resets all nodes in ZK, except for zk.MEMBERS_PATH
    """
    with create_zk(config=conf) as zk:
        logging.debug("resetting all ZK nodes")
        all_nodes = zk.get_root_children()
        if all_nodes is None:
            logging.error("Could not get nodes to reset")
            all_nodes = []
        nodes_to_delete = [
            x for x in all_nodes
            if x not in (zk.MEMBERS_PATH, zk.MAINTENANCE_PATH, zk.CLUSTER_STATE_PATH, zk.SWITCHOVER_REQUEST_PATH)
        ]
        if not opts.force:
            prompt = f'Nodes to delete: {", ".join(nodes_to_delete + [zk.MAINTENANCE_PATH])}\n' \
                     f'This is a potentially dangerous action. Proceed [y/n]?\n'
            ans = input(prompt)
            if ans == 'n':
                return
            elif ans != 'y':
                print('Incorrect value, please type "y" or "n"')
                return
        enable_maintenance(zk, opts.timeout, True)
        for node in nodes_to_delete:
            logging.debug(f'resetting path "{node}"')
            if not zk.delete(node, recursive=True):
                raise ResetException(f'Could not reset node "{node}" in ZK')

        logging.debug(f'resetting path "{zk.MAINTENANCE_PATH}"')
        try:
            zk.write_maintenance_status('disable')
            # An in-flight enable iteration may add a child; daemons will retry after seeing disable.
            zk.delete_maintenance()
            deleted = helpers.await_for(
                functools.partial(maintenance_disabled, zk), opts.timeout, 'deleting maintenance node'
            )
        except ZookeeperException as exc:
            raise ResetException(f'Could not reset node "{zk.MAINTENANCE_PATH}" in ZK') from exc
        if not deleted:
            raise ResetException(f'Could not reset node "{zk.MAINTENANCE_PATH}" in ZK')
        logging.debug("ZK structures are reset")


def show_info(opts, conf):
    """
    Show cluster's information
    """
    info = _show_info(opts, conf)
    if info is not None:
        if opts.json:
            print(json.dumps(info, sort_keys=True, indent=4))
        else:
            print(yaml.dump(info, sort_keys=True, indent=4))


def _show_info(opts, conf):
    with create_zk(config=conf) as zk:
        state = zk.get_state()
        zk_state = state.to_dict()
        zk_state['primary'] = state.desired_primary if state.cluster_state_id is not None else state.lock_holder
        zk_state.pop('lock_holder')
        maintenance_path = zk.MAINTENANCE_PATH
        last_failover_path = zk.LAST_FAILOVER_TIME_PATH
    if state.maintenance.status is None:
        zk_state[maintenance_path] = None

    if opts.short:
        return {
            'alive': zk_state['alive'],
            'primary': zk_state['primary'],
            'last_failover_time': zk_state[last_failover_path],
            'maintenance': zk_state[maintenance_path],
            'replics_info': _short_replica_infos(state.replics_info),
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


def _short_replica_infos(replics: ReplicaInfos | None) -> dict[str, str]:
    ret: dict[str, str] = {}
    if replics is None:
        return ret
    for replica in replics:
        ret[replica.require('client_hostname')] = ', '.join(
            [
                replica.require('state'),
                'sync_state {0}'.format(replica.require('sync_state')),
                'replay_lag_msec {0}'.format(replica.require('replay_lag_msec')),
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
        help='Check if zookeeper initialization had already been performed for given hosts. Returns 0 if it had.',
    )
    initzk_arg.set_defaults(action=initzk)

    maintenance_arg = subarg.add_parser('maintenance', aliases=['maint'], help='maintenance mode')
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
        '-y', '--yes', help='deprecated; confirmation is assumed', default=False, action='store_true'
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
        default=None,
        metavar='<int>',
    )
    switch_arg.add_argument('--primary', help='override current primary hostname', default=None, metavar='<fqdn>')
    switch_arg.add_argument('--timeline', help='expected current primary timeline', type=int, default=None, metavar='<timeline>')
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
    reset_all_arg.add_argument(
        '-f',
        '--force',
        help='do not prompt',
        default=False,
        action='store_true',
    )
    reset_all_arg.add_argument(
        '-t', '--timeout', help='Set timeout for reset all command', type=int, default=5 * 60
    )
    reset_all_arg.set_defaults(action=reset_all)

    try:
        return arg.parse_args()
    except ValueError as err:
        arg.exit(message='%s\n' % err)
        exit(1)
