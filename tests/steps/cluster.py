#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import operator
import os
import time

import psycopg2
import yaml

from tests.steps import config
from tests.steps import helpers
from tests.steps import zk
from tests.steps.database import Postgres
from behave import given, register_type, then, when, use_step_matcher
from parse_type import TypeBuilder


register_type(WasOrNot=TypeBuilder.make_enum({"was": True, "was not": False}))
register_type(IsOrNot=TypeBuilder.make_enum({"is": True, "is not": False}))

use_step_matcher("re")

@given('a "(?P<cont_type>[a-zA-Z0-9_\-]+)" container common config')
def step_common_config(context, cont_type):
    context.config[cont_type] = yaml.safe_load(context.text) or {}


def _set_use_slots_in_pgconsul_config(config, use_slots):
    if 'config' not in config:
        config['config'] = {}
    if 'pgconsul.conf' not in config['config']:
        config['config']['pgconsul.conf'] = {}
    pgconsul_conf = config['config']['pgconsul.conf']
    if 'global' not in pgconsul_conf:
        pgconsul_conf['global'] = {}
    if 'commands' not in pgconsul_conf:
        pgconsul_conf['commands'] = {}
    if use_slots:
        pgconsul_conf['global']['use_replication_slots'] = 'yes'
        pgconsul_conf['commands']['generate_recovery_conf'] = '/usr/local/bin/gen_rec_conf_with_slot.sh %m %p'
    else:
        pgconsul_conf['global']['use_replication_slots'] = 'no'
        pgconsul_conf['commands']['generate_recovery_conf'] = '/usr/local/bin/gen_rec_conf_without_slot.sh %m %p'


class PGCluster(object):
    def __init__(self, members, docker_compose, use_slots=False):
        assert isinstance(members, dict)
        self.members = members
        self.services = docker_compose['services']

        self.primary = None
        self.replicas = {}

        # check all members and remember who is primary and replicas
        for member, conf in members.items():
            self.add_primary(member, conf)
            self.add_replica(member, conf)
            _set_use_slots_in_pgconsul_config(conf, use_slots)
        # add recovery.conf config to all replicas
        for replica in self.replicas.keys():
            assert replica in self.services, 'missing config for "{name}" in compose'.format(name=replica)
            if 'config' not in members[replica]:
                members[replica]['config'] = {}
            members[replica]['config'].update(
                {
                    'recovery.conf': {
                        'recovery_target_timeline': 'latest',
                        'primary_conninfo': 'host={host} application_name={app}'.format(
                            host=self.member_fqdn(self.replicas[replica]), app=self.member_appname(replica)
                        ),
                        'restore_command': 'rsync -a --password-file=/etc/archive.passwd'
                        ' rsync://archive@pgconsul_backup1_1.pgconsul_pgconsul_net:'
                        '/archive/%f %p',
                    },
                    'standby.signal': {},
                }
            )
            # add primary_slot_name to recovery.conf if we are using slots
            if use_slots:
                members[replica]['config']['recovery.conf'].update(
                    {
                        'primary_slot_name': self.member_slotname(replica),
                    }
                )

    def add_primary(self, member, conf):
        role = conf['role']
        if role == 'primary':
            assert self.primary is None, 'detected more than 1 primary {primaries}'.format(
                primaries=[self.primary, member]
            )
            self.primary = member

    def add_replica(self, member, conf):
        role = conf['role']
        if role == 'replica':
            self.replicas[member] = conf.get('stream_from', self.primary)

    def member_type(self, member):
        return self.members[member].get('type', 'pgconsul')

    def member_fqdn(self, member):
        return '{host}.{domain}'.format(
            host=self.services[member]['hostname'],
            domain=self.services[member]['domainname'],
        )

    def member_appname(self, member):
        return self.member_fqdn(member).replace('.', '_')

    def member_slotname(self, member):
        return self.member_appname(member)

    def config(self, member):
        return self.members[member].get('config', dict())

    def get_primary(self):
        return self.primary

    def get_replicas(self):
        return self.replicas

    def get_pg_members(self):
        return [self.get_primary()] + list(self.get_replicas().keys())


def execute_step_with_config(context, step, step_config):
    context.execute_steps('{step}\n"""\n{config}\n"""'.format(step=step, config=step_config))


@given('a following cluster with "(?P<lock_type>[a-zA-Z0-9_-]+)" (?P<with_slots>[a-zA-Z0-9_-]+) replication slots')
def step_cluster(context, lock_type, with_slots):
    use_slots = with_slots == 'with'

    cluster = PGCluster(yaml.safe_load(context.text) or {}, context.compose, use_slots)

    context.execute_steps(""" Given a "backup" container "backup1" """)

    zk_names = []
    # If we use zookeeper we need to create it in separate containers.
    if lock_type == 'zookeeper':
        # Find all zookeepers in compose and start it
        for name, service_config in context.compose['services'].items():
            image_type = helpers.build_config_get_path(service_config['build'])
            if not image_type.endswith('zookeeper'):
                continue
            zk_names.append(name)
            context.execute_steps(
                """
                Given a "zookeeper" container "{name}"
            """.format(
                    name=name
                )
            )

    # Start containers
    for member in cluster.members:
        execute_step_with_config(
            context,
            'Given a "{cont_type}" container "{name}" with following config'.format(
                cont_type=cluster.member_type(member), name=member
            ),
            yaml.dump(cluster.config(member), default_flow_style=False),
        )

    # Wait while containers starts in a separate cycle
    # after creation of all containers
    for member in cluster.members:
        context.execute_steps(
            """
            Then container "{name}" has status "running"
        """.format(
                name=member
            )
        )

    if use_slots:
        # create replication slots on primary
        for replica in cluster.get_replicas().keys():
            context.execute_steps(
                """
                Given a replication slot "{name}" in container "{primary}"
            """.format(
                    primary=cluster.get_replicas()[replica], name=cluster.member_slotname(replica)
                )
            )

    # Check that expected to be primary container is primary
    context.execute_steps(
        """
        Then container "{name}" became a primary
    """.format(
            name=cluster.get_primary()
        )
    )

    # Check that all replicas are replicas
    for replica in cluster.get_replicas().keys():
        context.execute_steps(
            """
            Then container "{replica}" is a replica of container "{primary}"
        """.format(
                replica=replica, primary=cluster.get_replicas()[replica]
            )
        )

    if use_slots:
        # Check that replication follows via slots if we using it
        # or otherwise not via slots if they are not used
        slots = []
        for replica in cluster.get_replicas().keys():
            if cluster.get_replicas()[replica] == cluster.get_primary():
                slots.append(
                    {
                        'slot_type': 'physical',
                        'slot_name': cluster.member_slotname(replica),
                        'active': use_slots,
                    }
                )
        execute_step_with_config(
            context,
            'Then container "{name}" has following replication slots'.format(name=cluster.get_primary()),
            yaml.dump(slots, default_flow_style=False),
        )

    # Check that all zk nodes is alive
    for name in zk_names:
        context.execute_steps(
            """
            Then zookeeper "{name}" node is alive
            """.format(
                name=name
            )
        )

    # Check that pgbouncer running on all dbs and tried_remaster flag for all hosts in 'no'
    for container in cluster.get_pg_members():
        context.execute_steps(
            """
            Then pgbouncer is running in container "{name}"
            And zookeeper "{zk_name}" has value "no" for key "/pgconsul/postgresql/all_hosts/pgconsul_{name}_1.pgconsul_pgconsul_net/tried_remaster"
        """.format(
                name=container, zk_name=zk_names[0]
            )
        )

    # Start woodpecker client (inserts to master via target_session_attrs) if in compose
    if 'woodpecker' in context.compose.get('services', {}):
        pg_hosts = ','.join(cluster.get_pg_members())
        woodpecker_config = {'environment': {'PGHOST': pg_hosts}}
        execute_step_with_config(
            context,
            'Given a "woodpecker" container "woodpecker" with following config',
            yaml.dump(woodpecker_config, default_flow_style=False),
        )
        context.execute_steps('Then container "woodpecker" has status "running"')


@given('a "(?P<cont_type>[a-zA-Z0-9_-]+)" container "(?P<name>[a-zA-Z0-9_-]+)"')
def step_container(context, cont_type, name):
    context.execute_steps(
        '''
        Given a "{cont_type}" container "{name}" with following config
        """
        """
    '''.format(
            name=name, cont_type=cont_type
        )
    )


@given('a "(?P<cont_type>[a-zA-Z0-9_-]+)" container "(?P<name>[a-zA-Z0-9_-]+)" with following config')
def step_container_with_config(context, cont_type, name):
    conf = yaml.safe_load(context.text) or {}
    docker_config = copy.deepcopy(context.compose['services'][name])

    # Check that image type is correct
    build = docker_config.pop('build')
    image_type = helpers.build_config_get_path(build)
    assert image_type.endswith(cont_type), (
        'invalid container type, '
        'expected "{cont_type}", docker-compose.yml has '
        'build "{build}"'.format(cont_type=cont_type, build=image_type)
    )

    # Pop keys that will be changed
    networks = docker_config.pop('networks')
    docker_config.pop('name', None)
    docker_config.pop('ports', None)

    # Merge environment from config (e.g. for woodpecker PGHOST)
    env_config = conf.pop('environment', None) or context.config.get(cont_type, {}).get('environment')
    if env_config:
        existing_env = docker_config.get('environment') or {}
        if isinstance(existing_env, list):
            existing_env = dict(kv.split('=', 1) for kv in existing_env if '=' in kv)
        existing_env.update(env_config)
        docker_config['environment'] = existing_env

    # while jepsen test use another image for container pgconsul
    # we need to create pgconsul container from our custom image
    # not image from docker-compose.yml
    image = (
        os.environ.get('PGCONSUL_IMAGE')
        if cont_type == 'pgconsul'
        else '{project}-{name}'.format(project=context.project, name=name)
    )

    # create dict {container_port: None} for each container's
    # exposed port (docker will use next free port automatically)
    ports = {}
    for port in helpers.CONTAINER_PORTS[cont_type]:
        ports[port] = ('0.0.0.0', None)

    # Create container
    container = helpers.DOCKER.containers.create(image, **docker_config, name=name, ports=ports)

    context.containers[name] = container

    # Connect container to network
    for netname, network in networks.items():
        context.networks[netname].connect(container, **network)

    # Process configs (exclude 'environment' which is not a config file)
    common_config = context.config.get(cont_type, {})
    filenames = set(list(common_config.keys()) + list(conf.keys())) - {'environment'}
    for conffile in filenames:
        confobj = config.fromfile(conffile, helpers.container_get_conffile(container, conffile))
        # merge existing config with common config
        confobj.merge(common_config.get(conffile, {}))
        # merge existing config with step config
        confobj.merge(conf.get(conffile, {}))
        helpers.container_inject_config(container, conffile, confobj)

    container.start()
    container.reload()

    if cont_type == 'pgconsul':
        container.exec_run("/usr/local/bin/generate_certs.sh")
        container.exec_run("/usr/local/bin/supervisorctl restart pgconsul")
    elif cont_type == 'zookeeper':
        container.exec_run("/usr/local/bin/generate_certs.sh")
        container.exec_run("/usr/local/bin/supervisorctl restart zookeeper")


@given('a replication slot "(?P<slot_name>[a-zA-Z0-9_-]+)" in container "(?P<name>[a-zA-Z0-9_-]+)"')
@helpers.retry_on_assert
def step_replication_slot(context, slot_name, name):
    container = context.containers[name]
    db = Postgres(host=helpers.container_get_host(), port=helpers.container_get_tcp_port(container, 5432))
    db.create_replication_slot(slot_name)


@then('container "(?P<name>[a-zA-Z0-9_-]+)" has following replication slots')
@helpers.retry_on_assert
def step_container_has_replication_slots(context, name):
    container = context.containers[name]
    db = Postgres(host=helpers.container_get_host(), port=helpers.container_get_tcp_port(container, 5432))
    exp_values = sorted(yaml.safe_load(context.text) or [], key=operator.itemgetter('slot_name'))
    assert isinstance(exp_values, list), 'expected list, got {got}'.format(got=type(exp_values))

    actual_values = sorted(db.get_replication_slots(), key=operator.itemgetter('slot_name'))
    result_equal, err = helpers.are_dicts_subsets_of(exp_values, actual_values)

    assert result_equal, err


@when('we drop replication slot "(?P<slot>[a-zA-Z0-9_-]+)" in container "(?P<name>[a-zA-Z0-9_-]+)"')
def step_container_drop_replication_slot(context, slot, name):
    container = context.containers[name]
    db = Postgres(host=helpers.container_get_host(), port=helpers.container_get_tcp_port(container, 5432))
    db.drop_replication_slot(slot)


@then('container "(?P<name>[a-zA-Z0-9_-]+)" is primary')
def step_container_is_primary(context, name):
    container = context.containers[name]
    db = Postgres(host=helpers.container_get_host(), port=helpers.container_get_tcp_port(container, 5432))
    assert db.is_primary(), 'container "{name}" is not primary'.format(name=name)


@then('container "(?P<name>[a-zA-Z0-9_-]+)" replication state is "(?P<state>[a-zA-Z0-9_-]+)"')
@helpers.retry_on_assert
def step_container_replication_state(context, name, state):
    container = context.containers[name]
    db = Postgres(host=helpers.container_get_host(), port=helpers.container_get_tcp_port(container, 5432))
    actual_state = db.get_replication_state()[0]
    assert (
        actual_state == state
    ), f'container "{name}" replication state is "{actual_state}", while expected is "{state}"'


@then('one of the containers "(?P<containers>[,a-zA-Z0-9_-]+)" became a primary, and we remember it')
@helpers.retry_on_assert
def step_one_of_containers_became_primary(context, containers):
    containers = containers.split(',')
    primaries = []
    for container in containers:
        try:
            step_container_became_primary_no_retries(context, container)
            primaries.append(container)
        except AssertionError:
            continue
    assert len(primaries) == 1, 'expected one of {containers} is primary, but primaries are "{primaries}"'.format(
        containers=containers, primaries=primaries
    )
    context.remembered_container = primaries[0]

def _get_another_container(context, containers):
    containers = containers.split(',')
    assert len(containers) == 2, 'expected exactly two containers in list'
    assert context.remembered_container is not None, 'primary was not remembered by previous steps'
    assert context.remembered_container in containers, 'remebered primary not in containers list'
    return [c for c in containers if c != context.remembered_container][0]

@then('another of the containers "(?P<containers>[,a-zA-Z0-9_-]+)" is a replica')
def step_one_of_containers_became_replica(context, containers):
    replica = _get_another_container(context, containers)
    context.execute_steps(
        """
        Then container "{replica}" is a replica of container "{primary}"
        """.format(
            replica=replica, primary=context.remembered_container
        )
    )

@then('postgresql in another of the containers "(?P<containers>[,a-zA-Z0-9_-]+)" was(?P<not_rewinded>| not) rewinded')
def step_one_of_containers_became_replica(context, containers, not_rewinded):
    replica = _get_another_container(context, containers)
    context.execute_steps(
        """
        Then postgresql in container "{replica}" was{not_rewinded} rewinded
        """.format(
            replica=replica, not_rewinded=not_rewinded
        )
    )

def step_container_became_primary_no_retries(context, name):
    container = context.containers[name]
    db = Postgres(host=helpers.container_get_host(), port=helpers.container_get_tcp_port(container, 5432))
    assert db.is_primary(), 'container "{name}" is not primary'.format(name=name)


@then('container "(?P<name>[a-zA-Z0-9_-]+)" became a primary')
@helpers.retry_on_assert
def step_container_became_primary(context, name):
    step_container_became_primary_no_retries(context, name)


def assert_container_is_replica(context, replica_name, primary_name):
    replica = context.containers[replica_name]
    primary = context.containers[primary_name]
    try:
        replicadb = Postgres(host=helpers.container_get_host(), port=helpers.container_get_tcp_port(replica, 5432))

        assert replicadb.is_primary() is False, 'container "{name}" is primary'.format(name=replica_name)

        assert replicadb.get_walreceiver_stat(), 'wal receiver not started'

        primarydb = Postgres(host=helpers.container_get_host(), port=helpers.container_get_tcp_port(primary, 5432))
        replicas = primarydb.get_replication_stat()
    except psycopg2.Error as error:
        raise AssertionError(error.pgerror)

    ips = list(helpers.container_get_ip_address(replica))
    myfqdn = helpers.container_get_fqdn(replica)

    # Find replica by one of container ip addresses
    # and check that fqdn is same as container fqdn
    for stat_replica in replicas:
        if any(stat_replica['client_addr'] == ip for ip in ips):
            assert (
                stat_replica['client_hostname'] == myfqdn
            ), 'incorrect replica fqdn on primary "{fqdn}", expected "{exp}"'.format(
                fqdn=stat_replica['client_hostname'], exp=myfqdn
            )
            break
    else:
        assert False, 'container {replica} is not replica of container "{primary}"'.format(
            replica=replica_name, primary=primary_name
        )


@then('container "(?P<replica_name>[a-zA-Z0-9_-]+)" is a replica of container "(?P<primary_name>[a-zA-Z0-9_-]+)"')
@helpers.retry_on_assert
def step_container_is_replica(context, replica_name, primary_name):
    return assert_container_is_replica(context, replica_name, primary_name)


@then('container "(?P<replica_name>[a-zA-Z0-9_-]+)" is a replica of container "(?P<primary_name>[a-zA-Z0-9_-]+)" and streaming')
@helpers.retry_on_assert
def step_container_is_replica_and_streaming(context, replica_name, primary_name):
    assert_container_is_replica(context, replica_name, primary_name)
    step_container_is_in_quorum_group_and_streaming(context, replica_name)


@then('"(?P<service>[a-z]+)" is "(?P<status>[A-Z]+)" in container "(?P<name>[a-zA-Z0-9_-]+)"')
def step_service_is_in_status(context, service, status, name):
    actual_status = service_status(context, service, name)
    assert status == actual_status, \
        'for service "{service}" expected status "{status}", actual "{actual_status}"' \
        .format(service=service, status=status, actual_status=actual_status)


@then('"(?P<service>[a-z]+)" is "(?P<status>[A-Z]+)" in container "(?P<name>[a-zA-Z0-9_-]+)" within "(?P<sec>[.0-9]+)" seconds')
def step_service_is_in_status_within(context, service, status, name, sec):
    sec = float(sec)
    timeout = time.time() + sec
    actual_status = ''
    while time.time() < timeout:
        actual_status = service_status(context, service, name)
        if status == actual_status:
            return
        time.sleep(context.interval)

    assert status == actual_status, \
        'for service "{service}" expected status "{status}", actual "{actual_status}"' \
        .format(service=service, status=status, actual_status=actual_status)


def service_status(context, service, name):
    cmd = 'supervisorctl status {service}'.format(service=service)
    _, status = ensure_exec(context, name, cmd)
    return status.split()[1]

@then('pgbouncer is running in container "(?P<name>[a-zA-Z0-9_-]+)"')
@helpers.retry_on_assert
def step_pgbouncer_running(context, name):
    container = context.containers[name]
    db = Postgres(host=helpers.container_get_host(), port=helpers.container_get_tcp_port(container, 6432))
    assert db.ping(), 'pgbouncer is not running in container "{name}"'.format(name=name)

@then('pgbouncer is running in remembered container')
@helpers.retry_on_assert
def step_pgbouncer_running_in_remembered_container(context):
    assert context.remembered_container is not None, 'primary was not remembered by previous steps'
    container = context.containers[context.remembered_container]
    db = Postgres(host=helpers.container_get_host(), port=helpers.container_get_tcp_port(container, 6432))
    assert db.ping(), 'pgbouncer is not running in container "{name}"'.format(name=context.remembered_container)

@then('container "(?P<replica_name>[a-zA-Z0-9_-]+)" is a replica of remembered container')
@helpers.retry_on_assert
def step_container_is_replica_of_remembered_host(context, replica_name):
    assert context.remembered_container is not None, 'primary was not remembered by previous steps'
    return step_container_is_replica(context, replica_name, context.remembered_container)


@then('pgbouncer is not running in container "(?P<name>[a-zA-Z0-9_-]+)"')
@helpers.retry_on_assert
def step_pgbouncer_not_running(context, name):
    container = context.containers[name]
    try:
        if helpers.container_get_status(container) == 'exited':
            # container is shut down, consider that pgbouncer is also down
            return
        Postgres(host=helpers.container_get_host(), port=helpers.container_get_tcp_port(container, 6432))
    except AssertionError as ae:
        err = ae.args[0]
        if isinstance(err, psycopg2.OperationalError) and any(
            match in err.args[0]
            for match in [
                'Connection refused',  # If container is shut and docker-proxy is not listening
                'timeout expired',  # If container is disconnected from network and not reachable within timeout
                'server closed the connection unexpectedly',  # If docker-proxy accepted connection but bouncer is down
            ]
        ):
            # pgbouncer is really not running, it is what we want
            return

        raise AssertionError(
            f'pgbouncer is running in container "{name}" but connection can\'t be established. Error is {err!r}'
        )
    # pgbouncer is running
    raise AssertionError('pgbouncer is running in container "{name}"'.format(name=name))


@then('container "(?P<name>[a-zA-Z0-9_-]+)" has following config')
@helpers.retry_on_assert
def step_container_has_config(context, name):
    container = context.containers[name]
    conf = yaml.safe_load(context.text) or {}
    for conffile, confvalue in conf.items():
        confobj = config.fromfile(conffile, helpers.container_get_conffile(container, conffile))
        valid, err = confobj.check_values_equal(confvalue)
        assert valid, err


@then('postgresql in container "(?P<name>[a-zA-Z0-9_-]+)" has value "(?P<value>[/a-zA-Z0-9_-]+)" for option "(?P<option>[a-zA-Z0-9_-]+)"')
@helpers.retry_on_assert
def step_postgresql_option_has_value(context, name, value, option):
    container = context.containers[name]
    db = Postgres(host=helpers.container_get_host(), port=helpers.container_get_tcp_port(container, 5432))
    val = db.get_config_option(option)
    assert val == value, 'option "{opt}" has value "{val}", expected "{exp}"'.format(opt=option, val=val, exp=value)


@then('postgresql in container "(?P<name>[a-zA-Z0-9_-]+)" has empty option "(?P<option>[a-zA-Z0-9_-]+)"')
@helpers.retry_on_assert
def step_postgresql_empty_option(context, name, option):
    step_postgresql_option_has_value(context, name, '', option)


@when('run in container "(?P<name>[a-zA-Z0-9_-]+)" "(?P<sessions>[0-9]+)" sessions with timeout (?P<timeout>[0-9]+)')
@helpers.retry_on_assert
def step_postgresql_make_sessions(context, name, sessions, timeout):
    container = context.containers[name]
    for connect in range(int(sessions)):
        db = Postgres(
            host=helpers.container_get_host(), port=helpers.container_get_tcp_port(container, 5432), async_=True
        )
        db.pg_sleep(timeout)


@then('pgbouncer in container "(?P<name>[a-zA-Z0-9_-]+)" has value "(?P<value>[/a-zA-Z0-9_-]+)" for option "(?P<option>[a-zA-Z0-9_-]+)"')
@helpers.retry_on_assert
def step_pgbouncer_option_has_value(context, name, value, option):
    container = context.containers[name]
    db = Postgres(
        dbname='pgbouncer',
        host=helpers.container_get_host(),
        port=helpers.container_get_tcp_port(container, 6432),
        autocommit=True,
    )
    db.cursor.execute('SHOW config')
    for row in db.cursor.fetchall():
        if str(row['key']) == str(option):
            assert row['value'] == value, 'option "{opt}" has value "{val}", expected "{exp}"'.format(
                opt=option, val=row['value'], exp=value
            )
            break
    else:
        assert False, 'missing option "{opt}" in pgboncer config'.format(opt=option)


@then('container "(?P<name>[a-zA-Z0-9_-]+)" has status "(?P<status>[a-zA-Z0-9_-]+)"')
@helpers.retry_on_assert
def step_container_status(context, name, status):
    container = context.containers[name]
    container.reload()
    current_status = helpers.container_get_status(container)
    expected_status = str(status).lower()
    assert current_status == expected_status, 'Unexpected container state "{state}", expected "{exp}"'.format(
        state=current_status, exp=status
    )


@when('we kill container "(?P<name>[a-zA-Z0-9_-]+)" with signal "(?P<signal>[a-zA-Z0-9_-]+)"')
def step_kill_container(context, name, signal):
    container = context.containers[name]
    helpers.kill(container, signal)
    container.reload()


def ensure_exec(context, container_name, cmd):
    container = context.containers[container_name]
    return helpers.exec(container, cmd)


def ensure_exec_nowait(context, container_name, cmd):
    container = context.containers[container_name]
    return helpers.exec_nowait(container, cmd)


@when('we kill "(?P<service>[a-zA-Z0-9_-]+)" in container "(?P<name>[a-zA-Z0-9_-]+)" with signal "(?P<signal>[a-zA-Z0-9_-]+)"')
def step_kill_service(context, service, name, signal):
    ensure_exec(context, name, 'pkill --signal %s %s' % (signal, service))


@when('we gracefully stop "(?P<service>[a-zA-Z0-9_-]+)" in container "(?P<name>[a-zA-Z0-9_-]+)"')
def step_stop_service(context, service, name):
    if service == 'postgres':
        pgdata = _container_get_pgdata(context, name)
        code, output = ensure_exec(
            context, name, f'sudo -u postgres /usr/bin/postgresql/pg_ctl stop -s -m fast -w -t 60 -D {pgdata}'
        )
        assert code == 0, f'Could not stop postgres: {output}'
    else:
        ensure_exec(context, name, 'supervisorctl stop %s' % service)


def _parse_pgdata(lsclusters_output):
    """
    Parse pgdata from 1st row
    """
    for row in lsclusters_output.split('\n'):
        if not row:
            continue
        _, _, _, _, _, pgdata, _ = row.split()
        return pgdata


def _container_get_pgdata(context, name):
    """
    Get pgdata in container by name
    """
    code, clusters_str = ensure_exec(context, name, 'pg_lsclusters --no-header')
    assert code == 0, f'Could not list clusters: {clusters_str}'
    return _parse_pgdata(clusters_str)


@when('we start "(?P<service>[a-zA-Z0-9_-]+)" in container "(?P<name>[a-zA-Z0-9_-]+)"')
def step_start_service(context, service, name):
    if service == 'postgres':
        pgdata = _container_get_pgdata(context, name)
        code, output = ensure_exec(context, name, f'sudo -u postgres /usr/bin/postgresql/pg_ctl start -D {pgdata}')
        assert code == 0, f'Could not start postgres: {output}'
    else:
        ensure_exec(context, name, 'supervisorctl start %s' % service)


@when('we stop container "(?P<name>[a-zA-Z0-9_-]+)"')
def step_stop_container(context, name):
    context.execute_steps(
        """
        When we kill container "{name}" with signal "SIGTERM"
        Then container "{name}" has status "exited"
    """.format(
            name=name
        )
    )


@when('we start container "(?P<name>[a-zA-Z0-9_-]+)"')
def step_start_container(context, name):
    container = context.containers[name]
    container.reload()
    status = helpers.container_get_status(container)
    assert status == 'exited', 'Unexpected container state "{state}", expected "exited"'.format(state=status)
    container.start()
    container.reload()


@when('we disconnect from network container "(?P<name>[a-zA-Z0-9_-]+)"')
def step_disconnect_container(context, name):
    networks = context.compose['services'][name]['networks']
    container = context.containers[name]
    for netname in networks:
        context.networks[netname].disconnect(container)


@when('we connect to network container "(?P<name>[a-zA-Z0-9_-]+)"')
def step_connect_container(context, name):
    networks = context.compose['services'][name]['networks']
    container = context.containers[name]
    for netname, network in networks.items():
        context.networks[netname].connect(container, **network)


@when('we disconnect from ZK container "(?P<name>[a-zA-Z0-9_-]+)"')
def step_disconnect_from_zk_container(context, name):
    context.execute_steps(
        '''
        When we run following command on host "{name}"
        """
        sh -c "iptables -I OUTPUT -m tcp -p tcp --dport 2281 -j DROP"
        """
    '''.format(
            name=name
        )
    )


@when('we connect to ZK container "(?P<name>[a-zA-Z0-9_-]+)"')
def step_connect_to_zk_container(context, name):
    context.execute_steps(
        '''
        When we run following command on host "{name}"
        """
        sh -c "iptables -D OUTPUT -m tcp -p tcp --dport 2281 -j DROP"
        """
    '''.format(
            name=name
        )
    )


@then('we fail')
def step_fail(_):
    raise AssertionError('You asked - we failed')


@when('we wait "(?P<interval>[.0-9]+)" seconds')
def step_sleep(_, interval):
    interval = float(interval)
    time.sleep(interval)


@when('we wait until "(?P<interval>[.0-9]+)" seconds to failover of "(?P<container_name>[a-zA-Z0-9_-]+)" left in zookeeper "(?P<zk_name>[a-zA-Z0-9_-]+)"')
def step_sleep_until_failover_cooldown(context, interval, container_name, zk_name):
    interval = float(interval)
    last_failover_ts = helpers.get_zk_value(context, zk_name, '/pgconsul/postgresql/last_failover_time')
    assert last_failover_ts is not None, 'last_failover_ts should not be "None"'
    last_failover_ts = float(last_failover_ts)

    timeout = config.getint(context, container_name, 'pgconsul.conf', 'replica', 'min_failover_timeout')
    now = time.time()
    wait_duration = (last_failover_ts + timeout) - now - interval
    assert wait_duration >= 0, 'we can\'t wait negative amount of time'
    time.sleep(wait_duration)


@when('we block postgres traffic from "(?P<host_from>[a-zA-Z0-9_-]+)" to "(?P<host_to>[a-zA-Z0-9_-]+)"')
def step_block_postgres_traffic(context, host_from: str, host_to: str):
    _operations_with_postgres_traffic_between_hosts(context, host_from, host_to, 'I')


@when('we unblock postgres traffic from "(?P<host_from>[a-zA-Z0-9_-]+)" to "(?P<host_to>[a-zA-Z0-9_-]+)"')
def step_unblock_postgres_traffic(context, host_from: str, host_to: str):
    _operations_with_postgres_traffic_between_hosts(context, host_from, host_to, 'D')


def _operations_with_postgres_traffic_between_hosts(context, host_from: str, host_to: str, operator: str):
    """
    [Un]block network postgres traffic between hosts
    """
    container_obj = context.containers[host_to]
    container_ips = list(helpers.container_get_ip_address(container_obj))

    iptables_commands = []
    for ip in container_ips:
        iptables_commands.append(f"iptables -{operator} INPUT -p tcp -m tcp -s {ip} -m multiport --dports 5432,6432 -j DROP")

    command = f"sh -c \"{'; '.join(iptables_commands)}\""

    context.execute_steps(f'''
        When we run following command on host "{host_from}"
        """
        {command}
        """
    ''')


@when('we disable archiving in "(?P<name>[a-zA-Z0-9_-]+)"')
def step_disable_archiving(context, name):
    container = context.containers[name]
    db = Postgres(host=helpers.container_get_host(), port=helpers.container_get_tcp_port(container, 5432))
    db.disable_archiving()


@when('we switch wal in "(?P<name>[a-zA-Z0-9_-]+)" "(?P<times>[0-9]+)" times')
def switch_wal(context, name, times):
    container = context.containers[name]
    db = Postgres(host=helpers.container_get_host(), port=helpers.container_get_tcp_port(container, 5432))
    context.wals = []
    for _ in range(int(times)):
        context.wals.append(db.switch_and_get_wal())
        time.sleep(1)


@then('wals present on backup "(?P<name>[a-zA-Z0-9_-]+)"')
@helpers.retry_on_assert
def check_wals(context, name):
    container = context.containers[name]
    for wal in context.wals:
        assert helpers.container_check_file_exists(
            container, '/archive/{wal}'.format(wal=wal)
        ), 'wal "{wal}" not present '.format(wal=wal)


@when('we run following command on host "(?P<name>[a-zA-Z0-9_-]+)"')
def step_host_run_command(context, name):
    context.last_exit_code, context.last_output = ensure_exec(context, name, context.text)


@when('we run following command on host "(?P<name>[a-zA-Z0-9_-]+)" nowait')
def step_host_run_command_nowait(context, name):
    result = ensure_exec_nowait(context, name, context.text)
    print('result: {result}'.format(result=result))


@then('command exit with return code "(?P<code>[0-9]+)"')
def step_command_return_code(context, code):
    assert (
        int(code) == context.last_exit_code
    ), f'Expected "{code}", got "{context.last_exit_code}", output was "{context.last_output}"'


@then('command result is following output')
def step_command_output_exact(context):
    assert context.text == context.last_output, f'Expected "{context.text}", got "{context.last_output}"'


@then('command result contains following output')
def step_command_output_contains(context):
    assert context.text in context.last_output, f'Expected "{context.text}" not found in got "{context.last_output}"'


@when('we promote host "(?P<name>[a-zA-Z0-9_-]+)"')
@helpers.retry_on_assert
def promote(context, name):
    container = context.containers[name]
    helpers.promote_host(container)


@when('we make switchover task with params "(?P<params>[ .a-zA-Z0-9_-]+)" in container "(?P<name>[a-zA-Z0-9_-]+)"')
def set_switchover_task(context, params, name):
    container = context.containers[name]
    if params == "None":
        params = ""
    helpers.set_switchover(container, params)


@then('pgconsul in container "(?P<name>[a-zA-Z0-9_-]+)" is connected to zookeeper')
@helpers.retry_on_assert
def step_check_pgconsul_zk_connection(context, name):
    container = context.containers[name]
    _, output = container.exec_run("bash -c '/usr/bin/lsof -i -a -p `supervisorctl pid pgconsul`'", privileged=True)
    pgconsul_conns = []
    for line in output.decode().split('\n'):
        conns = line.split()[8:]
        if '(ESTABLISHED)' in conns:
            pgconsul_conns += [c.split('->')[1].rsplit(':', 1) for c in conns if c != '(ESTABLISHED)']
    pgconsul_zk_conns = [c for c in pgconsul_conns if 'zookeeper' in c[0] and '2281' == c[1]]
    assert pgconsul_zk_conns, "pgconsul in container {name} is not connected to zookeeper".format(name=name)


@then('"(?P<x>[0-9]+)" containers are replicas of "(?P<primary_name>[a-zA-Z0-9_-]+)" within "(?P<sec>[.0-9]+)" seconds')
def step_x_containers_are_replicas_of(context, x, primary_name, sec):
    sec = float(sec)
    timeout = time.time() + sec
    while time.time() < timeout:
        replicas_count = 0
        for container_name in context.containers:
            if 'postgres' not in container_name:
                continue
            try:
                assert_container_is_replica(context, container_name, primary_name)
            except AssertionError:
                # this container is not a replica of primary, ok
                pass
            else:
                replicas_count += 1
        if replicas_count == int(x):
            return
        time.sleep(context.interval)
    assert False, "{x} containers are not replicas of {primary}".format(x=x, primary=primary_name)


@then('at least "(?P<x>[a-zA-Z0-9_-]+)" postgresql instances are running for "(?P<interval>[.0-9]+)" seconds')
def step_x_postgresql_are_running(context, x, interval):
    interval = float(interval)
    start_time = time.time()
    while time.time() < start_time + interval:
        x = int(x)
        running_count = 0
        for container in context.containers.values():
            try:
                db = Postgres(host=helpers.container_get_host(), port=helpers.container_get_tcp_port(container, 6432))
            except AssertionError:
                # ok, this db is not running right now
                pass
            else:
                if db.ping():
                    running_count += 1
        assert (
            running_count >= x
        ), "postgresql should be running in " + "{x} containers, but it is running in {y} containers".format(
            x=x, y=running_count
        )


def get_minimal_simultaneously_running_count(state_changes, cluster_size):
    running_count = 0
    is_cluster_completed = False
    minimal_running_count = None
    for change in state_changes:
        if change.new_state == helpers.DBState.shut_down:
            running_count -= 1
            if is_cluster_completed:
                minimal_running_count = min(minimal_running_count, running_count)
        elif change.new_state == helpers.DBState.working:
            running_count += 1
            if running_count == cluster_size:
                is_cluster_completed = True
                minimal_running_count = cluster_size
    return minimal_running_count


@then('container "(?P<name>[a-zA-Z0-9_-]+)" is in quorum group')
@helpers.retry_on_assert
def step_container_is_in_quorum_group_and_streaming(context, name):
    service = context.compose['services'][name]
    fqdn = f'{service["hostname"]}.{service["domainname"]}'
    assert zk.has_value_in_list(context, 'zookeeper1', '/pgconsul/postgresql/quorum', fqdn)
    assert zk.has_subset_of_values(
        context,
        'zookeeper1',
        '/pgconsul/postgresql/replics_info',
        {
            fqdn: {
                'state': 'streaming',
            }
        },
    )


@then('container "(?P<name>[a-zA-Z0-9_-]+)" is in sync group')
@helpers.retry_on_assert
def step_container_is_in_sync_group(context, name):
    service = context.compose['services'][name]
    fqdn = f'{service["hostname"]}.{service["domainname"]}'
    context.execute_steps(
        f'''
        Then zookeeper "zookeeper1" has holder "{fqdn}" for lock "/pgconsul/postgresql/sync_replica"
    '''
    )
    assert zk.has_subset_of_values(
        context,
        'zookeeper1',
        '/pgconsul/postgresql/replics_info',
        {
            fqdn: {
                'state': 'streaming',
                'sync_state': 'sync',
            }
        },
    )


@then('quorum replication is in normal state')
def step_quorum_replication_is_in_normal_state(context):
    pass


@then('sync replication is in normal state')
def step_single_sync_replication_is_in_normal_state(context):
    pass


@then('at least "(?P<x>[a-zA-Z0-9_-]+)" postgresql instances were running simultaneously during test')
def step_x_postgresql_were_running_simultaneously(context, x):
    x = int(x)
    state_changes = []
    cluster_size = 0
    for name, container in context.containers.items():
        if 'postgres' not in name:
            continue
        cluster_size += 1
        log_stream = helpers.container_get_filestream(container, "/var/log/postgresql/postgresql.log")
        logs = list(map(lambda line: line.decode('u8'), log_stream))
        state_changes.extend(helpers.extract_state_changes_from_postgresql_logs(logs))
    state_changes = sorted(state_changes)
    min_running = get_minimal_simultaneously_running_count(state_changes, cluster_size)
    assert (
        min_running >= x
    ), "postgresql had to be running in " + "{x} containers, but it was running in {y} containers".format(
        x=x, y=min_running
    )


@when('we set value "(?P<value>[/a-zA-Z0-9_-]+)" for option "(?P<option>[a-zA-Z0-9_-]+)" in section "(?P<section>[a-zA-Z0-9_-]+)" in pgconsul config in container "(?P<name>[a-zA-Z0-9_-]+)"')
def step_change_pgconsul_option(context, value, option, section, name):
    container = context.containers[name]
    conffile = 'pgconsul.conf'
    confobj = config.fromfile(conffile, helpers.container_get_conffile(container, conffile))
    confobj.merge({section: {option: value}})
    helpers.container_inject_config(container, conffile, confobj)


@when('we set value "(?P<value>[/a-zA-Z0-9_-]+)" for option "(?P<option>[a-zA-Z0-9_-]+)" in "(?P<conffile>[.a-zA-Z0-9_-]+)" config in container "(?P<name>[a-zA-Z0-9_-]+)"')
def step_change_option(context, value, option, conffile, name):
    container = context.containers[name]
    confobj = config.fromfile(conffile, helpers.container_get_conffile(container, conffile))
    confobj.merge({option: value})
    helpers.container_inject_config(container, conffile, confobj)


@when('we restart "(?P<service>[a-zA-Z0-9_-]+)" in container "(?P<name>[a-zA-Z0-9_-]+)"')
def step_restart_service(context, service, name):
    if service == 'postgres':
        pgdata = _container_get_pgdata(context, name)
        code, output = ensure_exec(
            context, name, f'sudo -u postgres /usr/bin/postgresql/pg_ctl restart -s -m fast -w -t 60 -D {pgdata}'
        )
        assert code == 0, f'Could not restart postgres: {output}'
    else:
        ensure_exec(context, name, f'supervisorctl restart {service}')


@then('"(?P<service>[a-zA-Z0-9_-]+)" is(?P<not_running>| not) running in container "(?P<name>[a-zA-Z0-9_-]+)"')
def step_service_running(context, service, not_running, name):
    exit_code, output = ensure_exec(context, name, f'supervisorctl status {service}')
    not_running = not_running.strip()
    if not_running == '':
        assert exit_code == 0, f'Service {service} is not running in container {name}'
    elif not_running == 'not':
        assert exit_code != 0, f'Service {service} is running in container {name}'
    else:
        raise AssertionError('Unknown step')


def get_postgres_start_time(context, name):
    container = context.containers[name]
    try:
        postgres = Postgres(host=helpers.container_get_host(), port=helpers.container_get_tcp_port(container, 5432))
        return postgres.get_start_time()
    except psycopg2.Error as error:
        raise AssertionError(error.pgerror)


@when('we remember postgresql start time in container "(?P<name>[a-zA-Z0-9_-]+)"')
def step_remember_pg_start_time(context, name):
    context.pg_start_time[name] = get_postgres_start_time(context, name)


@then('postgresql in container "(?P<name>[a-zA-Z0-9_-]+)" was(?P<not_restarted>| not) restarted')
def step_was_pg_restarted(context, name, not_restarted):
    not_restarted = not_restarted.strip()
    if not_restarted == '':
        assert get_postgres_start_time(context, name) != context.pg_start_time[name]
    elif not_restarted == 'not':
        assert get_postgres_start_time(context, name) == context.pg_start_time[name]
    else:
        raise AssertionError('Unknown step')


@then('postgresql in container "(?P<name>[a-zA-Z0-9_-]+)" was(?P<not_rewinded>| not) rewinded')
def step_was_pg_rewinded(context, name, not_rewinded):
    not_rewinded = not_rewinded.strip()
    container = context.containers[name]
    actual_rewinded = helpers.container_file_exists(container, '/tmp/rewind_called')
    assert not_rewinded in ('', 'not'), 'Unknown step'
    rewinded = not_rewinded == ''
    assert rewinded == actual_rewinded


@then('container "(?P<name>[a-zA-Z0-9_-]+)" is replaying WAL')
@helpers.retry_on_assert
def step_container_replaying_wal(context, name):
    container = context.containers[name]
    try:
        db = Postgres(host=helpers.container_get_host(), port=helpers.container_get_tcp_port(container, 5432))
        assert not db.is_wal_replay_paused()
    except psycopg2.Error as error:
        raise AssertionError(error.pgerror)


@when('we pause replaying WAL in container "(?P<name>[a-zA-Z0-9_-]+)"')
def step_container_pause_replaying_wal(context, name):
    container = context.containers[name]
    db = Postgres(host=helpers.container_get_host(), port=helpers.container_get_tcp_port(container, 5432))
    db.wal_replay_pause()


@when('we create database "(?P<database>[a-z0-9_]+)" on "(?P<name>[a-zA-Z0-9_-]+)"')
def step_create_database(context, database, name):
    container = context.containers[name]
    db = Postgres(host=helpers.container_get_host(), port=helpers.container_get_tcp_port(container, 5432))
    db.create_database(database)


@when('we run load testing')
def step_run_load_testing(context):
    """
    Run load testing with parameters specified in context.text.

    Expected format in context.text:
    ```yaml
    host: postgresql1
    pgbench:
      clients: 1
      jobs: 1
      time: 36000
    ```

    This step will:
    1. Create a database named "db1"
    2. Create a table "test" with a timestamp column
    3. Create an SQL file with an INSERT statement
    4. Run pgbench with the specified parameters
    5. Wait for the specified number of seconds
    """
    params = yaml.safe_load(context.text) or {}

    # Extract parameters with defaults
    host = params.get('host', 'postgresql1')
    pgbench_clients = params.get('pgbench', {}).get('clients', 1)
    pgbench_jobs = params.get('pgbench', {}).get('jobs', 1)
    pgbench_time = params.get('pgbench', {}).get('time', 36000)

    pgbench_port = 6432
    database = "db1"

    context.execute_steps(f'''
        # Create database
        When we create database "{database}" on "{host}"
        # Create table
        When we run following command on host "{host}"
        """
        su - postgres -c "psql -d {database} -c 'CREATE TABLE IF NOT EXISTS test (ts timestamp);'"
        """
        # Create SQL file
        When we run following command on host "{host}"
        """
        su - postgres -c "echo 'INSERT INTO test VALUES(now());' > /tmp/insert.sql"
        """
        # Run pgbench
        When we run following command on host "{host}" nowait
        """
        su - postgres -c "pgbench -n -f /tmp/insert.sql -c {pgbench_clients} -j {pgbench_jobs} -T {pgbench_time} -h {host} -p {pgbench_port} {database} > /tmp/pgbench.log"
        """
    ''')


@then('timing log in container "(?P<container_name>[a-zA-Z0-9_-]+)" contains "(?P<names>[,a-zA-Z0-9_-]+)"')
@helpers.retry_on_assert
def step_timing_log_contains(context, container_name, names):
    names_list = [name.strip() for name in names.split(',')]
    assert helpers.check_timing_log(context, names_list, container_name), f'Timing log does not contain all required entries: {names_list}'
