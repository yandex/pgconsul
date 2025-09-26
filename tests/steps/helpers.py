#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import logging
import os
import subprocess
import tarfile
import time
import datetime
import enum
import contextlib
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.security import make_digest_acl
from kazoo.handlers.threading import KazooTimeoutError

import docker
from docker.errors import APIError

# Connect to docker daemon
DOCKER = docker.from_env(timeout=600)

PGDATA = '/var/lib/postgresql/{pg_major}/main'.format(pg_major=os.environ.get('PG_MAJOR'))

CONFIG_ENVS = {
    'pgconsul.conf': '/etc/pgconsul.conf',
    'postgresql.conf': '{pgdata}/postgresql.conf'.format(pgdata=PGDATA),
    'postgresql.auto.conf': '{pgdata}/postgresql.auto.conf'.format(pgdata=PGDATA),
    'recovery.conf': '{pgdata}/conf.d/recovery.conf'.format(pgdata=PGDATA),
    'standby.signal': '{pgdata}/standby.signal'.format(pgdata=PGDATA),
    'pgbouncer.ini': '/etc/pgbouncer/pgbouncer.ini',
}

CONTAINER_PORTS = {'pgconsul': ['5432', '6432'], 'zookeeper': ['2181', '2281', '2188', '2189'], 'backup': ['873']}

LOG = logging.getLogger('helpers')

DB_SHUTDOWN_MESSAGE = 'database system is shut down'
DB_READY_MESSAGE = 'database system is ready to accept'
POSTGRES_LOG_TIME_FMT = '%Y-%m-%d %H:%M:%S.%f'

TIMING_LOG_FILE = '/tmp/timing.log'


class DBState(enum.Enum):
    shut_down = 1
    working = 2


class DBStateChange(object):
    def __init__(self, time, new_state):
        self.time = time
        self.new_state = new_state

    def __lt__(self, other):
        return self.time < other.time


def retry_on_error(function, errType):
    """
    Decorator for retrying. It catches AssertionError
    while timeout not exceeded.
    """

    def wrapper(*args, **kwargs):
        context = args[0]
        timeout = time.time() + float(context.timeout)
        while True:
            try:
                return function(*args, **kwargs)
            except errType as error:
                LOG.info(
                    '{time}: {func} call: {err}'.format(
                        time=datetime.datetime.now().strftime("%H:%M:%S"), func=str(function.__name__), err=error
                    )
                )
                # raise exception if timeout exceeded
                if time.time() > timeout:
                    raise
                time.sleep(context.interval)

    return wrapper


def retry_on_assert(function):
    return retry_on_error(function, AssertionError)


def retry_on_kazoo_timeout(function):
    return retry_on_error(function, KazooTimeoutError)


def is_dict_subset_of(left, right):
    for key, value in left.items():
        if key not in right:
            return False, f'missing "{key}", expected "{value}"'
        if value != right[key]:
            message = f'key "{key}" has value "{right[key]}" expected "{value}"'
            return False, message
    return True, None


def is_2d_dict_subset_of(subset, superset):
    for key, val in subset.items():
        if key not in superset:
            return False
        is_subset, _ = is_dict_subset_of(val, superset[key])
        if not is_subset:
            return False
    return True


def are_dicts_subsets_of(exp_values, actual_values):
    if len(actual_values) != len(exp_values):
        return False, 'expected {exp} values, got {got}'.format(exp=len(exp_values), got=len(actual_values))

    for i, expected in enumerate(exp_values):
        is_subset, err = is_dict_subset_of(expected, actual_values[i])

        # return immediately if values are not equal
        if not is_subset:
            return is_subset, err

    return True, None


def extract_time_from_log_line(line):
    str_time, _ = line.split('UTC')
    return datetime.datetime.strptime(str_time.strip(), POSTGRES_LOG_TIME_FMT)


def extract_state_changes_from_postgresql_logs(logs):
    state_changes = []
    for line in logs:
        if DB_READY_MESSAGE in line:
            state_changes.append(DBStateChange(extract_time_from_log_line(line), DBState.working))
        elif DB_SHUTDOWN_MESSAGE in line:
            state_changes.append(DBStateChange(extract_time_from_log_line(line), DBState.shut_down))
    return state_changes


def container_env(container, env_var):
    return container.exec_run('/bin/bash -c "echo ${env_var}"'.format(env_var=env_var)).decode().strip('\n')


def container_get_fqdn(container):
    container.reload()
    return '{hostname}.{domain}'.format(
        hostname=container.attrs['Config']['Hostname'], domain=container.attrs['Config']['Domainname']
    )


def container_get_ip_address(container):
    container.reload()
    for network in container.attrs['NetworkSettings']['Networks'].values():
        yield network['IPAddress']


def container_get_host():
    """
    Get exposed host (differs from localhost if you use docker-machine)
    """
    machine_name = os.getenv('DOCKER_MACHINE_NAME')
    if machine_name:
        return subprocess.check_output(['docker-machine', 'ip', machine_name]).decode('utf-8').rstrip()

    return 'localhost'


def container_get_tcp_port(container, port):
    container.reload()
    binding = container.attrs['NetworkSettings']['Ports'].get('{port}/tcp'.format(port=port))
    if binding:
        return binding[0]['HostPort']


def container_get_env(container, env):
    container.reload()
    for env_str in container.attrs['Config']['Env']:
        var, value = env_str.split('=')
        if var == str(env):
            return value


def container_get_status(container):
    container.reload()
    return container.status.strip().lower()


def container_file_exists(container, path):
    try:
        _, _ = container.get_archive(path)
        return True
    except docker.errors.NotFound:
        return False


def container_get_tar(container, path):
    archive, _ = container.get_archive(path)
    raw_tarfile = io.BytesIO()
    for chunk in archive:
        raw_tarfile.write(chunk)
    raw_tarfile.seek(0)
    return raw_tarfile


def container_get_files(container, path):
    tar = tarfile.open(mode='r', fileobj=container_get_tar(container, path))
    for member in tar.getmembers():
        if not member.isfile():
            continue
        yield tar.extractfile(member)
    tar.close()


def container_get_filecontent(container, filepath):
    tar = tarfile.open(mode='r', fileobj=container_get_tar(container, filepath))
    fname = os.path.split(filepath)[1]
    file_content = tar.extractfile(fname).read()
    tar.close()
    return file_content


def container_get_filestream(container, filepath):
    tar = tarfile.open(mode='r', fileobj=container_get_tar(container, filepath))
    fname = os.path.split(filepath)[1]
    for line in tar.extractfile(fname).readlines():
        yield line
    tar.close()


def container_get_conffile(container, filename):
    filepath = CONFIG_ENVS.get(filename, filename)
    try:
        file_content = container_get_filecontent(container, filepath)
        return io.StringIO(file_content.decode())
    except docker.errors.NotFound:
        return io.StringIO()


def kill(container, signal):
    """
    Stop container by Sending signal (not fails if container is not running)
    """
    try:
        container.kill(signal)
    except APIError as exc:
        if 'is not running' not in str(exc):
            raise


def container_inject_file(container, filename, fileobj):
    # convert file to byte via BytesIO
    content = fileobj.read().encode()
    infile = io.BytesIO(content)
    outfile = io.BytesIO()
    filepath = CONFIG_ENVS.get(filename, filename)
    path, name = os.path.split(filepath)

    # create tar archive
    tar = tarfile.open(mode='w', fileobj=outfile)
    tarinfo = tarfile.TarInfo(name)
    tarinfo.size = len(content)
    tarinfo.mode = 0o0666
    tar.addfile(tarinfo, infile)
    tar.close()
    container.put_archive(path, outfile.getvalue())


def container_inject_config(container, filename, confobj):
    # Write config into StringIO file
    conffile = io.StringIO()
    confobj.write(conffile)
    # We need to seek into begin after write
    conffile.seek(os.SEEK_SET)
    container_inject_file(container, filename, conffile)


def build_config_get_path(build):
    if isinstance(build, dict):
        return build['context']
    return build


def container_check_file_exists(container, filepath):
    try:
        container.get_archive(filepath)
        return True
    except docker.errors.NotFound:
        return False


def promote_host(container):
    container.exec_run('bash /usr/bin/promote')


def set_switchover(container, params):
    return container.exec_run('pgconsul-util switchover -y {params}'.format(params=params))


def get_zk(context, name):
    container = context.containers[name]
    acl = make_digest_acl('user1', 'testpassword123', all=True)
    return KazooClient(
        '{host}:{port}'.format(host=container_get_host(), port=container_get_tcp_port(container, 2181)),
        default_acl=[acl],
        auth_data=[('digest', '{username}:{password}'.format(username='user1', password='testpassword123'))],
    )


def get_zk_value(context, zk_name, key):
    with contextlib.suppress(Exception):
        zk = get_zk(context, zk_name)
        zk.start()
        try:
            value = zk.get(key)[0].decode()
        except NoNodeError:
            return None
        finally:
            zk.stop()
            zk.close()
        return value
    return None


def zk_has_key(context, zk_name, key):
    with contextlib.suppress(Exception):
        zk = get_zk(context, zk_name)
        zk.start()
        try:
            return zk.exists(key)
        finally:
            zk.stop()
            zk.close()
    return False


def exec(container, cmd):
    """
    Execute command inside of given container
    """
    result = container.exec_run(cmd)
    return result.exit_code, result.output.decode().rstrip('\n')


def exec_nowait(container, cmd):
    """
    Execute command inside of given container
    """
    result = container.exec_run(cmd, detach=True)
    return result


def check_timing_log(context, names, container_name):
    """
    Check if the timing log contains the given names
    """
    container = context.containers.get(container_name)
    if not container:
        LOG.error("Container '%s' not found", container_name)
        return False

    try:
        if not container_file_exists(container, TIMING_LOG_FILE):
            return False

        file_content = container_get_filecontent(container, TIMING_LOG_FILE)
        content = file_content.decode('utf-8')
        found = set()
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.replace(':', ' ').split(maxsplit=1)
            if len(parts) < 2:
                continue
            found.add(parts[0])
            try:
                float(parts[1])
            except ValueError:
                LOG.error("Invalid timing log line: %s", line)
                return False
        return found == set(names)
    except:
        LOG.error("Invalid timing log content: %s", list(found))
        return False
