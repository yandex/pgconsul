#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import signal

import docker
import yaml

import steps.helpers as helpers


def before_all(context):
    """
    Setup environment
    """
    # Connect to docker daemon
    context.docker = docker.from_env(timeout=600)
    client = context.docker

    context.timeout = float(os.environ.get('TIMEOUT', 360))
    context.interval = float(os.environ.get('INTERVAL', 1))
    context.project = str(os.environ.get('PROJECT'))

    context.config = {}

    context.zk_locks = {}
    context.zk = None

    context.compose = {}
    context.networks = {}
    context.containers = {}
    context.pg_start_time = {}
    context.remembered_container = None
    with open('docker-compose.yml', 'r') as compose_file:
        context.compose = yaml.safe_load(compose_file)

    # Clean all containers
    for name in context.compose.get('services', dict()):
        try:
            container = helpers.DOCKER.containers.get(name)
            container.remove(force=True)
        except (docker.errors.NotFound, docker.errors.APIError):
            pass

    # Create networks from docker-compose.yml
    net_opts = {
        'com.docker.network.bridge.enable_ip_masquerade': 'true',
        'com.docker.network.bridge.enable_icc': 'true',
        'com.docker.network.bridge.name': 'test_bridge',
    }
    for name, network in context.compose.get('networks', dict()).items():
        if 'external' in network:
            context.networks[name] = client.networks.get(network['external']['name'])
            continue
        existing_net = client.networks.list(names=[name])
        if existing_net:
            existing_net[0].remove()
        context.networks[name] = client.networks.create(
            name, driver=network.get('driver'), options=net_opts, ipam=network.get('ipam')
        )


def after_all(context):
    """
    Cleanup environment after tests run
    """
    # Cleanup networks
    for network in context.networks.values():
        network.remove()


def after_scenario(context, _):
    # Cleanup containers
    for container in context.containers.values():
        # Simply kill container if it not exited
        if helpers.container_get_status(container) != 'exited':
            helpers.kill(container, int(signal.SIGKILL))

        # Remove container's file system
        container.remove(v=True, force=True)

    context.containers.clear()
    # Cleanup config
    context.config.clear()

    # Cleanup zk locks and close connection
    context.zk_locks = {}
    if context.zk:
        context.zk.stop()
        context.zk.close()
        context.zk = None


def extract_log_file(container, cont_base_dir, log_path, log_filename):
    try:
        log_fullpath = os.path.join(log_path, log_filename)
        container_log_file = helpers.container_get_filestream(container, log_fullpath)
        with open(os.path.join(cont_base_dir, log_filename), 'w') as log_file:
            for line in container_log_file:
                log_file.write(line.decode('utf-8'))
    except Exception:
        pass  # Ok, there is no such log file in this container, let's move on


# Uncomment if you want to debug failed step via pdb
def after_step(context, step):
    if step.status == 'failed':
        if step.filename == '<string>':
            # Sub-step without filename, we don't need its output.
            # Same logs will be captured from outer failed step
            return
        base_dir = 'logs'
        os.makedirs(base_dir, exist_ok=True)
        for container in context.containers.values():
            hostname = container.attrs['Config']['Hostname']
            cont_base_dir = os.path.join(base_dir, step.filename, str(step.line), hostname)
            os.makedirs(cont_base_dir, exist_ok=True)
            if "zookeeper" in hostname:
                extract_log_file(container, cont_base_dir, '/var/log/zookeeper', 
                                 'zookeeper--server-{hostname}.log'.format(hostname=hostname))
                continue

            log_files = [
                ('/var/log/supervisor', 'pgconsul.log'),
                ('/var/log/postgresql', 'postgresql.log'),
                ('/var/log/postgresql', 'pgbouncer.log'),
            ]
            for log_path, log_file in log_files:
                extract_log_file(container, cont_base_dir, log_path, log_file)

        print('Logs for this run were placed in dir %s' % base_dir)
        if os.environ.get('DEBUG'):
            # -- ENTER DEBUGGER: Zoom in on failure location.
            # NOTE: Use pdb++ AKA pdbpp debugger,
            # same for pdb (basic python debugger).
            import pdb

            pdb.post_mortem(step.exc_traceback)
