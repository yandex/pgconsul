"""
Some helper functions and decorators
"""
# encoding: utf-8

import json
import logging
import operator
import os
import random
import re
import shutil
import socket
import subprocess
import time
import traceback
from functools import wraps


def get_input(*args, **kwargs):
    """
    Python cross-compatible input function
    """
    fun = input
    return fun(*args, **kwargs)


def confirm(prompt='yes', no_raise=False):
    """
    prompt user for confirmation. Raise if doesnt match.
    """
    confirmation = get_input('type "%s" to continue: ' % prompt)
    if confirmation.lower() == prompt:
        return True
    if no_raise:
        return None
    raise RuntimeError('there was no confirmation')


def load_json_or_default(data):
    if data == '':
        return []
    return json.loads(data)


def subprocess_popen(cmd, log_cmd=True):
    """
    subprocess popen wrapper
    """
    try:
        if log_cmd:
            logging.debug(cmd)
        return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception:
        logging.error("Could not run command '%s'", cmd)
        for line in traceback.format_exc().split('\n'):
            logging.error(line.rstrip())
        return None


def await_for_value(event, timeout, event_name):
    return get_exponentially_retrying(timeout, event_name, None, event)()


def await_for(event, timeout, event_name):
    return get_exponentially_retrying(timeout, event_name, False, return_none_on_false(event))()


def subprocess_call(cmd, fail_comment=None, log_cmd=True):
    """
    subprocess call wrapper
    """
    proc = subprocess_popen(cmd, log_cmd)
    if proc.wait() != 0:
        for line in proc.stdout:
            logging.error(line.rstrip())
        for line in proc.stderr:
            logging.error(line.rstrip())
        if fail_comment:
            logging.error(fail_comment)
    return proc.returncode


def app_name_from_fqdn(fqdn):
    return fqdn.replace('.', '_').replace('-', '_')


def get_hostname():
    """
    return fqdn of local machine
    """
    return socket.getfqdn()


def backup_dir(src, dst):
    """
    This function is basically 'rsync --delete -a <src> <dst>'
    """
    if os.path.exists(dst):
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def get_lockpath_prefix():
    """
    return lockpath prefix based on hostname
    """
    prefix = re.match('[a-z-]+[0-9]+', get_hostname()).group(0)
    return '/pgconsul/%s/' % prefix


def get_oldest_replica(replics_info):
    # "-1 * priority" used in sorting because we need to sorting like
    # ORDER BY write_location_diff ASC, priority DESC
    replics = sorted(replics_info, key=lambda x: (x['write_location_diff'], -1 * int(x['priority'])))
    if len(replics):
        return replics[0]['application_name']
    return None


def make_current_replics_quorum(replics_info, alive_hosts):
    """
    Returns set of replics which participate in quorum now.
    It is intersection of alive replics (holds alive lock) and streaming replics
    """
    streaming_replics = filter(lambda x: x['state'] == 'streaming', replics_info)
    alive_replics = set(map(operator.itemgetter('application_name'), streaming_replics))
    alive_hosts_map = {host: app_name_from_fqdn(host) for host in alive_hosts}
    return {host for host, app_name in alive_hosts_map.items() if app_name in alive_replics}


def check_last_failover_time(last, config):
    """
    Returns True if last failover has been done quite ago
    and False otherwise
    """
    min_failover = config.getfloat('replica', 'min_failover_timeout')
    now = time.time()
    if last:
        return (now - last) > min_failover
    else:
        return True


def return_none_on_error(func):
    """
    Decorator for function to return None on any exception (and log it)
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        """
        wrapper for function
        """
        try:
            return func(*args, **kwargs)
        except Exception:
            for line in traceback.format_exc().split('\n'):
                logging.error(line.rstrip())

            return None

    return wrapper


def return_none_on_false(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if func(*args, **kwargs):
            return True
        return None

    return wrapper


def get_exponentially_retrying(timeout, event_name, timeout_returnvalue, func):
    """
    This function returns an exponentially retrying decorator.
    If timeout == -1, then we won't stop waiting until we get the result.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        retrying_end = time.time() + timeout
        sleep_time = 1
        while timeout == -1 or time.time() < retrying_end:
            result = func(*args, **kwargs)
            if result is not None:
                return result
            if timeout == -1:
                current_sleep = sleep_time
            else:
                current_sleep = min(sleep_time, retrying_end - time.time())
            if current_sleep > 0:
                logging.info(f'Waiting {current_sleep} for {event_name}')
                time.sleep(current_sleep)
            sleep_time = 1.1 * sleep_time + 0.1 * random.random()
        logging.warning('Retrying timeout expired.')
        return timeout_returnvalue

    return wrapper


def write_status_file(db_state, zk_state, path):
    """
    Save json status file
    """
    try:
        data = {'zk_state': zk_state, 'db_state': db_state, 'ts': time.time()}
        fname = os.path.join(path, 'pgconsul.status')
        with open(fname, 'w') as fobj:
            fobj.write(json.dumps(data))
            fobj.flush()
    except Exception:
        logging.warning('Could not write status-file. Ignoring it.')


def func_name_logger(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logging.info('Called: {}'.format(func.__name__))
        return func(*args, **kwargs)

    return wrapper


def decorate_all_class_methods(decorator):
    def class_decorator(Cls):
        class NewCls(object):
            def __init__(self, *args, **kwargs):
                self.oInstance = Cls(*args, **kwargs)

            def __getattribute__(self, s):
                """
                this is called whenever any attribute of a NewCls object is accessed. This function first tries to
                get the attribute off NewCls. If it fails then it tries to fetch the attribute from self.oInstance (an
                instance of the decorated class). If it manages to fetch the attribute from self.oInstance, and
                the attribute is an instance method then `decorator` is applied.
                """
                try:
                    x = super(NewCls, self).__getattribute__(s)
                except AttributeError:
                    pass
                else:
                    return x
                x = self.oInstance.__getattribute__(s)
                if isinstance(x, type(self.__init__)):  # it is an instance method
                    return decorator(x)  # this is equivalent of just decorating the method
                else:
                    return x

        return NewCls

    return class_decorator


class IterationTimer:
    def __init__(self):
        self.start = time.time()

    def sleep(self, timeout):
        now = time.time()
        if now - self.start > float(timeout):
            return
        time.sleep(float(timeout) - (now - self.start))
