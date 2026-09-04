"""
Some helper functions and decorators
"""

# encoding: utf-8

import inspect
import json
import logging
import operator
import os
import random
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
from functools import wraps

import lockfile
from lockfile.pidlockfile import PIDLockFile

from .types import ReplicaInfos

_should_run = True


def register_sigterm_handler():
    signal.signal(signal.SIGTERM, _sigterm_handler)
    _set_should_run(True)


def should_run():
    global _should_run
    return _should_run


def _sigterm_handler(*_):
    _set_should_run(False)


def _set_should_run(value):
    global _should_run
    _should_run = value


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


def subprocess_popen(
    cmd,
    log_cmd=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
):
    """
    subprocess popen wrapper
    """
    try:
        if log_cmd:
            logging.debug('Running command: %s', cmd)
        return subprocess.Popen(
            cmd, shell=True, stdout=stdout, stderr=stderr,
            start_new_session=True,
        )
    except Exception:
        logging.exception("Could not run command '%s'", cmd)
        return None


def subprocess_start(cmd, log_cmd=True, return_process=False):
    """Start a command without waiting for its completion.

    When ``return_process`` is true, the caller owns the returned process and
    can poll it in later iterations. Output is discarded so an asynchronous
    command cannot block on a pipe.
    """
    try:
        if log_cmd:
            logging.debug('Starting command asynchronously: %s', cmd)
        process = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        return process if return_process else True
    except Exception:
        logging.exception("Could not start command '%s'", cmd)
        return None


def await_for_value(event, timeout: float, event_name: str):
    # ADR-0005 §1: infinite waits (timeout=-1) are prohibited.
    if timeout < 0:
        raise ValueError(f'await_for_value: infinite timeout (-1) is prohibited for "{event_name}"')
    return get_exponentially_retrying(timeout, event_name, None, event)()


def await_for(event, timeout: float, event_name: str):
    # ADR-0005 §1: infinite waits (timeout=-1) are prohibited.
    if timeout < 0:
        raise ValueError(f'await_for: infinite timeout (-1) is prohibited for "{event_name}"')
    return get_exponentially_retrying(timeout, event_name, False, return_none_on_false(event))()


def subprocess_call(
    cmd,
    fail_comment=None,
    log_cmd=True,
    save_output=False,
    output_file=None,
    timeout=None,
):
    """
    subprocess call wrapper
    """
    if save_output and output_file is not None:
        raise ValueError('save_output and output_file are mutually exclusive')

    redirected_output = None
    capture_output = output_file is None
    if output_file is not None:
        try:
            redirected_output = open(output_file, 'a', encoding='utf-8')
            redirected_output.write(
                '\n=== {} START: {} ===\n'.format(
                    time.strftime('%Y-%m-%d %H:%M:%S'), cmd,
                )
            )
            redirected_output.flush()
        except OSError:
            logging.exception(
                'Could not open command output log %s; inheriting output streams',
                output_file,
            )
            redirected_output = None

    proc = subprocess_popen(
        cmd,
        log_cmd,
        stdout=(redirected_output if redirected_output is not None else (subprocess.PIPE if capture_output else None)),
        stderr=(subprocess.STDOUT if output_file is not None else subprocess.PIPE),
    )
    if proc is None:
        if redirected_output is not None:
            redirected_output.close()
        return 1
    start_time = time.time()
    stdout = b''
    stderr = b''
    timed_out = False
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
        status = proc.returncode
    except subprocess.TimeoutExpired:
        timed_out = True
        logging.error('Command timed out after %.3fs: %s', timeout, cmd)
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (OSError, ProcessLookupError):
            logging.debug('Command process group already exited: %s', cmd)
        stdout, stderr = proc.communicate()
        status = 124
    elapsed = time.time() - start_time
    if redirected_output is not None:
        redirected_output.write(
            '=== {} END: exit code {}, {:.3f}s ===\n'.format(
                time.strftime('%Y-%m-%d %H:%M:%S'), status, elapsed,
            )
        )
        redirected_output.close()
    log_func = logging.error
    if status == 0:
        logging.debug('Command finished with exit code 0 in %.3fs: %s', elapsed, cmd)
        if save_output:
            log_func = logging.debug
    else:
        logging.debug('Command finished with exit code %d in %.3fs: %s', status, elapsed, cmd)
    if capture_output and (status != 0 or save_output):
        for line in (stdout or b'').splitlines():
            log_func(line.rstrip())
        for line in (stderr or b'').splitlines():
            log_func(line.rstrip())
        if fail_comment:
            log_func(fail_comment)
    return 124 if timed_out else status


def app_name_from_fqdn(fqdn):
    return fqdn.replace('.', '_').replace('-', '_')


def extract_host(conninfo):
    """
    Extract host= FQDN from a libpq conninfo string. Note: IPv6 literals are not supported.
    """
    if not conninfo:
        return None
    match = re.search(r'host=([\w.-]+)', conninfo)
    if match:
        return match.group(1)
    return None


def get_hostname():
    """
    return fqdn of local machine
    """
    return socket.getfqdn()


def get_host_path(path, hostname=None):
    """Substitute hostname into a ZK path template (containing %s)."""
    if hostname is None:
        hostname = get_hostname()
    return path % hostname


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
    match = re.match('[a-z-]+[0-9]+', get_hostname())
    if not match:
        raise ValueError(f"Hostname '{get_hostname()}' doesn't match expected pattern")
    return f'/pgconsul/{match.group(0)}/'


def get_oldest_replica(replics_info: ReplicaInfos):
    # "-1 * priority" used in sorting because we need to sorting like
    # ORDER BY write_location_diff ASC, priority DESC
    replics = sorted(replics_info, key=lambda x: (x['write_location_diff'], -1 * int(x['priority'])))  # type: ignore
    if len(replics):
        return replics[0]['application_name']
    return None


def make_current_replics_quorum(replics_info: ReplicaInfos, alive_hosts):
    """
    Returns set of replics which participate in quorum now.
    It is intersection of alive replics (holds alive lock) and streaming replics
    """
    streaming_replics = filter(lambda x: x['state'] == 'streaming', replics_info)
    alive_replics = set(map(operator.itemgetter('application_name'), streaming_replics))
    alive_hosts_map = {host: app_name_from_fqdn(host) for host in alive_hosts}
    return {host for host, app_name in alive_hosts_map.items() if app_name in alive_replics}


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
            logging.exception('Unhandled exception in %s', func.__name__)
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
        sleep_time: float = 1
        while (timeout == -1 or time.time() < retrying_end) and should_run():
            result = func(*args, **kwargs)
            if result is not None:
                return result
            if timeout == -1:
                current_sleep = sleep_time
            else:
                current_sleep = min(sleep_time, retrying_end - time.time())
            if current_sleep > 0:
                logging.debug(f'Waiting {current_sleep:.2f} for {event_name}'.format())
                time.sleep(current_sleep)
            sleep_time = 1.1 * sleep_time + 0.1 * random.random()
        if not should_run():
            logging.warning('Retrying stopped due to external signal.')
            sys.exit(1)

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
                if inspect.ismethod(x):
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


def is_op_destructive(op: str | None) -> bool:
    """Check whether the operation is destructive (e.g. rewind).

    None is treated as non-destructive (no operation recorded yet).
    """
    if op is None:
        return False
    # Operations that invalidate the host state and require special handling.
    DESTRUCTIVE_OPERATIONS: list[str] = ['rewind']

    return op in DESTRUCTIVE_OPERATIONS


def acquire_pid_lock(pid_file: str) -> 'PIDLockFile':
    """Acquire the daemon PID lock, recovering from stale locks, then release it.

    The lock is released before returning so that daemon.DaemonContext can
    acquire it cleanly in its __enter__. If the lock were left held,
    DaemonContext would raise AlreadyLocked on every startup.

    Scenarios:
      - Lock is free → acquired, released, returned.
      - Lock is held by a live process → print and sys.exit(1).
      - Lock is stale (PID file empty/corrupt → read_pid() is None,
        or PID no longer exists → OSError) → break lock and re-acquire.
    """
    pidfile = PIDLockFile(pid_file, timeout=-1)

    try:
        pidfile.acquire()
    except lockfile.AlreadyLocked:
        pid = pidfile.read_pid()
        if pid is not None:
            try:
                os.kill(pid, 0)
                print('Already running!')
                sys.exit(1)
            except OSError:
                pass

        try:
            pidfile.break_lock()
            pidfile.acquire()
        except OSError:
            logging.error('Failed to break stale PID lock %s', pid_file, exc_info=True)
            raise

    # Release the lock so DaemonContext can acquire it in __enter__.
    try:
        pidfile.break_lock()
    except OSError:
        logging.error('Failed to release PID lock %s before daemon start', pid_file, exc_info=True)
        raise

    return pidfile
