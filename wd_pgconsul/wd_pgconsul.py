#!/usr/bin/env python

import json
import os
import subprocess
import sys
import time

from pgconsul import read_config


def restart(comment=None):
    subprocess.call('/etc/init.d/pgconsul restart', shell=True, stdout=sys.stdout, stderr=sys.stderr)
    print('pgconsul has been restarted due to %s.' % comment)
    sys.exit(0)


def rewind_running():
    pids = [pid for pid in os.listdir('/proc') if pid.isdigit()]
    for pid in pids:
        try:
            cmd = open(os.path.join('/proc', pid, 'cmdline'), 'rb').read()
            if 'pg_rewind' in cmd:
                return True
        except IOError:
            # proc has already terminated
            continue
    return False


def main():
    config = read_config(filename='/etc/pgconsul.conf')
    work_dir = config.get('global', 'working_dir')
    stop_file = os.path.join(work_dir, 'pgconsul.stopped')

    if os.path.exists(stop_file):
        print('pgconsul has been stopped gracefully. Not doing anything.')
        sys.exit(0)

    p = subprocess.call('/etc/init.d/pgconsul status', shell=True, stdout=sys.stdout, stderr=sys.stderr)
    if p != 0:
        restart('dead service')

    status_file = os.path.join(work_dir, 'pgconsul.status')
    # We multiply on 3 because sanity checks and pg_rewind may take
    # some time without updating status-file
    timeout = config.getint('replica', 'recovery_timeout') * 3
    f = open(status_file, 'r')
    state = json.loads(f.read())
    f.close()
    if float(state['ts']) <= time.time() - timeout and not rewind_running():
        restart('stale info in status-file')


if __name__ == '__main__':
    main()
