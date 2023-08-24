#!/usr/bin/env python
"""
Run yapf on code and check that diff is empty
"""

import subprocess
import sys


OUT = subprocess.check_output(['yapf', sys.argv[1], '-rpd'])

if len(OUT.decode('utf-8').splitlines()) > 1:
    print(OUT.decode('utf-8'))
    sys.exit(1)
