# encoding: utf-8
"""
pytest conftest for unit tests that import src submodules directly.

Stubs out heavy third-party dependencies (psycopg2, kazoo, etc.) so that
src modules can be imported without the actual packages being installed.
Also registers the project root on sys.path so that `import src.*` works.

This file is loaded automatically by pytest before any test module in this
directory, so individual test files do not need to repeat this bootstrap.
"""

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

for _mod_name in (
    'psycopg2', 'psycopg2.extensions', 'psycopg2.sql',
    'kazoo', 'kazoo.client', 'kazoo.exceptions',
    'kazoo.handlers', 'kazoo.handlers.threading',
    'kazoo.recipe', 'kazoo.recipe.lock', 'kazoo.security',
    'lockfile', 'lockfile.pidlockfile', 'daemon',
):
    if _mod_name not in sys.modules:
        sys.modules[_mod_name] = MagicMock()

if 'src' not in sys.modules:
    _src_pkg = types.ModuleType('src')
    _src_pkg.__path__ = [str(_ROOT / 'src')]
    _src_pkg.__package__ = 'src'
    sys.modules['src'] = _src_pkg
