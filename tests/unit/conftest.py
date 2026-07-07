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

_ROOT = Path(__file__).parent.parent.parent
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


import pytest  # noqa: E402  (import after sys.path bootstrap)
from unittest.mock import MagicMock, patch  # noqa: E402


@pytest.fixture
def zk():
    """Create a Zookeeper instance with mocked Kazoo client and lock-path prefix."""
    with patch('src.zk.KazooClient'), \
         patch('src.zk.helpers.get_lockpath_prefix', return_value='/pgconsul/'):
        from src.zk import Zookeeper
        config = MagicMock()
        config.getint.return_value = 10
        config.getfloat.return_value = 5.0
        config.getboolean.return_value = False
        config.get.return_value = '/pgconsul/'
        return Zookeeper(config, plugins=MagicMock())
