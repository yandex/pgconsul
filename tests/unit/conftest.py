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
    'kazoo', 'kazoo.client',
    'kazoo.handlers',
    'kazoo.recipe', 'kazoo.recipe.lock', 'kazoo.security',
    'lockfile', 'lockfile.pidlockfile', 'daemon',
):
    if _mod_name not in sys.modules:
        sys.modules[_mod_name] = MagicMock()

# Stub psycopg2 with real exception classes so that
# `except psycopg2.OperationalError` etc. work correctly in unit tests.
if 'psycopg2' in sys.modules:
    _psycopg2 = sys.modules['psycopg2']
    _psycopg2.Error = type('Error', (Exception,), {})
    _psycopg2.OperationalError = type('OperationalError', (_psycopg2.Error,), {})
    _psycopg2.DatabaseError = type('DatabaseError', (_psycopg2.Error,), {})
    _psycopg2.InterfaceError = type('InterfaceError', (_psycopg2.Error,), {})

# Stub kazoo.exceptions with real exception classes so that
# `except KazooException` / `except NoNodeError` etc. work in unit tests.
if 'kazoo.exceptions' not in sys.modules:
    _kazoo_exc = types.ModuleType('kazoo.exceptions')
    _kazoo_exc.KazooException = type('KazooException', (Exception,), {})
    _kazoo_exc.NoNodeError = type('NoNodeError', (_kazoo_exc.KazooException,), {})
    _kazoo_exc.NodeExistsError = type('NodeExistsError', (_kazoo_exc.KazooException,), {})
    _kazoo_exc.SessionExpiredError = type('SessionExpiredError', (_kazoo_exc.KazooException,), {})
    _kazoo_exc.ConnectionClosedError = type('ConnectionClosedError', (_kazoo_exc.KazooException,), {})
    _kazoo_exc.LockTimeout = type('LockTimeout', (_kazoo_exc.KazooException,), {})
    sys.modules['kazoo.exceptions'] = _kazoo_exc

# Stub kazoo.handlers.threading with a real KazooTimeoutError class.
if 'kazoo.handlers.threading' not in sys.modules:
    _kazoo_threading = types.ModuleType('kazoo.handlers.threading')
    _kazoo_threading.KazooTimeoutError = type('KazooTimeoutError', (Exception,), {})
    _kazoo_threading.SequentialThreadingHandler = MagicMock()
    sys.modules['kazoo.handlers.threading'] = _kazoo_threading

if 'src' not in sys.modules:
    _src_pkg = types.ModuleType('src')
    _src_pkg.__path__ = [str(_ROOT / 'src')]
    _src_pkg.__package__ = 'src'
    sys.modules['src'] = _src_pkg


import pytest  # noqa: E402  (import after sys.path bootstrap)
from unittest.mock import MagicMock, patch  # noqa: E402


@pytest.fixture
def zk_client():
    """Create a ZkClient instance with mocked KazooClient."""
    with patch('src.zk_client.KazooClient'), \
         patch('src.zk_client.SequentialThreadingHandler'):
        from src.zk_client import ZkClient, ZkClientConfig
        config = ZkClientConfig(
            hosts='localhost:2181',
            timeout=5.0,
            connect_max_delay=10.0,
            max_delay_on_reinit=30,
            path_prefix='/pgconsul/',
            auth=False,
            ssl=False,
        )
        return ZkClient(config)


@pytest.fixture
def zk():
    """Create a Zookeeper instance with mocked Kazoo client and lock-path prefix."""
    with patch('src.zk_client.KazooClient'), \
         patch('src.zk_client.SequentialThreadingHandler'), \
         patch('src.zk.helpers.get_lockpath_prefix', return_value='/pgconsul/'):
        from src.zk import create_zk
        config = MagicMock()
        config.getint.return_value = 10
        config.getfloat.return_value = 5.0
        config.getboolean.return_value = False
        config.get.return_value = '/pgconsul/'
        return create_zk(config)
