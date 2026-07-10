# encoding: utf-8
"""
Unit tests for src/zk_client.py — low-level KazooClient wrapper.

Covers: domain exceptions, LockHandle, ZkClientConfig, create_zk_client factory,
ZkClient lifecycle (init, reconnect, is_alive, is_connected, close, listener),
data operations (get, lock_version, write, ensure_path, exists,
get_children, delete) and lock recipes (make_lock, make_read_lock).
"""

from unittest.mock import MagicMock, patch

import pytest

from src.zk_client import (
    LockHandle,
    ZkClient,
    ZkClientConfig,
    ZkClientError,
    ZkConnectionClosedError,
    ZkLockTimeout,
    ZkNoNodeError,
    ZkSessionExpiredError,
    create_zk_client,
)


# === Fixtures ===

@pytest.fixture
def cfg():
    return ZkClientConfig(
        hosts='localhost:2181',
        timeout=5.0,
        connect_max_delay=10.0,
        max_delay_on_reinit=30,
        path_prefix='/pgconsul',
    )


@pytest.fixture
def cfg_auth():
    return ZkClientConfig(
        hosts='localhost:2181',
        timeout=5.0,
        connect_max_delay=10.0,
        max_delay_on_reinit=30,
        path_prefix='/pgconsul',
        auth=True,
        username='user',
        password='pass',
    )


@pytest.fixture
def cfg_ssl():
    return ZkClientConfig(
        hosts='localhost:2181',
        timeout=5.0,
        connect_max_delay=10.0,
        max_delay_on_reinit=30,
        path_prefix='/pgconsul',
        ssl=True,
        cert='/cert.pem',
        key='/key.pem',
        ca='/ca.pem',
        verify_certs=True,
    )


@pytest.fixture
def client(cfg):
    """ZkClient with mocked KazooClient (not started).

    _create_kazoo_client is stubbed so that init()/reconnect() do not replace
    the injected mock with a fresh MagicMock from the KazooClient constructor.
    """
    with patch('src.zk_client.KazooClient') as kc_cls, \
         patch('src.zk_client.SequentialThreadingHandler'):
        c = ZkClient(config=cfg)
        kazoo = MagicMock()
        kc_cls.return_value = kazoo
        # Stub _create_kazoo_client to assign the same mock instance every time.
        c._create_kazoo_client = lambda: setattr(c, '_kazoo', kazoo)
        c._kazoo = kazoo
        return c


def _make_stat(last_modified=12345.0):
    stat = MagicMock()
    stat.last_modified = last_modified
    return stat


# === Domain exceptions ===

class TestDomainExceptions:
    """Domain exceptions form the expected hierarchy."""

    def test_all_inherit_from_zkclient_error(self):
        for exc in (ZkSessionExpiredError, ZkNoNodeError, ZkConnectionClosedError, ZkLockTimeout):
            assert issubclass(exc, ZkClientError)

    def test_zkclient_error_inherits_from_exception(self):
        assert issubclass(ZkClientError, Exception)


# === LockHandle ===

class TestLockHandle:
    """LockHandle translates kazoo exceptions to domain types."""

    def test_acquire_success(self):
        lock = MagicMock()
        lock.acquire.return_value = True
        handle = LockHandle(lock)
        assert handle.acquire(blocking=True, timeout=1) is True
        lock.acquire.assert_called_once_with(blocking=True, timeout=1)

    def test_acquire_lock_timeout(self):
        from kazoo.exceptions import LockTimeout
        lock = MagicMock()
        lock.acquire.side_effect = LockTimeout('timeout')
        handle = LockHandle(lock)
        with pytest.raises(ZkLockTimeout):
            handle.acquire()

    def test_acquire_kazoo_exception(self):
        from kazoo.exceptions import KazooException
        lock = MagicMock()
        lock.acquire.side_effect = KazooException('boom')
        handle = LockHandle(lock)
        with pytest.raises(ZkClientError):
            handle.acquire()

    def test_acquire_kazoo_timeout_error(self):
        from kazoo.handlers.threading import KazooTimeoutError
        lock = MagicMock()
        lock.acquire.side_effect = KazooTimeoutError('timeout')
        handle = LockHandle(lock)
        with pytest.raises(ZkClientError):
            handle.acquire()

    def test_release_success(self):
        lock = MagicMock()
        handle = LockHandle(lock)
        handle.release()
        lock.release.assert_called_once()

    def test_release_connection_closed(self):
        from kazoo.exceptions import ConnectionClosedError
        lock = MagicMock()
        lock.release.side_effect = ConnectionClosedError('closed')
        handle = LockHandle(lock)
        with pytest.raises(ZkConnectionClosedError):
            handle.release()

    def test_release_kazoo_exception(self):
        from kazoo.exceptions import KazooException
        lock = MagicMock()
        lock.release.side_effect = KazooException('boom')
        handle = LockHandle(lock)
        with pytest.raises(ZkClientError):
            handle.release()

    def test_contenders_success(self):
        lock = MagicMock()
        lock.contenders.return_value = ['a', 'b']
        handle = LockHandle(lock)
        assert handle.contenders() == ['a', 'b']

    def test_contenders_kazoo_exception(self):
        from kazoo.exceptions import KazooException
        lock = MagicMock()
        lock.contenders.side_effect = KazooException('boom')
        handle = LockHandle(lock)
        with pytest.raises(ZkClientError):
            handle.contenders()


# === ZkClientConfig / create_zk_client factory ===

class TestCreateZkClient:
    """create_zk_client builds ZkClientConfig from a configparser-like object."""

    def _base_config(self):
        config = MagicMock()
        config.getboolean.side_effect = lambda section, key: False
        config.getfloat.return_value = 5.0
        config.getint.return_value = 30
        config.get.return_value = 'localhost:2181'
        return config

    def test_basic_config(self):
        config = self._base_config()
        with patch('src.zk_client.ZkClient') as zk_cls, \
             patch('src.helpers.get_lockpath_prefix', return_value='/pgconsul'):
            create_zk_client(config)
        zk_cls.assert_called_once()
        kw = zk_cls.call_args
        passed_cfg = kw.kwargs['config']
        assert passed_cfg.hosts == 'localhost:2181'
        assert passed_cfg.timeout == 5.0
        assert passed_cfg.connect_max_delay == 5.0
        assert passed_cfg.max_delay_on_reinit == 30
        assert passed_cfg.path_prefix == '/pgconsul'
        assert passed_cfg.auth is False
        assert passed_cfg.ssl is False

    def test_auth_config(self):
        config = self._base_config()

        def getboolean(section, key):
            if key == 'zk_auth':
                return True
            if key == 'zk_ssl':
                return False
            if key == 'verify_certs':
                return True
            return False

        config.getboolean.side_effect = getboolean

        def get(section, key):
            if key == 'zk_username':
                return 'user'
            if key == 'zk_password':
                return 'pass'
            return 'localhost:2181'

        config.get.side_effect = get

        with patch('src.zk_client.ZkClient') as zk_cls, \
             patch('src.helpers.get_lockpath_prefix', return_value='/pgconsul'):
            create_zk_client(config)
        passed_cfg = zk_cls.call_args.kwargs['config']
        assert passed_cfg.auth is True
        assert passed_cfg.username == 'user'
        assert passed_cfg.password == 'pass'

    def test_auth_missing_credentials_logs_error(self, caplog):
        config = self._base_config()

        def getboolean(section, key):
            if key == 'zk_auth':
                return True
            return False

        config.getboolean.side_effect = getboolean
        config.get.return_value = ''  # empty username/password

        with patch('src.zk_client.ZkClient'), \
             patch('src.helpers.get_lockpath_prefix', return_value='/pgconsul'):
            create_zk_client(config)
        assert any('zk_username, zk_password required' in r.message for r in caplog.records)

    def test_ssl_config(self):
        config = self._base_config()

        def getboolean(section, key):
            if key == 'zk_ssl':
                return True
            if key == 'verify_certs':
                return True
            return False

        config.getboolean.side_effect = getboolean

        def get(section, key):
            if key == 'certfile':
                return '/cert.pem'
            if key == 'keyfile':
                return '/key.pem'
            if key == 'ca_cert':
                return '/ca.pem'
            return 'localhost:2181'

        config.get.side_effect = get

        with patch('src.zk_client.ZkClient') as zk_cls, \
             patch('src.helpers.get_lockpath_prefix', return_value='/pgconsul'):
            create_zk_client(config)
        passed_cfg = zk_cls.call_args.kwargs['config']
        assert passed_cfg.ssl is True
        assert passed_cfg.cert == '/cert.pem'
        assert passed_cfg.key == '/key.pem'
        assert passed_cfg.ca == '/ca.pem'
        assert passed_cfg.verify_certs is True

    def test_ssl_missing_certs_logs_error(self, caplog):
        config = self._base_config()

        def getboolean(section, key):
            if key == 'zk_ssl':
                return True
            return False

        config.getboolean.side_effect = getboolean
        config.get.return_value = ''

        with patch('src.zk_client.ZkClient'), \
             patch('src.helpers.get_lockpath_prefix', return_value='/pgconsul'):
            create_zk_client(config)
        assert any('certfile, keyfile, ca_cert required' in r.message for r in caplog.records)

    def test_explicit_path_prefix(self):
        config = self._base_config()
        with patch('src.zk_client.ZkClient') as zk_cls:
            create_zk_client(config, path_prefix='/custom')
        passed_cfg = zk_cls.call_args.kwargs['config']
        assert passed_cfg.path_prefix == '/custom'


# === ZkClient lifecycle ===

class TestZkClientInit:
    """init() starts Kazoo and registers listener."""

    def test_init_success(self, client):
        event = MagicMock()
        event.wait.return_value = None
        client._kazoo.start_async.return_value = event
        client._kazoo.connected = True

        assert client.init() is True
        client._kazoo.start_async.assert_called_once()
        event.wait.assert_called_once_with(client.config.timeout)
        client._kazoo.add_listener.assert_called_once_with(client._listener)

    def test_init_failure_not_connected(self, client):
        event = MagicMock()
        client._kazoo.start_async.return_value = event
        client._kazoo.connected = False

        assert client.init() is False

    def test_client_property_raises_before_init(self, cfg):
        with patch('src.zk_client.KazooClient'), \
             patch('src.zk_client.SequentialThreadingHandler'):
            c = ZkClient(config=cfg)
        with pytest.raises(AssertionError):
            _ = c._client


class TestZkClientReconnect:
    """reconnect() rebuilds connection with backoff."""

    def test_reconnect_success(self, client):
        client._kazoo.connected = True
        event = MagicMock()
        client._kazoo.start_async.return_value = event
        client._kazoo.state = MagicMock()  # will be compared via ==
        # Make is_connected return True: state == KazooState.CONNECTED
        from kazoo.client import KazooState
        client._kazoo.state = KazooState.CONNECTED

        assert client.reconnect() is True
        client._kazoo.remove_listener.assert_called_once_with(client._listener)
        client._kazoo.stop.assert_called_once()
        client._kazoo.close.assert_called_once()
        assert client._session_expired is False
        assert client._failed_inits_count == 0

    def test_reconnect_failure_increments_counter(self, client):
        event = MagicMock()
        client._kazoo.start_async.return_value = event
        client._kazoo.connected = False
        from kazoo.client import KazooState
        client._kazoo.state = KazooState.LOST

        assert client.reconnect() is False
        assert client._session_expired is True
        assert client._failed_inits_count == 1

    def test_reconnect_sleeps_when_failed_before(self, client):
        client._failed_inits_count = 2
        event = MagicMock()
        client._kazoo.start_async.return_value = event
        from kazoo.client import KazooState
        client._kazoo.state = KazooState.CONNECTED

        with patch('src.zk_client.time.sleep') as sleep_mock, \
             patch('src.zk_client.uniform', return_value=1.0):
            client.reconnect()
        sleep_mock.assert_called_once()

    def test_reconnect_no_sleep_on_first_attempt(self, client):
        """First reconnect (failed_inits_count == 0) must not sleep."""
        event = MagicMock()
        client._kazoo.start_async.return_value = event
        from kazoo.client import KazooState
        client._kazoo.state = KazooState.CONNECTED

        with patch('src.zk_client.time.sleep') as sleep_mock:
            client.reconnect()
        sleep_mock.assert_not_called()

    def test_reconnect_exception_returns_false(self, client):
        client._kazoo.remove_listener.side_effect = Exception('boom')

        assert client.reconnect() is False
        assert client._session_expired is True
        assert client._failed_inits_count == 1


class TestZkClientState:
    """is_alive / is_connected pure state checks."""

    def test_is_alive_true(self, client):
        from kazoo.client import KazooState
        client._kazoo.state = KazooState.CONNECTED
        client._session_expired = False
        assert client.is_alive() is True

    def test_is_alive_false_when_session_expired(self, client):
        from kazoo.client import KazooState
        client._kazoo.state = KazooState.CONNECTED
        client._session_expired = True
        assert client.is_alive() is False

    def test_is_alive_false_when_not_connected(self, client):
        from kazoo.client import KazooState
        client._kazoo.state = KazooState.SUSPENDED
        assert client.is_alive() is False

    def test_is_connected_true(self, client):
        from kazoo.client import KazooState
        client._kazoo.state = KazooState.CONNECTED
        assert client.is_connected() is True

    def test_is_connected_false(self, client):
        from kazoo.client import KazooState
        client._kazoo.state = KazooState.LOST
        assert client.is_connected() is False


class TestZkClientClose:
    """close() removes listener, stops and closes Kazoo."""

    def test_close_full(self, client):
        client.close()
        client._kazoo.remove_listener.assert_called_once_with(client._listener)
        client._kazoo.stop.assert_called_once()
        client._kazoo.close.assert_called_once()

    def test_close_when_not_initialized(self, cfg):
        with patch('src.zk_client.KazooClient'), \
             patch('src.zk_client.SequentialThreadingHandler'):
            c = ZkClient(config=cfg)
        # Should not raise.
        c.close()

    def test_close_swallows_exceptions(self, client):
        client._kazoo.remove_listener.side_effect = Exception('boom')
        client._kazoo.stop.side_effect = Exception('boom')
        # Should not raise.
        client.close()


# === Listener ===

class TestListener:
    """_listener updates session flags and notifies external listener."""

    def test_listener_lost_sets_session_expired(self, client):
        from kazoo.client import KazooState
        listener = MagicMock()
        client._state_listener = listener
        client._listener(KazooState.LOST)
        assert client._session_expired is True
        listener.assert_called_once()

    def test_listener_connected_clears_flags(self, client):
        from kazoo.client import KazooState
        client._session_expired = True
        client._failed_inits_count = 3
        listener = MagicMock()
        client._state_listener = listener
        client._listener(KazooState.CONNECTED)
        assert client._session_expired is False
        assert client._failed_inits_count == 0
        listener.assert_called_once()

    def test_listener_suspended_no_flag_change(self, client):
        from kazoo.client import KazooState
        listener = MagicMock()
        client._state_listener = listener
        client._listener(KazooState.SUSPENDED)
        # session_expired unchanged (False by default)
        listener.assert_called_once()

    def test_listener_without_external_listener(self, client):
        from kazoo.client import KazooState
        client._state_listener = None
        # Should not raise.
        client._listener(KazooState.LOST)

    def test_set_state_listener(self, client):
        listener = MagicMock()
        client.set_state_listener(listener)
        assert client._state_listener is listener


# === _resolve_path ===

class TestResolvePath:
    """_resolve_path prepends path_prefix when missing."""

    def test_prepends_prefix(self, client):
        assert client._resolve_path('master') == '/pgconsul/master'

    def test_no_double_prefix(self, client):
        assert client._resolve_path('/pgconsul/master') == '/pgconsul/master'


# === Data operations: get ===

class TestGet:
    """get() returns decoded str, None, or raises domain exceptions."""

    def test_get_returns_decoded_str(self, client):
        client._kazoo.get.return_value = (b'hello', _make_stat())
        assert client.get('master') == 'hello'
        client._kazoo.get.assert_called_once_with('/pgconsul/master')

    def test_get_returns_none_for_none_data(self, client):
        client._kazoo.get.return_value = (None, _make_stat())
        assert client.get('master') is None

    def test_get_no_node_error(self, client):
        from kazoo.exceptions import NoNodeError
        client._kazoo.get.side_effect = NoNodeError('missing')
        with pytest.raises(ZkNoNodeError):
            client.get('master')

    def test_get_session_expired(self, client):
        from kazoo.exceptions import SessionExpiredError
        client._kazoo.get.side_effect = SessionExpiredError('expired')
        with pytest.raises(ZkSessionExpiredError):
            client.get('master')

    def test_get_kazoo_exception(self, client):
        from kazoo.exceptions import KazooException
        client._kazoo.get.side_effect = KazooException('boom')
        with pytest.raises(ZkClientError):
            client.get('master')

    def test_get_kazoo_timeout(self, client):
        from kazoo.handlers.threading import KazooTimeoutError
        client._kazoo.get.side_effect = KazooTimeoutError('timeout')
        with pytest.raises(ZkClientError):
            client.get('master')


# === Data operations: lock_version ===

class TestLockVersion:
    """lock_version() returns min lock sequence or None."""

    def test_lock_version_returns_min(self, client):
        client._kazoo.get_children.return_value = ['lock__001', 'lock__003', 'lock__002']
        assert client.lock_version('master') == '001'

    def test_lock_version_empty_children(self, client):
        client._kazoo.get_children.return_value = []
        assert client.lock_version('master') is None

    def test_lock_version_no_node(self, client):
        from kazoo.exceptions import NoNodeError
        client._kazoo.get_children.side_effect = NoNodeError('missing')
        assert client.lock_version('master') is None

    def test_lock_version_kazoo_exception(self, client):
        from kazoo.exceptions import KazooException
        client._kazoo.get_children.side_effect = KazooException('boom')
        with pytest.raises(ZkClientError):
            client.lock_version('master')


# === Data operations: write ===

class TestWrite:
    """write() atomic set → create → set on race."""

    def test_write_set_existing(self, client):
        client._kazoo.set.return_value = _make_stat()
        assert client.write('master', 'value') is True
        client._kazoo.set.assert_called_once_with('/pgconsul/master', b'value')

    def test_write_create_when_no_node(self, client):
        from kazoo.exceptions import NoNodeError
        client._kazoo.set.side_effect = NoNodeError('missing')
        client._kazoo.create.return_value = '/pgconsul/master'
        assert client.write('master', 'value') is True
        client._kazoo.create.assert_called_once_with('/pgconsul/master', value=b'value', makepath=True)

    def test_write_create_then_node_exists_race(self, client):
        from kazoo.exceptions import NoNodeError, NodeExistsError
        client._kazoo.set.side_effect = NoNodeError('missing')
        client._kazoo.create.side_effect = NodeExistsError('exists')
        client._kazoo.set.return_value = _make_stat()
        # First set raises NoNodeError, create raises NodeExistsError, second set succeeds.
        # Need set to succeed on second call.
        client._kazoo.set.side_effect = [NoNodeError('missing'), _make_stat()]
        assert client.write('master', 'value') is True
        assert client._kazoo.set.call_count == 2

    def test_write_session_expired(self, client):
        from kazoo.exceptions import SessionExpiredError
        client._kazoo.set.side_effect = SessionExpiredError('expired')
        with pytest.raises(ZkSessionExpiredError):
            client.write('master', 'value')

    def test_write_kazoo_exception(self, client):
        from kazoo.exceptions import KazooException
        client._kazoo.set.side_effect = KazooException('boom')
        with pytest.raises(ZkClientError):
            client.write('master', 'value')


# === Data operations: ensure_path / exists / get_children / delete ===

class TestEnsurePath:
    def test_ensure_path_success(self, client):
        client._kazoo.ensure_path.return_value = 'stat'
        assert client.ensure_path('master') == 'stat'
        client._kazoo.ensure_path.assert_called_once_with('/pgconsul/master')

    def test_ensure_path_kazoo_exception(self, client):
        from kazoo.exceptions import KazooException
        client._kazoo.ensure_path.side_effect = KazooException('boom')
        with pytest.raises(ZkClientError):
            client.ensure_path('master')


class TestExists:
    def test_exists_true(self, client):
        client._kazoo.exists.return_value = _make_stat()
        assert client.exists('master') is True

    def test_exists_false(self, client):
        client._kazoo.exists.return_value = None
        assert client.exists('master') is False

    def test_exists_kazoo_exception(self, client):
        from kazoo.exceptions import KazooException
        client._kazoo.exists.side_effect = KazooException('boom')
        with pytest.raises(ZkClientError):
            client.exists('master')


class TestGetChildren:
    def test_get_children_returns_list(self, client):
        client._kazoo.get_children.return_value = ['a', 'b']
        assert client.get_children('master') == ['a', 'b']

    def test_get_children_no_node_returns_empty(self, client):
        from kazoo.exceptions import NoNodeError
        client._kazoo.get_children.side_effect = NoNodeError('missing')
        assert client.get_children('master') == []

    def test_get_children_kazoo_exception(self, client):
        from kazoo.exceptions import KazooException
        client._kazoo.get_children.side_effect = KazooException('boom')
        with pytest.raises(ZkClientError):
            client.get_children('master')


class TestDelete:
    def test_delete_success(self, client):
        assert client.delete('master') is True
        client._kazoo.delete.assert_called_once_with('/pgconsul/master', recursive=False)

    def test_delete_recursive(self, client):
        client.delete('master', recursive=True)
        client._kazoo.delete.assert_called_once_with('/pgconsul/master', recursive=True)

    def test_delete_no_node_returns_true(self, client):
        from kazoo.exceptions import NoNodeError
        client._kazoo.delete.side_effect = NoNodeError('missing')
        assert client.delete('master') is True

    def test_delete_kazoo_exception(self, client):
        from kazoo.exceptions import KazooException
        client._kazoo.delete.side_effect = KazooException('boom')
        with pytest.raises(ZkClientError):
            client.delete('master')


# === Lock recipes ===

class TestLockRecipes:
    """make_lock / make_read_lock wrap kazoo locks in LockHandle."""

    def test_make_lock(self, client):
        lock = MagicMock()
        client._kazoo.Lock.return_value = lock
        handle = client.make_lock('/pgconsul/master', 'host1')
        assert isinstance(handle, LockHandle)
        client._kazoo.Lock.assert_called_once_with('/pgconsul/master', 'host1')

    def test_make_read_lock(self, client):
        lock = MagicMock()
        client._kazoo.ReadLock.return_value = lock
        handle = client.make_read_lock('/pgconsul/master', 'host1')
        assert isinstance(handle, LockHandle)
        client._kazoo.ReadLock.assert_called_once_with('/pgconsul/master', 'host1')


# === _create_kazoo_client ===

class TestCreateKazooClient:
    """_create_kazoo_client builds KazooClient with proper args."""

    def test_basic(self, cfg):
        with patch('src.zk_client.KazooClient') as kc_cls, \
             patch('src.zk_client.SequentialThreadingHandler'):
            c = ZkClient(config=cfg)
            c._create_kazoo_client()
            kc_cls.assert_called_once()
            args = kc_cls.call_args.kwargs
            assert args['hosts'] == 'localhost:2181'
            assert args['timeout'] == 5.0
            assert 'connection_retry' in args
            assert 'command_retry' in args
            assert 'default_acl' not in args
            assert 'use_ssl' not in args

    def test_auth_adds_acl(self, cfg_auth):
        with patch('src.zk_client.KazooClient') as kc_cls, \
             patch('src.zk_client.SequentialThreadingHandler'), \
             patch('src.zk_client.make_digest_acl') as acl_mock:
            c = ZkClient(config=cfg_auth)
            c._create_kazoo_client()
            acl_mock.assert_called_once_with('user', 'pass', all=True)
            args = kc_cls.call_args.kwargs
            assert 'default_acl' in args
            assert 'auth_data' in args
            assert args['auth_data'] == [('digest', 'user:pass')]

    def test_ssl_adds_ssl_args(self, cfg_ssl):
        with patch('src.zk_client.KazooClient') as kc_cls, \
             patch('src.zk_client.SequentialThreadingHandler'):
            c = ZkClient(config=cfg_ssl)
            c._create_kazoo_client()
            args = kc_cls.call_args.kwargs
            assert args['use_ssl'] is True
            assert args['certfile'] == '/cert.pem'
            assert args['keyfile'] == '/key.pem'
            assert args['ca'] == '/ca.pem'
            assert args['verify_certs'] is True


# === _sleep_before_reconnect / flag clearing ===

class TestSleepAndFlags:
    """Exponential backoff and flag clearing helpers."""

    def test_sleep_before_reconnect(self, client):
        client._failed_inits_count = 2
        with patch('src.zk_client.time.sleep') as sleep_mock, \
             patch('src.zk_client.uniform', return_value=2.5) as uniform_mock:
            client._sleep_before_reconnect()
        uniform_mock.assert_called_once()
        sleep_mock.assert_called_once_with(2.5)

    def test_sleep_capped_by_max_delay(self, client):
        """max_sleep must not exceed config.max_delay_on_reinit."""
        client._failed_inits_count = 100  # huge count
        client.config.max_delay_on_reinit = 30
        with patch('src.zk_client.time.sleep'), \
             patch('src.zk_client.uniform') as uniform_mock:
            client._sleep_before_reconnect()
        # uniform(0, max_sleep) where max_sleep <= 30
        max_sleep_arg = uniform_mock.call_args.args[1]
        assert max_sleep_arg <= 30

    def test_clear_connection_state_flags(self, client):
        client._session_expired = True
        client._failed_inits_count = 5
        client._clear_connection_state_flags()
        assert client._session_expired is False
        assert client._failed_inits_count == 0

    def test_clear_connection_state_flags_no_reset_when_zero(self, client):
        client._failed_inits_count = 0
        client._clear_connection_state_flags()
        assert client._failed_inits_count == 0

    def test_clear_session_expired_flag(self, client):
        client._session_expired = True
        client._clear_session_expired_flag()
        assert client._session_expired is False

    def test_clear_session_expired_flag_noop(self, client):
        client._session_expired = False
        client._clear_session_expired_flag()
        assert client._session_expired is False
