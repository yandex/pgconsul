# encoding: utf-8
"""Unit tests for ZkClient class."""

import pytest
from unittest.mock import MagicMock, patch

from kazoo.client import KazooState

from src.zk_client import (
    ZkClientError,
    ZkConnectionClosedError,
    ZkConnectionState,
    ZkLockTimeout,
    ZkNoNodeError,
    ZkSessionExpiredError,
)

# Real exception classes for tests that exercise except-clauses
# (conftest stubs kazoo as MagicMock, so we define our own).
_NoNodeError = type('NoNodeError', (Exception,), {})
_NodeExistsError = type('NodeExistsError', (Exception,), {})
_KazooException = type('KazooException', (Exception,), {})
_SessionExpiredError = type('SessionExpiredError', (Exception,), {})
_LockTimeout = type('LockTimeout', (Exception,), {})
_ConnectionClosedError = type('ConnectionClosedError', (Exception,), {})


def create_mock_zk_client():
    """ZkClient with mocked _kazoo for testing."""
    from src.zk_client import ZkClient
    client = ZkClient(
        hosts='localhost:2181',
        timeout=5.0,
        connect_max_delay=10.0,
        max_delay_on_reinit=30,
        auth=False,
        ssl=False,
    )
    client._kazoo = MagicMock()
    client._kazoo.connected = True
    client._kazoo.state = KazooState.CONNECTED
    return client


class TestZkClientInit:

    def test_init_success(self):
        from src.zk_client import ZkClient
        mock_kazoo = MagicMock()
        mock_kazoo.connected = True
        mock_kazoo.start_async.return_value.wait.return_value = None

        with patch('src.zk_client.KazooClient', return_value=mock_kazoo), \
             patch('src.zk_client.SequentialThreadingHandler'):
            client = ZkClient(hosts='localhost:2181', timeout=5.0, connect_max_delay=10.0, max_delay_on_reinit=30)
            result = client.init()

        assert result is True
        mock_kazoo.start_async.assert_called_once()
        mock_kazoo.add_listener.assert_called_once()

    def test_init_failure_returns_false(self):
        from src.zk_client import ZkClient
        mock_kazoo = MagicMock()
        mock_kazoo.connected = False

        with patch('src.zk_client.KazooClient', return_value=mock_kazoo), \
             patch('src.zk_client.SequentialThreadingHandler'):
            client = ZkClient(hosts='localhost:2181', timeout=5.0, connect_max_delay=10.0, max_delay_on_reinit=30)
            result = client.init()

        assert result is False


class TestZkClientIsAlive:

    def test_is_alive_connected(self):
        client = create_mock_zk_client()
        client._kazoo.state = KazooState.CONNECTED
        client._session_expired = False
        assert client.is_alive() is True

    def test_is_alive_session_expired(self):
        """Session expired → False even if Kazoo reports CONNECTED."""
        client = create_mock_zk_client()
        client._kazoo.state = KazooState.CONNECTED
        client._session_expired = True
        assert client.is_alive() is False

    def test_is_alive_suspended(self):
        client = create_mock_zk_client()
        client._kazoo.state = KazooState.SUSPENDED
        assert client.is_alive() is False

    def test_is_alive_pure_no_side_effects(self):
        """is_alive() must not modify flags (pure query)."""
        client = create_mock_zk_client()
        client._kazoo.state = KazooState.CONNECTED
        client._session_expired = True
        client._failed_inits_count = 3

        client.is_alive()

        # Flags must not be cleared by is_alive — listener/reconnect owns that.
        assert client._session_expired is True
        assert client._failed_inits_count == 3


class TestZkClientIsConnected:

    def test_is_connected_when_connected(self):
        client = create_mock_zk_client()
        client._kazoo.state = KazooState.CONNECTED
        assert client.is_connected() is True

    def test_is_connected_when_suspended(self):
        client = create_mock_zk_client()
        client._kazoo.state = KazooState.SUSPENDED
        assert client.is_connected() is False

    def test_is_connected_no_side_effects(self):
        """is_connected() must not modify state flags."""
        client = create_mock_zk_client()
        client._kazoo.state = KazooState.CONNECTED
        client._session_expired = True
        client._failed_inits_count = 5

        client.is_connected()

        assert client._session_expired is True
        assert client._failed_inits_count == 5


class TestZkClientSetStateListener:

    def test_set_state_listener_replaces_existing(self):
        client = create_mock_zk_client()
        first = MagicMock()
        second = MagicMock()
        client.set_state_listener(first)
        client.set_state_listener(second)
        client._listener(KazooState.CONNECTED)
        first.assert_not_called()
        second.assert_called_once_with(ZkConnectionState.CONNECTED)


class TestZkClientReconnect:

    def test_reconnect_success(self):
        client = create_mock_zk_client()
        client._failed_inits_count = 3
        client._session_expired = True

        new_kazoo = MagicMock()
        new_kazoo.connected = True
        new_kazoo.state = KazooState.CONNECTED

        with patch.object(client, '_sleep_before_reconnect'), \
             patch('src.zk_client.KazooClient', return_value=new_kazoo), \
             patch('src.zk_client.SequentialThreadingHandler'):
            result = client.reconnect()

        assert result is True
        assert client._session_expired is False

    def test_reconnect_failure_increments_counter(self):
        client = create_mock_zk_client()
        client._kazoo.connected = False

        with patch.object(client, '_sleep_before_reconnect'):
            result = client.reconnect()

        assert result is False
        assert client._session_expired is True
        assert client._failed_inits_count == 1


class TestZkClientClose:

    def test_close_removes_listener_and_stops(self):
        client = create_mock_zk_client()
        client.close()
        client._kazoo.remove_listener.assert_called_once()
        client._kazoo.stop.assert_called_once()
        client._kazoo.close.assert_called_once()

    def test_close_handles_none_kazoo(self):
        from src.zk_client import ZkClient
        client = ZkClient(hosts='localhost:2181', timeout=5.0, connect_max_delay=10.0, max_delay_on_reinit=30)
        client.close()  # should not raise


class TestZkClientListener:

    def test_listener_lost_sets_session_expired(self):
        client = create_mock_zk_client()
        client._session_expired = False
        client._listener(KazooState.LOST)
        assert client._session_expired is True

    def test_listener_connected_clears_flags(self):
        client = create_mock_zk_client()
        client._session_expired = True
        client._failed_inits_count = 5
        client._listener(KazooState.CONNECTED)
        assert client._session_expired is False
        assert client._failed_inits_count == 0

    def test_listener_calls_external_callback_with_domain_state(self):
        """External callback receives ZkConnectionState, not KazooState."""
        client = create_mock_zk_client()
        callback = MagicMock()
        client._state_listener = callback
        client._listener(KazooState.CONNECTED)
        callback.assert_called_once_with(ZkConnectionState.CONNECTED)

    def test_listener_maps_lost_to_domain_state(self):
        client = create_mock_zk_client()
        callback = MagicMock()
        client._state_listener = callback
        client._listener(KazooState.LOST)
        callback.assert_called_once_with(ZkConnectionState.LOST)

    def test_listener_maps_suspended_to_domain_state(self):
        client = create_mock_zk_client()
        callback = MagicMock()
        client._state_listener = callback
        client._listener(KazooState.SUSPENDED)
        callback.assert_called_once_with(ZkConnectionState.SUSPENDED)

    def test_listener_no_error_when_callback_is_none(self):
        client = create_mock_zk_client()
        client._state_listener = None
        client._listener(KazooState.CONNECTED)  # must not raise


class TestZkClientStateFlags:

    def test_clear_connection_state_flags(self):
        client = create_mock_zk_client()
        client._session_expired = True
        client._failed_inits_count = 5
        client._clear_connection_state_flags()
        assert client._session_expired is False
        assert client._failed_inits_count == 0

    def test_clear_session_expired_flag_only(self):
        client = create_mock_zk_client()
        client._session_expired = True
        client._failed_inits_count = 5
        client._clear_session_expired_flag()
        assert client._session_expired is False
        assert client._failed_inits_count == 5


class TestZkClientResolvePath:

    def test_does_not_double_prefix(self):
        client = create_mock_zk_client()
        client._path_prefix = '/pgconsul/'
        assert client._resolve_path('/pgconsul/leader') == '/pgconsul/leader'


class TestZkClientDataOps:

    def test_get_returns_none_for_empty_node(self):
        client = create_mock_zk_client()
        client._kazoo.get.return_value = (None, MagicMock())
        assert client.get('leader') is None

    def test_get_raises_zk_no_node_error(self):
        client = create_mock_zk_client()
        with patch('src.zk_client.NoNodeError', _NoNodeError):
            client._kazoo.get.side_effect = _NoNodeError()
            with pytest.raises(ZkNoNodeError):
                client.get('missing')

    def test_get_raises_zk_session_expired_error(self):
        client = create_mock_zk_client()
        with patch('src.zk_client.SessionExpiredError', _SessionExpiredError):
            client._kazoo.get.side_effect = _SessionExpiredError()
            with pytest.raises(ZkSessionExpiredError):
                client.get('leader')

    def test_get_raises_zk_client_error_on_kazoo_exception(self):
        client = create_mock_zk_client()
        with patch('src.zk_client.KazooException', _KazooException):
            client._kazoo.get.side_effect = _KazooException("conn error")
            with pytest.raises(ZkClientError):
                client.get('leader')

    def test_write_retries_set_on_race(self):
        """NodeExistsError during create → retry set."""
        _NoNode = type('NoNodeError', (Exception,), {})
        _NodeExists = type('NodeExistsError', (Exception,), {})
        client = create_mock_zk_client()
        client._path_prefix = '/pgconsul/'
        with patch('src.zk_client.NoNodeError', _NoNode), \
             patch('src.zk_client.NodeExistsError', _NodeExists):
            client._kazoo.set.side_effect = [_NoNode(), None]
            client._kazoo.create.side_effect = _NodeExists()
            client.write('leader', 'host1')
        assert client._kazoo.set.call_count == 2

    def test_write_raises_zk_session_expired(self):
        client = create_mock_zk_client()
        with patch('src.zk_client.SessionExpiredError', _SessionExpiredError), \
             patch('src.zk_client.NoNodeError', _NoNodeError):
            client._kazoo.set.side_effect = _SessionExpiredError()
            with pytest.raises(ZkSessionExpiredError):
                client.write('leader', 'val')

    def test_exists_returns_true_when_node_exists(self):
        client = create_mock_zk_client()
        client._path_prefix = '/pgconsul/'
        client._kazoo.exists.return_value = MagicMock()
        assert client.exists('leader') is True

    def test_exists_returns_false_when_absent(self):
        client = create_mock_zk_client()
        client._path_prefix = '/pgconsul/'
        client._kazoo.exists.return_value = None
        assert client.exists('leader') is False

    def test_exists_raises_zk_client_error(self):
        client = create_mock_zk_client()
        with patch('src.zk_client.KazooException', _KazooException):
            client._kazoo.exists.side_effect = _KazooException("conn error")
            with pytest.raises(ZkClientError):
                client.exists('leader')

    def test_get_children_returns_list(self):
        client = create_mock_zk_client()
        client._path_prefix = '/pgconsul/'
        client._kazoo.get_children.return_value = ['a', 'b']
        assert client.get_children('all_hosts') == ['a', 'b']

    def test_get_children_returns_empty_on_no_node(self):
        client = create_mock_zk_client()
        with patch('src.zk_client.NoNodeError', _NoNodeError):
            client._kazoo.get_children.side_effect = _NoNodeError()
            assert client.get_children('missing') == []

    def test_get_children_raises_zk_client_error(self):
        client = create_mock_zk_client()
        with patch('src.zk_client.NoNodeError', _NoNodeError), \
             patch('src.zk_client.KazooException', _KazooException):
            client._kazoo.get_children.side_effect = _KazooException("conn error")
            with pytest.raises(ZkClientError):
                client.get_children('leader')

    def test_ensure_path_raises_zk_client_error(self):
        client = create_mock_zk_client()
        with patch('src.zk_client.KazooException', _KazooException):
            client._kazoo.ensure_path.side_effect = _KazooException("conn error")
            with pytest.raises(ZkClientError):
                client.ensure_path('some/path')

    def test_delete_returns_true_on_no_node(self):
        client = create_mock_zk_client()
        with patch('src.zk_client.NoNodeError', _NoNodeError):
            client._kazoo.delete.side_effect = _NoNodeError()
            assert client.delete('missing') is True

    def test_delete_raises_zk_client_error_on_kazoo_exception(self):
        client = create_mock_zk_client()
        with patch('src.zk_client.NoNodeError', _NoNodeError), \
             patch('src.zk_client.KazooException', _KazooException):
            client._kazoo.delete.side_effect = _KazooException("err")
            with pytest.raises(ZkClientError):
                client.delete('leader')


class TestZkClientLockRecipes:

    def test_make_lock_returns_lock_handle(self):
        from src.zk_client import LockHandle
        client = create_mock_zk_client()
        mock_lock = MagicMock()
        client._kazoo.Lock.return_value = mock_lock
        result = client.make_lock('/pgconsul/leader', 'host1')
        client._kazoo.Lock.assert_called_once_with('/pgconsul/leader', 'host1')
        assert isinstance(result, LockHandle)

    def test_make_read_lock_returns_lock_handle(self):
        from src.zk_client import LockHandle
        client = create_mock_zk_client()
        mock_lock = MagicMock()
        client._kazoo.ReadLock.return_value = mock_lock
        result = client.make_read_lock('/pgconsul/leader', 'host1')
        client._kazoo.ReadLock.assert_called_once_with('/pgconsul/leader', 'host1')
        assert isinstance(result, LockHandle)


class TestLockHandle:

    def _make_handle(self):
        from src.zk_client import LockHandle
        mock_lock = MagicMock()
        return LockHandle(mock_lock), mock_lock

    def test_acquire_delegates(self):
        handle, mock_lock = self._make_handle()
        mock_lock.acquire.return_value = True
        assert handle.acquire(blocking=True, timeout=5) is True
        mock_lock.acquire.assert_called_once_with(blocking=True, timeout=5)

    def test_acquire_translates_lock_timeout(self):
        handle, mock_lock = self._make_handle()
        with patch('src.zk_client.LockTimeout', _LockTimeout):
            mock_lock.acquire.side_effect = _LockTimeout()
            with pytest.raises(ZkLockTimeout):
                handle.acquire()

    def test_acquire_translates_kazoo_exception(self):
        handle, mock_lock = self._make_handle()
        with patch('src.zk_client.KazooException', _KazooException):
            mock_lock.acquire.side_effect = _KazooException()
            with pytest.raises(ZkClientError):
                handle.acquire()

    def test_release_delegates(self):
        handle, mock_lock = self._make_handle()
        handle.release()
        mock_lock.release.assert_called_once()

    def test_release_translates_connection_closed(self):
        handle, mock_lock = self._make_handle()
        with patch('src.zk_client.ConnectionClosedError', _ConnectionClosedError):
            mock_lock.release.side_effect = _ConnectionClosedError()
            with pytest.raises(ZkConnectionClosedError):
                handle.release()

    def test_release_translates_kazoo_exception(self):
        handle, mock_lock = self._make_handle()
        with patch('src.zk_client.KazooException', _KazooException):
            mock_lock.release.side_effect = _KazooException()
            with pytest.raises(ZkClientError):
                handle.release()

    def test_contenders_delegates(self):
        handle, mock_lock = self._make_handle()
        mock_lock.contenders.return_value = ['host1', 'host2']
        assert handle.contenders() == ['host1', 'host2']

    def test_contenders_translates_kazoo_exception(self):
        handle, mock_lock = self._make_handle()
        with patch('src.zk_client.KazooException', _KazooException):
            mock_lock.contenders.side_effect = _KazooException()
            with pytest.raises(ZkClientError):
                handle.contenders()


class TestZkClientGetMtime:

    def test_get_mtime_returns_float(self):
        client = create_mock_zk_client()
        client._path_prefix = '/pgconsul/'
        stat = MagicMock()
        stat.last_modified = 1234567890.5
        client._kazoo.get.return_value = (b'data', stat)
        assert client.get_mtime('leader') == 1234567890.5

    def test_get_mtime_returns_none_on_no_node(self):
        client = create_mock_zk_client()
        with patch('src.zk_client.NoNodeError', _NoNodeError):
            client._kazoo.get.side_effect = _NoNodeError()
            assert client.get_mtime('missing') is None

    def test_get_mtime_returns_none_when_stat_is_none(self):
        client = create_mock_zk_client()
        client._kazoo.get.return_value = (b'data', None)
        assert client.get_mtime('leader') is None

    def test_get_mtime_raises_zk_client_error(self):
        client = create_mock_zk_client()
        with patch('src.zk_client.KazooException', _KazooException):
            client._kazoo.get.side_effect = _KazooException("err")
            with pytest.raises(ZkClientError):
                client.get_mtime('leader')


class TestZkClientLockVersion:

    def test_lock_version_returns_min_sequence(self):
        client = create_mock_zk_client()
        client._path_prefix = '/pgconsul/'
        client._kazoo.get_children.return_value = ['host1__0000000003', 'host2__0000000001', 'host3__0000000002']
        assert client.lock_version('/pgconsul/leader') == '0000000001'

    def test_lock_version_returns_none_when_no_children(self):
        client = create_mock_zk_client()
        client._kazoo.get_children.return_value = []
        assert client.lock_version('/pgconsul/leader') is None

    def test_lock_version_returns_none_on_no_node(self):
        client = create_mock_zk_client()
        with patch('src.zk_client.NoNodeError', _NoNodeError):
            client._kazoo.get_children.side_effect = _NoNodeError()
            assert client.lock_version('/pgconsul/leader') is None

    def test_lock_version_raises_zk_client_error(self):
        client = create_mock_zk_client()
        with patch('src.zk_client.KazooException', _KazooException):
            client._kazoo.get_children.side_effect = _KazooException("err")
            with pytest.raises(ZkClientError):
                client.lock_version('/pgconsul/leader')

    def test_lock_version_single_child(self):
        client = create_mock_zk_client()
        client._kazoo.get_children.return_value = ['host1__0000000007']
        assert client.lock_version('/pgconsul/leader') == '0000000007'
