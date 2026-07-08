# encoding: utf-8
"""
ZkClient module. Low-level KazooClient wrapper for ZooKeeper connection management.
"""

import logging
import os
import time
from enum import Enum
from random import uniform
from typing import Callable, List, Optional

from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import (
    ConnectionClosedError,
    KazooException,
    LockTimeout,
    NodeExistsError,
    NoNodeError,
    SessionExpiredError,
)
from kazoo.handlers.threading import KazooTimeoutError, SequentialThreadingHandler
from kazoo.security import make_digest_acl


# === Domain exceptions ===

class ZkClientError(Exception):
    """Base ZkClient error; wraps all transport-level failures."""


class ZkSessionExpiredError(ZkClientError):
    """ZK session expired."""


class ZkNoNodeError(ZkClientError):
    """Requested ZK node does not exist."""


class ZkConnectionClosedError(ZkClientError):
    """ZK connection was closed."""


class ZkLockTimeout(ZkClientError):
    """Lock acquisition timed out."""


# === Connection state ===

class ZkConnectionState(Enum):
    CONNECTED = 'connected'
    SUSPENDED = 'suspended'
    LOST = 'lost'


_KAZOO_STATE_MAP = {
    KazooState.CONNECTED: ZkConnectionState.CONNECTED,
    KazooState.SUSPENDED: ZkConnectionState.SUSPENDED,
    KazooState.LOST: ZkConnectionState.LOST,
}


# === Lock handle ===

class LockHandle:
    """Wraps a kazoo lock; translates exceptions to domain types."""

    def __init__(self, lock):
        self._lock = lock

    def acquire(self, blocking=True, timeout=None) -> bool:
        try:
            return self._lock.acquire(blocking=blocking, timeout=timeout)
        except LockTimeout as e:
            raise ZkLockTimeout(e)
        except (KazooException, KazooTimeoutError) as e:
            raise ZkClientError(e)

    def release(self):
        try:
            return self._lock.release()
        except ConnectionClosedError as e:
            raise ZkConnectionClosedError(e)
        except (KazooException, KazooTimeoutError) as e:
            raise ZkClientError(e)

    def contenders(self):
        try:
            return self._lock.contenders()
        except (KazooException, KazooTimeoutError) as e:
            raise ZkClientError(e)


def create_zk_client(config, state_listener=None, path_prefix=None):
    """Factory: create ZkClient from config object."""
    zk_hosts = config.get('global', 'zk_hosts')
    timeout = config.getfloat('global', 'iteration_timeout')
    connect_max_delay = config.getfloat('global', 'zk_connect_max_delay')
    max_delay_on_reinit = config.getint('global', 'max_delay_on_zk_reinit')
    zk_auth = config.getboolean('global', 'zk_auth')
    zk_ssl = config.getboolean('global', 'zk_ssl')
    verify_certs = config.getboolean('global', 'verify_certs')

    zk_username = None
    zk_password = None
    if zk_auth:
        zk_username = config.get('global', 'zk_username')
        zk_password = config.get('global', 'zk_password')
        if not zk_username or not zk_password:
            logging.error('zk_username, zk_password required when zk_auth enabled')

    cert = None
    key = None
    ca = None
    if zk_ssl:
        cert = config.get('global', 'certfile')
        key = config.get('global', 'keyfile')
        ca = config.get('global', 'ca_cert')
        if not cert or not key or not ca:
            logging.error('certfile, keyfile, ca_cert required when zk_ssl enabled')

    if path_prefix is None:
        from . import helpers
        path_prefix = helpers.get_lockpath_prefix()

    return ZkClient(
        hosts=zk_hosts,
        timeout=timeout,
        connect_max_delay=connect_max_delay,
        max_delay_on_reinit=max_delay_on_reinit,
        auth=zk_auth,
        username=zk_username,
        password=zk_password,
        ssl=zk_ssl,
        cert=cert,
        key=key,
        ca=ca,
        verify_certs=verify_certs,
        state_listener=state_listener,
        path_prefix=path_prefix,
    )


class ZkClient(object):
    """
    Low-level ZooKeeper client wrapper.
    Manages KazooClient lifecycle, reconnection with exponential backoff,
    and sync data operations with path_prefix support.

    All data methods raise domain exceptions (ZkClientError hierarchy)
    instead of raw kazoo exceptions.
    """

    def __init__(
        self,
        hosts,
        timeout,
        connect_max_delay,
        max_delay_on_reinit,
        auth=False,
        username=None,
        password=None,
        ssl=False,
        cert=None,
        key=None,
        ca=None,
        verify_certs=True,
        state_listener: Optional[Callable] = None,
        path_prefix='/pgconsul/',
    ):
        self._hosts = hosts
        self._timeout = timeout
        self._connect_max_delay = connect_max_delay
        self._max_delay_on_reinit = max_delay_on_reinit
        self._auth = auth
        self._username = username
        self._password = password
        self._ssl = ssl
        self._cert = cert
        self._key = key
        self._ca = ca
        self._verify_certs = verify_certs
        self._path_prefix = path_prefix

        self._base_delay = 3
        self._failed_inits_count = 0
        self._session_expired = False

        self._state_listener: Optional[Callable] = state_listener
        # Assigned by _create_kazoo_client() before any data method is called.
        self._kazoo: Optional[KazooClient] = None

    @property
    def _client(self) -> KazooClient:
        """Live KazooClient; raises if accessed before init()."""
        assert self._kazoo is not None, "Kazoo client is not initialized"
        return self._kazoo

    def set_state_listener(self, listener: Callable) -> None:
        """Register or replace the external state-change callback."""
        self._state_listener = listener

    def init(self) -> bool:
        """Connect to ZK. Returns True on success."""
        logging.debug("Initializing ZooKeeper client")
        self._create_kazoo_client()
        event = self._client.start_async()
        event.wait(self._timeout)
        self._client.add_listener(self._listener)

        if not self._client.connected:
            logging.warning(
                "ZooKeeper client failed to connect within timeout (%ds). Hosts: %s",
                self._timeout,
                self._hosts,
            )
            return False

        logging.info("Successfully connected to ZooKeeper: %s", self._hosts)
        return True

    def reconnect(self) -> bool:
        """Rebuild the connection with exponential backoff. Returns True on success.

        Connection-only: does not touch locks (owned by Zookeeper.reconnect).
        """
        logging.debug("Reconnecting to ZooKeeper")
        if self._failed_inits_count > 0:
            self._sleep_before_reconnect()

        try:
            self._client.remove_listener(self._listener)
            self._client.stop()
            self._client.close()
            # Use is_connected() (pure state check) — after a fresh init the
            # session is brand new, so _session_expired must not block the check.
            connected = self.init() and self.is_connected()
        except Exception:
            logging.exception('Error during ZooKeeper reconnect')
            connected = False

        if connected:
            logging.info("Successfully reconnected to ZooKeeper")
            self._clear_session_expired_flag()
        else:
            self._session_expired = True
            self._failed_inits_count += 1
            logging.error(
                "Failed to reconnect to ZooKeeper (attempt #%d).",
                self._failed_inits_count,
            )
        return connected

    def is_alive(self) -> bool:
        """True if connected and session healthy. Pure query — no side effects."""
        if self._session_expired:
            return False
        return self._client.state == KazooState.CONNECTED

    def is_connected(self) -> bool:
        """Pure state check: True iff KazooState == CONNECTED. No side effects."""
        return self._client.state == KazooState.CONNECTED

    def close(self) -> None:
        """Explicit shutdown: remove listener, stop and close Kazoo."""
        if self._kazoo is None:
            return
        try:
            self._kazoo.remove_listener(self._listener)
        except Exception:
            logging.debug("Error removing listener during close", exc_info=True)
        try:
            self._kazoo.stop()
            self._kazoo.close()
        except Exception:
            logging.debug("Error stopping Kazoo during close", exc_info=True)

    def _create_kazoo_client(self):
        conn_retry_options = {
            'max_tries': 3,
            'delay': 0.5,
            'backoff': 1.5,
            'max_jitter': 0.9,
            'max_delay': self._connect_max_delay,
        }
        command_retry_options = {
            'max_tries': 0,
            'delay': 0,
            'backoff': 1,
            'max_jitter': 0.9,
            'max_delay': 5,
        }
        args = {
            'hosts': self._hosts,
            'handler': SequentialThreadingHandler(),
            'timeout': self._timeout,
            'connection_retry': conn_retry_options,
            'command_retry': command_retry_options,
        }
        if self._auth:
            acl = make_digest_acl(self._username, self._password, all=True)
            args.update(
                {
                    'default_acl': [acl],
                    'auth_data': [
                        ('digest', '{username}:{password}'.format(username=self._username, password=self._password)),
                    ],
                }
            )
        if self._ssl:
            args.update(
                {
                    'use_ssl': True,
                    'certfile': self._cert,
                    'keyfile': self._key,
                    'ca': self._ca,
                    'verify_certs': self._verify_certs,
                }
            )
        self._kazoo = KazooClient(**args)

    def _listener(self, state):
        """Internal Kazoo listener: update session flags first, then notify Zookeeper.

        Order matters: own flags settle before the domain callback reacts (drops locks).
        """
        if state == KazooState.LOST:
            self._session_expired = True
        elif state == KazooState.CONNECTED:
            self._clear_connection_state_flags()

        if self._state_listener:
            domain_state = _KAZOO_STATE_MAP.get(state)
            if domain_state is not None:
                self._state_listener(domain_state)

    def _sleep_before_reconnect(self):
        """Exponential backoff with jitter."""
        max_sleep = min(self._base_delay * 2 ** self._failed_inits_count, self._max_delay_on_reinit)
        sleep_time = uniform(0, max_sleep)
        logging.warning(
            "ZK reconnection attempt #%d. Sleeping %.2fs (max: %.2fs, configured max: %ds)",
            self._failed_inits_count,
            sleep_time,
            max_sleep,
            self._max_delay_on_reinit,
        )
        time.sleep(sleep_time)

    def _clear_connection_state_flags(self):
        self._clear_session_expired_flag()
        if self._failed_inits_count > 0:
            logging.debug("Clearing failed_inits_count (was %d)", self._failed_inits_count)
            self._failed_inits_count = 0

    def _clear_session_expired_flag(self):
        if self._session_expired:
            logging.debug("Clearing session expired flag")
            self._session_expired = False

    def _resolve_path(self, path):
        if not path.startswith(self._path_prefix):
            return os.path.join(self._path_prefix, path)
        return path

    # === Data operations ===

    def get(self, path):
        """Return (data, stat). Raises ZkNoNodeError, ZkSessionExpiredError, ZkClientError."""
        try:
            return self._client.get(self._resolve_path(path))
        except NoNodeError as e:
            raise ZkNoNodeError(e)
        except SessionExpiredError as e:
            raise ZkSessionExpiredError(e)
        except (KazooException, KazooTimeoutError) as e:
            raise ZkClientError(e)

    def write(self, path, data):
        """
        Atomic write: set → create → set on race.
        Returns True. Raises ZkSessionExpiredError, ZkClientError on failure.
        """
        full_path = self._resolve_path(path)
        encoded = data.encode()
        try:
            try:
                self._client.set(full_path, encoded)
            except NoNodeError:
                try:
                    self._client.create(full_path, value=encoded, makepath=True)
                except NodeExistsError:
                    self._client.set(full_path, encoded)
            return True
        except SessionExpiredError as e:
            raise ZkSessionExpiredError(e)
        except (KazooException, KazooTimeoutError) as e:
            raise ZkClientError(e)

    def ensure_path(self, path):
        """Ensure path exists. Returns stat. Raises ZkClientError on failure."""
        full_path = self._resolve_path(path)
        try:
            return self._client.ensure_path(full_path)
        except (KazooException, KazooTimeoutError) as e:
            raise ZkClientError(e)

    def exists(self, path):
        """
        Return True if path exists, False if absent.
        Raises ZkClientError on connection failure.
        """
        try:
            return bool(self._client.exists(self._resolve_path(path)))
        except (KazooException, KazooTimeoutError) as e:
            raise ZkClientError(e)

    def get_children(self, path) -> List[str]:
        """
        Return list of children ([] if node absent).
        Raises ZkClientError on connection failure.
        """
        full_path = self._resolve_path(path)
        try:
            return self._client.get_children(full_path)
        except NoNodeError:
            logging.debug('No node found at path: %s', full_path, exc_info=True)
            return []
        except (KazooException, KazooTimeoutError) as e:
            raise ZkClientError(e)

    def delete(self, path, recursive=False):
        """Delete path. Returns True (including when absent). Raises ZkClientError on error."""
        full_path = self._resolve_path(path)
        try:
            self._client.delete(full_path, recursive=recursive)
            return True
        except NoNodeError:
            logging.info('No node %s was found in ZK to delete it.', full_path)
            return True
        except (KazooException, KazooTimeoutError) as e:
            raise ZkClientError(e)

    # === Lock recipes ===

    def make_lock(self, path, identifier) -> LockHandle:
        return LockHandle(self._client.Lock(path, identifier))

    def make_read_lock(self, path, identifier) -> LockHandle:
        return LockHandle(self._client.ReadLock(path, identifier))
