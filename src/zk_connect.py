"""Prefer the first TCP-reachable endpoint without creating extra ZK sessions.

Kazoo 2.10.0 has no public host-provider hook. The small connection adapter
below only reorders its expanded (hostname, IP, port) list; Kazoo still owns
TLS, authentication, sessions, operations and reconnection.
"""

import asyncio
from dataclasses import dataclass
import logging
import socket
from typing import Callable

from kazoo.client import KazooClient
from kazoo.protocol.connection import ConnectionHandler
from kazoo.retry import ForceRetryError


_PROBE_TIMEOUT = 5.0


@dataclass(frozen=True)
class _Endpoint:
    hostname: str
    address: str
    port: int


async def _probe(endpoint: _Endpoint) -> _Endpoint | None:
    try:
        family = socket.AF_INET6 if ':' in endpoint.address else socket.AF_INET
        with socket.socket(family, socket.SOCK_STREAM) as sock:
            sock.setblocking(False)
            await asyncio.get_running_loop().sock_connect(sock, (endpoint.address, endpoint.port))
        return endpoint
    except OSError as exc:
        logging.warning('ZooKeeper TCP probe failed: %s (%s):%d: %s',
                        endpoint.hostname, endpoint.address, endpoint.port, exc)
        return None


async def _race(endpoints: list[_Endpoint], timeout: float, stopped: Callable[[], bool]) -> _Endpoint | None:
    if not endpoints or stopped():
        return None

    async def wait_stopped() -> _Endpoint | None:
        while not stopped():
            await asyncio.sleep(0.1)
        return None

    tasks = [asyncio.create_task(_probe(endpoint)) for endpoint in endpoints]
    tasks.append(asyncio.create_task(wait_stopped()))
    remaining = len(endpoints)
    try:
        for completed in asyncio.as_completed(tasks, timeout=timeout):
            endpoint = await completed
            if stopped():
                return None
            if endpoint is not None:
                return endpoint
            remaining -= 1
            if remaining == 0:
                break
    except asyncio.TimeoutError:
        logging.warning('ZooKeeper TCP probes exceeded shared deadline (%.2fs)', timeout)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return None


def _first_reachable(endpoints: list[_Endpoint], timeout: float, stopped: Callable[[], bool]) -> _Endpoint | None:
    # Called in Kazoo's connection thread, which has no running asyncio loop.
    return asyncio.run(_race(endpoints, timeout, stopped))


class _ParallelConnectionHandler(ConnectionHandler):
    def _expand_client_hosts(self):
        # Keep Kazoo's DNS handling, hostnames and randomized fallback order.
        host_ports = super()._expand_client_hosts()
        if not host_ports or self.client._stopped.is_set():
            return host_ports
        endpoints = [_Endpoint(*host_port) for host_port in host_ports]
        preferred = _first_reachable(
            endpoints, min(_PROBE_TIMEOUT, self.client._session_timeout / 1000.0),
            self.client._stopped.is_set,
        )
        if self.client._stopped.is_set():
            return host_ports
        if preferred is None:
            # Retry a parallel round, rather than spend one timeout per dead IP.
            # The configured ensemble remains intact for later DNS/retry rounds.
            raise ForceRetryError('No ZooKeeper endpoint passed the TCP probes')
        endpoints.remove(preferred)
        endpoints.insert(0, preferred)
        logging.info('Preferred ZooKeeper endpoint after TCP probes: %s (%s):%d',
                     preferred.hostname, preferred.address, preferred.port)
        return [(endpoint.hostname, endpoint.address, endpoint.port) for endpoint in endpoints]


class ParallelKazooClient(KazooClient):
    """Kazoo 2.10.0 with parallel TCP selection before each full host cycle."""

    _connection: ConnectionHandler

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # The original connection is not started and has no sockets/workers yet.
        # Reuse its normalized SASL options and copied connection retry policy.
        original = self._connection
        self._connection = _ParallelConnectionHandler(
            self, original.retry_sleeper, logger=original.logger,
            sasl_options=original.sasl_options,
        )
