"""
PostgreSQL database client for pgconsul faultstorm tests.

Implements DatabaseClient for PostgreSQL accessed via pgbouncer
in a pgconsul HA cluster running in Docker containers.
"""

import logging
from typing import Set, List

import psycopg2

from faultstorm.db_client import DatabaseClient

logger = logging.getLogger(__name__)


class PgConsulClient(DatabaseClient):
    """PostgreSQL client for pgconsul clusters in Docker.

    Connects to PostgreSQL nodes via pgbouncer (port 6432) using
    the Docker container naming convention: pgconsul_<node>_1.pgconsul_pgconsul_net
    """

    def __init__(self, db_nodes: List[str], port: int = 6432,
                 database: str = "postgres", user: str = "repl",
                 password: str = "repl", connect_timeout: int = 5):
        """Initialize PostgreSQL client.

        Args:
            db_nodes: List of PG node names
                      (e.g. ["postgresql1", "postgresql2", "postgresql3"])
            port: Database port (default: 6432 for pgbouncer)
            database: Database name
            user: Database user
            password: Database password
            connect_timeout: Connection timeout in seconds
        """
        self._db_nodes = db_nodes
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._connect_timeout = connect_timeout

    def get_db_nodes(self) -> List[str]:
        return list(self._db_nodes)

    def _connect(self, node: str):  # type: ignore[return]
        """Create a connection to the specified node.

        Args:
            node: Node name

        Returns:
            psycopg2 connection
        """
        host = f"pgconsul_{node}_1.pgconsul_pgconsul_net"
        return psycopg2.connect(
            host=host,
            port=self._port,
            database=self._database,
            user=self._user,
            password=self._password,
            connect_timeout=self._connect_timeout,
        )

    def setup(self, node: str) -> None:
        conn = self._connect(node)
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS set (value INT PRIMARY KEY)"
                )
        finally:
            conn.close()

    def add(self, node: str, value: int) -> None:
        conn = self._connect(node)
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("INSERT INTO set VALUES (%s)", (value,))
        finally:
            conn.close()

    def read(self, node: str) -> Set[int]:
        conn = self._connect(node)
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM set FOR UPDATE")
                return {row[0] for row in cur.fetchall()}
        finally:
            conn.close()

    def is_definite_failure(self, exc: Exception) -> bool:
        msg = str(exc)
        return 'cannot execute' in msg and 'read-only transaction' in msg
