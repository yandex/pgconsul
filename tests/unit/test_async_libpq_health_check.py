import selectors
import unittest
from unittest.mock import MagicMock, patch

import psycopg2

from src.main import pgconsul
from src.pg import wait_async_operation


class TestAsyncLibpqHealthCheck(unittest.TestCase):
    def setUp(self):
        self.pg = pgconsul.__new__(pgconsul)
        self.pg.config = MagicMock()
        self.pg.config.get.return_value = ''
        self.pg.config.getfloat.return_value = 1.0

    def test_async_query_reports_reachable_primary(self):
        conn = MagicMock()
        conn.poll.side_effect = [
            psycopg2.extensions.POLL_WRITE,
            psycopg2.extensions.POLL_OK,
            psycopg2.extensions.POLL_READ,
            psycopg2.extensions.POLL_OK,
        ]
        conn.cursor.return_value.fetchone.return_value = (42,)
        selector = MagicMock()
        selector.__enter__.return_value = selector
        selector.select.return_value = [(MagicMock(), selectors.EVENT_READ)]

        with patch('src.main.psycopg2.connect', return_value=conn) as connect, \
             patch('src.pg.selectors.DefaultSelector', return_value=selector):
            self.assertFalse(self.pg._check_primary_is_really_dead('primary'))

        connect.assert_called_once_with('host=primary  target_session_attrs=primary', async_=True)
        conn.cursor.return_value.execute.assert_called_once_with('SELECT 42')
        self.assertEqual(selector.select.call_count, 2)
        conn.close.assert_called_once_with()

    def test_stuck_async_query_marks_primary_unreachable_and_closes_connection(self):
        conn = MagicMock()
        conn.poll.side_effect = [psycopg2.extensions.POLL_OK, psycopg2.extensions.POLL_READ]
        selector = MagicMock()
        selector.__enter__.return_value = selector
        selector.select.return_value = []

        with patch('src.main.psycopg2.connect', return_value=conn), \
             patch('src.pg.selectors.DefaultSelector', return_value=selector):
            self.assertTrue(self.pg._check_primary_is_really_dead('primary'))

        conn.cursor.return_value.execute.assert_called_once_with('SELECT 42')
        selector.register.assert_called_once_with(conn.fileno(), selectors.EVENT_READ)
        conn.close.assert_called_once_with()

    def test_poll_registers_current_socket_each_time(self):
        conn = MagicMock()
        conn.poll.side_effect = [
            psycopg2.extensions.POLL_WRITE,
            psycopg2.extensions.POLL_WRITE,
            psycopg2.extensions.POLL_OK,
        ]
        conn.fileno.return_value = 42
        selectors_ = [MagicMock(), MagicMock()]
        for selector in selectors_:
            selector.__enter__.return_value = selector
            selector.select.return_value = [(MagicMock(), selectors.EVENT_WRITE)]

        with patch('src.pg.selectors.DefaultSelector', side_effect=selectors_):
            wait_async_operation(conn, float('inf'))

        for selector in selectors_:
            selector.register.assert_called_once_with(42, selectors.EVENT_WRITE)


if __name__ == '__main__':
    unittest.main()
