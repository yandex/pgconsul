"""
Unit tests for src/log_formatters.py
"""

import logging
import sys
import os
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from log_formatters import (
    format_db_state_for_log,
    format_zk_state_for_log,
    format_replics_info_for_log,
    log_separator,
    log_event,
)


class TestFormatDbStateForLog(unittest.TestCase):
    def test_empty_dict(self):
        result = format_db_state_for_log({})
        self.assertEqual(result, 'DB State: (empty)')

    def test_none(self):
        result = format_db_state_for_log(None)
        self.assertEqual(result, 'DB State: (empty)')

    def test_basic_primary(self):
        db_state = {
            'role': 'primary',
            'timeline': 5,
            'lsn': '0/1234ABCD',
            'running': True,
            'bouncer_running': True,
        }
        result = format_db_state_for_log(db_state)
        self.assertIn('DB State:', result)
        self.assertIn('Role: PRIMARY', result)
        self.assertIn('Timeline: 5', result)
        self.assertIn('LSN: 0/1234ABCD', result)
        self.assertIn('PostgreSQL: running', result)
        self.assertIn('Bouncer: running', result)
        self.assertIn('Replicas: none', result)

    def test_stopped_postgres(self):
        db_state = {
            'role': 'replica',
            'running': False,
            'bouncer_running': False,
        }
        result = format_db_state_for_log(db_state)
        self.assertIn('Role: REPLICA', result)
        self.assertIn('PostgreSQL: stopped', result)
        self.assertIn('Bouncer: stopped', result)

    def test_with_replicas(self):
        db_state = {
            'role': 'primary',
            'running': True,
            'bouncer_running': True,
            'replics_info': [
                {
                    'client_hostname': 'replica1.example.com',
                    'state': 'streaming',
                    'sync_state': 'sync',
                    'replay_lag_msec': 10,
                },
                {
                    'client_hostname': 'replica2.example.com',
                    'state': 'streaming',
                    'sync_state': 'async',
                    'replay_lag_msec': 250,
                },
            ],
        }
        result = format_db_state_for_log(db_state)
        self.assertIn('Replicas (2):', result)
        self.assertIn('replica1.example.com', result)
        self.assertIn('state=streaming', result)
        self.assertIn('sync=sync', result)
        self.assertIn('lag=10ms', result)
        self.assertIn('replica2.example.com', result)
        self.assertIn('sync=async', result)
        self.assertIn('lag=250ms', result)

    def test_with_archive_command(self):
        db_state = {
            'role': 'primary',
            'running': True,
            'archive_command': 'wal-g wal-push %p',
        }
        result = format_db_state_for_log(db_state)
        self.assertIn('Archive command: wal-g wal-push %p', result)

    def test_unknown_role(self):
        db_state = {'running': True}
        result = format_db_state_for_log(db_state)
        self.assertIn('Role: UNKNOWN', result)


class TestFormatZkStateForLog(unittest.TestCase):
    def test_empty_dict(self):
        result = format_zk_state_for_log({})
        self.assertEqual(result, 'ZK State: (empty)')

    def test_none(self):
        result = format_zk_state_for_log(None)
        self.assertEqual(result, 'ZK State: (empty)')

    def test_basic_state(self):
        zk_state = {
            'timeline': 3,
            'leader_lock_holder': 'primary.example.com',
            'quorum_lock_holders': ['primary.example.com', 'replica1.example.com'],
            'alive_lock_holders': ['primary.example.com', 'replica1.example.com', 'replica2.example.com'],
        }
        result = format_zk_state_for_log(zk_state)
        self.assertIn('ZK State:', result)
        self.assertIn('Timeline: 3', result)
        self.assertIn('Leader lock: primary.example.com', result)
        self.assertIn('Quorum locks (2):', result)
        self.assertIn('Alive locks (3):', result)

    def test_no_leader(self):
        zk_state = {
            'timeline': 1,
            'leader_lock_holder': None,
            'quorum_lock_holders': [],
        }
        result = format_zk_state_for_log(zk_state)
        self.assertIn('Leader lock: NONE', result)
        self.assertIn('Quorum locks: NONE', result)
        self.assertIn('Alive locks: NONE', result)

    def test_empty_alive_locks(self):
        zk_state = {
            'timeline': 1,
            'leader_lock_holder': 'primary.example.com',
            'alive_lock_holders': [],
        }
        result = format_zk_state_for_log(zk_state)
        self.assertIn('Alive locks: NONE', result)

    def test_switchover_state(self):
        zk_state = {
            'timeline': 2,
            'leader_lock_holder': 'primary.example.com',
            'switchover_state': 'initiated',
            'switchover_candidate': 'replica1.example.com',
        }
        result = format_zk_state_for_log(zk_state)
        self.assertIn('Switchover state: initiated', result)
        self.assertIn('Switchover candidate: replica1.example.com', result)

    def test_failover_state(self):
        zk_state = {
            'timeline': 4,
            'leader_lock_holder': None,
            'failover_state': 'promoting',
            'current_promoting_host': 'replica1.example.com',
        }
        result = format_zk_state_for_log(zk_state)
        self.assertIn('Failover state: promoting', result)
        self.assertIn('Promoting host: replica1.example.com', result)

    def test_maintenance(self):
        zk_state = {
            'timeline': 1,
            'leader_lock_holder': 'primary.example.com',
            'maintenance': 'primary.example.com',
        }
        result = format_zk_state_for_log(zk_state)
        self.assertIn('Maintenance: primary.example.com', result)

    def test_no_switchover_no_failover(self):
        zk_state = {
            'timeline': 1,
            'leader_lock_holder': 'primary.example.com',
        }
        result = format_zk_state_for_log(zk_state)
        self.assertNotIn('Switchover', result)
        self.assertNotIn('Failover', result)


class TestFormatReplicsInfoForLog(unittest.TestCase):
    def test_empty_list(self):
        result = format_replics_info_for_log([])
        self.assertEqual(result, 'Replicas: none')

    def test_none(self):
        result = format_replics_info_for_log(None)
        self.assertEqual(result, 'Replicas: none')

    def test_single_replica(self):
        replics_info = [
            {
                'client_hostname': 'replica1.example.com',
                'state': 'streaming',
                'sync_state': 'sync',
                'replay_lag_msec': 5,
                'sent_lsn': '0/5000000',
                'replay_lsn': '0/4FFFF00',
            }
        ]
        result = format_replics_info_for_log(replics_info)
        self.assertIn('Replicas (1):', result)
        self.assertIn('replica1.example.com', result)
        self.assertIn('state=streaming', result)
        self.assertIn('sync=sync', result)
        self.assertIn('lag=5ms', result)
        self.assertIn('sent_lsn=0/5000000', result)
        self.assertIn('replay_lsn=0/4FFFF00', result)

    def test_multiple_replicas(self):
        replics_info = [
            {'client_hostname': 'r1', 'state': 'streaming', 'sync_state': 'sync', 'replay_lag_msec': 0},
            {'client_hostname': 'r2', 'state': 'streaming', 'sync_state': 'async', 'replay_lag_msec': 100},
        ]
        result = format_replics_info_for_log(replics_info)
        self.assertIn('Replicas (2):', result)
        self.assertIn('r1', result)
        self.assertIn('r2', result)


class TestLogSeparator(unittest.TestCase):
    def test_log_separator_info(self):
        with self.assertLogs('log_formatters', level='INFO') as cm:
            log_separator(level='info')
        self.assertEqual(len(cm.output), 1)
        self.assertIn('=' * 60, cm.output[0])

    def test_log_separator_warning(self):
        with self.assertLogs('log_formatters', level='WARNING') as cm:
            log_separator(level='warning')
        self.assertEqual(len(cm.output), 1)
        self.assertIn('WARNING', cm.output[0])

    def test_log_separator_custom_char_and_length(self):
        with self.assertLogs('log_formatters', level='INFO') as cm:
            log_separator(level='info', char='-', length=30)
        self.assertIn('-' * 30, cm.output[0])


class TestLogEvent(unittest.TestCase):
    def test_log_event_warning(self):
        with self.assertLogs(level='WARNING') as cm:
            log_event('SWITCHOVER STARTED')
        self.assertEqual(len(cm.output), 3)
        self.assertIn('SWITCHOVER STARTED', cm.output[1])
        self.assertIn('=' * 60, cm.output[0])
        self.assertIn('=' * 60, cm.output[2])

    def test_log_event_error(self):
        with self.assertLogs(level='ERROR') as cm:
            log_event('FAILOVER: Primary has died', level='error')
        self.assertEqual(len(cm.output), 3)
        self.assertIn('ERROR', cm.output[0])
        self.assertIn('FAILOVER: Primary has died', cm.output[1])

    def test_log_event_with_detail(self):
        with self.assertLogs(level='WARNING') as cm:
            log_event('REWIND', detail='replica1.example.com', level='warning')
        self.assertIn('REWIND: replica1.example.com', cm.output[1])

    def test_log_event_custom_char_and_length(self):
        with self.assertLogs(level='WARNING') as cm:
            log_event('MAINTENANCE', char='-', length=30)
        self.assertIn('-' * 30, cm.output[0])
        self.assertIn('MAINTENANCE', cm.output[1])


if __name__ == '__main__':
    unittest.main()
