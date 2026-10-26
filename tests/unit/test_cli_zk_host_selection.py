import unittest
from configparser import RawConfigParser
from unittest.mock import patch

from src import cli


def make_config():
    conf = RawConfigParser()
    conf.add_section('global')
    conf.set('global', 'zk_session_timeout', '10')
    conf.set('global', 'zk_connect_max_delay', '60')
    conf.set('global', 'zk_auth', 'no')
    conf.set('global', 'zk_ssl', 'no')
    return conf


class TestCliZkHostSelection(unittest.TestCase):
    def test_tries_hosts_in_order_until_one_connects(self):
        conf = make_config()
        conf.set('global', 'zk_hosts', 'unreachable:2181,reachable:2181')
        clients = []

        class FakeKazooClient:
            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.stopped = False
                self.closed = False
                clients.append(self)

            def start(self, timeout):
                self.start_timeout = timeout
                if self.kwargs['hosts'].startswith('unreachable:'):
                    raise TimeoutError('connection timed out')

            def stop(self):
                self.stopped = True

            def close(self):
                self.closed = True

        with patch.object(cli, 'collect_hosts', return_value=([('unreachable', 2181), ('reachable', 2181)], None)), \
                patch.object(cli.random, 'shuffle'), patch.object(cli.zookeeper, 'KazooClient', FakeKazooClient):
            cli.select_zk_host(conf)

        self.assertEqual([client.kwargs['hosts'] for client in clients], ['unreachable:2181', 'reachable:2181'])
        self.assertEqual(conf.get('global', 'zk_hosts'), 'reachable:2181')
        self.assertTrue(all(client.stopped and client.closed for client in clients))
        self.assertTrue(all(client.start_timeout == 10.0 / 3 for client in clients))
        self.assertTrue(all(client.kwargs['connection_retry']['max_tries'] == 0 for client in clients))

    def test_custom_timeout_and_chroot_are_preserved(self):
        conf = make_config()
        conf.set('global', 'zk_hosts', 'zk1:2181/pgconsul')
        conf.set('global', 'zk_cli_connect_timeout', '2')
        conf.set('global', 'zk_auth', 'yes')
        conf.set('global', 'zk_username', 'user')
        conf.set('global', 'zk_password', 'password')
        conf.set('global', 'zk_ssl', 'yes')
        conf.set('global', 'certfile', '/tmp/client.crt')
        conf.set('global', 'keyfile', '/tmp/client.key')
        conf.set('global', 'ca_cert', '/tmp/ca.crt')
        conf.set('global', 'verify_certs', 'yes')
        with patch.object(cli, 'collect_hosts', return_value=([('zk1', 2181)], '/pgconsul')), \
                patch.object(cli.zookeeper, 'KazooClient') as client:
            cli.select_zk_host(conf)

        self.assertEqual(client.call_args.kwargs['hosts'], 'zk1:2181/pgconsul')
        self.assertEqual(client.call_args.kwargs['auth_data'], [('digest', 'user:password')])
        self.assertTrue(client.call_args.kwargs['use_ssl'])
        self.assertTrue(client.call_args.kwargs['verify_certs'])
        client.return_value.start.assert_called_once_with(timeout=2.0)
        self.assertEqual(conf.get('global', 'zk_hosts'), 'zk1:2181/pgconsul')

    def test_all_hosts_failed_leaves_original_list(self):
        conf = make_config()
        conf.set('global', 'zk_hosts', 'zk1:2181,zk2:2181')
        with patch.object(cli, 'collect_hosts', return_value=([('zk1', 2181), ('zk2', 2181)], None)), \
                patch.object(cli.random, 'shuffle'), patch.object(cli.zookeeper, 'KazooClient') as client:
            client.return_value.start.side_effect = TimeoutError('connection timed out')
            with self.assertRaisesRegex(RuntimeError, 'Could not connect to any ZooKeeper host'):
                cli.select_zk_host(conf)

        self.assertEqual(conf.get('global', 'zk_hosts'), 'zk1:2181,zk2:2181')
        self.assertEqual(client.call_count, 2)

    def test_daemon_client_keeps_ensemble_and_session_timeout(self):
        conf = make_config()
        conf.set('global', 'zk_hosts', 'zk1:2181,zk2:2181')
        with patch.object(cli.zookeeper, 'KazooClient') as client:
            cli.zookeeper.make_kazoo_client(conf)

        self.assertEqual(client.call_args.kwargs['hosts'], 'zk1:2181,zk2:2181')
        self.assertEqual(client.call_args.kwargs['timeout'], 10.0)
        self.assertEqual(client.call_args.kwargs['connection_retry']['max_tries'], 3)


if __name__ == '__main__':
    unittest.main()
