# encoding: utf-8
"""
Unit tests for Zookeeper.write_host_stat and get_replics_info_for_host (step 12d, Variant A).
"""
from unittest.mock import MagicMock


class TestZookeeperWriteHostStat:
    """Zookeeper.write_host_stat — pure ZK logic moved from main.py."""

    def test_ha_host_success(self, zk):
        zk.ensure_host_ha = MagicMock(return_value=True)
        zk.write_host_wal_receiver = MagicMock(return_value=True)
        zk.write_host_replics_info = MagicMock(return_value=True)
        db_state = {'wal_receiver': None, 'replics_info': None}

        assert zk.write_host_stat('host1', db_state, stream_from=None) is True
        zk.ensure_host_ha.assert_called_once_with('host1')

    def test_non_ha_deletes_host_ha(self, zk):
        zk.delete_host_ha = MagicMock(return_value=True)
        zk.write_host_wal_receiver = MagicMock(return_value=True)
        zk.write_host_replics_info = MagicMock(return_value=True)
        db_state = {'wal_receiver': None, 'replics_info': None}

        assert zk.write_host_stat('host1', db_state, stream_from='source') is True
        zk.delete_host_ha.assert_called_once_with('host1')

    def test_ensure_host_ha_fails_returns_false(self, zk):
        zk.ensure_host_ha = MagicMock(return_value=False)
        db_state = {'wal_receiver': None, 'replics_info': None}

        assert zk.write_host_stat('host1', db_state, stream_from=None) is False

    def test_delete_host_ha_fails_returns_false(self, zk):
        zk.delete_host_ha = MagicMock(return_value=False)
        db_state = {'wal_receiver': None, 'replics_info': None}

        assert zk.write_host_stat('host1', db_state, stream_from='source') is False

    def test_wal_receiver_write_fails_returns_false(self, zk):
        zk.ensure_host_ha = MagicMock(return_value=True)
        zk.write_host_wal_receiver = MagicMock(return_value=False)
        db_state = {'wal_receiver': {'info': 1}, 'replics_info': None}

        assert zk.write_host_stat('host1', db_state, stream_from=None) is False

    def test_replics_info_write_fails_returns_false(self, zk):
        zk.ensure_host_ha = MagicMock(return_value=True)
        zk.write_host_wal_receiver = MagicMock(return_value=True)
        zk.write_host_replics_info = MagicMock(return_value=False)
        db_state = {'wal_receiver': None, 'replics_info': [1, 2]}

        assert zk.write_host_stat('host1', db_state, stream_from=None) is False

    def test_writes_wal_receiver_when_present(self, zk):
        zk.ensure_host_ha = MagicMock(return_value=True)
        zk.write_host_wal_receiver = MagicMock(return_value=True)
        zk.write_host_replics_info = MagicMock(return_value=True)
        wal_receiver = {'last_msg_receipt_time_msec': 1000}
        db_state = {'wal_receiver': wal_receiver, 'replics_info': None}

        assert zk.write_host_stat('host1', db_state, stream_from=None) is True
        zk.write_host_wal_receiver.assert_called_once_with(wal_receiver, 'host1')

    def test_writes_replics_info_when_present(self, zk):
        zk.ensure_host_ha = MagicMock(return_value=True)
        zk.write_host_wal_receiver = MagicMock(return_value=True)
        zk.write_host_replics_info = MagicMock(return_value=True)
        replics_info = [{'host': 'h1'}]
        db_state = {'wal_receiver': None, 'replics_info': replics_info}

        assert zk.write_host_stat('host1', db_state, stream_from=None) is True
        zk.write_host_replics_info.assert_called_once_with(replics_info, 'host1')


