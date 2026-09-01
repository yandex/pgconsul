from unittest.mock import MagicMock, patch

from src.replication_manager import ReplicationManager, ReplicationManagerConfig


def _manager(timeout=10.0):
    config = ReplicationManagerConfig(
        priority=1,
        primary_unavailability_timeout=5.0,
        before_async_unavailability_timeout=timeout,
        quorum_removal_delay=0.0,
    )
    return ReplicationManager(config, MagicMock(), MagicMock())


def test_streaming_ha_replica_requires_sync():
    manager = _manager()
    db_state = {'replics_info': [
        {'application_name': 'ha-replica', 'state': 'streaming'},
        {'application_name': 'side-replica', 'state': 'streaming'},
    ]}

    with patch('src.replication_manager.helpers.app_name_from_fqdn', side_effect=lambda host: host):
        assert manager._get_needed_replication_type(db_state, ['ha-replica']) == 'sync'


def test_only_non_ha_streaming_replica_switches_to_async_after_timeout():
    manager = _manager(timeout=10.0)
    db_state = {'replics_info': [
        {'application_name': 'side-replica', 'state': 'streaming'},
    ]}

    with patch('src.replication_manager.helpers.app_name_from_fqdn', side_effect=lambda host: host), \
         patch('src.replication_manager.time.time', side_effect=[100.0, 111.0]):
        assert manager._get_needed_replication_type(db_state, ['ha-replica']) == 'sync'
        assert manager._get_needed_replication_type(db_state, ['ha-replica']) == 'async'


def test_streaming_ha_replica_resets_async_timeout():
    manager = _manager(timeout=10.0)
    missing = {'replics_info': []}
    streaming = {'replics_info': [
        {'application_name': 'ha-replica', 'state': 'streaming'},
    ]}

    with patch('src.replication_manager.helpers.app_name_from_fqdn', side_effect=lambda host: host), \
         patch('src.replication_manager.time.time', return_value=100.0):
        assert manager._get_needed_replication_type(missing, ['ha-replica']) == 'sync'
        assert manager._get_needed_replication_type(streaming, ['ha-replica']) == 'sync'

    assert manager._async_waiting_timestamp is None
