"""Regression tests for the public durability-manager operations."""
from configparser import RawConfigParser
from unittest.mock import MagicMock, patch

from src.durability_manager import DurabilityManager, build_durability_manager_config
from src.types import DurabilityConfig


def _manager():
    config = RawConfigParser()
    config.add_section('primary')
    config.set('primary', 'quorum_removal_delay', '0')
    db, zk = MagicMock(), MagicMock()
    return DurabilityManager(build_durability_manager_config(config), db, zk), db, zk


def test_pre_promote_ssn_is_applied_and_persisted():
    manager, db, zk = _manager()
    db.change_replication_type.return_value = True
    zk.write_ssn_on_changes.return_value = True

    with patch('src.durability_manager.helpers.get_hostname', return_value='candidate'):
        assert manager.set_ssn_before_promote(DurabilityConfig.build(['candidate', 'host1', 'host2']))

    db.change_replication_type.assert_called_once_with('ANY 1(host1,host2)')
    zk.write_ssn_on_changes.assert_called_once_with('ANY 1(host1,host2)')


def test_pre_promote_without_durability_sets_async():
    manager, db, zk = _manager()
    db.change_replication_type.return_value = True
    zk.write_ssn_on_changes.return_value = True

    assert manager.set_ssn_before_promote(None)
    db.change_replication_type.assert_called_once_with('')


def test_mandatory_replica_requires_primary_lock():
    manager, db, zk = _manager()
    zk.is_lock_holder.return_value = False

    assert not manager.set_mandatory_sync_replica(
        DurabilityConfig.build(['primary', 'candidate']), 'candidate',
    )
    db.change_replication_type.assert_not_called()


def test_ordinary_policy_keeps_only_alive_members_and_streaming_additions():
    manager, _, _ = _manager()
    with patch('src.durability_manager.helpers.get_hostname', return_value='primary'):
        target = manager.desired_durability(
            {'replication_state': ('sync', 'ANY 1(replica1)'), 'replics_info': [
                {'application_name': 'replica2', 'state': 'streaming'},
            ]},
            {'replica1', 'replica2'}, {'replica2'},
            DurabilityConfig.build(['primary', 'replica1']),
        )

    assert target == DurabilityConfig.build(['primary', 'replica2'])
