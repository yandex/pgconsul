from unittest.mock import MagicMock, patch

from src.replication_manager import ReplicationManager, ReplicationManagerConfig
from src.types import DurabilityConfig


def _manager(removal_delay=0.0):
    return ReplicationManager(
        ReplicationManagerConfig(
            priority=1,
            primary_unavailability_timeout=5.0,
            quorum_removal_delay=removal_delay,
        ),
        MagicMock(),
        MagicMock(),
    )


def _desired(manager, *, members, alive, replics_info):
    with patch('src.replication_manager.helpers.get_hostname', return_value='primary'), \
         patch('src.replication_manager.helpers.app_name_from_fqdn', side_effect=lambda host: host):
        return manager.desired_durability(
            {
                'replics_info': replics_info,
                'replication_state': ('sync', 'ANY 1(replica)'),
            },
            {'replica', 'new-replica'},
            alive,
            DurabilityConfig.build(members),
        )


def test_existing_alive_member_is_kept_when_streaming_is_lost():
    target = _desired(
        _manager(),
        members=['primary', 'replica'],
        alive={'replica'},
        replics_info=[],
    )

    assert target == DurabilityConfig.build(['primary', 'replica'])


def test_dead_member_is_removed_and_primary_becomes_async_after_removal():
    target = _desired(
        _manager(removal_delay=0.0),
        members=['primary', 'replica'],
        alive=set(),
        replics_info=[],
    )

    assert target == DurabilityConfig.build(['primary'])


def test_new_member_is_added_only_after_it_is_alive_and_streaming():
    target = _desired(
        _manager(),
        members=['primary', 'replica'],
        alive={'replica', 'new-replica'},
        replics_info=[
            {'application_name': 'new-replica', 'state': 'streaming'},
        ],
    )

    assert target == DurabilityConfig.build(['primary', 'replica', 'new-replica'])
