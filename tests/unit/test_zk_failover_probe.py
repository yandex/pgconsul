import json
from unittest.mock import MagicMock, patch

from src.failover import FailoverHealthReport, FailoverProbe
from src.types import DesiredPrimary, DurabilityConfig


def test_probe_counter_is_cas_incremented(zk):
    current = FailoverProbe(
        4, 'primary', ('a', 'primary'), 8, 'old', expires_at=99.0,
    )
    zk.get_failover_probe = MagicMock(return_value=(current, 11))
    zk.is_lock_holder = MagicMock(return_value=True)
    zk._zk_client.compare_and_set = MagicMock(return_value=12)

    with patch('src.zk.time.time', return_value=100.0), \
         patch('src.zk.uuid.uuid4') as uuid4:
        uuid4.return_value.hex = 'new-operation'
        probe = zk.start_failover_probe(
            'primary', (DurabilityConfig.build(['primary', 'a']),), 8, 10.0,
        )

    assert probe == FailoverProbe(
        5, 'primary', ('a', 'primary'), 8, 'new-operation',
        (('a', 'primary'),), 110.0,
    )
    path, value, version = zk._zk_client.compare_and_set.call_args.args
    assert path == 'failover_probe'
    assert json.loads(value)['probe_id'] == 5
    assert version == 11


def test_active_matching_probe_is_reused_without_cas(zk):
    current = FailoverProbe(
        4,
        'primary',
        ('a', 'primary'),
        8,
        'old-operation',
        (('a', 'primary'),),
        110.0,
    )
    zk.get_failover_probe = MagicMock(return_value=(current, 11))
    zk.is_lock_holder = MagicMock(return_value=True)
    zk._zk_client.compare_and_set = MagicMock()

    with patch('src.zk.time.time', return_value=100.0):
        probe = zk.start_failover_probe(
            'primary', (DurabilityConfig.build(['primary', 'a']),), 8, 10.0,
        )

    assert probe is current
    zk._zk_client.compare_and_set.assert_not_called()


def test_old_probe_report_is_ignored(zk):
    probe = FailoverProbe(5, 'primary', ('a', 'primary'), 8, 'operation')
    zk.get = MagicMock(return_value=FailoverHealthReport(
        4, 'primary', 8, True, True, 100,
    ).to_dict())

    assert zk.get_failover_health('a', probe) is None


def test_desired_primary_round_trips_through_cas(zk):
    desired = DesiredPrimary(None, 'failover-1', 'failover')
    zk._zk_client.compare_and_set = MagicMock(return_value=3)

    assert zk.write_desired_primary(desired, 2) == 3

    path, value, version = zk._zk_client.compare_and_set.call_args.args
    assert path == 'desired_primary'
    assert json.loads(value) == desired.to_dict()
    assert version == 2
