import logging
from dataclasses import replace
from unittest.mock import MagicMock, patch

import src

src.read_config = MagicMock()

from src.failover import FailoverRequest
from src.types import DurabilityConfig, DurabilityState
from src.utils import Failover


def _failover():
    failover = Failover.__new__(Failover)
    failover._log = logging.getLogger('test-failover')
    failover._zk = MagicMock()
    failover._zk.PRIMARY_LOCK_PATH = 'leader'
    failover._zk.LAST_PRIMARY_PATH = 'last-primary'
    failover._zk.get_failover_state.return_value = None
    failover._zk.get_current_lock_holder.return_value = 'old-primary'
    failover._zk.get.return_value = 'old-primary'
    return failover


def test_regular_manual_failover_only_creates_request():
    failover = _failover()
    failover._zk.get_failover_request.return_value = (None, None)
    failover._zk.write_failover_request.return_value = 0

    assert failover.initiate() is True

    request = failover._zk.write_failover_request.call_args.args[0]
    assert request.primary == 'old-primary'
    assert request.with_data_loss is False
    assert request.fence_wal_sources is True
    assert request.winner is None


def test_data_loss_failover_prints_votes_and_persists_interactive_winner(capsys):
    failover = _failover()
    stored: list[FailoverRequest | None] = [None]
    version = [None]

    def get_request():
        return stored[0], version[0]

    def write_request(request, expected_version):
        assert expected_version == version[0]
        if request.with_data_loss and not request.electorate:
            request = replace(request, electorate=('host-a', 'host-b'))
        stored[0] = request
        version[0] = 0 if version[0] is None else version[0] + 1
        return version[0]

    failover._zk.get_failover_request.side_effect = get_request
    failover._zk.write_failover_request.side_effect = write_request
    failover._zk.get_failover_version.side_effect = (
        lambda: stored[0].operation_id if stored[0] is not None else None
    )
    failover._zk.get_election_host_vote_with_timeline.side_effect = (
        lambda host, _: {
            'host-a': (100, 1, 2),
            'host-b': (200, 1, 1),
        }[host]
    )
    durability = DurabilityConfig.build(['old-primary', 'host-a', 'host-b'])
    failover._zk.get_durability_state.return_value = (
        DurabilityState(durability), 3,
    )
    failover._zk.get_timeline.return_value = 1

    with patch('builtins.input', return_value='host-b'):
        assert failover.initiate(with_data_loss=True, timeout=0) is True

    assert stored[0] is not None
    assert stored[0].winner == 'host-b'
    output = capsys.readouterr().out
    assert output.index('host-a') < output.index('host-b')
    assert 'host-b: UNSAFE' in output


def test_data_loss_yes_chooses_freshest_host_on_highest_timeline():
    failover = _failover()
    request = FailoverRequest('old-primary', 'operation-1', True, electorate=('host-a', 'host-b'))
    failover._zk.get_failover_request.side_effect = [
        (None, None),
        (request, 0),
        (request, 0),
    ]
    failover._zk.write_failover_request.return_value = 1
    failover._zk.get_failover_version.return_value = 'operation-1'
    failover._zk.get_election_host_vote_with_timeline.side_effect = (
        lambda host, _: {
            'host-a': (100, 1, 2),
            'host-b': (200, 1, 1),
        }[host]
    )
    durability = DurabilityConfig.build(['old-primary', 'host-a', 'host-b'])
    failover._zk.get_durability_state.return_value = (
        DurabilityState(durability), 3,
    )
    failover._zk.get_timeline.return_value = 2

    with patch('src.utils.uuid.uuid4') as generated:
        generated.return_value.hex = 'operation-1'
        assert failover.initiate(
            with_data_loss=True, timeout=0, yes=True,
        ) is True

    selected = failover._zk.write_failover_request.call_args_list[-1].args[0]
    assert selected.winner == 'host-a'


def test_unfenced_data_loss_failover_warns_and_is_never_reported_safe(capsys):
    failover = _failover()
    stored: list[FailoverRequest | None] = [None]
    version = [None]

    def get_request():
        return stored[0], version[0]

    def write_request(request, expected_version):
        assert expected_version == version[0]
        if request.with_data_loss and not request.electorate:
            request = replace(request, electorate=('host-a',))
        stored[0] = request
        version[0] = 0 if version[0] is None else version[0] + 1
        return version[0]

    failover._zk.get_failover_request.side_effect = get_request
    failover._zk.write_failover_request.side_effect = write_request
    failover._zk.get_failover_version.side_effect = (
        lambda: stored[0].operation_id if stored[0] is not None else None
    )
    failover._zk.get_election_host_vote_with_timeline.return_value = (100, 1, 1)
    durability = DurabilityConfig.build(['old-primary', 'host-a'])
    failover._zk.get_durability_state.return_value = (
        DurabilityState(durability), 3,
    )
    failover._zk.get_timeline.return_value = 1

    assert failover.initiate(
        with_data_loss=True,
        fence_wal_sources=False,
        timeout=0,
        yes=True,
    ) is True

    assert stored[0] is not None
    assert stored[0].fence_wal_sources is False
    output = capsys.readouterr().out
    assert 'WARNING: restore_command and walreceiver were not disabled' in output
    assert 'wal-fenced' in output
    assert 'host-a: UNSAFE' in output
    assert 'vote positions may change' in output


def test_data_loss_command_resumes_existing_request_without_replacing_it():
    failover = _failover()
    request = FailoverRequest('old-primary', 'operation-1', True, electorate=('host-a',))
    failover._zk.get_failover_request.side_effect = [
        (request, 2),
        (request, 2),
        (request, 2),
    ]
    failover._zk.get_failover_version.return_value = request.operation_id
    failover._zk.get_election_host_vote_with_timeline.return_value = (100, 1, 1)
    durability = DurabilityConfig.build(['old-primary', 'host-a'])
    failover._zk.get_durability_state.return_value = (
        DurabilityState(durability), 3,
    )
    failover._zk.get_timeline.return_value = 1
    failover._zk.write_failover_request.return_value = 3

    assert failover.initiate(
        with_data_loss=True,
        timeout=0,
        yes=True,
    ) is True

    failover._zk.get_failover_state.assert_not_called()
    assert failover._zk.write_failover_request.call_count == 1
    selected = failover._zk.write_failover_request.call_args.args[0]
    assert selected.operation_id == request.operation_id
    assert selected.winner == 'host-a'
