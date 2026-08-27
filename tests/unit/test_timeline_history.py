from unittest.mock import MagicMock, patch

import pytest

from src.command_manager import CommandManager, Commands
from src.pg import Postgres
from src.return_to_cluster import (
    ReturnObservation,
    TimelineSwitch,
    parse_timeline_history,
    timeline_requires_rewind,
    wal_filename_before_switch,
)


def test_parses_complete_timeline_ancestry():
    history = parse_timeline_history(
        '1\t0/4732390\tno recovery target specified\n'
        '2\t0/6000000\tno recovery target specified\n',
        target_timeline=3,
    )
    assert history == (
        TimelineSwitch(1, 0x4732390),
        TimelineSwitch(2, 0x6000000),
    )


def test_local_lsn_after_ancestor_switch_requires_rewind():
    history = (TimelineSwitch(1, 0x4732390),)
    assert timeline_requires_rewind(1, 0x5000000, 2, history) is True


def test_local_lsn_at_ancestor_switch_can_follow_target():
    history = (TimelineSwitch(1, 0x4732390),)
    assert timeline_requires_rewind(1, 0x4732390, 2, history) is False


def test_timeline_outside_target_ancestry_requires_rewind():
    history = (TimelineSwitch(1, 0x4732390), TimelineSwitch(3, 0x6000000))
    assert timeline_requires_rewind(2, 0x4000000, 4, history) is True


def test_rejects_empty_non_root_history():
    with pytest.raises(ValueError, match='Empty history'):
        parse_timeline_history('', target_timeline=2)


@pytest.mark.parametrize(
    ('switch_lsn', 'filename'),
    [
        (0x4732390, '000000010000000000000004.partial'),
        (0x5000000, '000000010000000000000004.partial'),
    ],
)
def test_wal_filename_before_switch_uses_old_timeline(switch_lsn, filename):
    assert wal_filename_before_switch(
        TimelineSwitch(1, switch_lsn), 16 * 1024 * 1024,
    ) == filename


def test_fork_wal_barrier_waits_for_archived_partial_segment():
    """ssn_before_promote.feature:75: promotion archives the fork segment as .partial."""
    assert wal_filename_before_switch(
        TimelineSwitch(1, 0x301EB10), 16 * 1024 * 1024,
    ) == '000000010000000000000003.partial'


def test_command_manager_substitutes_history_filename_and_destination():
    commands = MagicMock(spec=Commands)
    commands.fetch_timeline_history = 'archive-fetch %f %p'
    manager = CommandManager(commands)

    with patch('src.command_manager.helpers.subprocess_call', return_value=0) as call:
        assert manager.fetch_timeline_history(
            '00000002.history', '/tmp/history',
        ) == 0

    call.assert_called_once_with(
        'archive-fetch 00000002.history /tmp/history',
        save_output=False,
    )


def test_command_manager_starts_pooler_stop_without_waiting():
    commands = MagicMock(spec=Commands)
    commands.pooler_stop = 'supervisorctl stop pgbouncer'
    manager = CommandManager(commands)

    with patch('src.command_manager.helpers.subprocess_start', return_value=True) as start:
        assert manager.stop_pooler_async() is True

    start.assert_called_once_with('supervisorctl stop pgbouncer')


def test_postgres_returns_fetched_history_and_removes_temporary_file(tmp_path):
    postgres = Postgres.__new__(Postgres)
    postgres.config = MagicMock(working_dir=str(tmp_path))
    postgres._cmd_manager = MagicMock()

    def fetch(_filename, filepath):
        with open(filepath, 'w') as output:
            output.write('1\t0/4732390\tbranch\n')
        return 0

    postgres._cmd_manager.fetch_timeline_history.side_effect = fetch

    assert postgres.fetch_timeline_history(2) == '1\t0/4732390\tbranch\n'
    assert list(tmp_path.iterdir()) == []


def test_postgres_installs_validated_history_in_pg_wal(tmp_path):
    postgres = Postgres.__new__(Postgres)
    postgres.pgdata = str(tmp_path)
    (tmp_path / 'pg_wal').mkdir()

    value = '1\t0/4732390\tbranch\n'
    assert postgres.install_timeline_history(2, value) is True
    assert (tmp_path / 'pg_wal' / '00000002.history').read_text() == value
    assert not (tmp_path / 'pg_wal' / '00000002.history.pgconsul-new').exists()


def test_postgres_checks_wal_availability_without_keeping_download(tmp_path):
    postgres = Postgres.__new__(Postgres)
    postgres.config = MagicMock(working_dir=str(tmp_path))
    postgres._cmd_manager = MagicMock()

    def fetch(_filename, filepath):
        with open(filepath, 'wb') as output:
            output.write(b'wal')
        return 0

    postgres._cmd_manager.fetch_timeline_history.side_effect = fetch

    assert postgres.is_wal_archived('000000010000000000000004') is True
    assert list(tmp_path.iterdir()) == []


def test_return_observation_waits_for_fork_wal_after_fast_turn_failed():
    db = MagicMock()
    db.get_restore_command.return_value = '/bin/false'
    db.get_wal_flush_lsn.return_value = 0x5000000
    db.fetch_timeline_history.return_value = '1\t0/4732390\tbranch\n'
    db.get_wal_segment_size.return_value = 16 * 1024 * 1024
    db.is_wal_archived.return_value = False
    zk = MagicMock()
    zk.get_timeline.return_value = 2
    zk.noexcept_get.return_value = None
    zk.MEMBERS_PATH = '/members'

    observation = ReturnObservation.build(
        zk, db, 'replica', {'role': 'replica', 'timeline': 1},
        'new-primary', False, 60.0, simple_switch_tried=True,
    )

    assert observation.required_wal_filename == '000000010000000000000004.partial'
    assert observation.required_wal_archived is False
    db.is_wal_archived.assert_called_once_with('000000010000000000000004.partial')


def test_return_observation_reads_history_before_first_remaster():
    db = MagicMock()
    db.get_restore_command.return_value = '/bin/false'
    zk = MagicMock()
    zk.get_timeline.return_value = 2
    zk.noexcept_get.return_value = None
    zk.MEMBERS_PATH = '/members'

    observation = ReturnObservation.build(
        zk, db, 'replica', {'role': 'replica', 'timeline': 1},
        'new-primary', False, 60.0, simple_switch_tried=False,
    )

    db.fetch_timeline_history.assert_called_once_with(2)
    db.is_wal_archived.assert_not_called()


def test_return_observation_before_fork_does_not_probe_archive():
    db = MagicMock()
    db.get_restore_command.return_value = '/bin/false'
    db.get_wal_flush_lsn.return_value = 0x45AD3F8
    db.fetch_timeline_history.return_value = '1\t0/4732390\tbranch\n'
    db.get_wal_segment_size.return_value = 16 * 1024 * 1024
    zk = MagicMock()
    zk.get_timeline.return_value = 2
    zk.noexcept_get.return_value = None
    zk.MEMBERS_PATH = '/members'

    observation = ReturnObservation.build(
        zk, db, 'replica', {'role': 'replica', 'timeline': 1},
        'new-primary', False, 60.0, simple_switch_tried=False,
    )

    assert observation.timeline_history is not None
    assert observation.required_wal_archived is None
    db.is_wal_archived.assert_not_called()
