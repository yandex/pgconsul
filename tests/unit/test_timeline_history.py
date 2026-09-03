from unittest.mock import MagicMock, patch

import pytest

from src.command_manager import CommandManager, Commands, REWIND_LOG_PATH
from src.pg import Postgres
from src.return_to_cluster import (
    ReturnAction,
    ReturnObservation,
    TimelineSwitch,
    decide_return_action,
    parse_timeline_history,
    timeline_requires_rewind,
    wal_filename_before_switch,
    wal_filenames_before_switch,
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


def test_fork_wal_barrier_accepts_complete_or_partial_segment():
    assert wal_filenames_before_switch(
        TimelineSwitch(1, 0x301EB10), 16 * 1024 * 1024,
    ) == (
        '000000010000000000000003',
        '000000010000000000000003.partial',
    )


def test_command_manager_substitutes_history_filename_and_destination():
    commands = MagicMock(spec=Commands)
    commands.fetch_timeline_history = 'archive-fetch %f %p'
    commands.external_command_timeout = 60
    manager = CommandManager(commands)

    with patch('src.command_manager.helpers.subprocess_call', return_value=0) as call:
        assert manager.fetch_timeline_history(
            '00000002.history', '/tmp/history',
        ) == 0

        call.assert_called_once_with(
            'archive-fetch 00000002.history /tmp/history',
            save_output=False, timeout=60,
        )


def test_command_manager_starts_pooler_stop_without_waiting():
    commands = MagicMock(spec=Commands)
    commands.pooler_stop = 'supervisorctl stop pgbouncer'
    manager = CommandManager(commands)

    with patch('src.command_manager.helpers.subprocess_start', return_value=True) as start:
        assert manager.stop_pooler_async() is True

    start.assert_called_once_with('supervisorctl stop pgbouncer')


def test_command_manager_starts_postgresql_without_waiting():
    commands = MagicMock(spec=Commands)
    commands.pg_start = 'pg_ctl start -D %p -t %t'
    manager = CommandManager(commands)

    with patch('src.command_manager.helpers.subprocess_start', return_value=True) as start:
        assert manager.start_postgresql_async(300, '/pgdata') is True

    start.assert_called_once_with('pg_ctl start -D /pgdata -t 300')


def test_command_manager_redirects_rewind_output_to_log():
    commands = MagicMock(spec=Commands)
    commands.rewind = 'pg_rewind -D %p --source-server=%m'
    manager = CommandManager(commands)

    with patch('src.command_manager.helpers.subprocess_call', return_value=0) as call:
        assert manager.rewind('/pgdata', 'primary.example.com') == 0

    call.assert_called_once_with(
        'pg_rewind -D /pgdata --source-server=primary.example.com',
        output_file=REWIND_LOG_PATH,
    )


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


def test_postgres_predicts_next_timeline_from_local_history(tmp_path):
    postgres = Postgres.__new__(Postgres)
    postgres.pgdata = str(tmp_path)
    wal_dir = tmp_path / 'pg_wal'
    wal_dir.mkdir()
    (wal_dir / '0000000A.history').write_text('history')
    (wal_dir / '0000000B.history').write_text('history')

    assert postgres.next_local_timeline(9) == 12


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


def test_return_observation_accepts_complete_fork_wal_after_fast_turn_failed():
    db = MagicMock()
    db.get_restore_command.return_value = '/bin/false'
    db.get_wal_flush_lsn.return_value = 0x5000000
    db.fetch_timeline_history.return_value = '1\t0/4732390\tbranch\n'
    db.get_wal_segment_size.return_value = 16 * 1024 * 1024
    db.is_wal_archived.side_effect = lambda filename: not filename.endswith('.partial')
    zk = MagicMock()
    zk.get_timeline.return_value = 2
    zk.noexcept_get.return_value = None
    zk.MEMBERS_PATH = '/members'

    observation = ReturnObservation.build(
        zk, db, 'replica', {'role': 'replica', 'timeline': 1},
        'new-primary', False, 60.0,
    )

    assert observation.required_wal_filename == '000000010000000000000004'
    assert observation.required_wal_archived is True
    db.is_wal_archived.assert_called_once_with('000000010000000000000004')


def test_return_observation_former_primary_skips_lsn_read_before_rewind():
    """A former primary has no replay LSN; archive readiness is its barrier."""
    db = MagicMock()
    db.get_restore_command.return_value = '/bin/false'
    db.get_wal_flush_lsn.side_effect = AssertionError('must not read replay LSN on primary')
    db.fetch_timeline_history.return_value = '1\t0/4732390\tbranch\n'
    db.get_wal_segment_size.return_value = 16 * 1024 * 1024
    db.is_wal_archived.return_value = True
    zk = MagicMock()
    zk.get_timeline.return_value = 2
    zk.noexcept_get.return_value = None
    zk.MEMBERS_PATH = '/members'

    observation = ReturnObservation.build(
        zk, db, 'old-primary', {'role': 'primary', 'timeline': 1},
        'new-primary', False, 60.0,
    )

    db.get_wal_flush_lsn.assert_not_called()
    assert decide_return_action(observation) == ReturnAction.REWIND


def test_return_observation_reads_history_before_first_remaster():
    db = MagicMock()
    db.get_restore_command.return_value = '/bin/false'
    zk = MagicMock()
    zk.get_timeline.return_value = 2
    zk.noexcept_get.return_value = None
    zk.MEMBERS_PATH = '/members'

    observation = ReturnObservation.build(
        zk, db, 'replica', {'role': 'replica', 'timeline': 1},
        'new-primary', False, 60.0,
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
        'new-primary', False, 60.0,
    )

    assert observation.timeline_history is not None
    assert observation.required_wal_archived is None
    db.is_wal_archived.assert_not_called()
