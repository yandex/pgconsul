from unittest.mock import Mock

import pytest

from src.pg import Postgres


@pytest.mark.parametrize(('timeline', 'lsn', 'expected'), [
    (1, '1/FFFFFF00', False),  # Exactly at the first fork.
    (1, '2/1', True),          # Past the first fork, before the second.
    (2, '2/FF', False),
    (2, '2/100', False),
    (2, '2/101', True),
    (3, '2/101', False),
])
def test_replayed_past_fork(tmp_path, timeline, lsn, expected):
    db = Postgres.__new__(Postgres)
    db.pgdata = str(tmp_path)
    db._exec_query = Mock()
    db._exec_query.return_value.fetchone.return_value = (timeline, lsn)
    wal = tmp_path / 'pg_wal'
    wal.mkdir()
    (wal / '00000003.history').write_text('1\t1/FFFFFF00\tfirst promotion\n2\t2/100\tsecond promotion\n')

    assert db.replayed_past_fork(3) is expected


def test_missing_timeline_history(tmp_path):
    db = Postgres.__new__(Postgres)
    db.pgdata = str(tmp_path)

    assert not db.replayed_past_fork(3)
