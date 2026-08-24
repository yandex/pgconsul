import json
from unittest.mock import patch

import pytest

from src.local_state import LocalStateInvalid, LocalStateStore


def test_write_read_and_clear(tmp_path):
    store = LocalStateStore('state.json', {'first', 'second'}, directory=str(tmp_path))

    store.write('first')

    assert store.read() == 'first'
    store.clear()
    assert store.read() is None


def test_write_flushes_to_disk(tmp_path):
    store = LocalStateStore('state.json', {'first'}, directory=str(tmp_path))

    with patch('src.local_state.os.fsync') as fsync:
        store.write('first')

    fsync.assert_called_once()


@pytest.mark.parametrize('contents', ['{broken', json.dumps({'phase': 'unknown'})])
def test_invalid_state_is_cleared_and_raised(tmp_path, contents):
    store = LocalStateStore('state.json', {'first'}, directory=str(tmp_path))
    store.path.write_text(contents, encoding='utf-8')

    with pytest.raises(LocalStateInvalid):
        store.read()

    assert not store.path.exists()
