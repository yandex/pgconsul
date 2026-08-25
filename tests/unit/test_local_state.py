import json
import os
from unittest.mock import patch

import pytest

from src.local_state import LocalStateError, LocalStateInvalid, LocalStateStore


def test_write_read_and_clear(tmp_path):
    store = LocalStateStore('state.json', {'first', 'second'}, directory=str(tmp_path))

    store.write('first')

    assert store.read() == 'first'
    store.clear()
    assert store.read() is None


def test_write_fsyncs_file_and_directory(tmp_path):
    store = LocalStateStore('state.json', {'first'}, directory=str(tmp_path))

    with patch('src.local_state.os.fsync') as fsync:
        store.write('first')

    assert fsync.call_count == 2


def test_write_replaces_state_atomically(tmp_path):
    store = LocalStateStore('state.json', {'first', 'second'}, directory=str(tmp_path))
    store.write('first')

    with patch('src.local_state.os.replace', wraps=os.replace) as replace:
        store.write('second')

    source, destination = replace.call_args.args
    assert source.parent == store.path.parent
    assert destination == store.path
    assert store.read() == 'second'


def test_failed_replace_preserves_previous_state(tmp_path):
    store = LocalStateStore('state.json', {'first', 'second'}, directory=str(tmp_path))
    store.write('first')

    with patch('src.local_state.os.replace', side_effect=OSError('replace failed')):
        with pytest.raises(LocalStateError):
            store.write('second')

    assert store.read() == 'first'
    assert list(tmp_path.iterdir()) == [store.path]


def test_clear_fsyncs_directory(tmp_path):
    store = LocalStateStore('state.json', {'first'}, directory=str(tmp_path))
    store.write('first')

    with patch('src.local_state.os.fsync') as fsync:
        store.clear()

    fsync.assert_called_once()


def test_clear_retries_directory_fsync_after_file_was_unlinked(tmp_path):
    store = LocalStateStore('state.json', {'first'}, directory=str(tmp_path))
    store.write('first')

    with patch('src.local_state.os.fsync', side_effect=OSError('fsync failed')):
        with pytest.raises(LocalStateError):
            store.clear()

    assert not store.path.exists()
    with patch('src.local_state.os.fsync') as fsync:
        store.clear()
    fsync.assert_called_once()


@pytest.mark.parametrize('contents', ['{broken', json.dumps({'phase': 'unknown'})])
def test_invalid_state_is_cleared_and_raised(tmp_path, contents):
    store = LocalStateStore('state.json', {'first'}, directory=str(tmp_path))
    store.path.write_text(contents, encoding='utf-8')

    with pytest.raises(LocalStateInvalid):
        store.read()

    assert not store.path.exists()
