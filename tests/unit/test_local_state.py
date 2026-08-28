import json
import os
from unittest.mock import patch

import pytest

from src.local_state import LocalStateError, LocalStateInvalid, LocalStateStore


def test_write_read_and_clear(tmp_path):
    store = LocalStateStore('state.json', {'first', 'second'}, directory=str(tmp_path))

    store.write('op-1', 'first')

    assert store.read('op-1') == 'first'
    store.clear('op-1')
    assert store.read('op-1') is None


def test_state_from_another_operation_is_not_resumed(tmp_path):
    store = LocalStateStore('state.json', {'first'}, directory=str(tmp_path))
    store.write('old-operation', 'first')

    assert store.read('new-operation') is None


def test_stale_cleanup_does_not_clear_new_operation(tmp_path):
    store = LocalStateStore('state.json', {'first'}, directory=str(tmp_path))
    store.write('new-operation', 'first')

    store.clear('old-operation')

    assert store.read('new-operation') == 'first'


def test_write_fsyncs_file_and_directory(tmp_path):
    store = LocalStateStore('state.json', {'first'}, directory=str(tmp_path))

    with patch('src.local_state.os.fsync') as fsync:
        store.write('op-1', 'first')

    assert fsync.call_count == 2


def test_write_replaces_state_atomically(tmp_path):
    store = LocalStateStore('state.json', {'first', 'second'}, directory=str(tmp_path))
    store.write('op-1', 'first')

    with patch('src.local_state.os.replace', wraps=os.replace) as replace:
        store.write('op-1', 'second')

    source, destination = replace.call_args.args
    assert source.parent == store.path.parent
    assert destination == store.path
    assert store.read('op-1') == 'second'


def test_failed_replace_preserves_previous_state(tmp_path):
    store = LocalStateStore('state.json', {'first', 'second'}, directory=str(tmp_path))
    store.write('op-1', 'first')

    with patch('src.local_state.os.replace', side_effect=OSError('replace failed')):
        with pytest.raises(LocalStateError):
            store.write('op-1', 'second')

    assert store.read('op-1') == 'first'
    assert list(tmp_path.iterdir()) == [store.path]


def test_clear_fsyncs_directory(tmp_path):
    store = LocalStateStore('state.json', {'first'}, directory=str(tmp_path))
    store.write('op-1', 'first')

    with patch('src.local_state.os.fsync') as fsync:
        store.clear('op-1')

    fsync.assert_called_once()


def test_clear_retries_directory_fsync_after_file_was_unlinked(tmp_path):
    store = LocalStateStore('state.json', {'first'}, directory=str(tmp_path))
    store.write('op-1', 'first')

    with patch('src.local_state.os.fsync', side_effect=OSError('fsync failed')):
        with pytest.raises(LocalStateError):
            store.clear('op-1')

    assert not store.path.exists()
    with patch('src.local_state.os.fsync') as fsync:
        store.clear('op-1')
    fsync.assert_called_once()


@pytest.mark.parametrize('contents', ['{broken', json.dumps({'phase': 'unknown'})])
def test_invalid_state_is_cleared_and_raised(tmp_path, contents):
    store = LocalStateStore('state.json', {'first'}, directory=str(tmp_path))
    store.path.write_text(contents, encoding='utf-8')

    with pytest.raises(LocalStateInvalid):
        store.read('op-1')

    assert not store.path.exists()
