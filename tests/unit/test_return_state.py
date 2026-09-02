import json

from src.return_to_cluster.state import ReturnPhase, ReturnState, ReturnStateStore


def test_return_state_round_trip(tmp_path):
    store = ReturnStateStore(str(tmp_path))
    state = ReturnState(
        operation_id='failover-1',
        phase=ReturnPhase.STARTING,
        target_host='primary-2',
        target_timeline=4,
        role='replica',
        start_attempts=2,
        progress_signature='wal:0004',
        progress_since=12.5,
    )

    store.write(state)

    assert store.read() == state


def test_return_state_clear_is_scoped_by_operation_id(tmp_path):
    store = ReturnStateStore(str(tmp_path))
    store.write(ReturnState('new-operation', ReturnPhase.BLOCKED))

    store.clear('old-operation')

    assert store.read() == ReturnState('new-operation', ReturnPhase.BLOCKED)


def test_invalid_return_state_is_cleared_and_ignored(tmp_path):
    store = ReturnStateStore(str(tmp_path))
    store.path.parent.mkdir(parents=True, exist_ok=True)
    store.path.write_text(json.dumps({
        'operation_id': 'op',
        'phase': 'unknown',
    }))

    assert store.read() is None
    assert not store.path.exists()
