from unittest.mock import MagicMock

from src.primary_lock import try_acquire_primary_lock


def make_dependencies():
    zk = MagicMock()
    zk.PRIMARY_LOCK_PATH = 'leader'
    zk.get_current_promoting_host.return_value = None
    zk.get_timeline.return_value = 1
    get_local_timeline = MagicMock(return_value=1)
    return zk, get_local_timeline


def test_primary_lock_is_not_acquired_for_another_promoting_host():
    zk, get_local_timeline = make_dependencies()
    zk.get_current_promoting_host.return_value = 'new-primary'

    assert try_acquire_primary_lock(zk, get_local_timeline, 'old-primary') is False

    zk.try_acquire_lock.assert_not_called()
    zk.release_if_hold.assert_called_once_with('leader')


def test_primary_lock_is_not_acquired_on_an_older_local_timeline():
    zk, get_local_timeline = make_dependencies()
    get_local_timeline.return_value = 4
    zk.get_timeline.return_value = 5

    assert try_acquire_primary_lock(zk, get_local_timeline, 'primary') is False

    zk.try_acquire_lock.assert_not_called()
    zk.release_if_hold.assert_called_once_with('leader')


def test_primary_lock_is_released_when_promoting_host_changes_during_acquire():
    zk, get_local_timeline = make_dependencies()
    zk.try_acquire_lock.return_value = True
    zk.get_current_promoting_host.side_effect = [None, 'new-primary']

    assert try_acquire_primary_lock(zk, get_local_timeline, 'old-primary', timeout=10) is False

    zk.release_lock.assert_called_once_with('leader')


def test_primary_lock_is_released_when_zk_timeline_advances_during_acquire():
    zk, get_local_timeline = make_dependencies()
    zk.try_acquire_lock.return_value = True
    zk.get_timeline.side_effect = [4, 5]
    get_local_timeline.return_value = 4

    assert try_acquire_primary_lock(zk, get_local_timeline, 'primary') is False

    zk.release_lock.assert_called_once_with('leader')


def test_primary_lock_checks_fence_before_and_after_acquire():
    zk, get_local_timeline = make_dependencies()
    zk.try_acquire_lock.return_value = True

    assert try_acquire_primary_lock(
        zk,
        get_local_timeline,
        'primary',
        allow_queue=True,
        timeout=10,
    ) is True

    assert zk.get_current_promoting_host.call_count == 2
    assert get_local_timeline.call_count == 2
    assert zk.get_timeline.call_count == 2
    zk.try_acquire_lock.assert_called_once_with('leader', allow_queue=True, timeout=10)
