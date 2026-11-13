import logging
from collections.abc import Callable

from .zk import Zookeeper


def try_acquire_primary_lock(
    zk: Zookeeper,
    get_local_timeline: Callable[[], int],
    hostname: str,
    allow_queue: bool = False,
    timeout: float | None = None,
) -> bool:
    def fence_allows_acquire() -> bool:
        current_promoting_host = zk.get_current_promoting_host()
        if current_promoting_host is not None and current_promoting_host != hostname:
            logging.warning('Primary lock is fenced by promoting host %s', current_promoting_host)
            return False

        local_timeline = get_local_timeline()
        zk_timeline = zk.get_timeline()
        if zk_timeline is not None and local_timeline < zk_timeline:
            logging.warning(
                'Primary lock is fenced by newer ZK timeline: local=%s, zk=%s',
                local_timeline,
                zk_timeline,
            )
            return False
        return True

    if not fence_allows_acquire():
        zk.release_if_hold(zk.PRIMARY_LOCK_PATH)
        return False
    if not zk.try_acquire_lock(
        zk.PRIMARY_LOCK_PATH,
        allow_queue=allow_queue,
        timeout=timeout,
    ):
        return False
    if fence_allows_acquire():
        return True

    logging.warning('Primary lock fence changed while acquiring the lock, releasing it')
    zk.release_lock(zk.PRIMARY_LOCK_PATH)
    return False
