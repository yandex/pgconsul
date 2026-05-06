"""
Pure functions related to maintenance mode logic.
"""
# encoding: utf-8


def should_stop_pooler_in_maintenance(db_state: dict, zk_timeline: int | None) -> bool:
    """
    Determine whether pooler (odyssey/pgbouncer) and WAL archiving should be
    stopped during maintenance mode.

    This happens when a failover has occurred and the current node's timeline
    is behind the timeline recorded in ZooKeeper — meaning another node has
    already been promoted and this node is a stale primary.

    Args:
        db_state: Current database state dict from Postgres.get_state().
                  Must contain 'role', 'alive', and 'timeline' keys.
        zk_timeline: Timeline value stored in ZooKeeper (int or None).

    Returns:
        True if pooler should be stopped and archiving disabled, False otherwise.

    Important (MDB-43333):
        We MUST check db_state['alive'] before acting on a None db_timeline.
        If the DB is temporarily unavailable, db_timeline will be None due to
        a connection failure — NOT because of a failover. Stopping the pooler
        in that case would cause unnecessary client-visible downtime while the
        cluster is in maintenance mode.
    """
    role = db_state.get('role')
    db_alive = db_state.get('alive', False)
    db_timeline = db_state.get('timeline')

    return (
        role == 'primary'
        and db_alive
        and zk_timeline is not None
        and (db_timeline is None or zk_timeline > db_timeline)
    )
