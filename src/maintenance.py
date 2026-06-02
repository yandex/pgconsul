"""
Pure functions related to maintenance mode logic.
"""
# encoding: utf-8


def should_stop_pooler_in_maintenance(db_state: dict, zk_timeline: int | None) -> bool:
    role = db_state.get('role')
    db_alive = db_state.get('alive', False)
    db_timeline = db_state.get('timeline')

    return (
        role == 'primary'
        and db_alive
        and zk_timeline is not None
        and (db_timeline is None or zk_timeline > db_timeline)
    )
