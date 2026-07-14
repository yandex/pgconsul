"""Shared helpers for faultstorm step definitions."""

from faultstorm.cluster import ClusterManager


def find_primary(db_nodes):
    """Find the current PG primary among db_nodes using pg_is_in_recovery().

    Returns the node name of the primary, or None if no primary found.
    """
    for node in db_nodes:
        try:
            out = ClusterManager.exec_on_node(
                node,
                ["sudo", "-u", "postgres", "psql", "-tAc",
                 "SELECT NOT pg_is_in_recovery()"],
                timeout=5,
            )
            if out.strip() == "t":
                return node
        except Exception:
            continue
    return None
