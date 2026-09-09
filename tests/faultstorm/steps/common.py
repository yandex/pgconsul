"""Shared helpers for faultstorm step definitions."""

import time

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


def get_cluster_roles(db_nodes):
    """Return PostgreSQL recovery roles for every node, or ``None``."""
    roles = {}
    for node in db_nodes:
        try:
            out = ClusterManager.exec_on_node(
                node,
                ["sudo", "-u", "postgres", "psql", "-tAc",
                 "SELECT pg_is_in_recovery()"],
                timeout=5,
            ).strip()
        except Exception:
            return None
        if out not in ("t", "f"):
            return None
        roles[node] = out
    return roles


def wait_for_healthy_cluster(db_nodes, timeout=180):
    """Wait until every node accepts SQL and exactly one is primary."""
    deadline = time.time() + timeout
    last_roles = None
    while time.time() < deadline:
        roles = get_cluster_roles(db_nodes)
        if roles is not None:
            last_roles = roles
            if list(roles.values()).count("f") == 1 and list(roles.values()).count("t") == len(db_nodes) - 1:
                return roles
        time.sleep(2)
    raise AssertionError(
        "Cluster did not recover within {}s; last PostgreSQL roles: {}".format(
            timeout, last_roles,
        )
    )
