"""Step definitions for ResetupAction tests."""

import re
import time

from behave import given, when, then

from faultstorm.cluster import ClusterManager
from faultstorm.config import TestConfig
from faultstorm.network_latency import NetworkLatencyManager

from faultstorm_resetup import ResetupAction

MARKER_FILENAME = ".faultstorm_test_marker"


def _marker_path(pg_major):
    """Return the full marker file path for the given PG major version."""
    return f"/var/lib/postgresql/{pg_major}/main/{MARKER_FILENAME}"


# ---- Flag / serde steps ----

@given('a resetup action')
def step_given_resetup(context):
    context.action = ResetupAction(
        context.db_nodes, context.extra_nodes, ordinal=1,
        dc_map=context.dc_map,
    )


@given('a resetup action with ordinal {ordinal:d} and node "{node}"')
def step_given_resetup_with_node(context, ordinal, node):
    context.action = ResetupAction(
        context.db_nodes, context.extra_nodes, ordinal=ordinal,
        dc_map=context.dc_map,
        node=node,
    )


@given('a resetup action with ordinal {ordinal:d} and no node')
def step_given_resetup_no_node(context, ordinal):
    context.action = ResetupAction(
        context.db_nodes, context.extra_nodes, ordinal=ordinal,
        dc_map=context.dc_map,
    )


@when('I serialize and deserialize the resetup action')
def step_serde_resetup(context):
    serialized = context.action.serialize()
    context.deserialized = ResetupAction.deserialize(
        serialized, context.db_nodes, context.extra_nodes,
        dc_map=context.dc_map,
    )


@then('the deserialized resetup action has ordinal {ordinal:d} and node "{node}"')
def step_check_serde_resetup(context, ordinal, node):
    d = context.deserialized
    assert d.ordinal == ordinal, f"Expected ordinal {ordinal}, got {d.ordinal}"
    assert d.node == node, f"Expected node {node}, got {d.node}"


@then('the deserialized resetup action has ordinal {ordinal:d} and no node')
def step_check_serde_resetup_no_node(context, ordinal):
    d = context.deserialized
    assert d.ordinal == ordinal, f"Expected ordinal {ordinal}, got {d.ordinal}"
    assert d.node is None, f"Expected node None, got {d.node}"


# ---- Docker integration steps ----

@given('the node "{node}" is a replica')
def step_node_is_replica(context, node):
    out = ClusterManager.exec_on_node(
        node,
        ["sudo", "-u", "postgres", "psql", "-tAc",
         "SELECT pg_is_in_recovery()"],
        timeout=5,
    )
    assert out.strip() == "t", f"Node {node} is not a replica (pg_is_in_recovery={out.strip()})"


@given('pg_resetup service is stopped on "{node}"')
def step_stop_pg_resetup(context, node):
    ClusterManager.exec_on_node(
        node, ["supervisorctl", "stop", "pg_resetup"], timeout=10,
    )


@given('a marker file is placed in PGDATA on "{node}"')
def step_place_marker(context, node):
    marker = _marker_path(context.pg_major)
    ClusterManager.exec_on_node(
        node, ["touch", marker], timeout=5,
    )
    # Verify marker exists
    out = ClusterManager.exec_on_node(
        node, ["ls", marker], timeout=5,
    )
    assert MARKER_FILENAME in out, f"Marker file not created on {node}"


@when('I execute a resetup action on "{node}"')
def step_execute_resetup(context, node):
    context.action = ResetupAction(
        context.db_nodes, context.extra_nodes, ordinal=1,
        dc_map=context.dc_map,
        node=node,
    )
    context.action.execute()


@when('pg_resetup service is started on "{node}"')
def step_start_pg_resetup(context, node):
    ClusterManager.exec_on_node(
        node, ["supervisorctl", "start", "pg_resetup"], timeout=10,
    )


@when('I wait up to {seconds:d} seconds for the marker file to disappear on "{node}"')
def step_wait_marker_gone(context, node, seconds):
    marker = _marker_path(context.pg_major)
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            ClusterManager.exec_on_node(
                node, ["test", "-f", marker], timeout=5,
            )
        except Exception:
            return  # File gone or node restarting
        time.sleep(5)


@then('the marker file is gone on "{node}"')
def step_marker_gone(context, node):
    marker = _marker_path(context.pg_major)
    try:
        ClusterManager.exec_on_node(
            node, ["test", "-f", marker], timeout=5,
        )
    except Exception:
        return  # If test -f fails, marker is gone (or PGDATA was wiped)
    assert False, f"Marker file still exists on {node}: {marker}"


@then('postgres is running on "{node}"')
def step_postgres_running(context, node):
    # Wait a bit for postgres to come up after resetup
    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            out = ClusterManager.exec_on_node(
                node,
                ["sudo", "-u", "postgres", "psql", "-tAc", "SELECT 1"],
                timeout=5,
            )
            if out.strip() == "1":
                return
        except Exception:
            pass
        time.sleep(3)
    assert False, f"Postgres is not running on {node} after waiting 30 seconds"


# ---- Network latency + resetup steps ----


def _measure_ping_ms(source_node: str, target_node: str, count: int = 3) -> float:
    """Measure average ping RTT from source_node to target_node.

    Args:
        source_node: Node to ping from.
        target_node: Node to ping to.
        count: Number of ping packets.

    Returns:
        Average RTT in milliseconds.
    """
    output = ClusterManager.exec_on_node(
        source_node,
        ["ping", "-c", str(count), "-W", "5", target_node],
        timeout=count * 5 + 10,
    )
    match = re.search(r"rtt min/avg/max/mdev = [\d.]+/([\d.]+)/", output)
    if not match:
        match = re.search(r"round-trip min/avg/max(?:/\w+)? = [\d.]+/([\d.]+)/", output)
    assert match, f"Cannot parse ping output from {source_node} -> {target_node}:\n{output}"
    return float(match.group(1))


@given('a cross-DC latency of {delay:d}ms between "{dc_a}" and "{dc_b}" is applied')
def step_apply_cross_dc_latency(context, delay, dc_a, dc_b):
    """Apply cross-DC latency using NetworkLatencyManager."""
    config = TestConfig(
        db_nodes=list(context.db_nodes),
        extra_nodes=list(context.extra_nodes),
        cross_dc_delays={(dc_a, dc_b): delay},
    )
    context.latency_manager = NetworkLatencyManager(config)
    context.latency_manager.apply(context.dc_map)


@when('I wait up to {seconds:d} seconds for postgres to be running on "{node}"')
def step_wait_postgres_running(context, seconds, node):
    """Wait until postgres is accepting queries on the given node."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            out = ClusterManager.exec_on_node(
                node,
                ["sudo", "-u", "postgres", "psql", "-tAc", "SELECT 1"],
                timeout=5,
            )
            if out.strip() == "1":
                return
        except Exception:
            pass
        time.sleep(3)
    assert False, f"Postgres is not running on {node} after waiting {seconds} seconds"


@then('ping from "{source}" to "{target}" takes at least {threshold:d}ms')
def step_ping_at_least(context, source, target, threshold):
    avg_ms = _measure_ping_ms(source, target)
    assert avg_ms >= threshold * 0.8, (
        f"Ping {source} -> {target}: avg {avg_ms:.1f}ms is below "
        f"expected threshold {threshold}ms (with 20% tolerance: {threshold * 0.8:.1f}ms)"
    )
