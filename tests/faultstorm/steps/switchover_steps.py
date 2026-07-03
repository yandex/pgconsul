"""Step definitions for SwitchoverAction tests."""

import time

from behave import given, when, then

from faultstorm.cluster import ClusterManager

from faultstorm_switchover import SwitchoverAction


def _find_primary(db_nodes):
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


# ---- Flag / serde steps ----

@given('a switchover action')
def step_given_switchover(context):
    context.action = SwitchoverAction(
        context.db_nodes, context.extra_nodes, ordinal=1,
        load_node=context.load_node, dc_map=context.dc_map,
    )


@given('a switchover action with ordinal {ordinal:d} and node "{node}"')
def step_given_switchover_with_node(context, ordinal, node):
    context.action = SwitchoverAction(
        context.db_nodes, context.extra_nodes, ordinal=ordinal,
        load_node=context.load_node, dc_map=context.dc_map,
        node=node,
    )


@given('a switchover action with ordinal {ordinal:d} and no node')
def step_given_switchover_no_node(context, ordinal):
    context.action = SwitchoverAction(
        context.db_nodes, context.extra_nodes, ordinal=ordinal,
        load_node=context.load_node, dc_map=context.dc_map,
    )


@when('I serialize and deserialize the switchover action')
def step_serde_switchover(context):
    serialized = context.action.serialize()
    context.deserialized = SwitchoverAction.deserialize(
        serialized, context.db_nodes, context.extra_nodes,
        load_node=context.load_node, dc_map=context.dc_map,
    )


@then('the deserialized switchover action has ordinal {ordinal:d} and node "{node}"')
def step_check_serde_switchover(context, ordinal, node):
    d = context.deserialized
    assert d.ordinal == ordinal, f"Expected ordinal {ordinal}, got {d.ordinal}"
    assert d.node == node, f"Expected node {node}, got {d.node}"


@then('the deserialized switchover action has ordinal {ordinal:d} and no node')
def step_check_serde_switchover_no_node(context, ordinal):
    d = context.deserialized
    assert d.ordinal == ordinal, f"Expected ordinal {ordinal}, got {d.ordinal}"
    assert d.node is None, f"Expected node None, got {d.node}"


# ---- Docker integration steps ----

@given('a switchover action with node "{node}"')
def step_given_switchover_docker_node(context, node):
    context.action = SwitchoverAction(
        context.db_nodes, context.extra_nodes, ordinal=1,
        load_node=context.load_node, dc_map=context.dc_map,
        node=node,
    )


@given('a switchover action with no node')
def step_given_switchover_docker_no_node(context):
    context.action = SwitchoverAction(
        context.db_nodes, context.extra_nodes, ordinal=1,
        load_node=context.load_node, dc_map=context.dc_map,
    )


@when('I execute the switchover action')
def step_execute_switchover(context):
    context.action.execute()


@then('the switchover action node is one of the db nodes')
def step_switchover_node_in_db(context):
    assert context.action.node in context.db_nodes, (
        f"Node {context.action.node} not in {context.db_nodes}"
    )


@given('the current primary node is recorded')
def step_record_primary(context):
    primary = _find_primary(context.db_nodes)
    assert primary is not None, "Could not find a primary among db nodes"
    context.original_primary = primary


@given('a switchover action targeting the current primary')
def step_switchover_targeting_primary(context):
    context.action = SwitchoverAction(
        context.db_nodes, context.extra_nodes, ordinal=1,
        load_node=context.load_node, dc_map=context.dc_map,
        node=context.original_primary,
    )


@when('I wait up to {seconds:d} seconds for the primary to change')
def step_wait_primary_change(context, seconds):
    deadline = time.time() + seconds
    while time.time() < deadline:
        current = _find_primary(context.db_nodes)
        if current is not None and current != context.original_primary:
            context.new_primary = current
            return
        time.sleep(2)
    context.new_primary = _find_primary(context.db_nodes)


@then('the primary has changed')
def step_primary_changed(context):
    assert context.new_primary is not None, "No primary found after switchover"
    assert context.new_primary != context.original_primary, (
        f"Primary did not change: still {context.original_primary}"
    )
