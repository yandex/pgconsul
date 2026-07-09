"""Step definitions for MaintenanceAction tests."""

from behave import given, when, then

from faultstorm.cluster import ClusterManager

from faultstorm_maintenance import MaintenanceAction


# ---- Flag / serde steps ----

@given('a maintenance action')
def step_given_maintenance(context):
    context.action = MaintenanceAction(
        context.db_nodes, context.extra_nodes, ordinal=1,
        dc_map=context.dc_map,
    )


@given('a maintenance action with ordinal {ordinal:d} and node "{node}"')
def step_given_maintenance_with_node(context, ordinal, node):
    context.action = MaintenanceAction(
        context.db_nodes, context.extra_nodes, ordinal=ordinal,
        dc_map=context.dc_map,
        node=node,
    )


@given('a maintenance action with ordinal {ordinal:d} and no node')
def step_given_maintenance_no_node(context, ordinal):
    context.action = MaintenanceAction(
        context.db_nodes, context.extra_nodes, ordinal=ordinal,
        dc_map=context.dc_map,
    )


@when('I serialize and deserialize the maintenance action')
def step_serde_maintenance(context):
    serialized = context.action.serialize()
    context.deserialized = MaintenanceAction.deserialize(
        serialized, context.db_nodes, context.extra_nodes,
        dc_map=context.dc_map,
    )


@then('the deserialized maintenance action has ordinal {ordinal:d} and node "{node}"')
def step_check_serde_maintenance(context, ordinal, node):
    d = context.deserialized
    assert d.ordinal == ordinal, f"Expected ordinal {ordinal}, got {d.ordinal}"
    assert d.node == node, f"Expected node {node}, got {d.node}"


@then('the deserialized maintenance action has ordinal {ordinal:d} and no node')
def step_check_serde_maintenance_no_node(context, ordinal):
    d = context.deserialized
    assert d.ordinal == ordinal, f"Expected ordinal {ordinal}, got {d.ordinal}"
    assert d.node is None, f"Expected node None, got {d.node}"


# ---- Docker integration steps ----

@given('a maintenance action with node "{node}"')
def step_given_maintenance_docker_node(context, node):
    context.action = MaintenanceAction(
        context.db_nodes, context.extra_nodes, ordinal=1,
        dc_map=context.dc_map,
        node=node,
    )


@given('a maintenance action with no node')
def step_given_maintenance_docker_no_node(context):
    context.action = MaintenanceAction(
        context.db_nodes, context.extra_nodes, ordinal=1,
        dc_map=context.dc_map,
    )


@when('I execute the maintenance action')
def step_execute_maintenance(context):
    context.action.execute()


@when('I heal the maintenance action')
def step_heal_maintenance(context):
    context.action.heal()


@then('pgconsul-util maintenance show on "{node}" reports "{expected_status}"')
def step_check_maintenance_status(context, node, expected_status):
    out = ClusterManager.exec_on_node(
        node,
        ["pgconsul-util", "maintenance", "-m", "show"],
        timeout=10,
    )
    assert expected_status in out.lower(), (
        f"Expected '{expected_status}' in maintenance output, got: {out}"
    )


@then('the maintenance action node is one of the db nodes')
def step_maintenance_node_in_db(context):
    assert context.action.node in context.db_nodes, (
        f"Node {context.action.node} not in {context.db_nodes}"
    )
