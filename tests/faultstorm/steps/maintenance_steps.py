"""Step definitions for MaintenanceAction tests."""

from behave import given, when, then

from faultstorm.cluster import ClusterManager

from faultstorm_maintenance import MaintenanceAction


@given('a maintenance action with node "{node}"')
def step_given_maintenance_docker_node(context, node):
    context.action = MaintenanceAction(
        context.db_nodes, context.extra_nodes, ordinal=1,
        dc_map=context.dc_map,
        node=node,
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
