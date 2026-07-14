"""Step definitions for SwitchoverAction tests."""

import time

from behave import given, when, then

from faultstorm_switchover import SwitchoverAction

from steps.common import find_primary


@given('a switchover action with no node')
def step_given_switchover_docker_no_node(context):
    context.action = SwitchoverAction(
        context.db_nodes, context.extra_nodes, ordinal=1,
        dc_map=context.dc_map,
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
    primary = find_primary(context.db_nodes)
    assert primary is not None, "Could not find a primary among db nodes"
    context.original_primary = primary


@when('I wait up to {seconds:d} seconds for the primary to change')
def step_wait_primary_change(context, seconds):
    deadline = time.time() + seconds
    while time.time() < deadline:
        current = find_primary(context.db_nodes)
        if current is not None and current != context.original_primary:
            context.new_primary = current
            return
        time.sleep(2)
    context.new_primary = find_primary(context.db_nodes)


@then('the primary has changed')
def step_primary_changed(context):
    assert context.new_primary is not None, "No primary found after switchover"
    assert context.new_primary != context.original_primary, (
        f"Primary did not change: still {context.original_primary}"
    )
