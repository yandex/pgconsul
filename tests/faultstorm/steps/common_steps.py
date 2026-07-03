"""Shared step definitions for pgconsul faultstorm action flag and serde checks."""

from behave import then


@then('it is not healable')
def step_not_healable(context):
    assert not context.action.healable, "Expected action to NOT be healable"


@then('it is healable')
def step_is_healable(context):
    assert context.action.healable, "Expected action to be healable"


@then('it is not destructive')
def step_not_destructive(context):
    assert not context.action.destructive, "Expected action to NOT be destructive"


@then('it is destructive')
def step_is_destructive(context):
    assert context.action.destructive, "Expected action to be destructive"


@then('it is not host_targetable')
def step_not_host_targetable(context):
    assert not context.action.host_targetable, "Expected action to NOT be host_targetable"


@then('it is host_targetable')
def step_is_host_targetable(context):
    assert context.action.host_targetable, "Expected action to be host_targetable"
