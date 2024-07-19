#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
import json
import operator

import kazoo.exceptions
from kazoo.handlers.threading import KazooTimeoutError
import steps.helpers as helpers
import yaml
from behave import then, when, use_step_matcher

use_step_matcher('re')

@then('zookeeper "(?P<name>[a-zA-Z0-9_-]+)" has holder "(?P<holders>[.a-zA-Z0-9_-]+)" for lock "(?P<key>[./a-zA-Z0-9_-]+)"')
@then('zookeeper "(?P<name>[a-zA-Z0-9_-]+)" has one of holders "(?P<holders>[.a-zA-Z0-9_-]+)" for lock "(?P<key>[./a-zA-Z0-9_-]+)"')
@helpers.retry_on_assert
def step_zk_check_holders(context, name, holders, key):
    try:
        zk = helpers.get_zk(context, name)
        contender = None
        zk.start()
        lock = zk.Lock(key)
        contenders = lock.contenders()
        if contenders:
            contender = contenders[0]
    finally:
        zk.stop()
        zk.close()
    for holder in holders.split(','):
        if str(contender) == str(holder):
            return
    raise AssertionError(
        '{time}: lock "{key}" holder is "{holder}", expected one of "{exp}"'.format(
            key=key, holder=contender, exp=holders, time=datetime.now().strftime("%H:%M:%S")
        )
    )


@when('we lock "(?P<key>[./a-zA-Z0-9_-]+)" in zookeeper "(?P<name>[a-zA-Z0-9_-]+)"')
@when('we lock "(?P<key>[./a-zA-Z0-9_-]+)" in zookeeper "(?P<name>[a-zA-Z0-9_-]+)" with value "(?P<value>[ \[\]{},.:\'a-zA-Z0-9_-]+)"')
def step_zk_lock(context, key, name, value=None):
    if not context.zk:
        context.zk = helpers.get_zk(context, name)
        context.zk.start()
    if value and "'" in value:
        value = value.replace("'", '"')
    lock = context.zk.Lock(key, value)
    lock.acquire()
    context.zk_locks[key] = lock


@when('we release lock "(?P<key>[./a-zA-Z0-9_-]+)" in zookeeper "(?P<name>[a-zA-Z0-9_-]+)"')
def step_zk_release_lock(context, key, name):
    if key in context.zk_locks:
        context.zk_locks[key].release()


@then('zookeeper "(?P<name>[a-zA-Z0-9_-]+)" has no value for key "(?P<key>[./a-zA-Z0-9_-]+)"')
@helpers.retry_on_assert
def step_zk_no_value(context, name, key):
    zk_value = helpers.get_zk_value(context, name, key)
    assert zk_value is None, '{time}: node "{key}" exists and has value "{val}"'.format(
        key=key, val=zk_value, time=datetime.now().strftime("%H:%M:%S")
    )


@then('zookeeper "(?P<name>[a-zA-Z0-9_-]+)" node is alive')
def step_zk_is_alive(context, name):
    key = '/test_is_{0}_alive'.format(name)
    try:
        step_zk_set_value(context, name, key, name)
        step_zk_value(context, name, name, key)
    except (AssertionError, KazooTimeoutError):
        helpers.LOG.warn(
            '{time}: {name} zookeeper looks dead, try to repair'.format(
                name=name, time=datetime.now().strftime("%H:%M:%S")
            )
        )
        try_to_repair_zk_host(context, name)
        step_zk_set_value_with_retries(context, name, key, name)
        step_zk_value(context, name, name, key)


def try_to_repair_zk_host(context, name):
    container = context.containers[name]
    # https://stackoverflow.com/questions/57574298/zookeeper-error-the-current-epoch-is-older-than-the-last-zxid
    err = 'is older than the last zxid'
    container.exec_run(
        "grep '{err}' /var/log/zookeeper/zookeeper--server-pgconsul_{name}_1.log && rm -rf /tmp/zookeeper/version-2".format(
            err=err, name=name
        )
    )
    container.exec_run("/usr/local/bin/supervisorctl restart zookeeper")


@then('zookeeper "(?P<name>[a-zA-Z0-9_-]+)" has value "(?P<value>[ \[\]{},.:\'a-zA-Z0-9_-]+)" for key "(?P<key>[./a-zA-Z0-9_-]+)"')
@helpers.retry_on_assert
def step_zk_value(context, name, value, key):
    if value and "'" in value:
        value = value.replace("'", '"')
    zk_value = helpers.get_zk_value(context, name, key)
    assert str(zk_value) == str(value), '{time}: expected value "{exp}", got "{val}"'.format(
        exp=value, val=zk_value, time=datetime.now().strftime("%H:%M:%S")
    )


@then('zookeeper "(?P<name>[a-zA-Z0-9_-]+)" has key "(?P<key>[./a-zA-Z0-9_-]+)"')
@helpers.retry_on_assert
def step_zk_key(context, name, key):
    assert helpers.zk_has_key(context, name, key), '{time}: key "{key}" is missing'.format(
        time=datetime.now().strftime("%H:%M:%S"), key=key
    )


@then('zookeeper "(?P<name>[a-zA-Z0-9_-]+)" doesn\'t have key "(?P<key>[./a-zA-Z0-9_-]+)"')
@helpers.retry_on_assert
def step_zk_key(context, name, key):
    assert not helpers.zk_has_key(context, name, key), '{time}: key "{key}" is present'.format(
        time=datetime.now().strftime("%H:%M:%S"), key=key
    )


@then('zookeeper "(?P<name>[a-zA-Z0-9_-]+)" has "(?P<n>[0-9]+)" values for key "(?P<key>[./a-zA-Z0-9_-]+)"')
@helpers.retry_on_assert
def step_zk_key_has_n_values(context, name, n, key):
    n = int(n)
    zk_value = helpers.get_zk_value(context, name, key)
    assert zk_value is not None, 'key {key} does not exists'.format(key=key)
    actual_values = json.loads(zk_value)
    assert n == len(actual_values), 'expected {n} values in key {key}, but values are {values}'.format(
        n=n, key=key, values=actual_values
    )


@then('zookeeper "(?P<name>[a-zA-Z0-9_-]+)" has following values for key "(?P<key>[./a-zA-Z0-9_-]+)"')
@helpers.retry_on_assert
def step_zk_key_values(context, name, key):
    exp_values = sorted(yaml.safe_load(context.text) or [], key=operator.itemgetter('client_hostname'))
    assert isinstance(exp_values, list), '{time}: expected list, got {got}'.format(
        got=type(exp_values), time=datetime.now().strftime("%H:%M:%S")
    )
    zk_value = helpers.get_zk_value(context, name, key)
    assert zk_value is not None, '{time}: key {key} does not exists'.format(
        key=key, time=datetime.now().strftime("%H:%M:%S")
    )

    actual_values = sorted(json.loads(zk_value), key=operator.itemgetter('client_hostname'))

    equal, error = helpers.are_dicts_subsets_of(exp_values, actual_values)
    assert equal, error


def has_value_in_list(context, zk_name, key, value):
    zk_value = helpers.get_zk_value(context, zk_name, key)
    if zk_value is None or zk_value == "":
        return False

    zk_list = json.loads(zk_value)
    return value in zk_list


def has_subset_of_values(context, zk_name, key, exp_values):
    zk_value = helpers.get_zk_value(context, zk_name, key)
    if zk_value is None:
        return False

    zk_dicts = json.loads(zk_value)
    actual_values = {d['client_hostname']: d for d in zk_dicts}

    equal = helpers.is_2d_dict_subset_of(exp_values, actual_values)
    return equal


@helpers.retry_on_kazoo_timeout
def step_zk_set_value_with_retries(context, value, key, name):
    return step_zk_set_value(context, value, key, name)


@when('we set value "(?P<value>[ \[\]{},.:\'a-zA-Z0-9_-]+)" for key "(?P<key>[./a-zA-Z0-9_-]+)" in zookeeper "(?P<name>[a-zA-Z0-9_-]+)"')
def step_zk_set_value(context, value, key, name):
    try:
        zk = helpers.get_zk(context, name)
        zk.start()
        zk.ensure_path(key)
        # There is race condition, node can be deleted after ensure_path and
        # before set called. We need to catch exception and create it again.
        if value and "'" in value:
            value = value.replace("'", '"')
        try:
            zk.set(key, value.encode())
        except kazoo.exceptions.NoNodeError:
            zk.create(key, value.encode())
    finally:
        zk.stop()
        zk.close()


@when('we remove key "(?P<key>[./a-zA-Z0-9_-]+)" in zookeeper "(?P<name>[a-zA-Z0-9_-]+)"')
def step_zk_remove_key(context, key, name):
    try:
        zk = helpers.get_zk(context, name)
        zk.start()
        zk.delete(key, recursive=True)
    finally:
        zk.stop()
        zk.close()
