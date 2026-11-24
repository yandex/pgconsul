from configparser import RawConfigParser
from contextlib import nullcontext
from unittest.mock import patch

import pytest

from src import helpers
from src.exceptions import SwitchoverException
from src.types import ReplicaInfo
from src.zk import ZookeeperException

with patch('src.read_config', create=True), patch('src.init_logging', create=True):
    from src.cli import select_switchover_destination


PRIMARY = 'vla-primary.example.net'
HIGH_PRIORITY = 'rc1a-replica.example.net'
LOW_PRIORITY = 'edge-replica.example.net'
NON_HA = 'cascade-replica.example.net'


def replica_info(host, *, replay_lag=0, write_lag=0, state='streaming'):
    return ReplicaInfo(
        application_name=helpers.app_name_from_fqdn(host),
        state=state,
        replay_lag_msec=replay_lag,
        write_location_diff=write_lag,
    )


class FakeZooKeeper:
    def __init__(self):
        self.primary: str | None = PRIMARY
        self.ha_replicas: set[str] | None = {HIGH_PRIORITY, LOW_PRIORITY}
        self.quorum: list[str] | None = [HIGH_PRIORITY, LOW_PRIORITY]
        self.alive_hosts = {HIGH_PRIORITY, LOW_PRIORITY}
        self.priorities: dict[str, str | None] = {HIGH_PRIORITY: '100', LOW_PRIORITY: '50'}
        self.replicas: list[ReplicaInfo] | None = [replica_info(LOW_PRIORITY), replica_info(HIGH_PRIORITY)]
        self.ha_reads = 0
        self.replication_reads = 0
        self.priority_reads = 0
        self.alive_checks: list[tuple[str, float]] = []

    def get_current_lock_holder(self):
        return self.primary

    def get_ha_replics(self, my_hostname):
        self.ha_reads += 1
        return self.ha_replicas

    def get_quorum(self):
        return self.quorum

    def get_replics_info(self):
        self.replication_reads += 1
        return self.replicas

    def is_host_alive(self, host, timeout=0.0):
        self.alive_checks.append((host, timeout))
        return host in self.alive_hosts

    def get_host_prio(self, host):
        self.priority_reads += 1
        return self.priorities.get(host)


@pytest.fixture
def zk(monkeypatch):
    fake = FakeZooKeeper()
    monkeypatch.setattr('src.cli.create_zk', lambda *, config: nullcontext(fake))
    return fake


@pytest.fixture
def conf():
    config = RawConfigParser()
    config.read_dict({'replica': {'allow_potential_data_loss': 'no'}})
    return config


def test_prefers_lower_replay_lag_even_above_daemon_limit(zk, conf):
    conf.read_dict({'global': {'max_allowed_switchover_lag_ms': '500'}})
    zk.replicas = [replica_info(HIGH_PRIORITY, replay_lag=1000), replica_info(LOW_PRIORITY, replay_lag=600, write_lag=16)]

    assert select_switchover_destination(('vla-',), conf) == LOW_PRIORITY
    assert zk.priority_reads == 0


@pytest.mark.parametrize('first_priority, second_priority, expected', [
    pytest.param('50', '100', LOW_PRIORITY, id='numeric-priority'),
    pytest.param('-1', '0', LOW_PRIORITY, id='zero-beats-negative'),
    pytest.param('50', '50', HIGH_PRIORITY, id='equal-priority-keeps-snapshot-order'),
    pytest.param(None, '0', LOW_PRIORITY, id='first-priority-missing'),
])
def test_uses_priority_only_among_replicas_with_minimum_replay_lag(zk, conf, first_priority, second_priority, expected):
    slower = 'slow-replica.example.net'
    zk.ha_replicas = {LOW_PRIORITY, HIGH_PRIORITY, slower}
    zk.quorum = [LOW_PRIORITY, HIGH_PRIORITY, slower]
    zk.alive_hosts.add(slower)
    zk.replicas = [
        replica_info(slower, replay_lag=100),
        # The tied hosts appear in reverse alphabetical order to exercise snapshot ordering.
        replica_info(HIGH_PRIORITY, replay_lag=25),
        replica_info(LOW_PRIORITY, replay_lag=25),
    ]
    zk.priorities = {HIGH_PRIORITY: first_priority, LOW_PRIORITY: second_priority, slower: '1000'}

    assert select_switchover_destination(('vla-',), conf) == expected


def test_excludes_replicas_by_prefix(zk, conf):
    assert select_switchover_destination(('rc1a-', 'vla-'), conf) == LOW_PRIORITY

    with pytest.raises(SwitchoverException, match='No eligible HA replica'):
        select_switchover_destination(('rc1a-', 'vla-', 'edge-'), conf)
    assert zk.replication_reads == 1
    assert zk.alive_checks == [(LOW_PRIORITY, 1)]


@pytest.mark.parametrize('allow_data_loss, quorum, expected', [
    pytest.param('no', [LOW_PRIORITY], LOW_PRIORITY, id='require-quorum'),
    pytest.param('yes', [LOW_PRIORITY], HIGH_PRIORITY, id='allow-outside-quorum'),
    pytest.param('yes', None, HIGH_PRIORITY, id='allow-missing-quorum'),
])
def test_applies_quorum_policy(zk, conf, allow_data_loss, quorum, expected):
    zk.quorum = quorum
    conf.set('replica', 'allow_potential_data_loss', allow_data_loss)

    assert select_switchover_destination(('vla-',), conf) == expected


@pytest.mark.parametrize('ineligible_replica', [
    pytest.param(replica_info(HIGH_PRIORITY, state='catchup'), id='not-streaming'),
    pytest.param(replica_info(HIGH_PRIORITY, replay_lag=None), id='unknown-replay-lag'),
    pytest.param(ReplicaInfo(application_name=None, state='streaming', replay_lag_msec=0), id='missing-application-name'),
    pytest.param(replica_info(NON_HA), id='not-an-ha-replica'),
])
def test_skips_ineligible_replicas_from_replication_status(zk, conf, ineligible_replica):
    zk.quorum = [HIGH_PRIORITY, LOW_PRIORITY, NON_HA]
    zk.alive_hosts.add(NON_HA)
    zk.replicas = [ineligible_replica, replica_info(LOW_PRIORITY, replay_lag=50)]

    assert select_switchover_destination(('vla-',), conf) == LOW_PRIORITY
    assert zk.alive_checks == [(LOW_PRIORITY, 1)]


def test_skips_when_primary_is_already_outside_literal_prefixes(zk, conf, capsys):
    assert select_switchover_destination(('primary', 'vl[a]-', 'VLA-'), conf) is None
    assert f'Primary {PRIMARY} is already outside the excluded host prefixes' in capsys.readouterr().out
    assert zk.ha_reads == 0


def test_fails_when_no_host_holds_leader_lock(zk, conf):
    zk.primary = None

    with pytest.raises(SwitchoverException, match='No host holds the leader lock, or the ZooKeeper state could not be read'):
        select_switchover_destination(('vla-',), conf)
    assert zk.ha_reads == 0


@pytest.mark.parametrize('ha_replicas, quorum', [
    pytest.param(None, [HIGH_PRIORITY], id='ha-list-missing'),
    pytest.param({HIGH_PRIORITY}, None, id='quorum-missing'),
    pytest.param({HIGH_PRIORITY}, [], id='quorum-empty'),
    pytest.param({HIGH_PRIORITY}, [NON_HA], id='no-ha-replicas-in-quorum'),
])
def test_fails_before_reading_replication_status_if_no_hosts_pass_filters(zk, conf, ha_replicas, quorum):
    zk.ha_replicas = ha_replicas
    zk.quorum = quorum
    with pytest.raises(SwitchoverException, match='No eligible HA replica'):
        select_switchover_destination(('vla-',), conf)
    assert zk.replication_reads == 0
    assert not zk.alive_checks


def test_fails_when_replication_status_is_missing(zk, conf):
    zk.replicas = None
    with pytest.raises(SwitchoverException, match='No eligible HA replica'):
        select_switchover_destination(('vla-',), conf)


def test_checks_alive_lock_before_selecting_replica(zk, conf):
    zk.alive_hosts = {LOW_PRIORITY}
    assert select_switchover_destination(('vla-',), conf) == LOW_PRIORITY
    zk.alive_hosts.clear()
    with pytest.raises(SwitchoverException, match='No eligible HA replica'):
        select_switchover_destination(('vla-',), conf)


@pytest.mark.parametrize('priority, error, message', [
    pytest.param(None, SwitchoverException, 'No priority available for tied replicas', id='missing-priorities'),
    pytest.param('invalid-priority', ValueError, 'invalid-priority', id='invalid-priority'),
])
def test_fails_when_tied_priorities_are_unusable(zk, conf, priority, error, message):
    zk.priorities = {HIGH_PRIORITY: priority, LOW_PRIORITY: priority}
    with pytest.raises(error, match=message):
        select_switchover_destination(('vla-',), conf)


def test_propagates_zookeeper_connection_error(conf, monkeypatch):
    def fail_connect(*, config):
        raise Exception('Could not connect to ZK.')

    monkeypatch.setattr('src.cli.create_zk', fail_connect)
    with pytest.raises(Exception, match='Could not connect to ZK'):
        select_switchover_destination(('vla-',), conf)


@pytest.mark.parametrize('read', ['get_quorum', 'get_replics_info'])
def test_propagates_zookeeper_read_error(zk, conf, read, monkeypatch):
    def fail_read():
        raise ZookeeperException('ZooKeeper session expired')

    monkeypatch.setattr(zk, read, fail_read)
    with pytest.raises(ZookeeperException, match='ZooKeeper session expired'):
        select_switchover_destination(('vla-',), conf)
