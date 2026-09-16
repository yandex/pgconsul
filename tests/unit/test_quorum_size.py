# encoding: utf-8
from unittest.mock import MagicMock

from src.helpers import app_name_from_fqdn
from src.replication_manager import QuorumReplicationManager, ReplicationManagerConfig


HOSTS = [f'pgconsul{number}.test' for number in range(1, 7)]


class FakeZk:
    """
    ZK as `is_promote_safe` reads it: the quorum host list and the record written with it.
    A stale record is one the host list outlived, as a version that does not know about
    the record leaves it.
    """

    QUORUM_PATH = 'quorum'
    QUORUM_SIZE_PATH = 'quorum/size'

    def __init__(self, quorum_hosts, published, raises=(), stale_record=False):
        self._values = {self.QUORUM_PATH: quorum_hosts, self.QUORUM_SIZE_PATH: published}
        self._raises = set(raises)
        self._stale_record = stale_record

    def get(self, key, preproc=None):
        if key in self._raises:
            raise RuntimeError(f'reading {key} from ZK failed')
        return self._values.get(key)

    def noexcept_get(self, key, preproc=None):
        try:
            return self.get(key, preproc)
        except Exception:
            return None

    def get_mzxid(self, key):
        if key == self.QUORUM_SIZE_PATH:
            return 0 if self._stale_record else 1
        return 1


class FakePrimaryZk(FakeZk):
    """The same ZK as the primary drives it: the quorum locks, and every write recorded."""

    def __init__(self, quorum_hosts, quorum_list, published, events, stale_record=False):
        super().__init__(quorum_list, published, stale_record=stale_record)
        self._quorum_hosts = quorum_hosts
        self._events = events

    def get_sync_quorum_hosts(self):
        return self._quorum_hosts

    def write(self, key, data, preproc=None):
        self._values[key] = data
        self._events.append(('write', (key, data)))
        return True

    def write_ssn_on_changes(self, value):
        pass


def make(quorum_commit_virtual_witnesses: int, db=None, zk=None) -> QuorumReplicationManager:
    config = ReplicationManagerConfig(
        priority=0,
        primary_unavailability_timeout=1,
        change_replication_metric='count',
        weekday_change_hours='0-0',
        weekend_change_hours='0-0',
        overload_sessions_ratio=75,
        before_async_unavailability_timeout=15,
        quorum_commit_virtual_witnesses=quorum_commit_virtual_witnesses,
    )
    return QuorumReplicationManager(config, db or MagicMock(), zk or MagicMock())


def ssn(quorum_size: int, hosts) -> str:
    return f'ANY {quorum_size}({",".join(app_name_from_fqdn(host) for host in hosts)})'


def recorded(quorum_size: int, hosts) -> dict:
    """The record the primary writes next to the quorum host list."""
    return {'hosts': list(hosts), 'size': quorum_size}


def installed_ssn(witnesses: int, replicas: int) -> str:
    db = MagicMock()
    make(witnesses, db=db).change_replication_to_quorum(HOSTS[:replicas])
    return db.change_replication_type.call_args[0][0]


def installed_quorum(witnesses: int, replicas: int) -> int:
    return int(installed_ssn(witnesses, replicas).split(' ')[1].split('(')[0])


def primary_iteration(witnesses, quorum_hosts, current_ssn, quorum_list, published=None, stale_record=False):
    """
    One iteration of the primary over a group holding the quorum locks, as the events it
    produced: `('ssn', value)` for what it applied to PostgreSQL, `('write', (path, data))`
    for what it wrote to ZK, in the order they happened.
    """
    events = []
    db = MagicMock()
    db.get_replication_state.return_value = ('sync', current_ssn) if current_ssn else ('async', None)
    db.change_replication_type.side_effect = lambda value: events.append(('ssn', value)) or True
    zk = FakePrimaryZk(quorum_hosts, quorum_list, published, events, stale_record)
    db_state = {
        'replics_info': [
            {'application_name': app_name_from_fqdn(host), 'state': 'streaming'} for host in quorum_hosts
        ],
    }
    make(witnesses, db=db, zk=zk).update_replication_type(db_state, quorum_hosts)
    return events


def applied_ssn(witnesses, quorum_hosts, current_ssn, quorum_list, published=None) -> list:
    """What the same iteration applied to PostgreSQL. No `current_ssn` stands for async."""
    events = primary_iteration(witnesses, quorum_hosts, current_ssn, quorum_list, published)
    return [payload for kind, payload in events if kind == 'ssn']


def written(events) -> list:
    return [payload for kind, payload in events if kind == 'write']


def promote_safe(
    quorum_hosts,
    alive_hosts,
    published,
    streaming_hosts=None,
    quorum_commit_virtual_witnesses=0,
    raises=(),
    stale_record=False,
) -> bool:
    """
    `alive_hosts` hold the alive lock, `streaming_hosts` are streaming from the primary
    and the rest are catching up. Only a host that does both counts towards the quorum.
    """
    if streaming_hosts is None:
        streaming_hosts = alive_hosts
    replica_infos = [
        {
            'application_name': app_name_from_fqdn(host),
            'state': 'streaming' if host in streaming_hosts else 'catchup',
        }
        for host in HOSTS
    ]
    zk = FakeZk(quorum_hosts, published, raises=raises, stale_record=stale_record)
    return make(quorum_commit_virtual_witnesses, zk=zk).is_promote_safe(alive_hosts, replica_infos)


# ---------------------------------------------------------------------------
# quorum size the primary installs
# ---------------------------------------------------------------------------

def test_quorum_without_the_setting_is_a_majority_of_the_cluster():
    assert [installed_quorum(0, replicas) for replicas in range(1, 7)] == [1, 1, 2, 2, 3, 3]


def test_every_two_witnesses_buy_one_more_replica():
    assert [installed_quorum(1, replicas) for replicas in range(1, 7)] == [1, 2, 2, 3, 3, 4]
    assert [installed_quorum(2, replicas) for replicas in range(1, 7)] == [1, 2, 3, 3, 4, 4]
    assert [installed_quorum(4, replicas) for replicas in range(1, 7)] == [1, 2, 3, 4, 5, 5]


def test_quorum_is_capped_by_the_group_it_is_required_from():
    assert installed_quorum(3, replicas=2) == 2
    assert installed_quorum(9, replicas=4) == 4


def test_ssn_lists_every_quorum_replica():
    assert installed_ssn(1, replicas=2) == 'ANY 2(pgconsul1_test,pgconsul2_test)'


# ---------------------------------------------------------------------------
# what the primary records in ZK
# ---------------------------------------------------------------------------

def test_the_quorum_size_is_recorded_next_to_the_host_list():
    events = primary_iteration(1, HOSTS[:2], ssn(1, HOSTS[:2]), HOSTS[:2])
    assert written(events) == [
        (FakeZk.QUORUM_PATH, HOSTS[:2]),
        (FakeZk.QUORUM_SIZE_PATH, recorded(2, HOSTS[:2])),
    ]


def test_an_installed_and_recorded_quorum_is_left_alone():
    events = primary_iteration(1, HOSTS[:2], ssn(2, HOSTS[:2]), HOSTS[:2], recorded(2, HOSTS[:2]))
    assert events == []


def test_a_lost_record_is_written_without_touching_postgresql():
    # one failed write would otherwise leave every promote deciding by the majority
    events = primary_iteration(1, HOSTS[:2], ssn(2, HOSTS[:2]), HOSTS[:2], published=None)
    assert written(events) == [
        (FakeZk.QUORUM_PATH, HOSTS[:2]),
        (FakeZk.QUORUM_SIZE_PATH, recorded(2, HOSTS[:2])),
    ]
    assert [kind for kind, _ in events] == ['write', 'write']


def test_a_stale_record_is_written_again():
    # the host list outlived the record, so the record describes a list written since
    events = primary_iteration(
        1, HOSTS[:2], ssn(2, HOSTS[:2]), HOSTS[:2], recorded(2, HOSTS[:2]), stale_record=True
    )
    assert [kind for kind, _ in events] == ['write', 'write']


def test_a_record_with_a_size_of_another_type_is_written_again():
    # a hand-edited record must not stop the primary from changing replication at all
    events = primary_iteration(1, HOSTS[:2], ssn(1, HOSTS[:2]), HOSTS[:2], {'hosts': HOSTS[:2], 'size': '2'})
    assert [kind for kind, _ in events] == ['ssn', 'write', 'write']


def test_a_record_of_another_list_is_written_again():
    # the size alone says nothing: it has to be the size of this very group
    events = primary_iteration(1, HOSTS[:2], ssn(2, HOSTS[:2]), HOSTS[:2], recorded(2, HOSTS[2:4]))
    assert [kind for kind, _ in events] == ['write', 'write']


def test_a_quorum_on_its_way_down_is_recorded_before_it_is_installed():
    # a record claiming more than PostgreSQL requires would let too few hosts promote
    events = primary_iteration(0, HOSTS[:2], ssn(2, HOSTS[:2]), HOSTS[:2], recorded(2, HOSTS[:2]))
    assert [kind for kind, _ in events] == ['write', 'write', 'ssn', 'write', 'write']


def test_a_quorum_on_its_way_up_is_installed_before_it_is_recorded():
    events = primary_iteration(1, HOSTS[:2], ssn(1, HOSTS[:2]), HOSTS[:2], recorded(1, HOSTS[:2]))
    assert [kind for kind, _ in events] == ['ssn', 'write', 'write']


def test_a_changed_quorum_size_reaches_postgresql_without_a_membership_change():
    # the setting is raised while the very same replicas keep holding the quorum locks
    assert applied_ssn(1, HOSTS[:2], ssn(1, HOSTS[:2]), HOSTS[:2]) == [ssn(2, HOSTS[:2])]


def test_asynchronous_replication_is_turned_back_into_a_quorum():
    # nothing is installed, so the comparison has nothing to reach into
    assert applied_ssn(0, HOSTS[:2], None, HOSTS[:2]) == [ssn(1, HOSTS[:2])]


def test_a_stale_recorded_quorum_reaches_postgresql():
    # the quorum locks moved on, the list recorded in ZK did not
    assert applied_ssn(0, HOSTS[:2], ssn(1, HOSTS[:2]), HOSTS[1:3]) == [ssn(1, HOSTS[:2])]


def test_a_changed_quorum_membership_reaches_postgresql():
    # the same quorum size over another group of hosts
    assert applied_ssn(0, HOSTS[:2], ssn(1, HOSTS[1:3]), HOSTS[:2]) == [ssn(1, HOSTS[:2])]


def test_an_installed_quorum_is_read_in_any_spelling():
    # PostgreSQL keeps whatever spelling it was given, and a hand-set value is one of them
    assert applied_ssn(0, HOSTS[:2], 'any 1 (pgconsul1_test, pgconsul2_test)', HOSTS[:2]) == []


# ---------------------------------------------------------------------------
# promote safety
# ---------------------------------------------------------------------------

def test_promote_needs_a_host_of_every_possible_quorum():
    # with a quorum of one either host alone could hold the last commit
    assert promote_safe(HOSTS[:2], HOSTS[:1], recorded(1, HOSTS[:2])) is False
    assert promote_safe(HOSTS[:2], HOSTS[:2], recorded(1, HOSTS[:2])) is True
    # with a quorum of two every commit is on both, so one survivor is enough
    assert promote_safe(HOSTS[:2], HOSTS[:1], recorded(2, HOSTS[:2])) is True
    # the same, one host wider
    assert promote_safe(HOSTS[:4], HOSTS[:2], recorded(2, HOSTS[:4])) is False
    assert promote_safe(HOSTS[:4], HOSTS[:3], recorded(2, HOSTS[:4])) is True
    assert promote_safe(HOSTS[:4], HOSTS[:2], recorded(3, HOSTS[:4])) is True


def test_promote_follows_the_recorded_size_not_the_local_setting():
    # our own setting would allow a single survivor, but the quorum of record is one
    assert promote_safe(HOSTS[:2], HOSTS[:1], recorded(1, HOSTS[:2]), quorum_commit_virtual_witnesses=1) is False


def test_promote_falls_back_to_the_majority_if_the_record_is_unusable():
    # nothing recorded, an empty or partial record, a size the group cannot have,
    # a size of another type, and a record of another host list
    unusable = (
        None,
        {},
        'not a record',
        {'hosts': HOSTS[:4]},
        {'hosts': HOSTS[:4], 'size': 0},
        {'hosts': HOSTS[:4], 'size': 5},
        {'hosts': HOSTS[:4], 'size': '2'},
        {'hosts': 4, 'size': 2},
        recorded(4, HOSTS[1:5]),
    )
    for published in unusable:
        # the majority of four hosts is two, so its threshold demands three of them
        assert promote_safe(HOSTS[:4], HOSTS[:2], published, quorum_commit_virtual_witnesses=1) is False
        assert promote_safe(HOSTS[:4], HOSTS[:3], published, quorum_commit_virtual_witnesses=1) is True


def test_promote_falls_back_when_the_record_is_older_than_the_host_list():
    # a version that knows nothing about the record can rewrite the list under it
    assert promote_safe(HOSTS[:4], HOSTS[:2], recorded(3, HOSTS[:4]), stale_record=True) is False


def test_promote_falls_back_when_the_record_cannot_be_read():
    # a failed ZK read decides the promote, it does not take the daemon down
    assert promote_safe(HOSTS[:4], HOSTS[:2], recorded(3, HOSTS[:4]), raises=[FakeZk.QUORUM_SIZE_PATH]) is False


def test_promote_counts_only_hosts_that_are_both_alive_and_streaming():
    quorum = recorded(2, HOSTS[:2])
    assert promote_safe(HOSTS[:2], HOSTS[1:2], quorum) is True
    # the surviving host holds the alive lock, but the one that streams is another
    assert promote_safe(HOSTS[:2], HOSTS[1:2], quorum, streaming_hosts=HOSTS[:1]) is False


def test_promote_takes_the_election_votes_as_a_host_group():
    votes = {host: (1, 0) for host in HOSTS[:2]}
    assert promote_safe(HOSTS[:2], votes, recorded(1, HOSTS[:2])) is True
    assert promote_safe(HOSTS[:4], votes, recorded(2, HOSTS[:4])) is False


def test_promote_is_unsafe_without_a_quorum():
    assert promote_safe([], [], recorded(1, HOSTS[:2])) is False
    assert promote_safe(None, [], recorded(1, HOSTS[:2])) is False
