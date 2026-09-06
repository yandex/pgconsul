# encoding: utf-8
from unittest.mock import MagicMock

from src.helpers import app_name_from_fqdn
from src.replication_manager import QuorumReplicationManager, ReplicationManagerConfig


PRIMARY = 'pgconsul0.test'
HOSTS = [f'pgconsul{number}.test' for number in range(1, 7)]


class FakeZk:
    """
    ZK state as `is_promote_safe` reads it: the quorum host list the primary wrote
    and the primary's own synchronous_standby_names.
    """

    QUORUM_PATH = 'quorum'
    LAST_PRIMARY_PATH = 'last_leader'

    def __init__(self, quorum_hosts, primary, primary_ssn, raises=()):
        self._values = {
            self.QUORUM_PATH: quorum_hosts,
            self.LAST_PRIMARY_PATH: primary,
            self.get_ssn_value_path(primary): primary_ssn,
        }
        self._raises = set(raises)

    @staticmethod
    def get_ssn_value_path(hostname):
        return f'all_hosts/{hostname}/synchronous_standby_names/value'

    def get(self, key, preproc=None):
        if key in self._raises:
            raise RuntimeError(f'reading {key} from ZK failed')
        return self._values.get(key)

    def noexcept_get(self, key, preproc=None):
        try:
            return self.get(key, preproc)
        except Exception:
            return None


def make(quorum_includes_primary: bool, db=None, zk=None) -> QuorumReplicationManager:
    config = ReplicationManagerConfig(
        priority=0,
        primary_unavailability_timeout=1,
        change_replication_metric='count',
        weekday_change_hours='0-0',
        weekend_change_hours='0-0',
        overload_sessions_ratio=75,
        before_async_unavailability_timeout=15,
        quorum_includes_primary=quorum_includes_primary,
    )
    return QuorumReplicationManager(config, db or MagicMock(), zk or MagicMock())


def ssn(quorum_size: int, hosts) -> str:
    return f'ANY {quorum_size}({",".join(app_name_from_fqdn(host) for host in hosts)})'


def installed_ssn(quorum_includes_primary: bool, replicas: int) -> str:
    db = MagicMock()
    make(quorum_includes_primary, db=db).change_replication_to_quorum(HOSTS[:replicas])
    return db.change_replication_type.call_args[0][0]


def installed_quorum(quorum_includes_primary: bool, replicas: int) -> int:
    return int(installed_ssn(quorum_includes_primary, replicas).split(' ')[1].split('(')[0])


def applied_ssn(quorum_includes_primary: bool, quorum_hosts, current_ssn, recorded_quorum) -> list:
    """
    What a regular iteration of the primary applies to PostgreSQL when `quorum_hosts`
    hold the quorum locks, `current_ssn` is installed and ZK records `recorded_quorum`.
    """
    db = MagicMock()
    db.get_replication_state.return_value = ('sync', current_ssn)
    zk = MagicMock()
    zk.get_sync_quorum_hosts.return_value = quorum_hosts
    zk.get.return_value = recorded_quorum
    db_state = {
        'replics_info': [
            {'application_name': app_name_from_fqdn(host), 'state': 'streaming'} for host in quorum_hosts
        ],
    }
    make(quorum_includes_primary, db=db, zk=zk).update_replication_type(db_state, quorum_hosts)
    return [call[0][0] for call in db.change_replication_type.call_args_list]


def promote_safe(
    quorum_hosts,
    alive_hosts,
    primary_ssn,
    streaming_hosts=None,
    primary=PRIMARY,
    quorum_includes_primary=True,
    raises=(),
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
    zk = FakeZk(quorum_hosts, primary, primary_ssn, raises=raises)
    return make(quorum_includes_primary, zk=zk).is_promote_safe(alive_hosts, replica_infos)


# ---------------------------------------------------------------------------
# quorum size the primary installs
# ---------------------------------------------------------------------------

def test_quorum_with_primary_is_a_majority_of_the_cluster():
    assert [installed_quorum(True, replicas) for replicas in range(1, 7)] == [1, 1, 2, 2, 3, 3]


def test_quorum_without_primary_is_a_majority_of_the_replicas():
    assert [installed_quorum(False, replicas) for replicas in range(1, 7)] == [1, 2, 2, 3, 3, 4]


def test_ssn_lists_every_quorum_replica():
    assert installed_ssn(False, 2) == 'ANY 2(pgconsul1_test,pgconsul2_test)'


def test_a_changed_quorum_size_reaches_postgresql_without_a_membership_change():
    # the option is switched while the very same replicas keep holding the quorum locks
    assert applied_ssn(False, HOSTS[:2], ssn(1, HOSTS[:2]), HOSTS[:2]) == [ssn(2, HOSTS[:2])]


def test_an_installed_quorum_is_left_alone():
    assert applied_ssn(False, HOSTS[:2], ssn(2, HOSTS[:2]), HOSTS[:2]) == []


# ---------------------------------------------------------------------------
# promote safety
# ---------------------------------------------------------------------------

def test_promote_needs_a_host_of_every_possible_quorum():
    # with ANY 1 of two either host alone could hold the last commit
    assert promote_safe(HOSTS[:2], HOSTS[:1], ssn(1, HOSTS[:2])) is False
    assert promote_safe(HOSTS[:2], HOSTS[:2], ssn(1, HOSTS[:2])) is True
    # with ANY 2 of two every commit is on both, so one survivor is enough
    assert promote_safe(HOSTS[:2], HOSTS[:1], ssn(2, HOSTS[:2])) is True
    # the same, one host wider
    assert promote_safe(HOSTS[:4], HOSTS[:2], ssn(2, HOSTS[:4])) is False
    assert promote_safe(HOSTS[:4], HOSTS[:3], ssn(2, HOSTS[:4])) is True
    assert promote_safe(HOSTS[:4], HOSTS[:2], ssn(3, HOSTS[:4])) is True


def test_promote_follows_the_primary_not_the_local_option():
    # our own option would allow a single survivor, but the primary required ANY 1
    assert promote_safe(HOSTS[:2], HOSTS[:1], ssn(1, HOSTS[:2]), quorum_includes_primary=False) is False


def test_promote_falls_back_to_the_default_quorum_if_the_primary_ssn_is_unusable():
    # nothing written, the value a standby writes for itself, not a quorum value,
    # and quorum sizes the group cannot have
    for unusable in (None, '', 'None', 'pgconsul1_test', ssn(0, HOSTS[:2]), ssn(3, HOSTS[:2])):
        # the default quorum of two hosts demands both of them
        assert promote_safe(HOSTS[:2], HOSTS[:1], unusable, quorum_includes_primary=False) is False
        assert promote_safe(HOSTS[:2], HOSTS[:2], unusable, quorum_includes_primary=False) is True
    assert promote_safe(HOSTS[:2], HOSTS[:1], ssn(2, HOSTS[:2]), primary=None, quorum_includes_primary=False) is False


def test_promote_falls_back_when_the_primary_ssn_cannot_be_read():
    # a failed ZK read decides the promote, it does not take the daemon down
    for failing in (FakeZk.LAST_PRIMARY_PATH, FakeZk.get_ssn_value_path(PRIMARY)):
        assert promote_safe(
            HOSTS[:2], HOSTS[:1], ssn(2, HOSTS[:2]), raises=[failing], quorum_includes_primary=False
        ) is False


def test_promote_falls_back_when_the_primary_ssn_describes_another_group():
    # the primary took a fourth host into the quorum and died before writing the list
    assert promote_safe(HOSTS[:3], HOSTS[:1], ssn(3, HOSTS[:4])) is False


def test_promote_reads_the_quorum_of_a_spaced_ssn():
    # PostgreSQL keeps whatever spelling it was given, and a hand-set value is one of them
    assert promote_safe(HOSTS[:2], HOSTS[:1], 'ANY 2 (pgconsul1_test, pgconsul2_test)') is True


def test_promote_counts_only_hosts_that_are_both_alive_and_streaming():
    quorum_ssn = ssn(2, HOSTS[:2])
    assert promote_safe(HOSTS[:2], HOSTS[1:2], quorum_ssn) is True
    # the surviving host holds the alive lock, but the one that streams is another
    assert promote_safe(HOSTS[:2], HOSTS[1:2], quorum_ssn, streaming_hosts=HOSTS[:1]) is False


def test_promote_takes_the_election_votes_as_a_host_group():
    votes = {host: (1, 0) for host in HOSTS[:2]}
    assert promote_safe(HOSTS[:2], votes, ssn(1, HOSTS[:2])) is True
    assert promote_safe(HOSTS[:4], votes, ssn(2, HOSTS[:4])) is False


def test_promote_is_unsafe_without_a_quorum():
    assert promote_safe([], [], ssn(1, HOSTS[:2])) is False
    assert promote_safe(None, [], ssn(1, HOSTS[:2])) is False
