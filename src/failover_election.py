# encoding: utf-8
import logging
import time

from .replication_manager import ReplicationManager

from . import helpers
from .zk import Zookeeper

STATUS_CLEANUP = 'cleanup'
STATUS_FAILED = 'failed'
STATUS_DONE = 'done'
STATUS_SELECTION = 'selection'
STATUS_REGISTRATION = 'registration'


class ElectionError(Exception):
    """Base exception for all exceptions in election logic"""


class StatusChangeError(ElectionError):
    def __str__(self):
        return 'Failed to change election status.'


class NoWinnerError(ElectionError):
    def __str__(self):
        return 'No winner found in election.'


class VoteFailError(ElectionError):
    def __str__(self):
        return 'Failed to vote in election.'


class CleanupError(ElectionError):
    def __str__(self):
        return 'Failed to clean up current votes.'


class ElectionTimeout(ElectionError):
    def __str__(self):
        return 'Election process timed out.'


class FailoverElection(object):
    """
    Contains logic needed for failover election
    """

    def __init__(
        self,
        config,
        _zk: Zookeeper,
        timeout,
        replics_info,
        replication_manager: ReplicationManager,
        allow_data_loss,
        host_priority,
        host_lsn,
        quorum_size,
    ):
        self.config = config
        self._zk = _zk
        self._timeout = timeout
        self._replica_infos = replics_info
        self._replication_manager = replication_manager
        self._allow_data_loss = allow_data_loss
        self._host_priority = host_priority
        self._host_lsn = host_lsn
        self._quorum_size = quorum_size

    def _get_host_vote(self, hostname):
        lsn = self._zk.get(self._zk.get_election_vote_path(hostname) + '/lsn', preproc=int, debug=True)
        if lsn is None:
            logging.error("Failed to get '%s' lsn for elections.", hostname)
            return None
        priority = self._zk.get(self._zk.get_election_vote_path(hostname) + '/prio', preproc=int, debug=True)
        if priority is None:
            logging.error("Failed to get '%s' priority for elections.", hostname)
            return None
        return lsn, priority

    def _collect_votes(self):
        votes = {}
        app_name_map = {helpers.app_name_from_fqdn(host): host for host in self._zk.get_ha_hosts()}
        for info in self._replica_infos:
            app_name = info['application_name']
            replica = app_name_map.get(app_name)
            if not replica:
                continue
            vote = self._get_host_vote(replica)
            if vote is not None:
                votes[replica] = vote
        logging.info('Collected votes are: %s', votes)
        return votes

    @staticmethod
    def _determine_election_winner(votes):
        best_vote = None
        winner = None

        for replica, vote in votes.items():
            if vote is None:
                continue
            if best_vote is None or vote > best_vote:
                best_vote = vote
                winner = replica
        if winner is None:
            raise NoWinnerError
        return winner

    def _vote_in_election(self):
        logging.debug(f"Going to vote in election: lsn {self._host_lsn}, prio {self._host_priority}")
        if not self._zk.ensure_path(self._zk.get_election_vote_path()):
            raise VoteFailError
        if not self._zk.write(self._zk.get_election_vote_path() + '/lsn', self._host_lsn, need_lock=False):
            raise VoteFailError
        if not self._zk.write(self._zk.get_election_vote_path() + '/prio', self._host_priority, need_lock=False):
            raise VoteFailError
        logging.info("Successfully voted")

    def _is_election_valid(self, votes):
        if len(votes) < self._quorum_size:
            logging.error('Not enough votes for quorum.')
            return False
        is_promote_safe = self._replication_manager.is_promote_safe(
            votes,
            replica_infos=self._replica_infos,
        )
        if not self._allow_data_loss and not is_promote_safe:
            logging.error('Sync replica vote is required but was not found.')
            return False
        return True

    def _cleanup_votes(self):
        for replica in self._zk.get_ha_hosts():
            if not self._zk.delete(self._zk.get_election_vote_path(replica), recursive=True):
                raise CleanupError

    def _await_election_status(self, status):
        if not helpers.await_for(
            lambda: self._zk.get(self._zk.ELECTION_STATUS_PATH) == status, self._timeout, f'election status {status}'
        ):
            raise ElectionTimeout

    def _await_lock_holder_fits(self, lock, condition, condition_name):
        return helpers.await_for(
            lambda: condition(self._zk.get_current_lock_holder(lock)), self._timeout, condition_name
        )

    def _write_election_status(self, status):
        logging.debug('Changing election status to: %s', status)
        if not self._zk.write(self._zk.ELECTION_STATUS_PATH, status, need_lock=False):
            raise StatusChangeError

    def _participate_in_election(self):
        """
        Logic for election participant.
        :return: 'True' only if this host became a new leader as a result of election.
        """
        #
        # The order of actions inside this function is very important and was validated to avoid race conditions.
        #
        logging.info('Participate in election')
        self._await_election_status(STATUS_REGISTRATION)
        self._vote_in_election()
        self._await_election_status(STATUS_DONE)
        if self._zk.get(self._zk.ELECTION_WINNER_PATH) == helpers.get_hostname():
            if not self._zk.try_acquire_lock(self._zk.PRIMARY_LOCK_PATH, timeout=self._timeout):
                return False
            if not self._await_lock_holder_fits(
                self._zk.ELECTION_MANAGER_LOCK_PATH,
                lambda holder: holder is None,
                f'lock {self._zk.ELECTION_MANAGER_LOCK_PATH} is empty',
            ):
                raise ElectionTimeout
            if self._zk.get(self._zk.ELECTION_STATUS_PATH) == STATUS_FAILED:
                self._zk.release_lock(self._zk.PRIMARY_LOCK_PATH)
                return False
            return True
        return False

    def _manage_election(self):
        """
        Logic for election manager. Each election is guaranteed to have single manager.
        :return: 'True' only if this host became a new leader as a result of election.
        """
        #
        # The order of actions inside this function is very important and was validated to avoid race conditions.
        #
        logging.info('Manage election')
        self._cleanup_votes()
        self._write_election_status(STATUS_REGISTRATION)
        self._vote_in_election()
        time.sleep(self._timeout / 2.0)
        self._write_election_status(STATUS_SELECTION)
        votes = self._collect_votes()
        if not self._is_election_valid(votes):
            return False
        winner_host = FailoverElection._determine_election_winner(votes)
        logging.info('Elected %s', winner_host)
        if not self._zk.write(self._zk.ELECTION_WINNER_PATH, winner_host, need_lock=False):
            return False
        self._write_election_status(STATUS_DONE)
        if winner_host == helpers.get_hostname():
            return self._zk.try_acquire_lock(self._zk.PRIMARY_LOCK_PATH, timeout=self._timeout)
        if not self._await_lock_holder_fits(
            self._zk.PRIMARY_LOCK_PATH,
            lambda holder: holder is not None,
            f'lock {self._zk.PRIMARY_LOCK_PATH} is not empty',
        ):
            self._write_election_status(STATUS_FAILED)
            raise ElectionTimeout
        return False

    def make_election(self):
        """
        Take part in election as participant or as a manager.
        Returns True if this host is election winner and False otherwise.
        """
        #
        # The order of actions inside this function is very important and was validated to avoid race conditions.
        #
        if not self._zk.try_acquire_lock(self._zk.ELECTION_ENTER_LOCK_PATH, allow_queue=True, timeout=self._timeout):
            return False
        if self._zk.get_current_lock_holder(self._zk.ELECTION_MANAGER_LOCK_PATH):
            self._zk.release_lock(self._zk.ELECTION_ENTER_LOCK_PATH)
            return self._participate_in_election()
        if self._zk.get_current_lock_holder(self._zk.PRIMARY_LOCK_PATH):
            return False
        self._write_election_status(STATUS_CLEANUP)
        if not self._zk.try_acquire_lock(self._zk.ELECTION_MANAGER_LOCK_PATH, timeout=self._timeout):
            return False
        try:
            self._zk.release_lock(self._zk.ELECTION_ENTER_LOCK_PATH)
            is_winner = self._manage_election()
        finally:
            self._zk.release_lock(self._zk.ELECTION_MANAGER_LOCK_PATH)
        return is_winner
