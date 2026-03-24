# encoding: utf-8
"""
Strategies for managing replica removal from quorum
"""

from abc import ABC, abstractmethod
import logging
import time


class QuorumRemovalStrategy(ABC):
    """
    Abstract strategy for managing replica removal from quorum.
    
    Defines when and how replicas should be removed from quorum
    when they lose quorum locks in ZooKeeper.
    """
    
    @abstractmethod
    def should_remove_host(self, host: str, current_quorum: list[str], quorum_hosts: list[str]) -> bool:
        """
        Determines whether a host should be removed from quorum.
        
        Args:
            host: FQDN of the host to check
            current_quorum: Current list of hosts in quorum (from ZK /quorum)
            quorum_hosts: List of hosts currently holding quorum locks
            
        Returns:
            True if the host should be removed from quorum
        """
        raise NotImplementedError
    
    @abstractmethod
    def on_host_disappeared(self, host: str) -> None:
        """
        Called when a host disappears from quorum locks.
        
        Args:
            host: FQDN of the host that disappeared
        """
        raise NotImplementedError
    
    @abstractmethod
    def on_host_returned(self, host: str) -> None:
        """
        Called when a host returns to quorum locks.
        
        Args:
            host: FQDN of the host that returned
        """
        raise NotImplementedError
    
    def get_hosts_to_keep(self, current_quorum: list[str], quorum_hosts: list[str]) -> list[str]:
        """
        Returns the list of hosts to keep in quorum.
        
        This method combines the logic for determining which hosts remain.
        By default, uses should_remove_host for each host.
        
        Args:
            current_quorum: Current list of hosts in quorum
            quorum_hosts: List of hosts holding quorum locks
            
        Returns:
            List of hosts for the final quorum
        """
        result = set(quorum_hosts)
        
        for host in current_quorum:
            if host not in quorum_hosts:
                self.on_host_disappeared(host)
                if not self.should_remove_host(host, current_quorum, quorum_hosts):
                    result.add(host)
            else:
                self.on_host_returned(host)
        
        return list(result)


class ImmediateRemovalStrategy(QuorumRemovalStrategy):
    """
    Strategy for immediate replica removal from quorum.
    
    This is the current pgconsul behavior: as soon as a replica loses its quorum lock,
    it is immediately removed from quorum on the next master iteration.
    """
    
    def should_remove_host(self, host: str, current_quorum: list[str], quorum_hosts: list[str]) -> bool:
        """Always returns True - remove host immediately."""
        return True
    
    def on_host_disappeared(self, host: str) -> None:
        """Do nothing - removal happens immediately."""
        logging.debug(f'Host {host} disappeared from QUORUM locks (immediate removal)')
    
    def on_host_returned(self, host: str) -> None:
        """Do nothing - no state to clean up."""
        pass


class DelayedRemovalStrategy(QuorumRemovalStrategy):
    """
    Strategy with delayed replica removal from quorum.
    
    A replica is not removed immediately after losing its quorum lock.
    The master waits for a configurable time, and only if the replica doesn't return,
    it is removed from quorum.
    """
    
    def __init__(self, delay: float):
        """
        Args:
            delay: Delay in seconds before removing a replica
        """
        self._delay = delay
        self._removal_timestamps: dict[str, float] = {}
    
    def should_remove_host(self, host: str, current_quorum: list[str], quorum_hosts: list[str]) -> bool:
        """
        Returns True if enough time has passed since the host disappeared.
        """
        if host not in self._removal_timestamps:
            return False
        
        time_since_disappeared = time.monotonic() - self._removal_timestamps[host]
        should_remove = time_since_disappeared >= self._delay
        
        if should_remove:
            # Clean up timestamp after removal decision to prevent memory leak
            del self._removal_timestamps[host]
            logging.info(
                f'Host {host} will be removed from QUORUM after {time_since_disappeared:.1f}s '
                f'(delay is {self._delay}s)'
            )
        else:
            logging.debug(
                f'Host {host} kept in QUORUM (disappeared {time_since_disappeared:.1f}s ago, '
                f'delay is {self._delay}s)'
            )
        
        return should_remove
    
    def on_host_disappeared(self, host: str) -> None:
        """Records the time of the host's first disappearance."""
        if host not in self._removal_timestamps:
            self._removal_timestamps[host] = time.monotonic()
            logging.info(
                f'Host {host} disappeared from QUORUM locks, starting removal countdown '
                f'(delay: {self._delay}s)'
            )
    
    def on_host_returned(self, host: str) -> None:
        """Clears the timestamp for the returned host."""
        if host in self._removal_timestamps:
            time_was_gone = time.monotonic() - self._removal_timestamps[host]
            logging.info(
                f'Host {host} returned to QUORUM locks after {time_was_gone:.1f}s, '
                f'cancelling removal'
            )
            del self._removal_timestamps[host]
