# encoding: utf-8
"""
Strategy for managing element removal from lists with optional delay
"""

import logging
import time


class DelayedListRemovalStrategy:
    """
    Strategy with delayed element removal from list.
    
    An element is not removed immediately after disappearing from the active set.
    The manager waits for a configurable time, and only if the element doesn't return,
    it is removed from the list.
    
    When delay is 0, elements are removed immediately.
    """
    
    def __init__(self, delay: float, skip_removal_delay_hosts: set[str]|None=None):
        """
        Args:
            delay: Delay in seconds before removing a replica (0 for immediate removal)
            skip_removal_delay_hosts - delay will be 0 anyway for this hosts
        """
        self._skip_removal_delay_hosts = skip_removal_delay_hosts
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
                f'Host {host} will be removed from list after {time_since_disappeared:.1f}s '
                f'(delay is {self._delay}s)'
            )
        else:
            logging.debug(
                f'Host {host} kept in list (disappeared {time_since_disappeared:.1f}s ago, '
                f'delay is {self._delay}s)'
            )
        
        return should_remove
    
    def on_host_disappeared(self, host: str) -> None:
        """Records the time of the host's first disappearance."""
        if host not in self._removal_timestamps:
            self._removal_timestamps[host] = time.monotonic()
            logging.info(
                f'Host {host} disappeared from active set, starting removal countdown '
                f'(delay: {self._delay}s)'
            )
    
    def on_host_returned(self, host: str) -> None:
        """Clears the timestamp for the returned host."""
        if host in self._removal_timestamps:
            time_was_gone = time.monotonic() - self._removal_timestamps[host]
            logging.info(
                f'Host {host} returned to active set after {time_was_gone:.1f}s, '
                f'cancelling removal'
            )
            del self._removal_timestamps[host]
    
    def get_hosts_to_keep(self, current_quorum: list[str], quorum_hosts: list[str]) -> list[str]:
        """
        Returns the list of hosts to keep in quorum.
        
        This method combines the logic for determining which hosts remain.
        Uses should_remove_host for each host.
        
        Args:
            current_quorum: Current list of hosts in quorum
            quorum_hosts: List of hosts holding quorum locks
            
        Returns:
            List of hosts for the final quorum
        """
        result = set(quorum_hosts)
        
        for host in current_quorum:
            if host not in quorum_hosts:
                # Skip removal delay logic for own host - it cannot disappear from its own perspective
                if self._skip_removal_delay_hosts and host in self._skip_removal_delay_hosts:
                    continue
                    
                self.on_host_disappeared(host)
                if not self.should_remove_host(host, current_quorum, quorum_hosts):
                    result.add(host)
            else:
                self.on_host_returned(host)
        
        return list(result)
