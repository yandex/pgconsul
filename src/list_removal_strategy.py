# encoding: utf-8
"""
Strategy for managing removal of unavailable durability members.
"""

import logging
import time


class DelayedListRemovalStrategy:
    """
    Strategy with delayed removal from a membership list.

    An element is not removed immediately after disappearing from the available set.
    The manager waits for a configurable time, and only if the element doesn't return,
    it is removed from the list.
    When delay is 0, elements are removed immediately.
    """
    def __init__(self, delay: float):
        """
        Args:
            delay: Delay in seconds before removing a replica (0 for immediate removal)
        """
        self._delay = delay
        self._removal_timestamps: dict[str, float] = {}

    def should_remove_host(self, host: str) -> bool:
        """Return whether enough time has passed since the host disappeared."""
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
                f'Host {host} disappeared from available set, starting removal countdown '
                f'(delay: {self._delay}s)'
            )

    def on_host_returned(self, host: str) -> None:
        """Clears the timestamp for the returned host."""
        if host in self._removal_timestamps:
            time_was_gone = time.monotonic() - self._removal_timestamps[host]
            logging.info(
                f'Host {host} returned to available set after {time_was_gone:.1f}s, '
                f'cancelling removal'
            )
            del self._removal_timestamps[host]

    def get_hosts_to_keep(self, current_members: list[str], available_hosts: list[str]) -> list[str]:
        """
        Return the desired durability replicas.

        Args:
            current_members: Current durability members excluding the primary
            available_hosts: Members that are currently eligible to remain or join
        Returns:
            List of replicas for the desired durability membership
        """
        result = set(available_hosts)

        for host in current_members:
            if host not in available_hosts:
                self.on_host_disappeared(host)
                if not self.should_remove_host(host):
                    result.add(host)
            else:
                self.on_host_returned(host)
        
        return list(result)
