"""
Factory for creating ReplicationManager instances with configuration.
"""
import logging

from configparser import RawConfigParser
from dataclasses import dataclass


@dataclass
class ReplicationManagerConfig:
    priority: int
    primary_unavailability_timeout: float
    change_replication_metric: str
    weekday_change_hours: str
    weekend_change_hours: str
    overload_sessions_ratio: float
    before_async_unavailability_timeout: float
    quorum_removal_delay: float


def build_replication_manager_config(config: RawConfigParser) -> ReplicationManagerConfig:
    """
    Build ReplicationManagerConfig from RawConfigParser with validation.
    
    Args:
        config: RawConfigParser instance with pgconsul configuration
        
    Returns:
        ReplicationManagerConfig instance
    """
    quorum_removal_delay = config.getfloat('primary', 'quorum_removal_delay')
    
    # Validate and adjust quorum_removal_delay
    if quorum_removal_delay < 0:
        logging.warning(
            'quorum_removal_delay is negative (%s), setting to 0 (immediate removal)',
            quorum_removal_delay
        )
        quorum_removal_delay = 0
    elif quorum_removal_delay > 120:
        logging.warning(
            'quorum_removal_delay is set to %s seconds, which is quite large. '
            'This may lead to prolonged unavailability in case of replica failures. '
            'Recommended range: 0-60 seconds. Setting to 120 seconds.',
            quorum_removal_delay
        )
        quorum_removal_delay = 120
    
    return ReplicationManagerConfig(
        priority=config.getint('global', 'priority'),
        primary_unavailability_timeout=config.getfloat('replica', 'primary_unavailability_timeout'),
        change_replication_metric=config.get('primary', 'change_replication_metric'),
        weekday_change_hours=config.get('primary', 'weekday_change_hours'),
        weekend_change_hours=config.get('primary', 'weekend_change_hours'),
        overload_sessions_ratio=config.getfloat('primary', 'overload_sessions_ratio'),
        before_async_unavailability_timeout=config.getfloat('primary', 'before_async_unavailability_timeout'),
        quorum_removal_delay=quorum_removal_delay,
    )


def create_replication_manager(config: RawConfigParser, db, zk):
    """
    Create ReplicationManager instance based on configuration.
    
    Args:
        config: RawConfigParser instance with pgconsul configuration
        db: Postgres instance
        zk: Zookeeper instance
        
    Returns:
        ReplicationManager instance
    """
    # Import here to avoid circular dependencies and allow unit testing
    from .replication_manager import ReplicationManager
    
    replication_config = build_replication_manager_config(config)
    
    return ReplicationManager(
        replication_config,
        db,
        zk,
    )
