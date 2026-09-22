"""
Logging formatters for pgconsul

Provides functions to format complex state objects for readable logging.
"""

import logging

from .types import DbState, ReplicaInfos, ZkState

_logger = logging.getLogger(__name__)


def format_db_state_for_log(db_state: DbState | None) -> str:
    """Format db_state for readable line-by-line logging"""
    if db_state is None or not db_state._present_fields:
        return 'DB State: (empty)'

    lines = []
    lines.append('DB State:')
    lines.append('  Role: %s' % str(db_state.role if 'role' in db_state._present_fields else 'unknown').upper())
    lines.append('  Timeline: %s' % db_state.timeline)
    lines.append('  PostgreSQL: %s' % ('running' if db_state.running else 'stopped'))
    lines.append('  Bouncer: %s' % ('running' if db_state.opened else 'stopped'))

    replication_state = db_state.replication_state
    if replication_state:
        repl_type = replication_state[0] if len(replication_state) > 0 else 'unknown'
        ssn = replication_state[1] if len(replication_state) > 1 else ''
        lines.append('  Replication: %s' % repl_type)
        if ssn:
            lines.append('  SSN: %s' % ssn)

    replics = db_state.replics_info or []
    replics_str = format_replics_info_for_log(replics)
    for line in replics_str.splitlines():
        lines.append('  ' + line)

    return '\n'.join(lines)


def format_zk_state_for_log(zk_state: ZkState | None) -> str:
    """Format zk_state for readable line-by-line logging."""
    if zk_state is None:
        return 'ZK State: (empty)'

    lines = []
    lines.append('ZK State:')
    
    # Timeline
    timeline = zk_state.timeline
    lines.append('  Timeline: %s' % timeline)

    # Leader lock (real key is 'lock_holder', not 'leader_lock_holder')
    leader = zk_state.lock_holder
    lines.append('  Leader lock: %s' % (leader or 'NONE'))

    # Maintenance
    maintenance = zk_state.maintenance
    if maintenance and maintenance.status is not None:
        lines.append('  Maintenance: %s' % maintenance.status)
        ts = maintenance.ts
        if ts:
            lines.append('  Maintenance timestamp: %s' % ts)

    # Switchover state (real key is 'switchover/state')
    sw_state = zk_state.switchover_state
    if sw_state:
        lines.append('  Switchover state: %s' % sw_state)
        # Switchover candidate (real key is 'switchover/candidate')
        sw_candidate = zk_state.switchover_candidate
        if sw_candidate:
            lines.append('  Switchover candidate: %s' % sw_candidate)
        # Switchover side replicas (real key is 'switchover/side_replicas')
        sw_side_replicas = zk_state.switchover_side_replicas
        if sw_side_replicas:
            lines.append('  Switchover side replicas (%d): %s' % (len(sw_side_replicas), ', '.join(sw_side_replicas)))
        # Switchover primary info
        sw_primary = zk_state.switchover
        if sw_primary:
            hostname = sw_primary.hostname
            sw_timeline = sw_primary.timeline
            if hostname:
                lines.append('  Switchover primary (old): %s' % hostname)
                if sw_timeline:
                    lines.append('  Switchover primary timeline: %s' % sw_timeline)

    # Failover state
    fo_state = zk_state.failover_state
    if fo_state:
        lines.append('  Failover state: %s' % fo_state)
        promoting_host = zk_state.current_promoting_host
        if promoting_host:
            lines.append('  Promoting host: %s' % promoting_host)

    # Last failover time
    last_failover_time = zk_state.last_failover_time
    if last_failover_time:
        lines.append('  Last failover time: %s' % last_failover_time)

    # Last switchover time
    last_switchover_time = zk_state.last_switchover_time
    if last_switchover_time:
        lines.append('  Last switchover time: %s' % last_switchover_time)

    # Single node mode
    single_node = zk_state.single_node
    if single_node:
        lines.append('  Single node: %s' % single_node)

    # Last primary (real key is 'last_leader', not 'last_primary')
    last_primary = zk_state.last_leader
    if last_primary:
        lines.append('  Last primary: %s' % last_primary)

    # Synchronous standby names (dict host -> (value, ts))
    ssn = zk_state.synchronous_standby_names
    if ssn:
        lines.append('  Synchronous standby names:')
        for host, (value, ts) in ssn.items():
            if value:
                lines.append('    %s: %s' % (host, value))
                if ts:
                    lines.append('      (updated: %s)' % ts)

    return '\n'.join(lines)


def format_replics_info_for_log(replics_info: ReplicaInfos) -> str:
    """Format replics_info for readable logging"""
    if not replics_info:
        return 'Replicas: none'

    lines = ['Replicas (%d):' % len(replics_info)]
    for r in replics_info:
        lines.append(
            '  - %s: state=%s, sync=%s, lag=%sms'
            % (
                r.client_hostname if 'client_hostname' in r._present_fields else 'unknown',
                r.state if 'state' in r._present_fields else 'unknown',
                r.sync_state if 'sync_state' in r._present_fields else 'unknown',
                r.replay_lag_msec if 'replay_lag_msec' in r._present_fields else 'N/A',
            )
        )
    return '\n'.join(lines)


def log_separator(level: str = 'info', char: str = '=', length: int = 60) -> None:
    """Log a separator line at the given level"""
    line = char * length
    log_fn = getattr(_logger, level, _logger.info)
    log_fn(line)


def log_event(event: str, detail: str = '', level: str = 'warning', char: str = '=', length: int = 60) -> None:
    """Log a key event with separator lines for easy grep.

    Usage:
        log_event('SWITCHOVER STARTED')
        log_event('FAILOVER: Primary has died', level='error')
        log_event('REWIND: Starting pg_rewind', detail='from replica1.example.com', level='warning')
    """
    log_fn = getattr(_logger, level, _logger.warning)
    separator = char * length
    message = event if not detail else '%s: %s' % (event, detail)
    log_fn(separator)
    log_fn(message)
    log_fn(separator)
