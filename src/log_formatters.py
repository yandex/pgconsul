"""
Logging formatters for pgconsul

Provides functions to format complex state objects for readable logging.
"""

import logging

_logger = logging.getLogger(__name__)


def format_db_state_for_log(db_state: dict) -> str:
    """Format db_state for readable line-by-line logging"""
    if not db_state:
        return 'DB State: (empty)'

    lines = []
    lines.append('DB State:')
    lines.append('  Role: %s' % str(db_state.get('role', 'unknown')).upper())
    lines.append('  Timeline: %s' % db_state.get('timeline'))
    lines.append('  LSN: %s' % db_state.get('lsn'))
    lines.append('  PostgreSQL: %s' % ('running' if db_state.get('running') else 'stopped'))
    lines.append('  Bouncer: %s' % ('running' if db_state.get('bouncer_running') else 'stopped'))

    replication_state = db_state.get('replication_state')
    if replication_state:
        repl_type = replication_state[0] if len(replication_state) > 0 else 'unknown'
        ssn = replication_state[1] if len(replication_state) > 1 else ''
        lines.append('  Replication: %s' % repl_type)
        if ssn:
            lines.append('  SSN: %s' % ssn)

    archive_command = db_state.get('archive_command')
    if archive_command:
        lines.append('  Archive command: %s' % archive_command)

    replics = db_state.get('replics_info') or []
    replics_str = format_replics_info_for_log(replics)
    for line in replics_str.splitlines():
        lines.append('  ' + line)

    return '\n'.join(lines)


def format_zk_state_for_log(zk_state: dict) -> str:
    """Format zk_state for readable line-by-line logging"""
    if not zk_state:
        return 'ZK State: (empty)'

    lines = []
    lines.append('ZK State:')
    lines.append('  Timeline: %s' % zk_state.get('timeline'))

    leader = zk_state.get('leader_lock_holder')
    lines.append('  Leader lock: %s' % (leader or 'NONE'))

    quorum = zk_state.get('quorum_lock_holders', [])
    if quorum:
        lines.append('  Quorum locks (%d): %s' % (len(quorum), ', '.join(quorum)))
    else:
        lines.append('  Quorum locks: NONE')

    alive = zk_state.get('alive_lock_holders', [])
    if alive:
        lines.append('  Alive locks (%d): %s' % (len(alive), ', '.join(alive)))
    else:
        lines.append('  Alive locks: NONE')

    maintenance = zk_state.get('maintenance')
    if maintenance:
        lines.append('  Maintenance: %s' % maintenance)

    sw_state = zk_state.get('switchover_state')
    if sw_state:
        lines.append('  Switchover state: %s' % sw_state)
        lines.append('  Switchover candidate: %s' % zk_state.get('switchover_candidate'))

    fo_state = zk_state.get('failover_state')
    if fo_state:
        lines.append('  Failover state: %s' % fo_state)
        lines.append('  Promoting host: %s' % zk_state.get('current_promoting_host'))

    return '\n'.join(lines)


def format_replics_info_for_log(replics_info: list) -> str:
    """Format replics_info for readable logging"""
    if not replics_info:
        return 'Replicas: none'

    lines = ['Replicas (%d):' % len(replics_info)]
    for r in replics_info:
        lines.append(
            '  - %s: state=%s, sync=%s, lag=%sms, sent_lsn=%s, replay_lsn=%s'
            % (
                r.get('client_hostname', 'unknown'),
                r.get('state', 'unknown'),
                r.get('sync_state', 'unknown'),
                r.get('replay_lag_msec', 'N/A'),
                r.get('sent_lsn', 'N/A'),
                r.get('replay_lsn', 'N/A'),
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
