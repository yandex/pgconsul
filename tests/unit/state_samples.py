"""Expected persisted state, independent of model implementation."""

DB_DEFAULTS = {
    'alive': False, 'running': False, 'role': None, 'pgdata': '',
    'opened': False, 'timeline': None, 'wal_receiver': None,
    'replics_info': None, 'replication_state': None,
    'primary_fqdn': None, 'connection_timed_out': False,
}

# Shape emitted by the pre-dataclass pg_stat_replication query (38a5c3e^).
LEGACY_REPLICA_ROW = {
    'pid': 42, 'application_name': 'replica', 'client_addr': '192.0.2.1',
    'client_hostname': None, 'state': 'streaming', 'primary_location': '0/100',
    'sent_location_diff': 0, 'write_location_diff': None, 'replay_location_diff': None,
    'replay_lag_msec': None, 'backend_start_ts': 1000, 'reply_time_ms': None,
    'sync_state': 'sync',
}

NORMALIZED_REPLICA_ROW = {
    'pid': 42, 'application_name': 'replica', 'client_hostname': '',
    'state': 'streaming', 'primary_location': '0/100', 'write_location_diff': None,
    'replay_lag_msec': None, 'reply_time_ms': None, 'sync_state': 'sync', 'priority': 0,
}
