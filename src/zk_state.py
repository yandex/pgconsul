from dataclasses import dataclass
import json
from .zk import Zookeeper, ZookeeperException

@dataclass
class MaintenanceState:
    status: str
    ts: float


class ZookeeperState:
    REPLICS_INFO_PATH = 'replics_info'
    TIMELINE_INFO_PATH = 'timeline'
    FAILOVER_INFO_PATH = 'failover_state'
    FAILOVER_MUST_BE_RESET = 'failover_must_be_reset'
    CURRENT_PROMOTING_HOST = 'current_promoting_host'
    LAST_FAILOVER_TIME_PATH = 'last_failover_time'
    LAST_PRIMARY_AVAILABILITY_TIME = 'last_master_activity_time'
    LAST_SWITCHOVER_TIME_PATH = 'last_switchover_time'
    SWITCHOVER_ROOT_PATH = 'switchover'
    # A JSON string with primary fqmdn and its timeline
    SWITCHOVER_PRIMARY_PATH = f'{SWITCHOVER_ROOT_PATH}/master'
    # A simple string with current scheduled switchover state
    SWITCHOVER_STATE_PATH = f'{SWITCHOVER_ROOT_PATH}/state'
    MAINTENANCE_PATH = 'maintenance'
    MAINTENANCE_PATH = 'maintenance'
    MAINTENANCE_TIME_PATH = f'{MAINTENANCE_PATH}/ts'
    MAINTENANCE_PRIMARY_PATH = f'{MAINTENANCE_PATH}/master'
    SINGLE_NODE_PATH = 'is_single_node'

    def __init__(self, zk: Zookeeper):
        self.alive = zk.is_alive()
        if not self.alive:
            raise ZookeeperException("Zookeeper connection is unavailable now")
        self.replics_info = zk.get(self.REPLICS_INFO_PATH, preproc=json.loads)
        self.last_failover_time = zk.get(self.LAST_FAILOVER_TIME_PATH, preproc=float)
        self.failover_state = zk.get(self.FAILOVER_INFO_PATH)
        self.failover_must_be_reset = zk.exists_path(self.FAILOVER_MUST_BE_RESET)
        self.current_promoting_host = zk.get(self.CURRENT_PROMOTING_HOST)
        self.lock_version = zk.get_current_lock_version()
        self.lock_holder = zk.get_current_lock_holder()
        self.single_node = zk.exists_path(self.SINGLE_NODE_PATH)
        self.timeline = zk.get(self.TIMELINE_INFO_PATH, preproc=int)
        self.switchover = zk.get(self.SWITCHOVER_PRIMARY_PATH, preproc=json.loads)
        self.replics_info_written = zk.exists_path(self.REPLICS_INFO_PATH)
        self.maintenance: MaintenanceState | None = None
        if zk.exists_path(self.MAINTENANCE_PATH):
            self.maintenance = MaintenanceState(
                status=zk.get(self.MAINTENANCE_PATH),
                ts=zk.get(self.MAINTENANCE_TIME_PATH, preproc=float),
            )

    def as_dict(self) -> dict:
        return {
            'alive': self.alive,
            'replics_info': self.replics_info,
            'last_failover_time': self.last_failover_time,
            'failover_state': self.failover_state,
            'failover_must_be_reset': self.failover_must_be_reset,
            'current_promoting_host': self.current_promoting_host,
            'lock_version': self.lock_version,
            'primary': self.lock_holder,
            'single_node': self.single_node,
            'timeline': self.timeline,
            'switchover': self.switchover,
            'maintenance': vars(self.maintenance) if self.maintenance else None
        }
