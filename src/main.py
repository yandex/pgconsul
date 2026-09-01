"""
Main module. Pgconsul class defined here.
"""
# encoding: utf-8

import atexit
import logging
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass, replace

from configparser import RawConfigParser

from . import helpers, sdnotify
from .debug import DebugFailure, DebugFailureConfig
from .log_formatters import format_db_state_for_log, format_zk_state_for_log, log_event
from .command_executor import CommandExecutor
from .commands import PromotionResult
from .command_manager import CommandManager, create_command_manager
from .helpers import IterationTimer, get_hostname, register_sigterm_handler, should_run
from .exceptions import PostgresConnectionError, PostgresQueryError
from .maintenance import MaintenanceHandler, create_maintenance_handler
from .local_state import LocalStateStore
from .pg import Postgres, create_postgres
from .replication_manager import ReplicationManager, create_replication_manager
from .slot_manager import ReplicationSlotManager, create_replication_slot_manager
from .switchover import (
    CandidateSwitchoverMachine,
    DurabilityPinMode,
    PrimarySwitchoverMachine,
    SwitchoverMachineConfig,
    SwitchoverObservation,
    SwitchoverPhase,
    SwitchoverRecord,
    SwitchoverRoute,
    decide_switchover_route,
)
from .failover import (
    FailoverHealthReport,
    FailoverMachine,
    FailoverMachineConfig,
    FailoverObservation,
    FailoverPhase,
    FailoverProbe,
    FailoverRequest,
)
from .return_to_cluster import (
    ReturnAction,
    ReturnObservation,
    decide_return_action,
)
from .return_to_cluster.timeline_history import parse_timeline_history, wal_filenames_before_switch
from .timings import TimingTracker
from .types import DesiredPrimary, DurabilityConfig, ReplicaInfos, is_transition_allowed
from .zk import Zookeeper, ZookeeperException, create_zk


@dataclass
class PgconsulConfig:
    """All config values consumed by the Pgconsul orchestrator (ADR-0004)."""
    # [global]
    welcome_message: str
    working_dir: str
    iteration_timeout: float
    quorum_commit: bool
    update_prio_in_zk: bool
    use_replication_slots: bool
    replication_slots_polling: bool
    priority: str
    stream_from: str | None
    autofailover: bool
    switchover_timeout: float
    switchover_catchup_timeout: float
    max_rewind_retries: int
    do_consecutive_primary_switch: bool
    max_allowed_switchover_lag_ms: int
    # [replica]
    close_detached_after: float
    start_pooler: bool
    recovery_timeout: float
    can_delayed: bool
    primary_switch_disable_archive_restore: bool
    primary_switch_checks: int
    primary_switch_restart: bool
    primary_unavailability_timeout: float
    walreceiver_disable_timeout: float
    min_failover_timeout: float
    # [primary]
    change_replication_type: bool
    sync_replication_in_maintenance: bool
    # [debug]
    promote_checkpoint_sql: str | None
    failure_name: str | None
    failure_count: int
    sleep_before_disable_walreceiver: float
    election_lsn_read_sleep: float
    election_loser_timeout: int
    # [global]
    local_state_directory: str = '/var/cache/pgconsul'
    use_lwaldump: bool = False
    use_pg_patches: bool = False
    use_target_promote: bool = False


@dataclass(frozen=True)
class ReturnTarget:
    """Versioned primary identity used to abort stale return work."""

    hostname: str
    operation_id: str | None
    timeline: int | None


class Pgconsul:
    """
    pgconsul class
    """

    def __init__(
        self,
        config: PgconsulConfig,
        db: Postgres,
        zk: Zookeeper,
        cmd_manager: CommandManager,
        replication_manager: ReplicationManager,
        slot_manager: ReplicationSlotManager,
        timings: TimingTracker,
        maintenance_handler: MaintenanceHandler,
    ):
        logging.info('Initializing main class.')
        self.config = config
        if config.welcome_message:
            logging.info(config.welcome_message)

        self._cmd_manager = cmd_manager

        random.seed(os.urandom(16))

        self.db = db
        self.zk = zk
        self.startup_checks()

        register_sigterm_handler()

        self.checks = {'primary_switch': 0, 'rewind': 0}
        self._is_single_node: bool | None = False
        self.notifier = sdnotify.Notifier()
        self._debug_counters: dict[str, int] = {}
        self.last_zk_host_stat_write: float = 0
        self._health_primary: str | None = None
        self._health_unreachable_since: float | None = None
        self._health_wal_position: int | None = None
        self._health_wal_unchanged_since: float | None = None
        self._replication_manager = replication_manager
        self._slot_manager = slot_manager
        self._timings = timings
        self._maintenance = maintenance_handler
        promotion_phases = {'creating_slots', 'promoting', 'checkpointing'}
        self._local_states = {
            'switchover_primary': LocalStateStore(
                'switchover_primary_state.json',
                {'sync_set', 'pooler_stopped', 'pg_stopped'},
                directory=config.local_state_directory,
            ),
            'switchover_candidate': LocalStateStore(
                'switchover_candidate_state.json', promotion_phases, directory=config.local_state_directory
            ),
            'failover_participant': LocalStateStore(
                'failover_participant_state.json', promotion_phases, directory=config.local_state_directory
            ),
        }

        # Debug failure injection (step 14e, ADR-0004).
        self._debug_failure = DebugFailure(
            DebugFailureConfig(
                failure_name=config.failure_name,
                failure_count=config.failure_count,
            )
        )

        # Switchover machine config (ADR-0004).
        sw_cfg = SwitchoverMachineConfig(
            catchup_timeout=config.switchover_catchup_timeout,
            max_allowed_lag_ms=config.max_allowed_switchover_lag_ms,
            min_role_transition_timeout=config.min_failover_timeout,
        )

        # Command executor — single imperative shell for cluster-op machines (ADR-0006 §5).
        self._executor = CommandExecutor(
            zk=zk,
            db=db,
            replication_manager=replication_manager,
            timings=timings,
            stop_postgresql=self.stop_postgresql,
            store_replics_info=self._store_replics_info,
            rewind_from_source=self._rewind_from_source,
            promote=self._run_promotion,
            return_to_cluster=self._return_to_cluster,
            set_simple_primary_switch_try=self._set_simple_primary_switch_try,
            create_slots_for_hosts=self._slot_manager.create_slots_for_hosts,
            initialize_failover=self._initialize_failover_from_switchover,
            local_states=self._local_states,
        )

        # Primary-side switchover state machine (ADR-0005 §3, ADR-0006).
        self._sw_machine = PrimarySwitchoverMachine(
            config=sw_cfg,
            debug_failure=self._debug_failure,
        )

        # Candidate-side switchover state machine (ADR-0005 §3, ADR-0006).
        self._cand_machine = CandidateSwitchoverMachine(
            config=sw_cfg,
            debug_failure=self._debug_failure,
        )

        # Failover machine config (ADR-0007, ADR-0004).
        failover_cfg = FailoverMachineConfig(
            min_failover_timeout=config.min_failover_timeout,
            primary_unavailability_timeout=config.primary_unavailability_timeout,
            walreceiver_disable_timeout=config.walreceiver_disable_timeout,
            sleep_before_disable_walreceiver=config.sleep_before_disable_walreceiver,
            election_lsn_read_sleep=config.election_lsn_read_sleep,
        )

        self._failover_machine = FailoverMachine(
            config=failover_cfg,
            debug_failure=self._debug_failure,
        )

    def _build_switchover_observation(
        self,
        sw_record: SwitchoverRecord,
        db_state: dict,
        zk_state: dict,
        *,
        route: SwitchoverRoute,
    ) -> SwitchoverObservation:
        """Build the immutable input for one switchover step."""
        streaming_replicas: tuple[str, ...] = ()
        all_side_replicas_turned: bool = False
        switchover_candidate: str | None = None
        local_phase = None
        if route == SwitchoverRoute.PRIMARY:
            if db_state.get('alive', False):
                streaming_replicas = tuple(self._get_streaming_replicas())
                switchover_candidate = self._get_switchover_candidate(sw_record, db_state)
            else:
                logging.debug(
                    'Skipping PG-dependent reads in switchover observation '
                    '(local PG is dead, phase=%s)', sw_record.phase,
                )
            local_phase_value = self._local_states['switchover_primary'].read(
                sw_record.local_operation_id
            )
            local_phase = SwitchoverPhase(local_phase_value) if local_phase_value is not None else None
        elif route == SwitchoverRoute.CANDIDATE and sw_record.side_replicas:
            all_side_replicas_turned = self._all_side_replicas_turned_to_the_candidate(
                list(sw_record.side_replicas)
            )
        return SwitchoverObservation.build(
            record=sw_record,
            zk=self.zk,
            timings=self._timings,
            my_hostname=helpers.get_hostname(),
            db_state=db_state,
            zk_state=zk_state,
            streaming_replicas=streaming_replicas,
            all_side_replicas_turned=all_side_replicas_turned,
            switchover_candidate=switchover_candidate,
            local_phase=local_phase,
        )

    def handle_switchover(self, db_state: dict, zk_state: dict) -> bool:
        """Run one switchover step and claim every active switchover iteration."""
        record = SwitchoverRecord.from_zk_state(zk_state, self.zk)
        if record.phase is None:
            return False

        if record.protocol_version >= 2:
            hostname = helpers.get_hostname()
            if record.phase == SwitchoverPhase.FAILED:
                return False
            if (
                record.deadline_at is None
                and hostname == record.hostname
                and db_state.get('role') == 'primary'
                and zk_state.get('lock_holder') == hostname
            ):
                if not self._try_acquire_switchover_manager():
                    return True
                started_at = record.started_at if record.started_at is not None else time.time()
                self._write_bridge_record(
                    record,
                    started_at=started_at,
                    deadline_at=started_at + self.config.switchover_timeout,
                )
                return True
            if (
                db_state.get('role') == 'primary'
                and zk_state.get('lock_holder') == hostname
                and not self._replication_manager.resume_durability_transition()
            ):
                logging.info('Waiting for the persisted durability transition before bridge switchover')
                return True
            return self._handle_bridge_switchover(record, db_state, zk_state)

        route = decide_switchover_route(
            record,
            helpers.get_hostname(),
            db_state.get('role'),
            zk_state.get('lock_holder'),
        )
        machine: PrimarySwitchoverMachine | CandidateSwitchoverMachine
        match route:
            case SwitchoverRoute.GLOBAL | SwitchoverRoute.PRIMARY:
                machine = self._sw_machine
            case SwitchoverRoute.CANDIDATE:
                machine = self._cand_machine
            case SwitchoverRoute.REPLICA:
                self._handle_switchover_replica(record, db_state)
                return True
            case SwitchoverRoute.WAIT:
                logging.debug('Switchover in progress (phase %s), waiting', record.phase)
                return True

        observation = self._build_switchover_observation(
            record,
            db_state,
            zk_state,
            route=route,
        )
        self._executor.set_iteration_state(db_state, zk_state)
        self._executor.run(machine, observation)
        return True

    def _handle_bridge_switchover(
        self,
        record: SwitchoverRecord,
        db_state: dict,
        zk_state: dict,
    ) -> bool:
        """Run the manager-owned switchover protocol from ADR-0014.

        The old primary manages preparation; after it releases the leader lock,
        the candidate manages promotion and expansion.  Other replicas only
        publish acknowledgements and never write the global record.
        """
        hostname = helpers.get_hostname()
        holder = zk_state.get('lock_holder')

        desired, _ = self.zk.get_desired_primary()
        early_candidate_lock = bool(
            record.phase == SwitchoverPhase.TURNING_SIDES
            and record.operation_id is not None
            and record.selected_candidate is not None
            and desired is not None
            and desired.operation_id == record.operation_id
            and desired.hostname == record.selected_candidate
        )

        if record.phase == SwitchoverPhase.FAILED:
            if (
                record.version is not None
                and self._try_acquire_switchover_manager()
                and self.zk.cleanup_switchover(record.version)
            ):
                self.zk.release_if_hold(self.zk.SWITCHOVER_MANAGER_LOCK_PATH)
            # With no owner, ordinary failover probes P. A live P will reject
            # the probe; a dead P is recovered by the normal failover path.
            return holder is not None

        if (
            record.deadline_at is not None
            and time.time() >= record.deadline_at
            and not self._bridge_promotion_succeeded(record)
        ):
            timeout_result = self._handle_bridge_timeout(
                record, db_state, zk_state,
            )
            if timeout_result is not None:
                return timeout_result

        if (
            holder is None
            and not self._is_committed_bridge_handoff(record, zk_state)
            and not early_candidate_lock
        ):
            return self._recover_pre_handoff_switchover(record, db_state, zk_state)

        if (
            record.phase == SwitchoverPhase.FALLBACK
            or (
                not self._is_committed_bridge_handoff(record, zk_state)
                and holder is not None
                and holder != record.hostname
                and not (
                    early_candidate_lock
                    and holder == record.selected_candidate
                )
            )
        ):
            if not self._try_acquire_switchover_manager():
                return True
            if record.version is not None and self.zk.cleanup_switchover(record.version):
                self.zk.release_if_hold(self.zk.SWITCHOVER_MANAGER_LOCK_PATH)
            return True

        if hostname == record.hostname:
            return self._run_bridge_primary(record, db_state, holder)
        if hostname == record.selected_candidate:
            if (
                record.phase == SwitchoverPhase.HANDOFF_COMMITTED
                and not self._is_committed_bridge_handoff(record, zk_state)
                and holder is None
            ):
                # P disappeared before publishing the new branch fence.  C
                # was not allowed to promote, so ordinary old-timeline
                # failover remains safe.
                return False
            return self._run_bridge_candidate(
                record, db_state, holder, zk_state.get(self.zk.TIMELINE_INFO_PATH),
            )
        if record.handoff_is_committed() and holder is None:
            # C may already have died after the irreversible handoff.  Let the
            # ordinary failover machinery stop receivers and elect only the
            # new timeline; side replicas must not keep it blocked here.
            return False
        self._run_bridge_side_replica(record, db_state)
        return True

    def _bridge_promotion_succeeded(self, record: SwitchoverRecord) -> bool:
        """Promotion succeeds only after C publishes the expected timeline."""
        if record.phase == SwitchoverPhase.WAITING_ARCHIVE:
            return True
        candidate = record.selected_candidate
        if candidate is None or record.expected_timeline is None:
            return False
        ack = self._bridge_ack(record, candidate)
        return bool(
            ack
            and ack.get('promoted_timeline') == record.expected_timeline
        )

    def _handle_bridge_timeout(
        self,
        record: SwitchoverRecord,
        db_state: dict,
        zk_state: dict,
    ) -> bool | None:
        """Fail before handoff; after handoff request fenced failover."""
        if record.failure_reason is None:
            if not self._try_acquire_switchover_manager():
                return True
            # The ACK may have appeared while the manager lock was acquired.
            if self._bridge_promotion_succeeded(record):
                return None
            if not record.handoff_is_committed():
                if record.hostname is None or record.operation_id is None:
                    return True
                if (
                    record.use_pg_patches
                    and helpers.get_hostname() == record.hostname
                    and db_state.get('role') == 'primary'
                    and not self._replication_manager.set_ssn_before_promote(
                        DurabilityConfig.build(
                            record.original_durability_members,
                        )
                    )
                ):
                    return True
                desired, _ = self.zk.get_desired_primary()
                expected = desired.hostname if desired is not None else record.hostname
                if not self._set_desired_primary(
                    record.hostname,
                    record.operation_id,
                    'switchover',
                    expected_hostname=expected,
                ):
                    return True
                updated = self._write_bridge_record(
                    record,
                    phase=SwitchoverPhase.FAILED,
                    failure_reason='timeout',
                    durability_pin_mode=None,
                    durability_pin_owner=None,
                )
                if updated is not None:
                    self.zk.release_if_hold(
                        self.zk.SWITCHOVER_MANAGER_LOCK_PATH,
                    )
                return True
            updated = self._write_bridge_record(
                record, failure_reason='timeout',
            )
            if updated is None:
                return True
            record = updated

        if not record.handoff_is_committed():
            return True
        if record.expected_timeline is None:
            logging.error('Timed-out committed handoff has no expected timeline')
            return True
        if zk_state.get('lock_holder') is not None:
            # P still has to release the lock, or C is retrying promotion.
            return None
        if FailoverPhase.from_str(
            zk_state.get(self.zk.FAILOVER_STATE_PATH)
        ) is not None:
            return True
        timed_out_state = {
            **zk_state,
            self.zk.SWITCHOVER_RECORD_PATH: record.to_dict(),
            self.zk.SWITCHOVER_VERSION_KEY: record.version,
        }
        self._initialize_failover_from_switchover(db_state, timed_out_state)
        return True

    def _read_bridge_record(self) -> SwitchoverRecord:
        value, version = self.zk.get_switchover_record()
        return SwitchoverRecord.from_zk_state({
            self.zk.SWITCHOVER_RECORD_PATH: value,
            self.zk.SWITCHOVER_VERSION_KEY: version,
        }, self.zk)

    def _recover_pre_handoff_switchover(
        self,
        record: SwitchoverRecord,
        db_state: dict,
        zk_state: dict,
    ) -> bool:
        """Resume P after restart or initialize failover before FALLBACK."""
        if not self._try_acquire_switchover_manager():
            return True

        current = self._read_bridge_record()
        holder = self.zk.get_current_lock_holder(self.zk.PRIMARY_LOCK_PATH)
        timeline = self.zk.get_timeline()
        if (
            current.operation_id != record.operation_id
            or current.protocol_version < 2
            or holder is not None
            or (
                current.handoff_is_committed()
                and current.expected_timeline is not None
                and timeline == current.expected_timeline
            )
        ):
            return True

        hostname = helpers.get_hostname()
        if current.hostname is None:
            logging.error('Bridge switchover has no old primary for fallback')
            return True
        if current.phase != SwitchoverPhase.FALLBACK:
            if hostname == current.hostname:
                if db_state.get('role') == 'primary':
                    if current.operation_id is None:
                        logging.error('Bridge switchover has no operation ID for fallback')
                        return True
                    self._set_desired_primary(
                        hostname,
                        current.operation_id,
                        'switchover',
                        expected_hostname=hostname,
                    )
                    return True
            elif self.zk.is_host_alive(current.hostname, timeout=1):
                # Let a restarted P inspect its local PostgreSQL state.  Only
                # P can distinguish a live primary from a dead local server.
                self.zk.release_if_hold(self.zk.SWITCHOVER_MANAGER_LOCK_PATH)
                return True

        if FailoverPhase.from_str(zk_state.get(self.zk.FAILOVER_STATE_PATH)) is None:
            fallback_db_state = {**db_state, 'primary_fqdn': current.hostname}
            if not self._initialize_failover_from_switchover(fallback_db_state, zk_state):
                return True
        if FailoverPhase.from_str(zk_state.get(self.zk.FAILOVER_STATE_PATH)) is None:
            return True

        if current.phase != SwitchoverPhase.FALLBACK:
            self._write_bridge_record(current, phase=SwitchoverPhase.FALLBACK)
        return True

    def _try_acquire_switchover_manager(self) -> bool:
        if self.zk.is_lock_holder(self.zk.SWITCHOVER_MANAGER_LOCK_PATH):
            return True
        return self.zk.try_acquire_lock(
            self.zk.SWITCHOVER_MANAGER_LOCK_PATH,
            allow_queue=False,
            timeout=0,
        )

    def _write_bridge_record(self, record: SwitchoverRecord, **changes) -> SwitchoverRecord | None:
        """CAS-write a manager-owned record and return its new version."""
        if not self.zk.is_lock_holder(self.zk.SWITCHOVER_MANAGER_LOCK_PATH):
            logging.warning('Refusing to update bridge switchover without manager lock')
            return None
        updated = replace(record, **changes)
        version = self.zk.write_switchover_record(updated.to_dict(), record.version)
        if version is None:
            logging.info('Bridge switchover record changed concurrently; retrying')
            return None
        return replace(updated, version=version)

    @staticmethod
    def _bridge_required_side_replicas(durability: DurabilityConfig) -> int:
        """Replicas needed to preserve the pre-handoff commit availability."""
        return durability.required

    def _bridge_ack(self, record: SwitchoverRecord, hostname: str) -> dict | None:
        if record.operation_id is None:
            return None
        return self.zk.get_switchover_ack(hostname, record.operation_id)

    def _bridge_write_ack(self, record: SwitchoverRecord, **state) -> bool:
        if record.operation_id is None:
            return False
        return self.zk.write_switchover_ack(helpers.get_hostname(), record.operation_id, state)

    def _is_committed_bridge_handoff(self, record: SwitchoverRecord, zk_state: dict) -> bool:
        """Whether failover must fence and elect the new switchover branch."""
        return record.handoff_is_committed() and record.expected_timeline is not None

    def _run_bridge_primary(
        self,
        record: SwitchoverRecord,
        db_state: dict,
        holder: str | None,
    ) -> bool:
        """Old-primary half: contract, turn sides, bridge, then release."""
        hostname = helpers.get_hostname()
        if record.phase == SwitchoverPhase.HANDOFF_COMMITTED:
            # This is the durable point of no automatic return to old primary.
            # Do not wait for either shutdown or candidate promotion here.
            if db_state.get('role') == 'primary':
                if record.expected_timeline is None:
                    logging.error('Committed bridge handoff has no expected timeline')
                    return True
                if record.selected_candidate is None or record.operation_id is None:
                    return True
            if self._try_acquire_switchover_manager():
                self._bridge_confirm_promotion(record)
            return True

        if db_state.get('role') != 'primary':
            logging.warning('Bridge switchover old primary is not running as primary')
            return True
        if record.phase in (
            SwitchoverPhase.SCHEDULED,
            SwitchoverPhase.PREPARING_DURABILITY,
        ) and holder != hostname:
            logging.warning('Bridge switchover old primary no longer owns leader lock')
            return True
        if not self._try_acquire_switchover_manager():
            return True

        if record.phase == SwitchoverPhase.SCHEDULED:
            return self._bridge_begin(record, db_state)

        candidate = record.selected_candidate
        if candidate is None:
            logging.error('Bridge switchover has no candidate')
            return True
        target_pair = DurabilityConfig.build([hostname, candidate])

        if record.phase == SwitchoverPhase.PREPARING_DURABILITY:
            if record.use_pg_patches:
                original = DurabilityConfig.build(
                    record.original_durability_members,
                )
                if not self._replication_manager.set_mandatory_sync_replica(
                    original, candidate,
                ):
                    return True
            elif not self._replication_manager.change_replication_to_durability_config(target_pair):
                return True
            if record.operation_id is None:
                logging.error('Bridge switchover has no operation ID for WAL barrier')
                return True
            if not self.db.advance_wal_barrier(f'switchover:{record.operation_id}'):
                return True
            self._write_bridge_record(
                record,
                phase=SwitchoverPhase.PREPARING_CANDIDATE,
            )
            return True

        if record.phase == SwitchoverPhase.PREPARING_CANDIDATE:
            candidate_ack = self._bridge_ack(record, candidate)
            desired_durability = DurabilityConfig.build(
                record.original_durability_members,
            )
            if (
                not candidate_ack
                or candidate_ack.get('prepared_ssn') != desired_durability.to_dict()
                or candidate_ack.get('checkpointed') is not True
            ):
                return True
            expected_timeline = candidate_ack.get('expected_timeline')
            if not isinstance(expected_timeline, int):
                return True
            if record.expected_timeline is None:
                self._write_bridge_record(
                    record, expected_timeline=expected_timeline,
                )
                return True
            if record.expected_timeline != expected_timeline:
                logging.error('Candidate changed its planned timeline')
                return True
            self._write_bridge_record(
                record,
                phase=SwitchoverPhase.TURNING_SIDES,
                side_wait_started_at=time.time(),
            )
            return True

        if record.phase == SwitchoverPhase.TURNING_SIDES:
            if record.handoff_lsn is not None:
                # Legacy records entered TURNING_SIDES before their LSN
                # barrier completed. Replace it with the table barrier once.
                if record.operation_id is None:
                    return True
                if not self.db.advance_wal_barrier(f'switchover:{record.operation_id}'):
                    return True
                self._write_bridge_record(record, handoff_lsn=None)
                return True
            candidate_ack = self._bridge_ack(record, candidate)
            desired_durability = DurabilityConfig.build(
                record.original_durability_members,
            )
            if (
                not candidate_ack
                or candidate_ack.get('prepared_ssn') != desired_durability.to_dict()
                or candidate_ack.get('checkpointed') is not True
            ):
                return True
            expected_timeline = candidate_ack.get('expected_timeline')
            if not isinstance(expected_timeline, int):
                return True
            if record.expected_timeline is None:
                self._write_bridge_record(
                    record,
                    expected_timeline=expected_timeline,
                )
                return True
            if record.expected_timeline != expected_timeline:
                logging.error('Candidate changed its planned timeline')
                return True
            if record.operation_id is None:
                return True
            desired, _ = self.zk.get_desired_primary()
            if desired is None or desired.operation_id != record.operation_id or desired.hostname != candidate:
                if not self._set_desired_primary(
                    candidate,
                    record.operation_id,
                    'switchover',
                    expected_hostname=hostname,
                ):
                    return True
                return True
            if holder != candidate:
                logging.info('Waiting for candidate to acquire the leader lock before handoff')
                return True
            if not self._bridge_sides_ready(record):
                return True
            self.db.stop_pooler_async()
            if self._timings.get_start('downtime') is None:
                self._timings.start('downtime')
            self.stop_postgresql(wait=False)
            self._write_bridge_record(
                record,
                phase=SwitchoverPhase.HANDOFF_COMMITTED,
            )
            return True

        return True

    def _bridge_begin(self, record: SwitchoverRecord, db_state: dict) -> bool:
        """Freeze the initial stable durability set and select its candidate."""
        hostname = helpers.get_hostname()
        durability_state, _ = self.zk.get_durability_state()
        durability = durability_state.stable
        if durability is None or durability_state.transition is not None:
            logging.info('Waiting for a stable durability set before bridge switchover')
            return True
        if hostname not in durability.members:
            logging.error('Bridge switchover old primary is absent from stable durability members')
            return True
        candidate = self._get_switchover_candidate(record, db_state)
        if candidate is None or candidate not in durability.members:
            logging.error('Bridge switchover candidate must belong to stable durability members')
            return True
        if candidate == hostname:
            logging.error('Bridge switchover candidate equals old primary')
            return True
        sides = [host for host in durability.members if host not in (hostname, candidate)]
        operation_id = record.operation_id or uuid.uuid4().hex
        expected_timeline = None
        if self.config.use_target_promote:
            source_timeline = record.timeline or db_state.get('timeline')
            if not isinstance(source_timeline, int):
                logging.error('Cannot initialize timeline high-water mark')
                return True
            observed_highest = self.db.next_local_timeline(source_timeline) - 1
            expected_timeline = self.zk.reserve_timeline(
                operation_id, observed_highest,
            )
            if expected_timeline is None:
                return True
        if self._timings.get_start('switchover') is None:
            self._timings.start('switchover')
        self._write_bridge_record(
            record,
            phase=SwitchoverPhase.PREPARING_DURABILITY,
            candidate=candidate,
            side_replicas=sides,
            operation_id=operation_id,
            durability_pin_mode=(
                DurabilityPinMode.MANDATORY
                if self.config.use_pg_patches
                else DurabilityPinMode.CONTRACTING
            ),
            durability_pin_owner=hostname,
            original_durability_members=list(durability.members),
            required_side_replicas=self._bridge_required_side_replicas(durability),
            expected_timeline=expected_timeline,
            use_pg_patches=self.config.use_pg_patches,
            use_target_promote=self.config.use_target_promote,
        )
        return True

    def _bridge_sides_ready(self, record: SwitchoverRecord) -> bool:
        candidate = record.selected_candidate
        candidate_ack = self._bridge_ack(record, candidate) if candidate is not None else None
        if not candidate_ack or candidate_ack.get('slots_ready') is not True:
            return False
        streaming = candidate_ack.get('streaming_side_flush_lsns') or {}
        ready = [
            host for host in record.side_replicas
            if (ack := self._bridge_ack(record, host))
            and ack.get('source') == record.selected_candidate
            and ack.get('restore_disabled') is True
            and host in streaming
        ]
        required = record.required_side_replicas or 0
        if not record.side_replicas:
            return True
        logging.info('Bridge side replicas ready: %s, required: %s', sorted(ready), required)
        if (
            not ready
            and record.side_wait_started_at is not None
            and time.time() - record.side_wait_started_at >= self.config.switchover_catchup_timeout
        ):
            logging.warning('No live bridge replica appeared before the catch-up timeout')
            return True
        return len(ready) >= required

    def _bridge_streaming_side_flush_lsns(self, record: SwitchoverRecord) -> dict[str, int]:
        flush_lsns = self.db.get_replica_flush_lsns()
        return {
            host: flush_lsns[helpers.app_name_from_fqdn(host)]
            for host in record.side_replicas
            if helpers.app_name_from_fqdn(host) in flush_lsns
        }

    def _bridge_confirm_promotion(self, record: SwitchoverRecord) -> bool:
        """Advance only after the candidate reports the committed timeline."""
        candidate = record.selected_candidate
        candidate_ack = self._bridge_ack(record, candidate) if candidate is not None else None
        promoted_timeline = candidate_ack.get('promoted_timeline') if candidate_ack else None
        if promoted_timeline != record.expected_timeline:
            return False
        updated = self._write_bridge_record(
            record,
            phase=SwitchoverPhase.WAITING_ARCHIVE,
            durability_pin_mode=DurabilityPinMode.EXPANDING,
            durability_pin_owner=candidate,
            promoted_timeline=promoted_timeline,
        )
        if updated is not None:
            if self.zk.get_failover_state() is not None:
                self.zk.write_failover_state(FailoverPhase.FINISHED)
            # The post-promote candidate is allowed to become manager, but this
            # handover is outside the availability-critical promotion path.
            self.zk.release_if_hold(self.zk.SWITCHOVER_MANAGER_LOCK_PATH)
        return updated is not None

    def _run_bridge_candidate(
        self,
        record: SwitchoverRecord,
        db_state: dict,
        holder: str | None,
        zk_timeline: int | None = None,
    ) -> bool:
        """Candidate half: prepare SSN, promote, then expand only."""
        hostname = helpers.get_hostname()
        if record.phase in (
            SwitchoverPhase.PREPARING_CANDIDATE,
            SwitchoverPhase.TURNING_SIDES,
        ):
            if not self._slot_manager.create_slots_for_hosts(list(record.side_replicas)):
                return True
            durability = DurabilityConfig.build(record.original_durability_members)
            ack = self._bridge_ack(record, hostname)
            if ack and ack.get('prepared_ssn') == durability.to_dict() and ack.get('checkpointed') is True:
                self._bridge_write_ack(
                    record,
                    prepared_ssn=durability.to_dict(),
                    slots_ready=True,
                    checkpointed=True,
                    expected_timeline=ack.get('expected_timeline'),
                    streaming_side_flush_lsns=self._bridge_streaming_side_flush_lsns(record),
                )
                return True
            if record.use_target_promote:
                expected_timeline = record.expected_timeline
                if expected_timeline is None:
                    return True
            else:
                if not self.db.stop_restoring_wal():
                    return True
                if record.timeline is None:
                    return True
                expected_timeline = self.db.next_local_timeline(record.timeline)
            if self._replication_manager.set_ssn_before_promote(durability) and self.db.checkpoint():
                self._bridge_write_ack(
                    record,
                    prepared_ssn=durability.to_dict(),
                    slots_ready=True,
                    checkpointed=True,
                    expected_timeline=expected_timeline,
                    streaming_side_flush_lsns=self._bridge_streaming_side_flush_lsns(record),
                )
            return True

        if record.phase == SwitchoverPhase.HANDOFF_COMMITTED:
            if holder is not None and holder != hostname:
                return True
            if record.operation_id is None:
                return True
            desired_owner, _ = self.zk.get_desired_primary()
            if (
                desired_owner is None
                or desired_owner.operation_id != record.operation_id
                or desired_owner.hostname != hostname
            ):
                logging.warning('Waiting for desired primary to be committed to candidate')
                return True
            if db_state.get('role') == 'primary' and holder == hostname:
                if self._try_acquire_switchover_manager():
                    self._bridge_confirm_promotion(record)
                return True
            if not self.zk.is_lock_holder(self.zk.PRIMARY_LOCK_PATH):
                # The common desired-primary reconciliation owns lock acquisition.
                return True
            if record.use_target_promote:
                result = self._run_promotion(
                    'switchover_candidate',
                    operation_id=record.local_operation_id,
                    old_primary=record.hostname,
                    prepared=True,
                    target_timeline=record.expected_timeline,
                )
            else:
                result = self._run_promotion(
                    'switchover_candidate',
                    operation_id=record.local_operation_id,
                    old_primary=record.hostname,
                    prepared=True,
                )
            if result != PromotionResult.SUCCESS:
                return True
            self.start_pooler()
            self._bridge_write_ack(record, promoted_timeline=record.expected_timeline)
            return True

        if record.phase == SwitchoverPhase.WAITING_ARCHIVE:
            if holder != hostname or db_state.get('role') != 'primary':
                return True
            if not self._try_acquire_switchover_manager():
                return True
            ack = self._bridge_ack(record, hostname)
            if not ack or ack.get('post_promote_checkpointed') is not True:
                # pg_rewind needs the promoted timeline persisted in C's control file.
                if not self.db.checkpoint(query=self.config.promote_checkpoint_sql):
                    return True
                if not self._bridge_write_ack(record, post_promote_checkpointed=True):
                    return True
            desired = self._bridge_expansion_durability(record, db_state)
            if not self._replication_manager.change_replication_to_durability_config(desired):
                return True
            if not self._bridge_archive_ready(record):
                return True
            if record.failure_reason is not None:
                completed = self._write_bridge_record(
                    record,
                    phase=SwitchoverPhase.FAILED,
                    durability_pin_mode=None,
                    durability_pin_owner=None,
                ) is not None
            else:
                completed = bool(
                    record.version is not None
                    and self.zk.cleanup_switchover(record.version)
                )
            if completed:
                self._timings.stop('switchover')
                self.zk.release_if_hold(self.zk.SWITCHOVER_MANAGER_LOCK_PATH)
            return True

        return True

    def _bridge_expansion_durability(
        self,
        record: SwitchoverRecord,
        db_state: dict,
    ) -> DurabilityConfig:
        """Expand only with side replicas that still stream from candidate."""
        candidate = record.selected_candidate
        members = [
            host for host in (record.hostname, candidate)
            if host is not None
        ]
        streaming = {
            replica.get('application_name')
            for replica in db_state.get('replics_info') or []
            if replica.get('state') == 'streaming'
        }
        for host in record.side_replicas:
            ack = self._bridge_ack(record, host)
            if (
                ack
                and ack.get('source') == candidate
                and ack.get('restore_disabled') is True
                and helpers.app_name_from_fqdn(host) in streaming
            ):
                members.append(host)
        return DurabilityConfig.build(members)

    def _run_bridge_side_replica(self, record: SwitchoverRecord, db_state: dict) -> None:
        """Turn a side replica once and pin it to the candidate until cleanup."""
        hostname = helpers.get_hostname()
        if hostname not in record.side_replicas:
            return
        if record.phase not in (
            SwitchoverPhase.TURNING_SIDES,
            SwitchoverPhase.HANDOFF_COMMITTED,
            SwitchoverPhase.WAITING_ARCHIVE,
        ):
            return
        candidate = record.selected_candidate
        if candidate is None:
            return
        if not self.db.stop_restoring_wal():
            return
        if db_state.get('primary_fqdn') != candidate:
            self._return_to_cluster(candidate, 'replica', is_dead=False)
            return
        ack_state: dict[str, object] = {
            'source': candidate,
            'restore_disabled': True,
        }
        if record.phase == SwitchoverPhase.WAITING_ARCHIVE:
            # WAITING_ARCHIVE is written only after C acknowledged promotion.
            # Persist the new timeline in the standby control file. If the
            # replica is stopped immediately after switching to the new
            # timeline, pg_controldata may otherwise report the old checkpoint
            # timeline and trigger an unnecessary rewind. Failure is harmless
            # and cannot block switchover.
            ack = self._bridge_ack(record, hostname)
            attempted = bool(
                ack and ack.get('post_promote_checkpoint_attempted') is True
            )
            if attempted:
                ack_state['post_promote_checkpoint_attempted'] = True
                ack_state['post_promote_checkpointed'] = bool(
                    ack and ack.get('post_promote_checkpointed') is True
                )
            else:
                checkpointed = False
                try:
                    checkpointed = bool(self.db.checkpoint())
                except (PostgresConnectionError, PostgresQueryError):
                    logging.warning(
                        'Best-effort post-switchover replica checkpoint failed',
                        exc_info=True,
                    )
                ack_state['post_promote_checkpoint_attempted'] = True
                ack_state['post_promote_checkpointed'] = checkpointed
        self._bridge_write_ack(record, **ack_state)

    def _bridge_archive_ready(self, record: SwitchoverRecord) -> bool:
        """Require history and the last old-timeline segment before returns resume."""
        if record.promoted_timeline is None:
            return False
        history = self.db.fetch_timeline_history(record.promoted_timeline)
        if history is None:
            return False
        try:
            switches = parse_timeline_history(history, record.promoted_timeline)
            segment_size = self.db.get_wal_segment_size()
            if not switches or segment_size is None:
                return False
            filenames = wal_filenames_before_switch(switches[-1], segment_size)
        except (TypeError, ValueError):
            logging.warning('Could not parse promoted timeline history', exc_info=True)
            return False
        return any(self.db.is_wal_archived(filename) for filename in filenames)

    def _handle_switchover_replica(
        self,
        record: SwitchoverRecord,
        db_state: dict,
    ) -> None:
        candidate = record.selected_candidate
        if not record.can_follow_candidate() or candidate is None:
            logging.debug('Switchover in progress (phase %s), waiting for candidate', record.phase)
            return

        current_source = db_state.get('primary_fqdn')
        if current_source == candidate:
            logging.debug('Already streaming from switchover candidate %s', candidate)
            return
        if current_source != record.hostname:
            logging.debug(
                'Not streaming from switchover primary %s, waiting', record.hostname,
            )
            return

        if self.config.primary_switch_disable_archive_restore:
            self.db.stop_restoring_wal()
        self._return_to_cluster(candidate, 'replica', is_dead=False)

    def re_init_db(self):
        """Reinit db connection. Exits only if cache is corrupt (incomplete)."""
        try:
            self.db.re_init()
        except KeyError:
            logging.exception('Could not get data from PostgreSQL and cache-file. Exiting.')
            sys.exit(1)

    def _rewind_flag_path(self):
        return os.path.join(self.config.working_dir, '.pgconsul_rewind_fail.flag')

    def is_rewind_flag_set(self):
        return os.path.exists(self._rewind_flag_path())

    def set_rewind_flag(self):
        with open(self._rewind_flag_path(), 'w') as fobj:
            fobj.write(str(time.time()))

    def startup_checks(self):
        """
        Perform some basic checks on startup
        """
        logging.info('Running startup checks')

        prev_state = self.db.get_prev_state()
        if prev_state:
            # Ok, it means that current start is not the first one.
            # In this case we should check that we are able to do pg_rewind.
            if not self.db.is_alive():
                self.db.pgdata = prev_state['pgdata']
            if not self.db.is_ready_for_pg_rewind():
                sys.exit(1)

        # An existing cluster must not bootstrap from an empty membership.
        tli = self.db.get_timeline()
        if not self.zk.get_members_retry(self.config.iteration_timeout) and (
            prev_state or (tli is not None and tli > 1)
        ):
            logging.error(
                'ZK "%s" empty for an existing cluster (timeline=%s)',
                self.zk.MEMBERS_PATH,
                tli,
            )
            self.db.pgpooler('stop')
            sys.exit(1)

        if (
            self.config.quorum_commit
            and not self.config.use_lwaldump
        ):
            logging.error(
                'Safe quorum failover requires use_lwaldump=yes'
            )
            sys.exit(1)

        if (
            self.db.is_alive()
            and self.config.use_lwaldump
            and not self.db.check_extension_installed('lwaldump')
        ):
            logging.error('lwaldump is not installed')
            sys.exit(1)

        if self.db.is_alive() and not self.db.ensure_archive_mode():
            logging.error("archive mode is not enabled on instance - pgconsul support only archive mode yet ")
            sys.exit(1)

        logging.info('Startup checks passed')

    # pylint: disable=W0212
    def stop(self, *_):
        """
        Stop iterations
        """
        logging.info('Stopping')
        atexit._run_exitfuncs()
        os._exit(0)

    def _init_zk(self, my_prio):
        if not self._replication_manager.init_zk():
            return False

        members = self.zk.get_members() or []
        if self.config.update_prio_in_zk or helpers.get_hostname() not in members:
            if not self.zk.write_host_prio(my_prio):
                return False

        # clear path created by mistake
        self.zk.delete_legacy_timings_path()

        return True

    def start(self):
        """
        Start iterations
        """
        if not self.config.use_replication_slots and self.config.replication_slots_polling:
            logging.warning('Force disable replication_slots_polling because use_replication_slots is disabled.')
            self.config.replication_slots_polling = False

        my_prio = self.config.priority
        self.notifier.ready()
        while True:
            if self._init_zk(my_prio):
                break
            logging.error('Failed to init ZK')
            self.zk.re_init()

        while should_run():
            try:
                self.run_iteration(my_prio)
            except PostgresConnectionError as e:
                # Expected transient DB error (ADR-0002 §1): restart iteration.
                logging.warning('PostgreSQL error during iteration, will retry: %s', e)
            except Exception:
                logging.exception('Unexpected error during run_iteration')
        self.stop()

    def run_iteration(self, my_prio):
        logging.info('Start iteration on host: %s', helpers.get_hostname())
        timer = IterationTimer()
        if self.is_rewind_flag_set():
            logging.error('Rewind fail flag is set, skipping iteration. Remove %s to resume.', self._rewind_flag_path())
            self.finish_iteration(timer)
            return
        _, terminal_state = self.db.is_alive_and_in_terminal_state()
        if not terminal_state:
            logging.debug('Database is starting up or shutting down')

        db_state = self.db.get_state()
        role = db_state.get('role')
        logging.info('Role: %s', str(role))
        logging.debug('db_state: {}'.format(db_state))

        self.notifier.notify()
        db_state_for_debug = db_state.copy()
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(format_db_state_for_log(db_state_for_debug))

        try:
            zk_state = self.zk.get_state()
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(format_zk_state_for_log(zk_state))
            helpers.write_status_file(db_state, zk_state, self.config.working_dir)
            self._maintenance.update_status(db_state, zk_state, self._is_single_node)
            self._zk_alive_refresh(role, db_state, zk_state)
            self.write_iteration_state(db_state, role, my_prio)
            self._update_failover_health(db_state, zk_state)
            self._answer_failover_probe(db_state, zk_state)
            if (
                getattr(self.config, 'use_target_promote', False)
                and role == 'primary'
                and zk_state.get('lock_holder') == helpers.get_hostname()
            ):
                source_timeline = db_state.get('timeline')
                high_watermark = self.zk.get_timeline_high_watermark()
                if (
                    isinstance(source_timeline, int)
                    and (
                        high_watermark is None
                        or source_timeline > high_watermark
                    )
                ):
                    observed_highest = (
                        self.db.next_local_timeline(source_timeline) - 1
                    )
                    if not self.zk.ensure_timeline_high_watermark(
                        observed_highest,
                    ):
                        logging.warning(
                            'Could not update timeline high-water mark',
                        )
        except ZookeeperException:
            logging.exception("Zookeeper exception while getting ZK state")
            if role == 'primary' and not self._maintenance.is_in_maintenance and not self._is_single_node:
                logging.debug("Upper exception was for primary")
                my_hostname = helpers.get_hostname()
                self.resolve_zk_primary_lock(my_hostname)
            elif role == 'replica' and not self._maintenance.is_in_maintenance:
                logging.debug("Upper exception was for replica")
                self.handle_detached_replica(db_state)
                self.zk.re_init()
            else:
                self.zk.re_init()

            self.finish_iteration(timer)
            return

        if self._maintenance.is_in_maintenance:
            logging.warning('Cluster in maintenance mode')
            self.finish_iteration(timer)
            return

        if self._reconcile_desired_primary(db_state, zk_state):
            self.finalize_iteration(timer)
            return

        if self.handle_failover(db_state, zk_state):
            self.finalize_iteration(timer)
            return

        if self.handle_switchover(db_state, zk_state):
            self.finalize_iteration(timer)
            return

        if self._start_failover(db_state, zk_state):
            self.finalize_iteration(timer)
            return

        stream_from = self.config.stream_from
        if role is None:
            self.dead_iter(db_state, zk_state, is_in_terminal_state=terminal_state)
        elif role == 'primary':
            if self._is_single_node:
                self.single_node_primary_iter(db_state, zk_state)
            else:
                self.primary_iter(db_state, zk_state)
        elif role == 'replica':
            if stream_from:
                self.non_ha_replica_iter(db_state, zk_state)
            else:
                self.replica_iter(db_state, zk_state)

        self.finalize_iteration(timer)

    def write_iteration_state(self, db_state, role, my_prio):
        replication_state = db_state.get('replication_state')
        if replication_state is not None:
            if not self.zk.write_ssn_on_changes(replication_state[1]):
                raise ZookeeperException('Failed to write SSN state')

        if self._maintenance.is_in_maintenance:
            if not self.zk.write_host_maintenance_enabled():
                raise ZookeeperException('Failed to write maintenance state')

        # Dead PostgreSQL probably means
        # that our node is being removed.
        # No point in updating all_hosts
        # in this case
        all_hosts = self.zk.get_members(catch_except=False)
        prio = self.zk.get_host_prio(catch_except=False)
        if role and all_hosts and not prio:
            if not self.zk.write_host_prio(my_prio):
                raise ZookeeperException('Failed to write host priority')

    def finalize_iteration(self, timer):
        self.re_init_db()
        self.zk.re_init()
        self.finish_iteration(timer)

    def finish_iteration(self, timer):
        logging.info('Finished iteration ==============================')
        timer.sleep(self.config.iteration_timeout)

    def release_lock_and_return_to_cluster(self):
        my_hostname = helpers.get_hostname()
        self.db.pgpooler('stop')
        holder = self.zk.get_current_lock_holder()
        if holder == my_hostname:
            self.zk.release_lock()
        elif holder is not None:
            logging.warning('Lock in ZK is being held by %s. We should return to cluster here.', holder)
            self._return_to_cluster(holder, 'primary')

    def single_node_primary_iter(self, db_state, zk_state):
        """
        Iteration if local postgresql is single node
        """
        my_hostname = helpers.get_hostname()
        logging.info('primary is in single node state')
        if not self.zk.try_acquire_lock():
            logging.warning('Failed to aquire primary lock.')
            self.resolve_zk_primary_lock(my_hostname, close_master_without_lock=False)
            return None
        self._store_replics_info(db_state, zk_state)

        self.zk.write_timeline(db_state['timeline'])

        self.db.ensure_pooler_started()
        self.db.ensure_archiving_wal()

        # Enable async replication
        current_replication = self.db.get_replication_state()
        if current_replication[0] != 'async':
            self._replication_manager.change_replication_to_async()

    def primary_iter(self, db_state, zk_state):
        """
        Iteration if local postgresql is primary
        """
        my_hostname = helpers.get_hostname()
        try:
            stream_from = self.config.stream_from
            last_op = self.zk.get_host_op(my_hostname)
            # If we were promoting or rewinding
            # and failed we should not acquire lock
            if helpers.is_op_destructive(last_op):
                logging.warning('Could not acquire lock due to destructive operation fail: %s', last_op)
                return self.release_lock_and_return_to_cluster()
            if stream_from:
                logging.warning('Host not in HA group. We should return to stream_from.')
                return self.release_lock_and_return_to_cluster()

            # We shouldn't try to acquire leader lock if our current timeline is incorrect
            if self.zk.get_current_lock_holder() is None:
                # Timeline holdoff (ADR-0005 §1): after releasing the leader lock
                # due to a newer ZK timeline, skip lock acquisition for a grace
                # period to let the newer-timeline primary take over.
                if self._is_timeline_holdoff_active():
                    return None
                # Make sure local timeline corresponds to that of the cluster.
                if not self._verify_timeline(db_state, zk_state, without_leader_lock=True):
                    return None

            if not self.zk.try_acquire_lock():
                self.resolve_zk_primary_lock(my_hostname)
                return None
            self._reset_simple_primary_switch_try()

            # release replication source locks
            self._acquire_replication_source_slot_lock(None)

            self._slot_manager.handle_slots()

            self._store_replics_info(db_state, zk_state)

            # Make sure local timeline corresponds to that of the cluster.
            if not self._verify_timeline(db_state, zk_state):
                return None

            # Repairs: pooler, timings, archiving, replication type.
            self.db.ensure_pooler_started()
            # Here we are primary and pooler is opened, so clear stale downtime.
            self._timings.stop('downtime')

            # Ensure that wal archiving is enabled. It can be disabled earlier due to
            # some zk connectivity issues.
            self.db.ensure_archiving_wal()

            # Check if replication type (sync/normal) change is needed.
            ha_replics_config = self.zk.get_ha_replics(helpers.get_hostname())
            if ha_replics_config is None:
                return None
            try:
                logging.debug('Checking ha replics for aliveness')
                alive_hosts = self.zk.get_alive_hosts(timeout=3, catch_except=False)
                ha_replics = {replica for replica in ha_replics_config if replica in alive_hosts}
                logging.debug('alive_hosts: {}'.format(alive_hosts))
                logging.debug('ha_replics: {}'.format(ha_replics))
            except ZookeeperException:
                logging.exception('Fail to get replica status')
                ha_replics = ha_replics_config
            if len(ha_replics) != len(ha_replics_config):
                logging.debug(
                    'Some of the replics is unavailable, config replics %s alive replics %s',
                    str(ha_replics_config),
                    str(ha_replics),
                )
            logging.debug('Checking if changing replication type is needed.')
            change_replication = self.config.change_replication_type
            if change_replication:
                self._replication_manager.update_replication_type(db_state, ha_replics)

        except ZookeeperException:
            if not self.zk.try_acquire_lock():
                logging.error("Zookeeper error during primary iteration:")
                self.resolve_zk_primary_lock(my_hostname)
                return None

    def resolve_zk_primary_lock(self, my_hostname, close_master_without_lock=True):
        holder = self.zk.get_current_lock_holder()
        if holder is None:
            if close_master_without_lock and self._replication_manager.should_close():
                self.db.pgpooler('stop')
                # We need to stop archiving WAL because when network connectivity
                # returns, it can be another primary in cluster. We need to stop
                # archiving to prevent "wrong" WAL appears in archive.
                self.db.stop_archiving_wal()
            else:
                self.start_pooler()
            logging.warning('Lock in ZK is released but could not be acquired. Reconnecting to ZK.')
            self.zk.reconnect()
        elif holder != my_hostname:
            self.db.pgpooler('stop')
            logging.warning('Lock in ZK is being held by %s. We should return to cluster here.', holder)
            self._return_to_cluster(holder, 'primary')

    def handle_detached_replica(self, db_state):
        close_detached_replica_after = self.config.close_detached_after
        if not close_detached_replica_after:
            return
        now = time.time()
        zk_write_delay = now - self.last_zk_host_stat_write
        if zk_write_delay < close_detached_replica_after:
            logging.debug(
                f'Replica ZK write delay {zk_write_delay:.2f} within '
                f'{close_detached_replica_after} seconds; keeping replica open'
            )
            return
        if not db_state['wal_receiver']:
            logging.debug('Stopping pooler for replica with lost ZK connection and without walreceiver running')
            self.db.pgpooler('stop')
            return
        walreceiver_delay = now - db_state['wal_receiver']['last_msg_receipt_time_msec'] // 1000
        if walreceiver_delay > close_detached_replica_after:
            logging.debug(
                f'Stopping pooler for replica with lost ZK connection '
                f'and walreceiver delay {walreceiver_delay} > {close_detached_replica_after}'
            )
            self.db.pgpooler('stop')
        else:
            logging.debug(
                f'Replica write delay {zk_write_delay}, but walreceiver delay {walreceiver_delay} within '
                f'{close_detached_replica_after}; keeping replica open'
            )

    def write_host_stat(self, hostname, db_state):
        # ZK logic moved to zk.write_host_stat (step 12d, Variant A)
        if self.zk.write_host_stat(hostname, db_state, self.config.stream_from):
            self.last_zk_host_stat_write = time.time()
            return True
        return False

    def remove_stale_operation(self, hostname):
        last_op = self.zk.get_host_op(hostname)
        if helpers.is_op_destructive(last_op):
            logging.warning('Stale operation %s detected. Removing track from zk.', last_op)
            self.zk.delete_host_op(hostname)

    def start_pooler(self):
        start_pooler = self.config.start_pooler
        _, pooler_service_running = self.db.pgpooler('status')
        if not pooler_service_running and start_pooler:
            self.db.pgpooler('start')

    def get_replics_info(self, zk_state) -> ReplicaInfos | None:
        if self.config.stream_from:
            return self.zk.get_stream_source_replics_info(self.config.stream_from)
        return zk_state[self.zk.REPLICS_INFO_PATH]

    def change_primary(self, db_state, primary):
        logging.warning(
            'Seems that primary has been switched to %s '
            'while we are streaming WAL from %s. '
            'We should switch primary '
            'here.',
            primary,
            db_state['primary_fqdn'],
        )
        return self._return_to_cluster(primary, 'replica')

    def replica_return(self, db_state, zk_state):
        my_hostname = helpers.get_hostname()
        self.write_host_stat(my_hostname, db_state)
        holder = zk_state['lock_holder']
        limit = self.config.recovery_timeout

        logging.debug('ACTION. Replica is returning. So we resume WAL replay to {}'.format(holder))
        self.db.ensure_replaying_wal()

        if not self._check_archive_recovery(holder, limit) and not self._wait_for_streaming(holder, limit):
            # Wal receiver is not running and
            # postgresql isn't in archive recovery
            # We should try to restart
            logging.warning('We should try switch primary to {} again'.format(holder))
            return self._return_to_cluster(holder, 'replica', is_dead=False)

    def _get_streaming_replica_from_replics_info(self, fqdn, replics_info: ReplicaInfos):
        if not replics_info:
            return None
        app_name = helpers.app_name_from_fqdn(fqdn)
        for replica in replics_info:
            if replica['application_name'] == app_name and replica['state'] == 'streaming':
                return replica
        return None

    def _get_streaming_replicas(self):
        replics_info = self.db.get_replics_info('primary')
        streaming_app_names = {r['application_name'] for r in replics_info}
        all_hosts = self.zk.get_members() or []
        return [fqdn for fqdn in all_hosts if helpers.app_name_from_fqdn(fqdn) in streaming_app_names]

    def non_ha_replica_iter(self, db_state, zk_state):
        logging.info('Current replica is non ha.')
        if not zk_state['alive']:
            return None
        my_hostname = helpers.get_hostname()
        self.write_host_stat(my_hostname, db_state)
        stream_from = self.config.stream_from
        can_delayed = self.config.can_delayed
        replics_info = self.get_replics_info(zk_state) or []
        streaming = self._get_streaming_replica_from_replics_info(
            my_hostname, replics_info
        ) and bool(db_state['wal_receiver'])
        streaming_from_primary = self._get_streaming_replica_from_replics_info(
            my_hostname, zk_state.get(self.zk.REPLICS_INFO_PATH)
        ) and bool(db_state['wal_receiver'])
        logging.info(
            'Streaming: %s, streaming from primary: %s, wal_receiver: %s, replics_info: %s',
            streaming,
            streaming_from_primary,
            db_state['wal_receiver'],
            replics_info,
        )
        current_primary = zk_state['lock_holder']

        if streaming_from_primary and not streaming:
            self._acquire_replication_source_slot_lock(current_primary)

        if streaming:
            self._acquire_replication_source_slot_lock(stream_from)
        elif not can_delayed:
            logging.warning('Seems that we are not really streaming WAL from %s.', stream_from)
            self._replication_manager.leave_sync_group()
            replication_source_is_dead = self.db.is_host_unreachable(primary=stream_from, check_primary=False)
            replication_source_replica_info = self._get_streaming_replica_from_replics_info(
                stream_from, zk_state.get(self.zk.REPLICS_INFO_PATH)
            )
            wal_receiver_info = self.zk.get_host_wal_receiver(stream_from)
            logging.debug('wal_receiver_info: {}'.format(wal_receiver_info))
            replication_source_streams = bool(
                wal_receiver_info and wal_receiver_info.get('status') == 'streaming'
            )
            logging.debug('replication_source_replica_info: %s', replication_source_replica_info)

            if replication_source_is_dead:
                # Replication source is dead. We need to streaming from primary while it became alive and start streaming from primary.
                if stream_from == current_primary or current_primary is None:
                    logging.warning(
                        'My replication source %s seems dead and it was primary. Waiting new primary appears in cluster or old became alive.',
                        stream_from,
                    )
                elif not streaming_from_primary:
                    logging.warning(
                        'My replication source %s seems dead. Try to stream from primary %s',
                        stream_from,
                        current_primary,
                    )
                    return self._return_to_cluster(current_primary, 'replica', is_dead=False)
                else:
                    logging.warning(
                        'My replication source %s seems dead. We are already streaming from primary %s. Waiting replication source became alive.',
                        stream_from,
                        current_primary,
                    )
            else:
                # Replication source is alive. We need to wait while it starts streaming from primary and start streaming from it.
                if replication_source_streams:
                    logging.warning(
                        'My replication source %s seems alive and streams, try to stream from it',
                        stream_from,
                    )
                    return self._return_to_cluster(
                        stream_from,
                        'replica',
                        is_dead=False,
                        track_primary_epoch=False,
                    )
                elif stream_from == current_primary:
                    logging.warning(
                        'My replication source %s seems alive and it is current primary, try to stream from it',
                        stream_from,
                    )
                    return self._return_to_cluster(
                        stream_from,
                        'replica',
                        is_dead=False,
                        track_primary_epoch=False,
                    )
                else:
                    logging.warning(
                        'My replication source %s seems alive. But it don\'t streaming. Waiting it starts streaming from primary.',
                        stream_from,
                    )
        self.start_pooler()
        if self.config.primary_switch_disable_archive_restore:
            self.db.ensure_restoring_wal()
        self._reset_simple_primary_switch_try()
        self._slot_manager.handle_slots()

        # Stale cleanup runs last (ADR-0005 §2).
        self.remove_stale_operation(my_hostname)

    def replica_iter(self, db_state, zk_state):
        """
        Iteration if local postgresql is replica
        """
        if not zk_state['alive']:
            return None
        my_hostname = helpers.get_hostname()
        my_app_name = helpers.app_name_from_fqdn(my_hostname)
        holder = zk_state['lock_holder']
        self.write_host_stat(my_hostname, db_state)

        if self._is_single_node:
            logging.error("HA replica shouldn't exist inside a single node cluster")
            return None

        replics_info = zk_state[self.zk.REPLICS_INFO_PATH]
        streaming = False
        for i in replics_info or []:
            if i['application_name'] != my_app_name:
                continue
            if i['state'] == 'streaming':
                streaming = True

        if holder is None:
            logging.debug('No primary lock holder, waiting for top-level failover handler')
            return None

        if holder != db_state['primary_fqdn'] and holder != my_hostname:
            self._replication_manager.leave_sync_group()
            return self.change_primary(db_state, holder)

        self._acquire_replication_source_slot_lock(holder)

        logging.debug('ACTION. Ensuring WAL replaying from {}'.format(holder))
        self.db.ensure_replaying_wal()

        if not streaming:
            logging.warning('Seems that we are not really streaming WAL from %s.', holder)
            self._replication_manager.leave_sync_group()

            return self.replica_return(db_state, zk_state)

        if self.config.primary_switch_disable_archive_restore:
            self.db.ensure_restoring_wal()

        self.start_pooler()
        self._reset_simple_primary_switch_try()

        self._replication_manager.enter_sync_group()
        self._slot_manager.handle_slots()

        # Stale cleanup runs last (ADR-0005 §2).
        self.remove_stale_operation(my_hostname)

    def dead_iter(self, db_state, zk_state, is_in_terminal_state):
        """
        Iteration if local postgresql is dead
        """
        if not zk_state['alive'] or db_state['alive']:
            return None

        self.db.pgpooler('stop')
        if not is_in_terminal_state:
            logging.warning('Waiting for PostgreSQL to finish starting or stopping.')
            return None

        if self._is_single_node:
            logging.info('ACTION. We are in single mode, starting Postgres')
            return self.db.start_postgresql()

        self._replication_manager.leave_sync_group()
        self.zk.release_if_hold(self.zk.PRIMARY_LOCK_PATH)

        role = self.db.role  # it's previous role, before death
        last_primary = None
        if role == 'replica':
            prev_state = self.db.get_prev_state()
            last_primary = prev_state.get('primary_fqdn')

        holder = self.zk.get_current_lock_holder()
        if holder and holder != helpers.get_hostname():
            last_op = self.zk.get_host_op(helpers.get_hostname())
            if role == 'replica' and holder == last_primary and not helpers.is_op_destructive(last_op):
                if not is_in_terminal_state:
                    logging.warning('Waiting for postgres to finish starting or stopping.')
                    return None
                self._acquire_replication_source_slot_lock(last_primary)
                logging.info('Seems that primary has not changed but PostgreSQL is dead. Starting it.')
                return self.db.start_postgresql()

            #
            # We can get here in two cases:
            # We were primary and now we are dead.
            # We were replica, primary has changed and now we are dead.
            #
            logging.warning(
                'Seems that primary is %s and local PostgreSQL is dead. We should return to cluster here.', holder
            )
            return self._return_to_cluster(holder, role, is_dead=is_in_terminal_state)

        else:
            #
            # The only case we get here is absence of primary (no one holds the
            # lock) and our PostgreSQL is dead.
            #
            # TODO: BUG? should be acquire lock before starting PG ? replica may be promoting right now
            logging.error('Seems that all hosts (including me) are dead. Trying to start PostgreSQL.')
            if role == 'primary':
                last_tli = self.db.get_timeline()
                if not last_tli:
                    logging.error('Seems we have an error. Not doing anything.')
                    return None

                zk_timeline = zk_state[self.zk.TIMELINE_INFO_PATH]
                if zk_timeline is not None and zk_timeline != last_tli:
                    logging.error(
                        'Seems that I was primary before but not the last one in the cluster. Not doing anything.'
                    )
                    return None
            #
            # Role was primary. We need to disable archive_command before
            # starting postgres to prevent "wrong" last WAL in archive.
            #
            self.db.stop_archiving_wal_stopped()
            return self.db.start_postgresql()

    def _verify_timeline(self, db_state, zk_state, without_leader_lock=False):
        """
        Make sure current timeline corresponds to the rest of the cluster (@ZK).
        Save timeline and some related info into zk
        """
        # Skip if role is not primary
        if self.db.role != 'primary':
            logging.error('We are not primary. Not doing anything.')
            return None

        # Establish whether local timeline corresponds to primary timeline at ZK.
        tli_res = zk_state[self.zk.TIMELINE_INFO_PATH] == db_state['timeline']
        # If it does, but there is no info on replicas,
        # close local PG instance.
        if tli_res:
            if zk_state.get('replics_info_written') is False:
                logging.error('Some error with ZK.')
                # Actually we should never get here but checking it just in case.
                # Here we should end iteration and check and probably close primary
                # at the begin of primary_iter
                return None
        # If ZK does not have timeline info, write it.
        elif zk_state[self.zk.TIMELINE_INFO_PATH] is None:
            if without_leader_lock:
                return True
            logging.warning('Could not get timeline from ZK. Saving it.')
            self.zk.write_timeline(db_state['timeline'])
        # If there is a mismatch in timeline:
        # - If ZK timeline is greater than local, there must be another primary.
        #   In that case local instance have no business holding the lock.
        # - If local timeline is greater, local instance has likely been
        #   promoted recently.
        #   Update ZK structure to reflect that.
        elif tli_res is False:
            self.db.checkpoint()
            zk_tli = zk_state[self.zk.TIMELINE_INFO_PATH]
            db_tli = db_state['timeline']
            if zk_tli and zk_tli > db_tli:
                logging.error('ZK timeline is newer than local. Releasing leader lock')
                self.db.pgpooler('stop')

                self.zk.release_lock()
                # Holdoff marker (ADR-0005 §1): let the newer-timeline primary
                # acquire the lock. Replaces the former blocking time.sleep.
                self._start_timeline_holdoff()
                return None
            elif zk_tli and zk_tli < db_tli:
                if without_leader_lock:
                    return True
                logging.warning('Timeline in ZK is older than ours. Updating it it ZK.')
                self.zk.write_timeline(db_tli)
        logging.debug('Timeline verification succeeded')
        return True

    # Timeline holdoff grace period (ADR-0005 §1): replaces the former
    # blocking time.sleep(10 * iteration_timeout) in _verify_timeline.
    TIMELINE_HOLDOFF_NAME = 'timeline_holdoff'
    TIMELINE_HOLDOFF_MULTIPLIER = 10

    def _start_timeline_holdoff(self) -> None:
        """Write holdoff timestamp to ZK so next iterations skip lock acquisition."""
        self.zk.write_timing(self.TIMELINE_HOLDOFF_NAME, time.time())

    def _is_timeline_holdoff_active(self) -> bool:
        """Check if timeline holdoff is still active; clear it if expired."""
        holdoff_ts = self.zk.get_timing(self.TIMELINE_HOLDOFF_NAME)
        if holdoff_ts is None:
            return False
        if time.time() - holdoff_ts < self.TIMELINE_HOLDOFF_MULTIPLIER * self.config.iteration_timeout:
            logging.debug('Timeline holdoff active, skipping lock acquisition')
            return True
        logging.info('Timeline holdoff expired, resuming lock acquisition')
        self.zk.delete_timing(self.TIMELINE_HOLDOFF_NAME)
        return False

    def _reset_simple_primary_switch_try(self):
        logging.debug('Resetting simple primary switch try')
        self.checks['primary_switch'] = 0
        self.zk.reset_simple_primary_switch_tried(get_hostname())

    def _set_simple_primary_switch_try(self, new_primary: str):
        self.zk.set_simple_primary_switch_tried(new_primary, get_hostname())

    def _is_simple_primary_switch_tried(self, new_primary: str):
        return self.zk.get_simple_primary_switch_tried(new_primary, get_hostname())

    def _capture_return_target(self, new_primary: str) -> ReturnTarget | None:
        """Capture the materialized primary epoch, if one is available."""
        desired, _ = self.zk.get_desired_primary()
        timeline = self.zk.get_timeline()
        if desired is not None:
            operation_id = (
                desired.operation_id
                if desired.hostname == new_primary
                else None
            )
            return ReturnTarget(new_primary, operation_id, timeline)
        holder = self.zk.get_current_lock_holder(self.zk.PRIMARY_LOCK_PATH)
        if holder is None:
            return None
        return ReturnTarget(new_primary, None, timeline)

    def _return_target_is_current(self, target: ReturnTarget | None) -> bool:
        if target is None:
            return True
        desired, _ = self.zk.get_desired_primary()
        if target.operation_id is not None:
            identity_matches = bool(
                desired is not None
                and desired.hostname == target.hostname
                and desired.operation_id == target.operation_id
            )
        else:
            identity_matches = bool(
                desired is None
                and self.zk.get_current_lock_holder(
                    self.zk.PRIMARY_LOCK_PATH,
                ) == target.hostname
            )
        timeline_matches = (
            target.timeline is None
            or self.zk.get_timeline() == target.timeline
        )
        if not identity_matches or not timeline_matches:
            logging.info(
                'Return target changed while returning to %s; replanning',
                target.hostname,
            )
            return False
        return True

    @staticmethod
    def _return_target_marker(
        new_primary: str,
        target: ReturnTarget | None,
    ) -> str:
        if target is None:
            return new_primary
        return '{}|{}|{}'.format(
            target.hostname,
            target.operation_id or '',
            target.timeline if target.timeline is not None else '',
        )

    def _activate_return_target(self, target: ReturnTarget | None) -> None:
        if target is None or getattr(self, '_active_return_target', None) == target:
            return
        self._active_return_target = target
        self.checks['primary_switch'] = 0
        self.checks['rewind'] = 0

    def _ensure_restoring_wal(self):
        """Restore archive recovery (undo restore_command=/bin/false)."""
        logging.info('Ensuring WAL restoring is enabled')
        self.db.ensure_restoring_wal()

    def _try_simple_primary_switch_with_lock(self, *args, **kwargs):
        if not self.config.do_consecutive_primary_switch:
            return self._simple_primary_switch(*args, **kwargs)
        lock_holder = self.zk.get_current_lock_holder(self.zk.PRIMARY_SWITCH_LOCK_PATH)
        # Lock is free — try to acquire it. If acquisition fails, skip the switch.
        if lock_holder is None:
            if not self.zk.try_acquire_lock(self.zk.PRIMARY_SWITCH_LOCK_PATH):
                return True
        elif lock_holder != helpers.get_hostname():
            # Lock held by another host — skip.
            return True
        result = self._simple_primary_switch(*args, **kwargs)
        self.zk.release_lock(self.zk.PRIMARY_SWITCH_LOCK_PATH)
        return result

    def _simple_primary_switch(
        self,
        limit,
        new_primary,
        is_dead,
        return_target: ReturnTarget | None = None,
    ):
        primary_switch_checks = self.config.primary_switch_checks
        need_restart = self.config.primary_switch_restart
        target_marker = self._return_target_marker(new_primary, return_target)

        logging.info('Starting simple primary switch to {}'.format(new_primary))
        if self.checks['primary_switch'] >= primary_switch_checks:
            self._set_simple_primary_switch_try(target_marker)

        if not self._return_target_is_current(return_target):
            return None

        if need_restart and not is_dead and self.stop_postgresql(timeout=limit) != 0:
            logging.error('Could not stop PostgreSQL. Will retry.')
            self._reset_simple_primary_switch_try()
            return True

        if not self._return_target_is_current(return_target):
            return None
        if self.db.recovery_conf('create', new_primary) != 0:
            logging.error('Could not generate recovery.conf. Will retry.')
            self._reset_simple_primary_switch_try()
            return True

        if not is_dead and not need_restart:
            if not self.db.reload():
                logging.error('Could not reload PostgreSQL. Skipping it.')
            logging.debug('ACTION. Ensuring WAL replaying from {}'.format(new_primary))
            self.db.ensure_replaying_wal()
        else:
            if not self.db.enable_wal_receiver_stopped():
                logging.error('Could not enable walreceiver before PostgreSQL start. Will retry.')
                return True
            if self.db.start_postgresql() != 0:
                logging.error('Could not start PostgreSQL. Skipping it.')

        logging.debug('Waiting for recovery and archive recovery')
        if self._wait_for_recovery(new_primary, limit, return_target):
            self.db.ensure_replaying_wal()
            if self._check_archive_recovery(new_primary, limit, return_target):
                #
                # We have reached consistent state but there is a small
                # chance that we are not streaming changes from new primary
                # with: "new timeline N forked off current database system
                # timeline N-1 before current recovery point M".
                # Checking it with the info from ZK.
                #
                if self._wait_for_streaming(new_primary, limit, return_target):
                    #
                    # The easy way succeeded.
                    #
                    logging.info('Simple switch primary to {} succeeded'.format(new_primary))
                    self._reset_simple_primary_switch_try()
                    return True
                # Streaming did not start within the timeout — WAL likely
                # diverged. Fall through to signal failure so the caller
                # proceeds to pg_rewind.
                logging.warning('Simple primary switch: streaming did not start; fast return failed')
                return False
            # Archive recovery did not complete — fall through to failure.
            logging.warning('Simple primary switch: archive recovery check failed; fast return failed')
            return False
        # Recovery did not complete — fall through to failure.
        logging.warning('Simple primary switch: recovery did not complete; fast return failed')
        return False

    def _rewind_from_source(
        self,
        is_postgresql_dead,
        limit,
        new_primary,
        return_target: ReturnTarget | None = None,
    ):
        log_event('REWIND', detail='Starting pg_rewind from %s' % new_primary, level='warning')

        if not self._return_target_is_current(return_target):
            return None

        # Trying to connect to a new_primary. If not succeeded - exiting
        def source_ready():
            if not self._return_target_is_current(return_target):
                return False
            if not self.db.is_host_unreachable(new_primary, check_primary=False):
                return True
            return None

        if not helpers.await_for_value(
            source_ready,
            limit,
            'source database alive and ready for rewind',
        ):
            return None

        if not self._return_target_is_current(return_target):
            return None
        if not self.zk.write_host_op('rewind', helpers.get_hostname()):
            logging.error('Unable to save destructive op state: rewind')
            return None

        self.db.pgpooler('stop')

        if not is_postgresql_dead and self.stop_postgresql(timeout=limit) != 0:
            logging.error('Could not stop PostgreSQL. Will retry.')
            return None

        if not self._return_target_is_current(return_target):
            return None
        self.checks['rewind'] += 1
        if not self.db.resume_restoring_wal_stopped():
            logging.error('Could not enable archive access for pg_rewind. Will retry.')
            return True
        if self.db.do_rewind(new_primary) != 0:
            logging.error('Error while using pg_rewind. Will retry.')
            return True

        # pg_rewind is a blocking external command. Let an obsolete invocation
        # finish, but never start PostgreSQL against its stale source.
        if not self._return_target_is_current(return_target):
            return None

        # pg_rewind copies postgresql.auto.conf from the failover winner,
        # including its temporary vote fence.
        if not self.db.resume_restoring_wal_stopped():
            logging.error('Could not enable WAL restoring after pg_rewind. Will retry.')
            return True

        if return_target is None:
            attached = self._attach_to_primary(new_primary, limit)
        else:
            attached = self._attach_to_primary(
                new_primary,
                limit,
                return_target,
            )
        if attached or self._return_target_is_current(return_target):
            self.zk.delete_host_op(helpers.get_hostname())
        return attached

    def _attach_to_primary(
        self,
        new_primary,
        limit,
        return_target: ReturnTarget | None = None,
    ):
        """
        Generate recovery.conf and start PostgreSQL.
        """
        logging.info('Converting role to replica of %s.', new_primary)
        if not self._return_target_is_current(return_target):
            return None
        if self.db.recovery_conf('create', new_primary) != 0:
            logging.error('Could not generate recovery.conf. Will retry.')
            self._reset_simple_primary_switch_try()
            return None

        if not self.db.enable_wal_receiver_stopped():
            logging.error('Could not enable walreceiver before PostgreSQL start. Will retry.')
            return None
        if not self._return_target_is_current(return_target):
            return None
        if self.db.start_postgresql() != 0:
            logging.error('Could not start PostgreSQL. Skipping it.')

        if not self._wait_for_recovery(new_primary, limit, return_target):
            self._reset_simple_primary_switch_try()
            return None

        if not self._wait_for_streaming(new_primary, limit, return_target):
            self._reset_simple_primary_switch_try()
            return None

        logging.info('Seems, that returning to cluster succeeded. Unbelievable!')
        self.db.checkpoint()
        return True

    def _get_db_state(self):
        state = self.db.get_database_cluster_state()
        if not state or state == '':
            logging.error('Could not get info from controlfile about current cluster state.')
            return None
        logging.info('Database cluster state is: %s' % state)
        return state

    def _acquire_replication_source_slot_lock(self, source):
        if not self.config.replication_slots_polling:
            return
        # We need to drop the slot in the old primary.
        # But we don't know who the primary was (probably there are many of them).
        # So, we need to release the lock on all hosts.
        replication_sources = self.zk.get_children(self.zk.HOST_REPLICATION_SOURCES)
        if replication_sources:
            for host in replication_sources:
                if source != host:
                    self.zk.release_if_hold(os.path.join(self.zk.HOST_REPLICATION_SOURCES, host), read_lock=True)
        else:
            logging.warning(
                'Could not get all hosts list from ZK.'
                'Can not release old replication slot locks. We will skip it this time'
            )
        if source:
            # And acquire lock (then new_primary will create replication slot)
            self.zk.acquire_lock(os.path.join(self.zk.HOST_REPLICATION_SOURCES, source), read_lock=True)

    def _return_to_cluster(
        self,
        new_primary,
        role,
        is_dead=False,
        track_primary_epoch=True,
    ):
        """Return to cluster via decide_return_action (MDB-41951, ADR-0006).

        One action per call: SIMPLE_SWITCH or REWIND. If simple switch fails,
        the next iteration re-derives the action (will be REWIND if timelines
        diverge, or SIMPLE_SWITCH retry if they match).
        """
        logging.info('Starting return to cluster. New primary: {}'.format(new_primary))
        return_target = (
            self._capture_return_target(new_primary)
            if track_primary_epoch
            else None
        )
        counter_target = return_target or ReturnTarget(
            new_primary,
            None,
            None,
        )
        self._activate_return_target(counter_target)
        if not self._return_target_is_current(return_target):
            return
        target_marker = self._return_target_marker(new_primary, return_target)
        self.checks['primary_switch'] += 1

        self._acquire_replication_source_slot_lock(new_primary)
        limit = self.config.recovery_timeout
        state = self._get_db_state()
        if not state:
            return

        db_state = self.db.get_state() or {}

        obs = ReturnObservation.build(
            zk=self.zk, db=self.db, my_hostname=helpers.get_hostname(),
            db_state=db_state, new_primary=new_primary,
            is_dead=is_dead, recovery_timeout=limit,
            simple_switch_tried=self._is_simple_primary_switch_tried(target_marker),
            fallback_role=role,
        )

        action = decide_return_action(obs)

        if not self._return_target_is_current(return_target):
            return

        if action in (ReturnAction.WAIT_HISTORY, ReturnAction.WAIT_ARCHIVE):
            return

        if (
            action == ReturnAction.SIMPLE_SWITCH
            and obs.local_timeline != obs.zk_timeline
            and obs.zk_timeline is not None
            and obs.timeline_history_value is not None
            and not self.db.install_timeline_history(
                obs.zk_timeline, obs.timeline_history_value,
            )
        ):
            return

        if action == ReturnAction.SIMPLE_SWITCH:
            if obs.local_timeline != obs.zk_timeline and obs.archive_restore_disabled:
                self._ensure_restoring_wal()
            switch_kwargs = {}
            if return_target is not None:
                switch_kwargs['return_target'] = return_target
            switched = self._simple_primary_switch(
                limit, new_primary, is_dead, **switch_kwargs,
            )
            if not self._return_target_is_current(return_target):
                return
            if switched:
                if obs.archive_restore_disabled and obs.local_timeline == obs.zk_timeline:
                    self._ensure_restoring_wal()
                return  # success
            self._set_simple_primary_switch_try(target_marker)
            return  # retry next iteration (will go to REWIND if timelines diverge)

        # action == ReturnAction.REWIND
        self._set_simple_primary_switch_try(target_marker)
        rewind_kwargs = {}
        if return_target is not None:
            rewind_kwargs['return_target'] = return_target
        self._rewind_from_source(
            is_postgresql_dead=is_dead,
            limit=limit,
            new_primary=new_primary,
            **rewind_kwargs,
        )
        if not self._return_target_is_current(return_target):
            return
        if self.checks['rewind'] > self.config.max_rewind_retries:
            self.db.pgpooler('stop')
            self.stop_postgresql(timeout=limit)
            self.set_rewind_flag()
            log_event('RESETUP: Could not rewind %d times, setting rewind-failed flag' % self.config.max_rewind_retries, level='error')

    def _promote(self, target_timeline: int | None = None):
        if self.db.get_role() == 'primary':
            logging.info('PostgreSQL is already primary, skipping promote command')
            return True

        promoted = (
            self.db.promote()
            if target_timeline is None
            else self.db.promote(timeline=target_timeline)
        )
        if not promoted:
            logging.error('Could not promote me as a new primary. We should release the lock in ZK here.')
            # We need to close here and recheck postgres role. If it was no actual
            # promote, we need to return to cluster. If self primary we need to
            # continue promote despite the exit code
            # because self already accepted some data modification which will be loss if
            # we simply return False here.
            if self.db.get_role() != 'primary':
                self.db.pgpooler('stop')
                return False

            logging.info('Promote command failed but we are current primary. Continue')

        return True

    def _finish_promote(
        self,
        *,
        checkpoint: bool = True,
        expected_timeline: int | None = None,
    ) -> bool:
        """Run the retryable post-promote command group."""
        self._timings.stop('downtime')
        self._slot_manager.reset_on_promote()
        if checkpoint:
            logging.debug('Doing checkpoint after promoting.')
            try:
                if not self.db.checkpoint(query=self.config.promote_checkpoint_sql):
                    return False
            except PostgresConnectionError:
                logging.warning('Could not checkpoint after promotion.', exc_info=True)
                return False

        if expected_timeline is not None and not checkpoint:
            try:
                my_tli = self.db.get_live_timeline()
            except (PostgresConnectionError, PostgresQueryError):
                logging.warning(
                    'Could not identify current timeline after promote; checkpointing before fallback.',
                    exc_info=True,
                )
                try:
                    if not self.db.checkpoint(query=self.config.promote_checkpoint_sql):
                        return False
                except PostgresConnectionError:
                    logging.warning('Could not checkpoint after promoting.', exc_info=True)
                    return False
                # The checkpoint makes pg_controldata a safe fallback for the new timeline.
                my_tli = self.db.get_timeline()
        else:
            my_tli = self.db.get_timeline()
        if expected_timeline is not None and my_tli != expected_timeline:
            logging.error(
                'Promoted timeline %s differs from committed switchover timeline %s',
                my_tli,
                expected_timeline,
            )
            return False
        if not self.zk.write_timeline(my_tli):
            logging.warning('Could not write timeline to ZK.')
            return False
        return True

    def _promote_handle_slots(self):
        hosts = self.zk.get_ha_replics(helpers.get_hostname())
        if hosts is None:
            logging.error(
                'Could not get all hosts list from ZK. '
                'Replication slots should be created but we '
                'are unable to do it. Releasing the lock.'
            )
            return False
        return self._slot_manager.create_slots_for_hosts(list(hosts))

    def _get_switchover_candidate(
        self,
        record: SwitchoverRecord,
        db_state: dict | None = None,
    ):
        if record.destination is not None:
            return record.destination
        replica_infos = self._get_extended_replica_infos(db_state)
        if not replica_infos:
            return None
        return self._replication_manager.get_ensured_sync_replica(replica_infos)

    def _get_extended_replica_infos(self, db_state: dict | None = None) -> ReplicaInfos | None:
        if db_state is not None and db_state.get('replics_info') is not None:
            replica_infos = db_state['replics_info']
        else:
            replica_infos = self.zk.get_replics_info()
        if replica_infos is None:
            logging.error('Unable to get replica infos from ZK or db_state.')
            return None
        app_name_map = {helpers.app_name_from_fqdn(host): host for host in self.zk.get_ha_hosts()}
        for info in replica_infos:
            hostname = app_name_map.get(info['application_name'])
            if not hostname:
                continue
            prio = self.zk.get_host_prio(hostname)
            info['priority'] = int(prio) if prio is not None else None
        return replica_infos

    def _build_failover_observation(
        self,
        phase: FailoverPhase | None,
        db_state: dict,
        *,
        automatic: bool = True,
        must_reset: bool = False,
        fence_mismatched_timelines: bool = False,
        branch_record: SwitchoverRecord | None = None,
    ) -> FailoverObservation:
        """Build the immutable input for one failover step."""
        observation = FailoverObservation.build(
            phase=phase,
            zk=self.zk,
            db=self.db,
            timings=self._timings,
            my_hostname=helpers.get_hostname(),
            db_state=db_state,
            host_priority=int(self.config.priority),
            autofailover=self.config.autofailover if automatic else True,
            check_primary_unreachable=False,
            check_wal_replay=phase is not None,
            must_reset=must_reset,
            fence_mismatched_timelines=fence_mismatched_timelines,
        )
        if (
            branch_record is None
            or not branch_record.handoff_is_committed()
            or branch_record.expected_timeline is None
            or branch_record.timeline is None
            or branch_record.selected_candidate is None
        ):
            return observation
        target_durability = DurabilityConfig.build(
            branch_record.original_durability_members,
        )
        source_durability = DurabilityConfig.build([
            host for host in (
                branch_record.hostname,
                branch_record.selected_candidate,
            ) if host is not None
        ])
        try:
            commit_members = tuple(
                target_durability.replicas_for(
                    branch_record.selected_candidate,
                )
            )
        except ValueError:
            return observation
        return replace(
            observation,
            branch_source_timeline=branch_record.timeline,
            branch_target_timeline=branch_record.expected_timeline,
            branch_old_primary=branch_record.hostname,
            branch_candidate=branch_record.selected_candidate,
            branch_commit_members=commit_members,
            branch_commit_required=target_durability.required,
            branch_source_durability=source_durability,
            branch_target_durability=target_durability,
        )

    def _failover_trigger(self, db_state: dict, zk_state: dict) -> bool:
        """Return whether an ordinary automatic failover should start."""
        if (
            db_state.get('role') != 'replica'
            or self.config.stream_from
            or self._is_single_node
        ):
            return False

        return self.config.autofailover

    def _update_failover_health(self, db_state: dict, zk_state: dict) -> None:
        """Track how long both the primary and local replay have been still."""
        if db_state.get('role') != 'replica' or self.config.stream_from:
            return
        primary = zk_state.get('lock_holder') or zk_state.get(self.zk.LAST_PRIMARY_PATH)
        if primary is None:
            return
        now = time.time()
        if primary != getattr(self, '_health_primary', None):
            self._health_primary = primary
            self._health_unreachable_since = None
            self._health_wal_position = None
            self._health_wal_unchanged_since = None

        unreachable = self.db.is_host_unreachable(primary=primary, check_primary=False)
        if unreachable:
            if self._health_unreachable_since is None:
                self._health_unreachable_since = now
        else:
            self._health_unreachable_since = None

        position = self.db.get_replay_diff()
        if position is None:
            self._health_wal_position = None
            self._health_wal_unchanged_since = None
        elif position != self._health_wal_position:
            self._health_wal_position = position
            self._health_wal_unchanged_since = now

    def _health_report(self, probe: FailoverProbe) -> FailoverHealthReport | None:
        hostname = helpers.get_hostname()
        if hostname not in probe.durability_members or hostname == probe.primary:
            return None
        if probe.primary != getattr(self, '_health_primary', None):
            return None
        primary_unreachable, wal_stalled = self._local_health_ready(probe.primary)
        return FailoverHealthReport(
            probe_id=probe.probe_id,
            primary=probe.primary,
            durability_version=probe.durability_version,
            primary_unreachable=primary_unreachable,
            wal_stalled=wal_stalled,
            wal_position=getattr(self, '_health_wal_position', None),
        )

    def _local_health_ready(self, primary: str) -> tuple[bool, bool]:
        if primary != getattr(self, '_health_primary', None):
            return False, False
        now = time.time()
        timeout = self.config.primary_unavailability_timeout
        unreachable_since = getattr(self, '_health_unreachable_since', None)
        wal_unchanged_since = getattr(self, '_health_wal_unchanged_since', None)
        return (
            unreachable_since is not None and now - unreachable_since >= timeout,
            wal_unchanged_since is not None and now - wal_unchanged_since >= timeout,
        )

    def _answer_failover_probe(self, db_state: dict, zk_state: dict) -> None:
        if db_state.get('role') != 'replica' or self.config.stream_from:
            return
        probe = zk_state.get(self.zk.FAILOVER_PROBE_PATH)
        if isinstance(probe, dict):
            try:
                probe = FailoverProbe.from_dict(probe)
            except (KeyError, TypeError, ValueError):
                return
        if not isinstance(probe, FailoverProbe):
            return
        report = self._health_report(probe)
        if report is not None:
            self.zk.write_failover_health(report)

    @staticmethod
    def _probe_quorum_size(probe: FailoverProbe) -> int:
        return max(
            len(config.members) - 1 - config.required + 1
            for config in map(DurabilityConfig.build, probe.quorum_memberships)
        )

    def _probe_has_quorum(self, probe: FailoverProbe) -> bool:
        reports = {
            host: self.zk.get_failover_health(host, probe)
            for host in set(probe.durability_members) - {probe.primary}
        }
        for members in probe.quorum_memberships:
            config = DurabilityConfig.build(members)
            replicas = set(config.members) - {probe.primary}
            accepted = sum(
                1 for host in replicas
                if (report := reports.get(host)) is not None
                and report.primary_unreachable
                and report.wal_stalled
            )
            required = len(replicas) - config.required + 1
            if accepted < required:
                return False
        return True

    def _set_desired_primary(
        self,
        hostname: str | None,
        operation_id: str,
        operation_type: str,
        *,
        expected_hostname: str | None,
    ) -> bool:
        current, version = self.zk.get_desired_primary()
        if current is not None and current.operation_id == operation_id:
            if current.hostname == hostname:
                return True
            expected_hostname = current.hostname
        elif current is not None and current.hostname != expected_hostname:
            logging.warning(
                'Desired primary changed concurrently: expected=%s actual=%s',
                expected_hostname, current.hostname,
            )
            return False
        desired = DesiredPrimary(hostname, operation_id, operation_type)
        return self.zk.write_desired_primary(desired, version) is not None

    def _reconcile_desired_primary(self, db_state: dict, zk_state: dict) -> bool:
        """Fence a local primary that is no longer the materialized owner."""
        desired = zk_state.get(self.zk.DESIRED_PRIMARY_PATH)
        if isinstance(desired, dict):
            try:
                desired = DesiredPrimary.from_dict(desired)
            except (KeyError, TypeError, ValueError):
                return False
        if not isinstance(desired, DesiredPrimary):
            return False
        switchover = zk_state.get(self.zk.SWITCHOVER_RECORD_PATH)
        if (
            desired.operation_type == 'switchover'
            and isinstance(switchover, dict)
            and switchover.get('phase')
            and int(switchover.get('protocol_version', 1)) < 2
        ):
            # Protocol v1 still owns its lock transitions directly.
            return False
        hostname = helpers.get_hostname()
        holder = zk_state.get('lock_holder')
        if desired.hostname == hostname:
            if (
                holder is None
                and desired.operation_type in ('failover', 'switchover')
            ):
                self.zk.try_acquire_lock(
                    self.zk.PRIMARY_LOCK_PATH,
                    allow_queue=False,
                    timeout=0,
                )
            return False
        if desired.operation_type == 'switchover' and holder != hostname:
            # The lock was already transferred. Keep P's normal iteration
            # available to finish the handoff protocol.
            return False
        if db_state.get('role') != 'primary' and holder != hostname:
            return False
        if desired.operation_type == 'switchover':
            # Planned switchovers transfer the ownership lock before handoff.
            # P keeps serving synchronously until the protocol itself observes
            # enough replicas at C and initiates shutdown.
            self.zk.release_if_hold(self.zk.PRIMARY_LOCK_PATH)
            logging.info(
                'Released leader lock for planned primary %s',
                desired.hostname,
            )
            return True
        else:
            self.db.pgpooler('stop')
            if db_state.get('role') == 'primary':
                self.db.stop_archiving_wal()
        self.zk.release_if_hold(self.zk.PRIMARY_LOCK_PATH)
        logging.warning('Local primary fenced; desired primary is %s', desired.hostname)
        return True

    def handle_failover(self, db_state: dict, zk_state: dict) -> bool:
        """Run one failover step and claim the iteration while failover exists."""
        bridge_record = SwitchoverRecord.from_zk_state(zk_state, self.zk)
        raw_phase = zk_state.get(self.zk.FAILOVER_STATE_PATH)
        if self._is_committed_bridge_handoff(bridge_record, zk_state):
            candidate = bridge_record.selected_candidate
            holder = zk_state.get('lock_holder')
            if (
                bridge_record.hostname == helpers.get_hostname()
                and raw_phase is None
            ):
                # P's PostgreSQL is stopped, but its daemon may still hold the
                # manager lease and must observe C's promotion even while the
                # fence-only failover epoch is active.
                return self._run_bridge_primary(bridge_record, db_state, holder)
            if (
                candidate == helpers.get_hostname()
                and (
                    db_state.get('role') == 'replica'
                    or (db_state.get('role') == 'primary' and holder == candidate)
                )
            ):
                # C is the only old-timeline host allowed to cross the branch
                # fence.  This also lets it resume after a failed first promote.
                return self._run_bridge_candidate(
                    bridge_record,
                    db_state,
                    holder,
                    zk_state.get(self.zk.TIMELINE_INFO_PATH),
                )
        must_reset = bool(zk_state.get(self.zk.FAILOVER_MUST_BE_RESET))
        phase = FailoverPhase.from_str(raw_phase)

        if raw_phase is not None and phase is None:
            logging.error('Invalid failover state %r, cleaning it up', raw_phase)
            must_reset = True

        if phase is not None and self.config.stream_from:
            return True

        if phase is not None or must_reset:
            self._run_failover_step(
                phase,
                db_state,
                zk_state,
                must_reset=must_reset,
            )
            return True

        return False

    def _start_failover(self, db_state: dict, zk_state: dict) -> bool:
        """Initialize ordinary failover and claim the iteration when triggered."""
        request = zk_state.get(self.zk.FAILOVER_REQUEST_PATH)
        if isinstance(request, dict):
            try:
                request = FailoverRequest.from_dict(request)
            except (KeyError, TypeError, ValueError):
                logging.error('Invalid manual failover request: %r', request)
                return True
        if isinstance(request, FailoverRequest):
            return self._start_requested_failover(request, db_state, zk_state)

        if not self._failover_trigger(db_state, zk_state):
            return False

        if not is_transition_allowed(
            zk_state.get(self.zk.LAST_FAILOVER_TIME_PATH),
            self.config.min_failover_timeout,
        ):
            return False
        primary = zk_state.get('lock_holder') or zk_state.get(self.zk.LAST_PRIMARY_PATH)
        if primary is None:
            return False
        primary_unreachable, wal_stalled = self._local_health_ready(primary)
        # Avoid taking the manager lock on every healthy iteration.
        if not primary_unreachable or not wal_stalled:
            return False
        if not self._try_acquire_failover_coordinator():
            return False
        durability_state, durability_version = self.zk.get_durability_state()
        durability = durability_state.stable
        durabilities = durability_state.failover_configs()
        if (
            durability is None
            or not durabilities
            or durability_version is None
            or any(primary not in config.members for config in durabilities)
        ):
            self.zk.release_lock(self.zk.ELECTION_MANAGER_LOCK_PATH)
            return False
        probe = self.zk.start_failover_probe(primary, durabilities, durability_version)
        if probe is None:
            self.zk.release_lock(self.zk.ELECTION_MANAGER_LOCK_PATH)
            return False
        own_report = self._health_report(probe)
        if own_report is not None:
            self.zk.write_failover_health(own_report)
        probe_timeout = max(2 * self.config.iteration_timeout, 1.0)
        if not helpers.await_for_value(
            lambda: True if self._probe_has_quorum(probe) else None,
            probe_timeout,
            f'failover probe {probe.probe_id} quorum',
        ):
            self.zk.release_lock(self.zk.ELECTION_MANAGER_LOCK_PATH)
            return True
        self._initialize_failover(
            db_state,
            zk_state,
            automatic=True,
            failed_primary=primary,
            verified_probe=probe,
        )
        return True

    def _start_requested_failover(
        self,
        request: FailoverRequest,
        db_state: dict,
        zk_state: dict,
    ) -> bool:
        """Let one HA replica turn an operator request into failover state."""
        if (
            db_state.get('role') != 'replica'
            or self.config.stream_from
            or self._is_single_node
        ):
            return False
        primary = zk_state.get('lock_holder') or zk_state.get(self.zk.LAST_PRIMARY_PATH)
        if primary != request.primary:
            logging.error(
                'Manual failover primary changed: requested=%s actual=%s',
                request.primary,
                primary,
            )
            return True
        if not self._try_acquire_failover_coordinator():
            return True
        return self._initialize_failover(
            db_state,
            zk_state,
            automatic=False,
            failed_primary=request.primary,
            manual_request=request,
        )

    def _initialize_failover_from_switchover(self, db_state: dict, zk_state: dict) -> bool:
        record = SwitchoverRecord.from_zk_state(zk_state, self.zk)
        failed_primary = (
            record.selected_candidate if record.handoff_is_committed() else None
        )
        return self._initialize_failover(
            db_state,
            zk_state,
            automatic=False,
            failed_primary=failed_primary,
        )

    def _initialize_failover(
        self,
        db_state: dict,
        zk_state: dict,
        *,
        automatic: bool,
        failed_primary: str | None = None,
        verified_probe: FailoverProbe | None = None,
        manual_request: FailoverRequest | None = None,
    ) -> bool:
        """Persist the first failover phase after all entry checks pass."""
        if FailoverPhase.from_str(zk_state.get(self.zk.FAILOVER_STATE_PATH)) is not None:
            return True

        if self.config.stream_from:
            return False

        if not self._try_acquire_failover_coordinator():
            return False
        if (
            verified_probe is None
            and manual_request is None
            and self.zk.get_current_lock_holder(self.zk.PRIMARY_LOCK_PATH)
        ):
            self.zk.release_lock(self.zk.ELECTION_MANAGER_LOCK_PATH)
            return False
        if verified_probe is not None:
            current_durability, current_version = self.zk.get_durability_state()
            if (
                current_version != verified_probe.durability_version
                or tuple(
                    config.members for config in current_durability.failover_configs()
                ) != verified_probe.quorum_memberships
            ):
                logging.warning('Durability changed while failover probe was running')
                self.zk.release_lock(self.zk.ELECTION_MANAGER_LOCK_PATH)
                return False
        bridge_record = SwitchoverRecord.from_zk_state(zk_state, self.zk)
        fence_mismatched_timelines = self._is_committed_bridge_handoff(bridge_record, zk_state)
        if fence_mismatched_timelines:
            observation = self._build_failover_observation(
                None,
                db_state,
                automatic=automatic and verified_probe is None,
                fence_mismatched_timelines=True,
                branch_record=bridge_record,
            )
        else:
            observation = self._build_failover_observation(
                None,
                db_state,
                automatic=automatic and verified_probe is None,
            )
        if verified_probe is None and not fence_mismatched_timelines and not self._failover_machine.can_start(observation):
            logging.warning('Failover entry checks failed — not starting failover')
            self.zk.release_lock(self.zk.ELECTION_MANAGER_LOCK_PATH)
            return False
        if not self.zk.is_lock_holder(self.zk.ELECTION_MANAGER_LOCK_PATH):
            return False

        durability = observation.durability
        durabilities = observation.durability_quorums
        if fence_mismatched_timelines:
            failed_primary = bridge_record.selected_candidate
        failed_primary = failed_primary or db_state.get('primary_fqdn') or zk_state.get(self.zk.LAST_PRIMARY_PATH)
        if failed_primary is None:
            logging.error('Cannot freeze failover electorate without failed primary')
            self.zk.release_lock(self.zk.ELECTION_MANAGER_LOCK_PATH)
            return False
        if fence_mismatched_timelines:
            source = observation.branch_source_durability
            target = observation.branch_target_durability
            if source is None or target is None:
                logging.error('Committed handoff has no frozen branch durability')
                self.zk.release_lock(self.zk.ELECTION_MANAGER_LOCK_PATH)
                return False
            durabilities = (source, target)
            durability = target
            electorate = sorted({
                host
                for config in durabilities
                for host in config.members
                if host != failed_primary
            })
        elif durability is None:
            if manual_request is None or not manual_request.with_data_loss:
                logging.error('Cannot freeze failover electorate without durability')
                self.zk.release_lock(self.zk.ELECTION_MANAGER_LOCK_PATH)
                return False
            ha_hosts = self.zk.get_ha_hosts()
            if ha_hosts is None:
                logging.error('Cannot freeze failover electorate without HA members')
                self.zk.release_lock(self.zk.ELECTION_MANAGER_LOCK_PATH)
                return False
            electorate = sorted({host for host in ha_hosts if host != failed_primary})
        else:
            if not durabilities or any(
                failed_primary not in config.members for config in durabilities
            ):
                logging.error('Failed primary is absent from a durability transition endpoint')
                self.zk.release_lock(self.zk.ELECTION_MANAGER_LOCK_PATH)
                return False
            electorate = sorted({
                host
                for config in durabilities
                for host in config.members
                if host != failed_primary
            })
        if not electorate:
            logging.error('Failover electorate is empty')
            self.zk.release_lock(self.zk.ELECTION_MANAGER_LOCK_PATH)
            return False

        for path, recursive in (
            (self.zk.ELECTION_VOTES_PATH, True),
            (self.zk.ELECTION_WINNER_PATH, False),
            (self.zk.FAILOVER_PARTICIPANTS_PATH, True),
        ):
            if not self.zk.delete(path, recursive=recursive):
                self.zk.release_lock(self.zk.ELECTION_MANAGER_LOCK_PATH)
                return False
        if not self.zk.write_failover_members(electorate):
            self.zk.release_lock(self.zk.ELECTION_MANAGER_LOCK_PATH)
            return False
        failover_version = (
            verified_probe.operation_id
            if verified_probe is not None
            else manual_request.operation_id
            if manual_request is not None
            else uuid.uuid4().hex
        )
        if not self.zk.write_failover_version(failover_version):
            self.zk.release_lock(self.zk.ELECTION_MANAGER_LOCK_PATH)
            return False

        if not self.zk.is_lock_holder(self.zk.ELECTION_MANAGER_LOCK_PATH):
            return False
        if not self.zk.write_failover_state(FailoverPhase.WALRECEIVER_DISABLING):
            self.zk.release_lock(self.zk.ELECTION_MANAGER_LOCK_PATH)
            return False
        desired, _ = self.zk.get_desired_primary()
        expected_desired = desired.hostname if desired is not None else failed_primary
        if not self._set_desired_primary(
            None,
            failover_version,
            'failover',
            expected_hostname=expected_desired,
        ):
            logging.error('Could not clear desired primary for failover fencing')
            return False

        zk_state[self.zk.FAILOVER_STATE_PATH] = FailoverPhase.WALRECEIVER_DISABLING
        log_event('FAILOVER: Primary has died, starting failover procedure', level='error')
        logging.error('According to ZK primary has died. Starting failover.')
        return True

    def _try_acquire_failover_coordinator(self) -> bool:
        """Try to acquire failover coordinator ownership."""
        holder = self.zk.get_current_lock_holder(self.zk.ELECTION_MANAGER_LOCK_PATH)
        if holder == helpers.get_hostname():
            return True
        if holder is not None:
            return False
        return self.zk.try_acquire_lock(self.zk.ELECTION_MANAGER_LOCK_PATH)

    def _run_failover_step(
        self,
        phase: FailoverPhase | None,
        db_state: dict,
        zk_state: dict,
        *,
        must_reset: bool,
    ) -> None:
        """Run one failover machine step (ADR-0007 §5)."""
        if not self.zk.get_current_lock_holder(
            self.zk.ELECTION_MANAGER_LOCK_PATH
        ):
            # Failover is active but no coordinator holds the lock (e.g. after
            # restart). Try to become the coordinator to resume the process.
            if self._try_acquire_failover_coordinator():
                logging.info('Resumed failover coordination (phase=%s)', phase)

        if (
            phase not in (None, FailoverPhase.FINISHED, FailoverPhase.FAILED)
            and self.zk.is_lock_holder(self.zk.ELECTION_MANAGER_LOCK_PATH)
            and self.zk.get_election_winner() is None
        ):
            failover_version = self.zk.get_failover_version()
            desired, _ = self.zk.get_desired_primary()
            if failover_version is None:
                return
            if desired is None or desired.operation_id != failover_version or desired.hostname is not None:
                expected = desired.hostname if desired is not None else zk_state.get(self.zk.LAST_PRIMARY_PATH)
                if not self._set_desired_primary(
                    None,
                    failover_version,
                    'failover',
                    expected_hostname=expected,
                ):
                    return

        bridge_record = SwitchoverRecord.from_zk_state(zk_state, self.zk)
        committed_handoff = self._is_committed_bridge_handoff(
            bridge_record, zk_state,
        )
        if committed_handoff:
            obs = self._build_failover_observation(
                phase,
                db_state,
                must_reset=must_reset,
                fence_mismatched_timelines=True,
                branch_record=bridge_record,
            )
        else:
            obs = self._build_failover_observation(
                phase,
                db_state,
                must_reset=must_reset,
            )
        self._executor.set_iteration_state(db_state, zk_state)
        self._executor.run(self._failover_machine, obs)

    def _run_promotion(
        self,
        scope,
        operation_id: str,
        old_primary=None,
        start_postgresql=False,
        prepared=False,
        expected_timeline: int | None = None,
        target_timeline: int | None = None,
    ) -> PromotionResult:
        """Resume the current host-local promotion command group."""
        state = self._local_states[scope]
        try:
            phase = state.read(operation_id) or 'creating_slots'
            if (
                target_timeline is None
                and getattr(self.config, 'use_target_promote', False)
                and scope == 'failover_participant'
                and not start_postgresql
                and phase != 'checkpointing'
                and self.db.get_role() != 'primary'
            ):
                source_timeline = self.db.get_timeline()
                observed_highest = (
                    self.db.next_local_timeline(source_timeline) - 1
                )
                target_timeline = self.zk.reserve_timeline(
                    f'failover:{operation_id}', observed_highest,
                )
                if target_timeline is None:
                    return PromotionResult.RETRY
            if start_postgresql:
                if self.db.start_postgresql() != 0:
                    logging.error('Could not start PostgreSQL to resume promotion')
                    return PromotionResult.RETRY
                logging.info('PostgreSQL started; promotion will resume on the next iteration')
                return PromotionResult.RETRY
            if phase == 'creating_slots':
                state.write(operation_id, phase)
                self.db.pg_wal_replay_resume()
                if not prepared:
                    if not self._promote_handle_slots():
                        return PromotionResult.RETRY
                    if not self._replication_manager.set_ssn_before_promote(
                        self.zk.get_durability_config()
                    ):
                        logging.error('Failed to set SSN before promote, aborting promote')
                        return PromotionResult.RETRY
                state.write(operation_id, 'promoting')
                phase = 'promoting'

            if phase == 'promoting':
                if self._debug_failure('before_promote'):
                    return PromotionResult.RETRY
                promoted = (
                    self._promote()
                    if target_timeline is None
                    else self._promote(target_timeline=target_timeline)
                )
                if not promoted:
                    return PromotionResult.REJECTED
                state.write(operation_id, 'checkpointing')
                phase = 'checkpointing'

            if phase == 'checkpointing':
                if prepared:
                    finished = self._finish_promote(
                        checkpoint=False,
                        expected_timeline=expected_timeline,
                    )
                else:
                    finished = self._finish_promote()
                if not finished:
                    return PromotionResult.RETRY
                if (
                    scope == 'failover_participant'
                    and not self._replication_manager.discard_transition_after_failover()
                ):
                    logging.warning('Could not discard stale durability transition after failover')
                    return PromotionResult.RETRY
                self._replication_manager.leave_sync_group()

            return PromotionResult.SUCCESS
        except PostgresConnectionError:
            logging.warning('DB connection lost during promotion.', exc_info=True)
            return PromotionResult.RETRY

    def _wait_for_recovery(
        self,
        new_primary,
        limit,
        return_target: ReturnTarget | None = None,
    ):
        """Stop until postgresql complete recovery (ADR-0005 §1: no infinite wait)."""

        def check_recovery_completion():
            if not self._return_target_is_current(return_target):
                return False
            self._acquire_replication_source_slot_lock(new_primary)
            is_db_alive, terminal_state = self.db.is_alive_and_in_terminal_state()
            if not terminal_state:
                logging.debug('PostgreSQL in nonterminal state.')
                return None
            if is_db_alive:
                logging.debug('PostgreSQL has completed recovery.')
                return True
            if self.db.get_postgresql_status() != 0:
                logging.error('PostgreSQL service seems to be dead. No recovery is possible in this case.')
                return False
            return None

        return helpers.await_for_value(check_recovery_completion, limit, "PostgreSQL has completed recovery")

    def _check_archive_recovery(
        self,
        new_primary,
        limit,
        return_target: ReturnTarget | None = None,
    ):
        """
        Returns True if postgresql is in recovery from archive
        and False if it hasn't started recovery within `limit` seconds
        """

        def check_recovery_start():
            if not self._return_target_is_current(return_target):
                return False
            if self._check_postgresql_streaming(new_primary):
                logging.debug('PostgreSQL is already streaming from {}'.format(new_primary))
                return True

            # we can get here with another role or
            # have role changed during this retrying cycle
            role = self.db.get_role()
            if role != 'replica':
                logging.warning('PostgreSQL role changed during archive recovery check. Now it doesn\'t make sense')
                self.db.pgpooler('stop')
                return False

            if self.db.is_replaying_wal(1):
                logging.debug('PostgreSQL is in archive recovery')
                return True
            return None

        return helpers.await_for_value(check_recovery_start, limit, 'PostgreSQL started archive recovery')

    def _get_replics_info_from_zk(self, primary) -> ReplicaInfos | None:
        if primary:
            return self.zk.get_host_replics_info(primary)
        else:
            return self.zk.get_replics_info()

    @staticmethod
    def _is_caught_up(replica_infos: ReplicaInfos):
        my_app_name = helpers.app_name_from_fqdn(helpers.get_hostname())
        for replica in replica_infos:
            if replica['application_name'] == my_app_name and replica['state'] == 'streaming':
                return True
        return False

    def _check_postgresql_streaming(self, primary):
        self._acquire_replication_source_slot_lock(primary)
        is_db_alive, terminal_state = self.db.is_alive_and_in_terminal_state()
        if not terminal_state:
            logging.debug('PostgreSQL in nonterminal state.')
            return None

        if not is_db_alive:
            logging.error('PostgreSQL is dead. Waiting for streaming is useless.')
            return False

        # we can get here with another role or
        # have role changed during this retrying cycle
        if self.db.get_role() != 'replica':
            self.db.pgpooler('stop')
            logging.warning("PostgreSQL is not a replica, so it can't be streaming.")
            return False

        try:
            if self.db.get_primary_fqdn() == primary and self.db.check_walreceiver():
                logging.debug('PostgreSQL has started streaming from {}'.format(primary))
                return True
        except PostgresConnectionError:
            logging.warning('DB connection lost during streaming check', exc_info=True)
            return None

        try:
            replica_infos = self._get_replics_info_from_zk(primary)
        except ZookeeperException:
            logging.error("Can't get replics_info from ZK. Won't wait for timeout.")
            return False

        # Best-Effort (ADR-0002 §3): self-healing — a DB loss returns None so
        # the _wait_for_streaming loop retries on the next tick.
        try:
            if replica_infos is not None and (pgconsul._is_caught_up(replica_infos) and self.db.check_walreceiver()):
                logging.debug('PostgreSQL has started streaming from {}'.format(primary))
                return True
        except PostgresConnectionError:
            logging.warning('DB connection lost during streaming check', exc_info=True)

        return None

    def _wait_for_streaming(
        self,
        primary,
        limit,
        return_target: ReturnTarget | None = None,
    ):
        """Stop until postgresql start streaming from primary (ADR-0005 §1: no infinite wait)."""
        def check_streaming():
            if not self._return_target_is_current(return_target):
                return False
            return self._check_postgresql_streaming(primary)

        return helpers.await_for_value(check_streaming, limit, 'PostgreSQL started streaming from {}'.format(primary))

    def _all_side_replicas_turned_to_the_candidate(self, side_replicas):
        side_replicas_app_names = {helpers.app_name_from_fqdn(r) for r in side_replicas}
        logging.debug('Side replicas names: %s', side_replicas_app_names)
        # Switchover critical section (ADR-0002 §2): return False on DB loss so
        # the await_for loop keeps waiting.
        try:
            replics_info = self.db.get_replics_info('replica')
        except PostgresConnectionError:
            logging.warning('Could not get replics info from candidate, assuming not all replicas turned yet', exc_info=True)
            return False
        turned_replicas_names = set()
        for r in replics_info:
            if r['application_name'] in side_replicas_app_names and r['state'] == 'streaming':
                turned_replicas_names.add(r['application_name'])
        waiting_replicas_names = side_replicas_app_names - turned_replicas_names
        logging.info('Replicas streaming from the candidate: %s, waiting for %s', turned_replicas_names, waiting_replicas_names)
        return turned_replicas_names == side_replicas_app_names

    def _zk_alive_refresh(self, role, db_state, zk_state):
        self._replication_manager.drop_zk_fail_timestamp()
        if role is None:
            self.zk.release_lock(self.zk.get_host_alive_lock_path())
        else:
            self._is_single_node = self.zk.update_single_node_status(role)
            if self.zk.get_current_lock_holder(self.zk.get_host_alive_lock_path()) is None:
                logging.warning("I don't hold my alive lock, let's acquire it")
                self.zk.try_acquire_lock(self.zk.get_host_alive_lock_path())

    def _store_replics_info(self, db_state, zk_state):
        tli_res = None
        if zk_state[self.zk.TIMELINE_INFO_PATH]:
            tli_res = zk_state[self.zk.TIMELINE_INFO_PATH] == db_state['timeline']

        replics_info = db_state.get('replics_info')

        zk_state['replics_info_written'] = None
        if tli_res and replics_info is not None:
            zk_state['replics_info_written'] = self.zk.write_replics_info(replics_info)
            self.write_host_stat(helpers.get_hostname(), db_state)
            return True

        return False
    
    def stop_postgresql(self, timeout=60, wait=True):
        return self.db.stop_postgresql(timeout=timeout, wait=wait)


def build_pgconsul_config(config: RawConfigParser) -> PgconsulConfig:
    """Parse INI sections for the orchestrator (ADR-0004)."""
    return PgconsulConfig(
        # [global]
        welcome_message=config.get('global', 'welcome_message', fallback=''),
        working_dir=config.get('global', 'working_dir'),
        iteration_timeout=config.getfloat('global', 'iteration_timeout'),
        quorum_commit=config.getboolean('global', 'quorum_commit'),
        update_prio_in_zk=config.getboolean('global', 'update_prio_in_zk'),
        use_replication_slots=config.getboolean('global', 'use_replication_slots'),
        replication_slots_polling=config.getboolean('global', 'replication_slots_polling'),
        priority=config.get('global', 'priority'),
        stream_from=config.get('global', 'stream_from', fallback=None),
        autofailover=config.getboolean('global', 'autofailover'),
        switchover_timeout=config.getfloat('global', 'switchover_timeout'),
        switchover_catchup_timeout=config.getfloat('global', 'switchover_catchup_timeout'),
        max_rewind_retries=config.getint('global', 'max_rewind_retries'),
        do_consecutive_primary_switch=config.getboolean('global', 'do_consecutive_primary_switch'),
        max_allowed_switchover_lag_ms=config.getint('global', 'max_allowed_switchover_lag_ms'),
        # [replica]
        close_detached_after=config.getfloat('replica', 'close_detached_after'),
        start_pooler=config.getboolean('replica', 'start_pooler'),
        recovery_timeout=config.getfloat('replica', 'recovery_timeout'),
        can_delayed=config.getboolean('replica', 'can_delayed'),
        primary_switch_disable_archive_restore=config.getboolean('replica', 'primary_switch_disable_archive_restore'),
        primary_switch_checks=config.getint('replica', 'primary_switch_checks'),
        primary_switch_restart=config.getboolean('replica', 'primary_switch_restart'),
        primary_unavailability_timeout=config.getfloat('replica', 'primary_unavailability_timeout'),
        walreceiver_disable_timeout=config.getfloat('replica', 'walreceiver_disable_timeout'),
        min_failover_timeout=config.getfloat('replica', 'min_failover_timeout'),
        # [primary]
        change_replication_type=config.getboolean('primary', 'change_replication_type'),
        sync_replication_in_maintenance=config.getboolean('primary', 'sync_replication_in_maintenance'),
        # [debug]
        promote_checkpoint_sql=config.get('debug', 'promote_checkpoint_sql', fallback=None),
        failure_name=config.get('debug', 'failure_name', fallback=None),
        failure_count=int(config.get('debug', 'failure_count', fallback='100000000')),
        sleep_before_disable_walreceiver=config.getfloat('debug', 'sleep_before_disable_walreceiver', fallback=0),
        election_lsn_read_sleep=config.getfloat('debug', 'election_lsn_read_sleep', fallback=0),
        election_loser_timeout=config.getint('debug', 'election_loser_timeout', fallback=0),
        local_state_directory=config.get('global', 'local_state_directory', fallback='/var/cache/pgconsul'),
        use_lwaldump=config.getboolean('global', 'use_lwaldump', fallback=False),
        use_pg_patches=config.getboolean(
            'global', 'use_pg_patches', fallback=False,
        ),
        use_target_promote=config.getboolean(
            'global', 'use_target_promote', fallback=False,
        ),
    )


def create_pgconsul(config: RawConfigParser) -> 'Pgconsul':
    """Create all components and inject them into Pgconsul (ADR-0004)."""
    pgconsul_config = build_pgconsul_config(config)

    cmd_manager = create_command_manager(config)
    db = create_postgres(config=config, cmd_manager=cmd_manager)
    zk = create_zk(config=config)
    replication_manager = create_replication_manager(config, db, zk)
    slot_manager = create_replication_slot_manager(config, db, zk)
    timings = TimingTracker(zk, config.get('commands', 'log_timing', fallback=None))
    maintenance_handler = create_maintenance_handler(config, db, zk, replication_manager)

    return Pgconsul(
        config=pgconsul_config,
        db=db,
        zk=zk,
        cmd_manager=cmd_manager,
        replication_manager=replication_manager,
        slot_manager=slot_manager,
        timings=timings,
        maintenance_handler=maintenance_handler,
    )


# Backward-compat alias: tests and external code import `pgconsul` (lowercase).
pgconsul = Pgconsul
