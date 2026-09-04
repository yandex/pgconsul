"""Imperative shell for actions selected by :mod:`switchover.machine`."""

from __future__ import annotations

import logging
import time
from typing import Any

from ..commands import SwitchoverStep
from .. import helpers
from ..types import ReplicaInfos
from .types import SwitchoverPhase


class SwitchoverExecutor:
    """Execute the single typed switchover step selected for an iteration.

    The owner supplies infrastructure-bound primitives.  Keeping this dispatch
    outside ``main.py`` prevents the main loop from being a second phase router.
    """

    def __init__(self, owner: Any) -> None:
        self._owner = owner

    @staticmethod
    def select_candidate(owner: Any, replica_infos: ReplicaInfos) -> str | None:
        """Choose the highest-priority live streaming stable member."""
        durability = owner.zk.get_durability_config()
        if durability is None:
            return None
        hosts_by_app = {
            helpers.app_name_from_fqdn(host): host for host in durability.members
        }
        candidates: list[tuple[int, int, str]] = []
        for info in replica_infos:
            if info.get('state') != 'streaming':
                continue
            host = hosts_by_app.get(str(info.get('application_name')))
            if host is None or not owner.zk.is_host_alive(
                host, timeout=1, catch_except=False,
            ):
                continue
            priority = owner.zk.get_host_prio(host, catch_except=False)
            if priority is None:
                logging.warning('No priority is available for switchover candidate %s', host)
                continue
            try:
                candidates.append((
                    int(priority), int(info.get('write_location_diff', 0)), host,
                ))
            except (TypeError, ValueError):
                logging.warning('Invalid switchover candidate state for %s', host)
        return min(candidates, key=lambda item: (-item[0], item[1], item[2]))[2] if candidates else None

    def execute(self, step: SwitchoverStep) -> bool:
        owner = self._owner
        record = step.record
        action = step.action
        if action == 'cleanup_invalid':
            logging.error('Invalid switchover record; removing it')
            if not owner._try_acquire_switchover_manager():
                return False
            if record.version is not None:
                owner.zk.cleanup_switchover(record.version)
            return owner.zk.release_if_hold(owner.zk.SWITCHOVER_MANAGER_LOCK_PATH)
        if action == 'cleanup':
            return owner._cleanup_switchover(record)
        if action == 'initialize_deadline':
            record = owner._claim_switchover_manager(record)
            if record is None:
                return False
            started_at = record.started_at if record.started_at is not None else time.time()
            return owner._write_switchover_record(
                record,
                started_at=started_at,
                deadline_at=started_at + owner.config.switchover_timeout,
            ) is not None
        if action == 'rollback_pre_handoff_timeout':
            owner._rollback_switchover_before_handoff(
                record, dict(step.db_state), dict(step.zk_state),
            )
            return True
        if action == 'recover_committed_handoff_timeout':
            owner._recover_committed_switchover_timeout(
                record, dict(step.db_state), dict(step.zk_state),
            )
            return True
        if action == 'recover_pre_handoff':
            return owner._recover_pre_handoff_switchover(
                record, dict(step.db_state), dict(step.zk_state),
            )
        if action == 'schedule_cleanup':
            record = owner._claim_switchover_manager(record)
            if record is None:
                return False
            scheduled = bool(
                record.version is not None
                and owner._schedule_switchover_cleanup(record)
            )
            if record.phase == SwitchoverPhase.FAILED and scheduled:
                logging.info('Scheduled failed switchover cleanup')
            return scheduled
        holder = step.zk_state.get('lock_holder')
        primary_actions = {
            'primary_schedule': owner._switchover_primary_schedule,
            'primary_prepare_durability': owner._switchover_primary_prepare_durability,
            'primary_prepare_candidate': owner._switchover_primary_prepare_candidate,
            'primary_turn_sides': owner._switchover_primary_turn_sides,
            'primary_confirm_promotion': owner._switchover_primary_confirm_promotion,
            'primary_fence_return': owner._switchover_primary_fence_return,
        }
        if action in primary_actions:
            return primary_actions[action](record, dict(step.db_state), holder)
        candidate_actions = {
            'candidate_prepare': owner._switchover_candidate_prepare,
            'candidate_promote': owner._switchover_candidate_promote,
            'candidate_wait_archive': owner._switchover_candidate_wait_archive,
            'candidate_wait_recovery': owner._switchover_candidate_wait_recovery,
        }
        if action in candidate_actions:
            return candidate_actions[action](record, dict(step.db_state), holder)
        if action in {'side_turn', 'side_wait_archive'}:
            owner._switchover_side_turn(
                record,
                dict(step.db_state),
                checkpoint_after_promote=action == 'side_wait_archive',
            )
            return True
        logging.error('Unknown switchover action: %s', action)
        return False
