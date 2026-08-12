# encoding: utf-8
"""
Return-to-cluster domain types (MDB-41951, ADR-0006).

Stateless machine: phases are in-memory only, re-derived from observations
each call. Distinguishes transient simple-switch failures from real WAL
divergence to avoid unnecessary pg_rewind.
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..pg import Postgres
    from ..zk import Zookeeper


class ReturnPhase(str, Enum):
    """In-memory phases (not persisted to ZK)."""

    INIT = 'init'
    SIMPLE_SWITCH = 'simple_switch'
    CHECK_DIVERGENCE = 'check_divergence'
    WAIT_CANDIDATE = 'wait_candidate'
    RETRY_SIMPLE = 'retry_simple'
    REWIND = 'rewind'
    DONE = 'done'

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class ReturnObservation:
    """Immutable snapshot — sole handler input (ADR-0006 §1)."""

    new_primary: str
    role: str | None
    local_timeline: int | None
    zk_timeline: int | None
    last_op: str | None
    simple_switch_tried: bool
    candidate_reachable: bool | None
    archive_restore_disabled: bool
    recovery_timeout: float
    is_dead: bool
    skip_check: bool
    failover_state: str | None

    @classmethod
    def build(
        cls,
        zk: 'Zookeeper',
        db: 'Postgres',
        my_hostname: str,
        db_state: dict,
        new_primary: str,
        is_dead: bool,
        skip_check: bool,
        recovery_timeout: float,
        *,
        simple_switch_tried: bool,
    ) -> 'ReturnObservation':
        """Assemble the observation — sole I/O read point for a step."""
        role = db_state.get('role')
        local_timeline = db_state.get('timeline')

        zk_timeline = zk.get_timeline()
        last_op = zk.noexcept_get('%s/%s/op' % (zk.MEMBERS_PATH, my_hostname))
        failover_state = zk.get_failover_state()

        candidate_reachable: bool | None
        try:
            candidate_reachable = not db.is_host_unreachable(new_primary, check_primary=False)
        except Exception:
            candidate_reachable = None

        archive_restore_disabled = False
        try:
            restore_cmd = db._get_param_value('restore_command')
            archive_restore_disabled = restore_cmd == '/bin/false' or restore_cmd == 'false'
        except Exception:
            pass

        return cls(
            new_primary=new_primary,
            role=role,
            local_timeline=local_timeline,
            zk_timeline=zk_timeline,
            last_op=last_op,
            simple_switch_tried=simple_switch_tried,
            candidate_reachable=candidate_reachable,
            archive_restore_disabled=archive_restore_disabled,
            recovery_timeout=recovery_timeout,
            is_dead=is_dead,
            skip_check=skip_check,
            failover_state=failover_state,
        )


@dataclass
class ReturnMachineConfig:
    """Config subset for the return-to-cluster machine."""

    primary_switch_disable_archive_restore: bool = False
    primary_switch_checks: int = 3
    max_rewind_retries: int = 3
    recovery_timeout: float = 60.0


def is_op_destructive(last_op: str | None) -> bool:
    """Delegate to helpers.is_op_destructive (kept local for purity)."""
    if last_op is None:
        return False
    from ..helpers import is_op_destructive as _is_destructive
    return _is_destructive(last_op)


def timelines_match(local: int | None, zk: int | None) -> bool:
    """True if both timelines are known and equal."""
    return local is not None and zk is not None and local == zk
