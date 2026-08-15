# encoding: utf-8
"""
Return-to-cluster domain types (MDB-41951, ADR-0006).

Stateless machine: phases are in-memory only, re-derived from observations
each call. Distinguishes transient simple-switch failures from real WAL
divergence to avoid unnecessary pg_rewind.
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..types import StrEnum

if TYPE_CHECKING:
    from ..pg import Postgres
    from ..zk import Zookeeper


class ReturnPhase(StrEnum):
    """In-memory phases (not persisted to ZK).

    Only the phases reachable via _derive_phase are defined here.
    """

    SIMPLE_SWITCH = 'simple_switch'
    CHECK_DIVERGENCE = 'check_divergence'
    REWIND = 'rewind'


@dataclass(frozen=True)
class ReturnObservation:
    """Immutable snapshot — sole handler input (ADR-0006 §1)."""

    new_primary: str
    role: str | None
    local_timeline: int | None
    zk_timeline: int | None
    last_op: str | None
    simple_switch_tried: bool
    archive_restore_disabled: bool
    recovery_timeout: float
    is_dead: bool
    # Previous role before PG death — used when role is None (dead PG).
    # dead_iter() passes self.db.role so the machine can detect former
    # primaries and force REWIND instead of SIMPLE_SWITCH.
    fallback_role: str | None = None

    @classmethod
    def build(
        cls,
        zk: 'Zookeeper',
        db: 'Postgres',
        my_hostname: str,
        db_state: dict,
        new_primary: str,
        is_dead: bool,
        recovery_timeout: float,
        *,
        simple_switch_tried: bool,
        fallback_role: str | None = None,
    ) -> 'ReturnObservation':
        """Assemble the observation — sole I/O read point for a step."""
        role = db_state.get('role')
        local_timeline = db_state.get('timeline')

        zk_timeline = zk.get_timeline()
        last_op = zk.noexcept_get('%s/%s/op' % (zk.MEMBERS_PATH, my_hostname))

        archive_restore_disabled = False
        try:
            restore_cmd = db.get_restore_command()
            archive_restore_disabled = restore_cmd == '/bin/false' or restore_cmd == 'false'
        except Exception:
            logging.debug('get_restore_command failed, archive_restore_disabled=False', exc_info=True)

        return cls(
            new_primary=new_primary,
            role=role,
            local_timeline=local_timeline,
            zk_timeline=zk_timeline,
            last_op=last_op,
            simple_switch_tried=simple_switch_tried,
            archive_restore_disabled=archive_restore_disabled,
            recovery_timeout=recovery_timeout,
            is_dead=is_dead,
            fallback_role=fallback_role,
        )


def timelines_match(local: int | None, zk: int | None) -> bool:
    """True if both timelines are known and equal."""
    return local is not None and zk is not None and local == zk
