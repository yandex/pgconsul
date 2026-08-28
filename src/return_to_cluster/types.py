# encoding: utf-8
"""
Return-to-cluster domain types (MDB-41951, ADR-0006).

Stateless decision: action is re-derived from observation each call.
Distinguishes transient simple-switch failures from real WAL divergence
to avoid unnecessary pg_rewind.
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..exceptions import PostgresConnectionError
from ..helpers import is_op_destructive
from .timeline_history import (
    TimelineSwitch,
    parse_timeline_history,
    timeline_requires_rewind,
    wal_filenames_before_switch,
)

if TYPE_CHECKING:
    from ..pg import Postgres
    from ..zk import Zookeeper


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
    local_lsn: int | None = None
    timeline_history: tuple[TimelineSwitch, ...] | None = None
    timeline_history_value: str | None = None
    required_wal_filename: str | None = None
    required_wal_archived: bool | None = None

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

        local_lsn: int | None = None
        timeline_history: tuple[TimelineSwitch, ...] | None = None
        timeline_history_value: str | None = None
        required_wal_filename: str | None = None
        required_wal_archived: bool | None = None
        if (
            local_timeline is not None
            and zk_timeline is not None
            and local_timeline != zk_timeline
        ):
            try:
                local_lsn = db.get_wal_flush_lsn()
            except PostgresConnectionError:
                logging.debug('Could not read local WAL LSN', exc_info=True)
            if zk_timeline == 1:
                timeline_history = ()
                required_wal_archived = True
            else:
                history_value = db.fetch_timeline_history(zk_timeline)
                if history_value is not None:
                    try:
                        timeline_history = parse_timeline_history(
                            history_value, zk_timeline,
                        )
                        timeline_history_value = history_value
                        needs_rewind = (
                            (role or fallback_role) == 'primary'
                            or is_op_destructive(last_op)
                            or local_lsn is None
                            or timeline_requires_rewind(
                                local_timeline,
                                local_lsn,
                                zk_timeline,
                                timeline_history,
                            )
                        )
                        if needs_rewind:
                            segment_size = db.get_wal_segment_size()
                            if timeline_history and segment_size is not None:
                                filenames = wal_filenames_before_switch(
                                    timeline_history[-1], segment_size,
                                )
                                required_wal_filename = filenames[0]
                                for filename in filenames:
                                    required_wal_filename = filename
                                    if db.is_wal_archived(filename):
                                        required_wal_archived = True
                                        break
                                else:
                                    required_wal_archived = False
                    except (TypeError, ValueError):
                        logging.warning(
                            'Invalid timeline %s history fetched from archive',
                            zk_timeline,
                            exc_info=True,
                        )

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
            local_lsn=local_lsn,
            timeline_history=timeline_history,
            timeline_history_value=timeline_history_value,
            required_wal_filename=required_wal_filename,
            required_wal_archived=required_wal_archived,
        )


def timelines_match(local: int | None, zk: int | None) -> bool:
    """True if both timelines are known and equal."""
    return local is not None and zk is not None and local == zk
