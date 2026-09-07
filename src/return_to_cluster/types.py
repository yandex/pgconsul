# encoding: utf-8
"""
Return-to-cluster domain types (MDB-41951, ADR-0006).

Stateless decision: action is re-derived from observation each call.
Distinguishes transient simple-switch failures from real WAL divergence
to avoid unnecessary pg_rewind.
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping

from ..exceptions import PostgresConnectionError
from ..helpers import is_op_destructive
from .timeline_history import (
    TimelineSwitch,
    parse_timeline_history,
    wal_filenames_from_checkpoint_to_target,
    wal_filename_on_timeline,
)

if TYPE_CHECKING:
    from ..pg import Postgres
    from ..zk import Zookeeper


@dataclass(frozen=True)
class ReturnObservation:
    """Immutable snapshot — sole handler input (ADR-0006 §1)."""

    role: str | None
    local_timeline: int | None
    zk_timeline: int | None
    last_op: str | None
    # Previous role before PG death — used when role is None (dead PG).
    # dead_iter() passes self.db.role so the machine can detect former
    # primaries and force REWIND instead of SIMPLE_SWITCH.
    fallback_role: str | None = None
    local_lsn: int | None = None
    timeline_history: tuple[TimelineSwitch, ...] | None = None
    timeline_history_value: str | None = None
    required_wal_filename: str | None = None
    required_wal_archived: bool | None = None
    fork_lsn: int | None = None
    # A replica may first ask the target primary for pre-fork WAL.  Archive
    # recovery is the bounded fallback when that source makes no progress.
    primary_first: bool = False

    @classmethod
    def build(
        cls,
        zk: 'Zookeeper',
        db: 'Postgres',
        my_hostname: str,
        db_state: Mapping[str, Any],
        *,
        fallback_role: str | None = None,
        primary_first: bool = False,
    ) -> 'ReturnObservation':
        """Assemble the observation — sole I/O read point for a step."""
        role = db_state.get('role')
        local_timeline = db_state.get('timeline')

        zk_timeline = zk.get_timeline()
        last_op = zk.noexcept_get('%s/%s/op' % (zk.MEMBERS_PATH, my_hostname))
        forced_rewind = (
            (role or fallback_role) == 'primary'
            or is_op_destructive(last_op)
        )

        local_lsn: int | None = None
        timeline_history: tuple[TimelineSwitch, ...] | None = None
        timeline_history_value: str | None = None
        required_wal_filename: str | None = None
        required_wal_filenames: tuple[str, ...] = ()
        required_wal_archived: bool | None = None
        fork_lsn: int | None = None
        if (
            local_timeline is not None
            and zk_timeline is not None
            and local_timeline != zk_timeline
        ):
            # Former primaries have no replay LSN and always rewind after the archive barrier.
            if not forced_rewind:
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
                        switch = next(
                            (
                                item for item in timeline_history
                                if item.timeline == local_timeline
                            ),
                            None,
                        )
                        fork_lsn = switch.switch_lsn if switch is not None else None
                        barrier_switch = switch or (timeline_history[-1] if timeline_history else None)
                        if barrier_switch is not None:
                            segment_size = db.get_wal_segment_size()
                            if segment_size is not None:
                                if switch is not None:
                                    checkpoint_redo_lsn = db.get_checkpoint_redo_lsn()
                                    if checkpoint_redo_lsn is not None:
                                        try:
                                            required_wal_filenames = wal_filenames_from_checkpoint_to_target(
                                                local_timeline=local_timeline,
                                                checkpoint_redo_lsn=checkpoint_redo_lsn,
                                                target_timeline=zk_timeline,
                                                history=timeline_history,
                                                segment_size=segment_size,
                                            )
                                        except ValueError:
                                            logging.warning(
                                                'Could not derive archive WAL chain from checkpoint to timeline %s',
                                                zk_timeline,
                                                exc_info=True,
                                            )
                                if not required_wal_filenames:
                                    required_wal_filenames = (wal_filename_on_timeline(
                                        barrier_switch,
                                        zk_timeline,
                                        segment_size,
                                    ),)
                                missing_wal = next(
                                    (
                                        filename for filename in required_wal_filenames
                                        if not db.is_wal_archived(filename)
                                    ),
                                    None,
                                )
                                required_wal_filename = missing_wal or required_wal_filenames[-1]
                                required_wal_archived = missing_wal is None
                    except (TypeError, ValueError):
                        logging.warning(
                            'Invalid timeline %s history fetched from archive',
                            zk_timeline,
                            exc_info=True,
                        )

        return cls(
            role=role,
            local_timeline=local_timeline,
            zk_timeline=zk_timeline,
            last_op=last_op,
            fallback_role=fallback_role,
            local_lsn=local_lsn,
            timeline_history=timeline_history,
            timeline_history_value=timeline_history_value,
            required_wal_filename=required_wal_filename,
            required_wal_archived=required_wal_archived,
            fork_lsn=fork_lsn,
            primary_first=primary_first,
        )
