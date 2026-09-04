# encoding: utf-8
"""Persistent host-local state for return-to-cluster reconciliation."""

from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

from ..local_state import LocalStateError
from ..types import StrEnum


class ReturnPhase(StrEnum):
    BLOCKED = 'blocked'
    REQUESTED = 'requested'
    WAITING_ARCHIVE = 'waiting_archive'
    ARCHIVE_CATCHUP = 'archive_catchup'
    STARTING = 'starting'
    REWINDING = 'rewinding'
    STARTING_AFTER_REWIND = 'starting_after_rewind'
    RESETUP_REQUIRED = 'resetup_required'


@dataclass(frozen=True)
class ReturnState:
    operation_id: str
    phase: ReturnPhase
    target_host: str | None = None
    target_timeline: int | None = None
    role: str | None = None
    is_postgresql_dead: bool = False
    track_primary_epoch: bool = True
    start_attempts: int = 0
    rewind_attempts: int = 0
    progress_signature: str | None = None
    progress_since: float | None = None
    target_operation_id: str | None = None
    archive_fork_lsn: int | None = None

    def evolve(self, **changes: Any) -> 'ReturnState':
        return replace(self, **changes)


class ReturnStateStore:
    """Atomically persist the complete return machine state."""

    def __init__(self, directory: str) -> None:
        self.path = Path(directory) / 'return_to_cluster_state.json'

    def read(self) -> ReturnState | None:
        try:
            with self.path.open(encoding='utf-8') as state_file:
                value = json.load(state_file)
            operation_id = value['operation_id']
            if not isinstance(operation_id, str) or not operation_id:
                raise ValueError('invalid operation_id')
            phase = ReturnPhase(value['phase'])
            target_timeline = value.get('target_timeline')
            if target_timeline is not None and not isinstance(target_timeline, int):
                raise ValueError('invalid target_timeline')
            archive_fork_lsn = value.get('archive_fork_lsn')
            if archive_fork_lsn is not None and not isinstance(archive_fork_lsn, int):
                raise ValueError('invalid archive_fork_lsn')
            return ReturnState(
                operation_id=operation_id,
                phase=phase,
                target_host=value.get('target_host'),
                target_timeline=target_timeline,
                target_operation_id=value.get('target_operation_id'),
                archive_fork_lsn=archive_fork_lsn,
                role=value.get('role'),
                is_postgresql_dead=bool(value.get('is_postgresql_dead', False)),
                track_primary_epoch=bool(value.get('track_primary_epoch', True)),
                start_attempts=int(value.get('start_attempts', 0)),
                rewind_attempts=int(value.get('rewind_attempts', 0)),
                progress_signature=value.get('progress_signature'),
                progress_since=value.get('progress_since'),
            )
        except FileNotFoundError:
            return None
        except (KeyError, TypeError, ValueError) as error:
            logging.error('Invalid return state in %s: %s', self.path, error)
            self.clear()
            return None
        except OSError as error:
            raise LocalStateError(str(error)) from error

    def write(self, state: ReturnState) -> None:
        temp_path: Path | None = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd, temp_name = tempfile.mkstemp(
                prefix=f'.{self.path.name}.',
                dir=self.path.parent,
            )
            temp_path = Path(temp_name)
            value = asdict(state)
            value['phase'] = state.phase.value
            with os.fdopen(fd, 'w', encoding='utf-8') as state_file:
                json.dump(value, state_file)
                state_file.flush()
                os.fsync(state_file.fileno())
            os.replace(temp_path, self.path)
            temp_path = None
            self._fsync_directory()
        except OSError as error:
            raise LocalStateError(str(error)) from error
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    logging.exception('Could not remove temporary return state %s', temp_path)

    def clear(self, operation_id: str | None = None) -> None:
        try:
            if operation_id is not None:
                current = self.read()
                if current is None or current.operation_id != operation_id:
                    return
            self.path.unlink(missing_ok=True)
            self._fsync_directory()
        except OSError as error:
            raise LocalStateError(str(error)) from error

    def _fsync_directory(self) -> None:
        directory_fd = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
