# encoding: utf-8
"""Host-local persistence for retryable state-machine command groups."""

import json
import logging
import os
import tempfile
from pathlib import Path


class LocalStateError(Exception):
    """Base error for host-local state persistence."""


class LocalStateInvalid(LocalStateError):
    """The local state file is malformed or contains an unknown phase."""


class LocalStateStore:
    """Persist one current command-group name with atomic durable updates."""

    def __init__(self, filename: str, allowed_phases: set[str], directory: str) -> None:
        self.path = Path(directory) / filename
        self._allowed_phases = frozenset(allowed_phases)

    def read(self) -> str | None:
        try:
            with self.path.open(encoding='utf-8') as state_file:
                value = json.load(state_file)
            phase = value['phase']
            if not isinstance(phase, str) or phase not in self._allowed_phases:
                raise ValueError(f'unknown phase: {phase!r}')
            return phase
        except FileNotFoundError:
            return None
        except (KeyError, TypeError, ValueError) as error:
            logging.error('Invalid local state in %s: %s', self.path, error)
            try:
                self.clear()
            except LocalStateError:
                logging.exception('Could not clear invalid local state %s', self.path)
            raise LocalStateInvalid(str(error)) from error
        except OSError as error:
            raise LocalStateError(str(error)) from error

    def write(self, phase: str) -> None:
        if phase not in self._allowed_phases:
            raise LocalStateInvalid(f'unknown phase: {phase!r}')
        temp_path: Path | None = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd, temp_name = tempfile.mkstemp(
                prefix=f'.{self.path.name}.',
                dir=self.path.parent,
            )
            temp_path = Path(temp_name)
            with os.fdopen(fd, 'w', encoding='utf-8') as state_file:
                json.dump({'phase': phase}, state_file)
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
                    logging.exception('Could not remove temporary local state %s', temp_path)

    def clear(self) -> None:
        try:
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass
            self._fsync_directory()
        except FileNotFoundError:
            return
        except OSError as error:
            raise LocalStateError(str(error)) from error

    def _fsync_directory(self) -> None:
        directory_fd = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
