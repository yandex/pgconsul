"""Pytest configuration: make the ``failure_analyzer`` package importable.

Tests live inside the package (``failure_analyzer/tests``), so the parent
``scripts`` directory must be on ``sys.path`` for ``import failure_analyzer``
to work when running pytest from the repo root.
"""

from __future__ import annotations

import os
import sys

_SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)
