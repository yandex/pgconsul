#!/usr/bin/env python3
"""Analyze failed behave test logs and pinpoint the root cause.

This file is now a thin shim that delegates to the :mod:`failure_analyzer`
package. The original monolithic implementation has been split into modules
(see ``scripts/failure_analyzer/``); behavior and the CLI are unchanged.

Usage:
    # Auto-discover the latest test logs (logs/ or logs.local/)
    python scripts/analyze_failed_scenario.py

    # Point at a specific log directory
    python scripts/analyze_failed_scenario.py logs.local/logs-failover_with_network_inconsistency

    # Also scan a second log tree (e.g. logs.local/2/logs)
    python scripts/analyze_failed_scenario.py logs.local/logs-failover_with_network_inconsistency logs.local/2/logs

    # Machine-readable JSON for CI
    python scripts/analyze_failed_scenario.py --format json
"""

from __future__ import annotations

import os
import sys

# Allow running as a standalone script (``python scripts/analyze_failed_scenario.py``)
# by putting the script directory on sys.path so ``failure_analyzer`` is importable.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

from failure_analyzer.cli import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
