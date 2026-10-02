from __future__ import annotations

import io
import json
from pathlib import Path

from scripts.analyze_jepsen_failure import analyze, main


def _write_logs(root: Path) -> None:
    (root / "postgresql2").mkdir(parents=True)
    (root / "jepsen.log").write_text(
        "INFO [2026-09-12 01:05:04,678] worker\t:info\t:kill\t"
        '{:value [:killed :pgconsul :on "pgconsul_postgresql2_1.pgconsul_pgconsul_net"]}\n'
        "INFO [2026-09-12 02:56:00,000] worker\t:invoke\t:read\tnil\n"
        "INFO [2026-09-12 02:56:00,001] worker\t:info\t:read\tnil\tnoop\n"
        'INFO [2026-09-12 02:57:28,001] runner {:valid? false, :error "Set was never read"}\n'
    )
    (root / "postgresql2" / "pgconsul.log").write_text(
        "2026-09-12 01:05:05,778 ERROR : Could not initialize ZooKeeper connection\n"
        "2026-09-12 01:05:07,983 ERROR : Could not initialize ZooKeeper connection\n"
    )


def test_correlates_nemesis_kill_with_zk_startup_failures(tmp_path: Path) -> None:
    _write_logs(tmp_path)

    result = analyze(tmp_path)

    assert result.valid is False
    assert result.checker_error == "Set was never read"
    assert result.operations["read"] == {"invoke": 1, "ok": 0, "fail": 0, "info": 1}
    assert result.restart_failures[0].node == "postgresql2"
    assert result.restart_failures[0].failures == 2
    assert result.likely_root_cause == "Jepsen checker failed: Set was never read"


def test_does_not_correlate_old_startup_failure(tmp_path: Path) -> None:
    _write_logs(tmp_path)
    log = tmp_path / "postgresql2" / "pgconsul.log"
    log.write_text("2026-09-12 01:04:00,000 ERROR : Could not initialize ZooKeeper connection\n")

    result = analyze(tmp_path)

    assert result.restart_failures == []
    assert result.likely_root_cause == "Jepsen checker failed: Set was never read"


def test_repeated_startup_failures_are_likely_root_cause(tmp_path: Path) -> None:
    _write_logs(tmp_path)
    log = tmp_path / "postgresql2" / "pgconsul.log"
    log.write_text(
        "".join(
            f"2026-09-12 01:05:{second:02d},000 ERROR : Could not initialize ZooKeeper connection\n"
            for second in (5, 8, 11, 15)
        )
    )

    result = analyze(tmp_path)

    assert "repeatedly failed to restart while ZooKeeper was unavailable" in result.likely_root_cause


def test_json_cli_returns_failure_status(tmp_path: Path) -> None:
    _write_logs(tmp_path)
    stdout = io.StringIO()
    stderr = io.StringIO()

    status = main([str(tmp_path), "--format", "json"], stdout, stderr)

    assert status == 1
    assert stderr.getvalue() == ""
    assert json.loads(stdout.getvalue())["restart_failures"][0]["node"] == "postgresql2"
