"""
Tests for the monitored-build task summary rollup.

Focus:
- collecting monitored-run telemetry
- rendering a readable Markdown summary
"""
from __future__ import annotations

import json
from pathlib import Path

from helpers import load_script_module


task_summary = load_script_module("task_monitor_summary", "Scripts/QA_QC/07_task_monitor_summary.py")


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def test_collect_run_records_and_render_markdown(tmp_path: Path) -> None:
    run_root = tmp_path / "Checks" / "real_parity_runs"
    write_json(
        run_root / "run_a" / "build_telemetry.json",
        {
            "run_id": "run_a",
            "termination_reason": "completed",
            "returncode": 0,
            "wall_clock_seconds": 125.0,
            "first_partition_seconds": 30.0,
            "peak_work_bytes": 1024,
            "peak_duckdb_bytes": 2048,
            "peak_temp_bytes": 512,
            "peak_partition_count": 20,
            "final_partition_count": 20,
            "last_observed_phase": "year 2023 write complete",
            "build_log": "/tmp/run_a/build.log",
            "monitor_log": "/tmp/run_a/monitor.log",
        },
    )
    write_json(
        run_root / "run_a" / "run_meta.json",
        {
            "started_at": "2026-03-10T10:00:00",
            "command": [
                "python3",
                "Scripts/06_build_wide_panel.py",
                "--years",
                "2023:2023",
                "--duckdb-memory-limit",
                "8GB",
                "--scalar-conflict-buckets",
                "16",
            ],
        },
    )
    write_json(
        run_root / "run_b" / "build_telemetry.json",
        {
            "run_id": "run_b",
            "termination_reason": "exit_1",
            "returncode": 1,
            "wall_clock_seconds": 75.0,
            "first_partition_seconds": None,
            "peak_work_bytes": 512,
            "peak_duckdb_bytes": 256,
            "peak_temp_bytes": 128,
            "peak_partition_count": 0,
            "final_partition_count": 0,
            "last_observed_phase": "register parquet input end",
            "build_log": "/tmp/run_b/build.log",
            "monitor_log": "/tmp/run_b/monitor.log",
        },
    )
    write_json(
        run_root / "run_b" / "run_meta.json",
        {
            "started_at": "2026-03-09T09:00:00",
            "command": [
                "python3",
                "Scripts/06_build_wide_panel.py",
                "--years",
                "2004:2023",
                "--duckdb-memory-limit",
                "12GB",
            ],
        },
    )

    rows = task_summary.collect_run_records(run_root)

    assert [row["run_id"] for row in rows] == ["run_a", "run_b"]
    assert rows[0]["duckdb_memory_limit"] == "8GB"
    assert rows[1]["termination_reason"] == "exit_1"

    md = task_summary.render_markdown_summary(rows, markdown_limit=2)

    assert "# Task Monitor Summary" in md
    assert "run_a" in md
    assert "Completed: `1`" in md
