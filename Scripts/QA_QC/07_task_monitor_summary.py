#!/usr/bin/env python3
"""
QA 07: summarize monitored-build telemetry into readable cross-run artifacts.

Reads:
- `Checks/real_parity_runs/*/build_telemetry.json`
- optional `run_meta.json` files from the same run directories

Writes:
- `Checks/real_parity_runs/summary/task_monitor_summary.csv`
- `Checks/real_parity_runs/summary/task_monitor_summary.md`

Focus:
- lightweight run history
- operator-facing monitoring summary
- one place to inspect monitored-build outcomes
"""
from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import datetime
from pathlib import Path


def default_data_root() -> Path:
    return Path(os.environ.get("IPEDSDB_ROOT", "/Users/markjaysonfarol13/Projects/IPEDSDB_Paneling"))


def parse_args() -> argparse.Namespace:
    data_root = default_data_root()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run-dir-root", default=str(data_root / "Checks" / "real_parity_runs"), help="Root containing monitored run folders")
    p.add_argument("--output-dir", default=None, help="Where summary artifacts should be written")
    p.add_argument("--markdown-limit", type=int, default=12, help="How many recent runs to include in the Markdown table")
    return p.parse_args()


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def command_arg_value(command: list[str], flag: str) -> str:
    for idx, token in enumerate(command):
        if token == flag and idx + 1 < len(command):
            return str(command[idx + 1])
    return ""


def format_bytes(num_bytes: int | None) -> str:
    if num_bytes is None:
        return ""
    value = float(num_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if value < 1024.0 or unit == "TB":
            return f"{value:.1f}{unit}"
        value /= 1024.0
    return ""


def format_seconds(seconds: float | int | None) -> str:
    if seconds is None:
        return ""
    whole = max(0, int(round(float(seconds))))
    hours, rem = divmod(whole, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def collect_run_records(run_dir_root: Path) -> list[dict]:
    rows: list[dict] = []
    for telemetry_path in sorted(run_dir_root.glob("*/build_telemetry.json")):
        run_dir = telemetry_path.parent
        telemetry = load_json(telemetry_path)
        meta_path = run_dir / "run_meta.json"
        meta = load_json(meta_path) if meta_path.exists() else {}
        command = meta.get("command", []) if isinstance(meta.get("command"), list) else []

        rows.append(
            {
                "run_id": str(telemetry.get("run_id", run_dir.name)),
                "started_at": str(meta.get("started_at", "")),
                "termination_reason": str(telemetry.get("termination_reason", "")),
                "returncode": telemetry.get("returncode"),
                "wall_clock_seconds": telemetry.get("wall_clock_seconds"),
                "first_partition_seconds": telemetry.get("first_partition_seconds"),
                "peak_work_bytes": telemetry.get("peak_work_bytes"),
                "peak_duckdb_bytes": telemetry.get("peak_duckdb_bytes"),
                "peak_temp_bytes": telemetry.get("peak_temp_bytes"),
                "peak_partition_count": telemetry.get("peak_partition_count"),
                "final_partition_count": telemetry.get("final_partition_count"),
                "last_observed_phase": str(telemetry.get("last_observed_phase", "")),
                "years": command_arg_value(command, "--years"),
                "duckdb_memory_limit": command_arg_value(command, "--duckdb-memory-limit"),
                "scalar_conflict_buckets": command_arg_value(command, "--scalar-conflict-buckets"),
                "run_dir": str(run_dir),
                "build_log": str(telemetry.get("build_log", "")),
                "monitor_log": str(telemetry.get("monitor_log", "")),
            }
        )
    rows.sort(key=lambda row: str(row.get("started_at", "")), reverse=True)
    return rows


def write_summary_csv(out_path: Path, rows: list[dict]) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "run_id",
        "started_at",
        "termination_reason",
        "returncode",
        "wall_clock_seconds",
        "first_partition_seconds",
        "peak_work_bytes",
        "peak_duckdb_bytes",
        "peak_temp_bytes",
        "peak_partition_count",
        "final_partition_count",
        "last_observed_phase",
        "years",
        "duckdb_memory_limit",
        "scalar_conflict_buckets",
        "run_dir",
        "build_log",
        "monitor_log",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def render_markdown_summary(rows: list[dict], markdown_limit: int) -> str:
    generated_at = datetime.now().isoformat(timespec="seconds")
    completed = sum(1 for row in rows if row.get("termination_reason") == "completed")
    failed = len(rows) - completed
    latest = rows[0] if rows else None

    lines = [
        "# Task Monitor Summary",
        "",
        f"Generated at: `{generated_at}`",
        "",
        f"- Runs discovered: `{len(rows)}`",
        f"- Completed: `{completed}`",
        f"- Non-completed: `{failed}`",
    ]
    if latest:
        lines.extend(
            [
                f"- Latest run: `{latest['run_id']}`",
                f"- Latest status: `{latest['termination_reason']}`",
                f"- Latest elapsed: `{format_seconds(latest.get('wall_clock_seconds'))}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Recent Runs",
            "",
            "| Run | Started | Status | Elapsed | First partition | Peak work | Peak DuckDB | Peak temp | Last phase |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )

    for row in rows[: max(0, int(markdown_limit))]:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("run_id", "")),
                    str(row.get("started_at", ""))[:19],
                    str(row.get("termination_reason", "")),
                    format_seconds(row.get("wall_clock_seconds")),
                    format_seconds(row.get("first_partition_seconds")),
                    format_bytes(row.get("peak_work_bytes")),
                    format_bytes(row.get("peak_duckdb_bytes")),
                    format_bytes(row.get("peak_temp_bytes")),
                    str(row.get("last_observed_phase", "")),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Files",
            "",
            "- `task_monitor_summary.csv` is the machine-readable run log.",
            "- Each run directory still contains the original `build.log`, `monitor.log`, and `build_telemetry.json` files.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    run_dir_root = Path(args.run_dir_root)
    output_dir = Path(args.output_dir) if args.output_dir else (run_dir_root / "summary")
    rows = collect_run_records(run_dir_root)

    csv_path = output_dir / "task_monitor_summary.csv"
    md_path = output_dir / "task_monitor_summary.md"
    write_summary_csv(csv_path, rows)
    md_path.write_text(render_markdown_summary(rows, args.markdown_limit), encoding="utf-8")

    print(f"summary_csv: {csv_path}")
    print(f"summary_md: {md_path}")
    print(f"runs: {len(rows)}")


if __name__ == "__main__":
    main()
