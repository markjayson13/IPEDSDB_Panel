"""
Dry-run tests for Stage 00 orchestration.

Focus:
- stage ordering
- skip-flag behavior
- QA routing and custom-panel input selection
"""
from __future__ import annotations

from pathlib import Path

from helpers import run_script


def command_lines(stdout: str) -> list[str]:
    return [line.strip() for line in stdout.splitlines() if line.strip().startswith("+ ")]


def test_run_all_dry_run_orders_full_pipeline_with_cleaning_and_qaqc(tmp_path: Path) -> None:
    root = tmp_path / "data_root"

    result = run_script(
        "Scripts/00_run_all.py",
        "--root",
        root,
        "--years",
        "2022:2023",
        "--run-cleaning",
        "--run-qaqc",
        "--dry-run",
        env={"IPEDSDB_ROOT": str(root)},
    )

    assert result.returncode == 0, result.stdout
    cmds = command_lines(result.stdout)
    ordered_scripts = [
        "01_download_access_databases.py",
        "02_extract_access_db.py",
        "03_dictionary_ingest.py",
        "04_harmonize.py",
        "05_stitch_long.py",
        "06_build_wide_panel.py",
        "07_clean_panel.py",
        "00_dictionary_qaqc.py",
        "01_panel_qa.py",
    ]
    assert len(cmds) == len(ordered_scripts)
    for line, script_name in zip(cmds, ordered_scripts, strict=True):
        assert script_name in line


def test_run_all_dry_run_respects_skip_flags_and_routes_custom_from_wide_when_not_cleaning(tmp_path: Path) -> None:
    root = tmp_path / "data_root"
    custom_out = root / "Panels" / "custom_subset.parquet"

    result = run_script(
        "Scripts/00_run_all.py",
        "--root",
        root,
        "--years",
        "2022:2023",
        "--skip-download",
        "--skip-extract",
        "--skip-dictionary",
        "--skip-harmonize",
        "--skip-stitch",
        "--build-custom",
        "--custom-vars",
        "INSTNM,CONTROL",
        "--custom-output",
        custom_out,
        "--run-qaqc",
        "--dry-run",
        env={"IPEDSDB_ROOT": str(root)},
    )

    assert result.returncode == 0, result.stdout
    cmds = command_lines(result.stdout)
    assert len(cmds) == 3
    assert "06_build_wide_panel.py" in cmds[0]
    assert "08_build_custom_panel.py" in cmds[1]
    assert "00_dictionary_qaqc.py" in cmds[2]
    assert "01_panel_qa.py" not in result.stdout

    expected_wide = root / "Panels" / "panel_wide_analysis_2022_2023.parquet"
    assert f"--input {expected_wide}" in cmds[1]
    assert f"--output {custom_out}" in cmds[1]
