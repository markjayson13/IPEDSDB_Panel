"""
Tests for release hardening scripts added for public infrastructure use.

Focus:
- runtime environment reports are machine-readable
- external benchmark reconciliation has a clear no-benchmark review state
- entity-continuity outputs flag join-risk cases
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from helpers import run_script, write_parquet


def test_environment_report_writes_lock_metadata(tmp_path: Path) -> None:
    root = tmp_path / "root"
    out = root / "Checks" / "release_metadata" / "environment_report.json"
    result = run_script("Scripts/QA_QC/20_environment_report.py", "--root", root, "--out", out, timeout=60)
    assert result.returncode == 0, result.stdout
    report = json.loads(out.read_text(encoding="utf-8"))
    assert report["python"]["version"]
    assert report["requirements_lock"]["path"] == "requirements-lock.txt"
    assert report["requirements_lock"]["sha256"]
    assert "pandas" in report["direct_packages"]


def test_external_benchmark_reconciliation_review_and_pass(tmp_path: Path) -> None:
    root = tmp_path / "root"
    panel = root / "Panels" / "panel_clean_analysis_2022_2023.parquet"
    write_parquet(
        panel,
        [
            {"UNITID": 1, "year": 2022, "FINVAL": 10.0},
            {"UNITID": 2, "year": 2022, "FINVAL": 20.0},
            {"UNITID": 1, "year": 2023, "FINVAL": 30.0},
        ],
    )
    columns = [
        "benchmark_id",
        "year",
        "metric",
        "column",
        "expected_value",
        "tolerance_abs",
        "tolerance_rel",
        "source",
        "notes",
    ]
    empty_benchmarks = tmp_path / "empty_benchmarks.csv"
    pd.DataFrame(columns=columns).to_csv(empty_benchmarks, index=False)
    out_dir = root / "Checks" / "external_benchmarks"
    review = run_script(
        "Scripts/QA_QC/21_external_benchmark_reconciliation.py",
        "--root",
        root,
        "--panel",
        panel,
        "--benchmarks",
        empty_benchmarks,
        "--out-dir",
        out_dir,
        timeout=60,
    )
    assert review.returncode == 0, review.stdout
    review_rows = pd.read_csv(out_dir / "external_benchmark_reconciliation.csv")
    assert review_rows.loc[0, "status"] == "REVIEW"

    filled_benchmarks = tmp_path / "benchmarks.csv"
    pd.DataFrame(
        [
            {
                "benchmark_id": "rows_2022",
                "year": "2022",
                "metric": "panel_rows",
                "column": "",
                "expected_value": "2",
                "tolerance_abs": "0",
                "tolerance_rel": "0",
                "source": "synthetic",
                "notes": "",
            },
            {
                "benchmark_id": "sum_2023",
                "year": "2023",
                "metric": "sum",
                "column": "FINVAL",
                "expected_value": "30",
                "tolerance_abs": "0",
                "tolerance_rel": "0",
                "source": "synthetic",
                "notes": "",
            },
        ],
        columns=columns,
    ).to_csv(filled_benchmarks, index=False)
    passed = run_script(
        "Scripts/QA_QC/21_external_benchmark_reconciliation.py",
        "--root",
        root,
        "--panel",
        panel,
        "--benchmarks",
        filled_benchmarks,
        "--out-dir",
        out_dir,
        timeout=60,
    )
    assert passed.returncode == 0, passed.stdout
    rows = pd.read_csv(out_dir / "external_benchmark_reconciliation.csv")
    assert set(rows["status"]) == {"PASS"}


def test_entity_continuity_crosswalk_flags_join_risk(tmp_path: Path) -> None:
    root = tmp_path / "root"
    panel = root / "Panels" / "panel_clean_analysis_2020_2023.parquet"
    write_parquet(
        panel,
        [
            {"UNITID": 1, "year": 2020, "OPEID": "001", "OPEID6": "001000", "PRCH_F": "1"},
            {"UNITID": 1, "year": 2022, "OPEID": "002", "OPEID6": "001000", "PRCH_F": "6"},
            {"UNITID": 2, "year": 2021, "OPEID": "003", "OPEID6": "003000", "PRCH_F": "1"},
            {"UNITID": 2, "year": 2022, "OPEID": "003", "OPEID6": "003000", "PRCH_F": "1"},
        ],
    )
    out_dir = root / "Checks" / "entity_continuity"
    result = run_script(
        "Scripts/QA_QC/22_build_entity_continuity_crosswalk.py",
        "--root",
        root,
        "--years",
        "2020:2023",
        "--panel",
        panel,
        "--out-dir",
        out_dir,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout
    crosswalk = pd.read_csv(out_dir / "entity_continuity_crosswalk.csv")
    unit1 = crosswalk[crosswalk["UNITID"] == 1].iloc[0]
    assert unit1["join_risk_level"] == "review"
    assert "multi_opeid" in unit1["join_risk_flags"]
    assert "internal_year_gap" in unit1["join_risk_flags"]
    assert "parent_child_flag_observed" in unit1["join_risk_flags"]
    summary = pd.read_csv(out_dir / "entity_continuity_summary.csv")
    assert int(summary.loc[summary["metric"] == "review_unitids", "value"].iloc[0]) == 1
