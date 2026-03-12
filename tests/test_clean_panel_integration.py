"""
Integration tests for Stage 07 PRCH cleaning.

Focus:
- preserving wide-panel row structure
- nulling targeted child-row values
- retaining review-only finance values
- emitting complete PRCH QC coverage outputs
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from helpers import run_script, write_parquet


def write_dictionary(path: Path) -> None:
    write_parquet(
        path,
        [
            {"varname": "FINVAL", "source_file": "F_F"},
            {"varname": "COMPDRV", "source_file": "DRVC"},
            {"varname": "OTHER", "source_file": "HD"},
        ],
    )


def test_clean_panel_preserves_rows_and_applies_prch_policy(tmp_path: Path) -> None:
    root = tmp_path / "data_root"
    panels = root / "Panels"
    checks = root / "Checks" / "prch_qc"
    dictionary = root / "Dictionary" / "dictionary_lake.parquet"
    input_path = panels / "panel_wide_analysis_2022_2023.parquet"
    output_path = panels / "panel_clean_analysis_2022_2023.parquet"
    log_path = root / "Checks" / "logs" / "07_clean_panel_test.log"

    write_dictionary(dictionary)
    write_parquet(
        input_path,
        [
            {
                "year": 2022,
                "UNITID": 100654,
                "PRCH_F": 4,
                "PRCH_C": 2,
                "PRCH_OM": 2,
                "FINVAL": 100.0,
                "COMPDRV": 5.0,
                "OTHER": "keep-child-row",
            },
            {
                "year": 2023,
                "UNITID": 100663,
                "PRCH_F": 6,
                "PRCH_C": 1,
                "PRCH_OM": 1,
                "FINVAL": 200.0,
                "COMPDRV": 6.0,
                "OTHER": "keep-review-row",
            },
        ],
    )

    result = run_script(
        "Scripts/07_clean_panel.py",
        "--input",
        input_path,
        "--output",
        output_path,
        "--dictionary",
        dictionary,
        "--qc-dir",
        checks,
        "--log-file",
        log_path,
        env={"IPEDSDB_ROOT": str(root)},
    )

    assert result.returncode == 0, result.stdout
    cleaned = pd.read_parquet(output_path)
    assert len(cleaned) == 2
    assert cleaned[["UNITID", "year"]].duplicated().sum() == 0

    child_row = cleaned.loc[cleaned["UNITID"] == 100654].iloc[0]
    review_row = cleaned.loc[cleaned["UNITID"] == 100663].iloc[0]

    assert pd.isna(child_row["FINVAL"])
    assert pd.isna(child_row["COMPDRV"])
    assert child_row["OTHER"] == "keep-child-row"

    assert review_row["FINVAL"] == 200.0
    assert review_row["COMPDRV"] == 6.0
    assert review_row["OTHER"] == "keep-review-row"

    summary = pd.read_csv(checks / "prch_clean_summary.csv")
    finance_2022 = summary[(summary["year"] == 2022) & (summary["flag"] == "PRCH_F")].iloc[0]
    assert finance_2022["child_rows_cleaned"] == 1
    assert bool(finance_2022["has_target_columns"]) is True

    code_counts = pd.read_csv(checks / "prch_flag_code_counts.csv")
    om_counts = code_counts[(code_counts["year"] == 2022) & (code_counts["flag"] == "PRCH_OM")].iloc[0]
    assert om_counts["policy_bucket"] == "child_apply"
    assert bool(om_counts["has_target_columns"]) is False
    assert om_counts["rows"] == 1

    finance_review = code_counts[
        (code_counts["year"] == 2023) & (code_counts["flag"] == "PRCH_F") & (code_counts["code"] == 6)
    ].iloc[0]
    assert finance_review["policy_bucket"] == "review_only"


def test_clean_panel_refuses_single_year_input(tmp_path: Path) -> None:
    root = tmp_path / "data_root"
    panels = root / "Panels"
    dictionary = root / "Dictionary" / "dictionary_lake.parquet"
    input_path = panels / "panel_wide_analysis_2023_2023.parquet"
    output_path = panels / "panel_clean_analysis_2023_2023.parquet"
    log_path = root / "Checks" / "logs" / "07_clean_panel_single_year.log"

    write_dictionary(dictionary)
    write_parquet(
        input_path,
        [
            {
                "year": 2023,
                "UNITID": 100663,
                "PRCH_F": 6,
                "FINVAL": 200.0,
                "COMPDRV": 6.0,
                "OTHER": "keep-review-row",
            }
        ],
    )

    result = run_script(
        "Scripts/07_clean_panel.py",
        "--input",
        input_path,
        "--output",
        output_path,
        "--dictionary",
        dictionary,
        "--qc-dir",
        root / "Checks" / "prch_qc",
        "--log-file",
        log_path,
        env={"IPEDSDB_ROOT": str(root)},
    )

    assert result.returncode != 0
    assert "Refusing to run: input appears to contain only one year" in result.stdout
