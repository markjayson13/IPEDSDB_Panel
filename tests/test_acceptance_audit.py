"""
Tests for the live-build acceptance audit summary.

Focus:
- core pass/fail aggregation across generated artifacts
- markdown rendering
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from helpers import load_script_module, write_parquet


audit_mod = load_script_module("acceptance_audit", "Scripts/QA_QC/08_acceptance_audit.py")


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def test_collect_acceptance_rows_and_render_markdown(tmp_path: Path) -> None:
    root = tmp_path / "data_root"
    years = [2022, 2023]

    write_parquet(
        root / "Panels" / "2022-2023" / "panel_long_varnum_2022_2023.parquet",
        [
            {"year": 2022, "UNITID": 100654, "varnumber": "00000001", "source_file": "HD"},
            {"year": 2023, "UNITID": 100663, "varnumber": "00000001", "source_file": "HD"},
        ],
    )
    write_parquet(
        root / "Panels" / "panel_wide_analysis_2022_2023.parquet",
        [
            {"year": 2022, "UNITID": 100654, "INSTNM": "A"},
            {"year": 2023, "UNITID": 100663, "INSTNM": "B"},
        ],
    )
    write_parquet(
        root / "Panels" / "panel_clean_analysis_2022_2023.parquet",
        [
            {"year": 2022, "UNITID": 100654, "INSTNM": "A"},
            {"year": 2023, "UNITID": 100663, "INSTNM": "B"},
        ],
    )
    write_parquet(
        root / "Dictionary" / "dictionary_lake.parquet",
        [{"year": 2022, "varnumber": "00000001", "varname": "INSTNM"}],
    )
    for year in years:
        write_parquet(
            root / "Cross_sections" / f"panel_long_varnum_{year}.parquet",
            [{"year": year, "UNITID": 100000 + year, "varnumber": "00000001", "source_file": "HD"}],
        )

    write_csv(
        root / "Checks" / "download_qc" / "release_inventory.csv",
        [
            {"year": 2022, "release_type": "Final", "download_status": "downloaded"},
            {"year": 2023, "release_type": "Final", "download_status": "existing"},
        ],
    )
    write_csv(
        root / "Checks" / "dictionary_qc" / "dictionary_qaqc_summary.csv",
        [
            {
                "duplicate_rows": 0,
                "source_file_conflicts": 0,
                "varnumber_collisions": 0,
                "unmapped_rows": 0,
                "needs_review_rows": 0,
            }
        ],
    )
    write_csv(
        root / "Checks" / "panel_qc" / "panel_qa_summary.csv",
        [
            {
                "raw_rows": 2,
                "clean_rows": 2,
                "suspicious_flags": 0,
            }
        ],
    )
    write_csv(
        root / "Checks" / "panel_qc" / "panel_qa_coverage_matrix.csv",
        [
            {"flag": "PRCH_F", "status": "cleaned"},
        ],
    )
    write_csv(
        root / "Checks" / "disc_qc" / "disc_conflicts_summary_all_years.csv",
        [
            {"high_signal": False},
        ],
    )

    rows = audit_mod.collect_acceptance_rows(root, years)
    assert rows
    assert all(row["passed"] for row in rows)

    md = audit_mod.render_markdown(rows, years)
    assert "# Acceptance Audit" in md
    assert "Checks passed" in md


def test_collect_acceptance_rows_flags_duplicate_clean_keys(tmp_path: Path) -> None:
    root = tmp_path / "data_root"
    years = [2023, 2024]

    write_parquet(
        root / "Panels" / "2023-2024" / "panel_long_varnum_2023_2024.parquet",
        [
            {"year": 2023, "UNITID": 100654, "varnumber": "00000001", "source_file": "HD"},
            {"year": 2024, "UNITID": 100663, "varnumber": "00000001", "source_file": "HD"},
        ],
    )
    write_parquet(
        root / "Panels" / "panel_wide_analysis_2023_2024.parquet",
        [
            {"year": 2023, "UNITID": 100654},
            {"year": 2024, "UNITID": 100663},
        ],
    )
    write_parquet(
        root / "Panels" / "panel_clean_analysis_2023_2024.parquet",
        [
            {"year": 2023, "UNITID": 100654},
            {"year": 2023, "UNITID": 100654},
        ],
    )
    write_parquet(
        root / "Dictionary" / "dictionary_lake.parquet",
        [{"year": 2023, "varnumber": "00000001", "varname": "INSTNM"}],
    )
    for year in years:
        write_parquet(
            root / "Cross_sections" / f"panel_long_varnum_{year}.parquet",
            [{"year": year, "UNITID": 100000 + year, "varnumber": "00000001", "source_file": "HD"}],
        )
    write_csv(
        root / "Checks" / "download_qc" / "release_inventory.csv",
        [
            {"year": 2023, "release_type": "Final", "download_status": "downloaded"},
            {"year": 2024, "release_type": "Final", "download_status": "downloaded"},
        ],
    )
    write_csv(root / "Checks" / "dictionary_qc" / "dictionary_qaqc_summary.csv", [{"duplicate_rows": 0, "source_file_conflicts": 0, "varnumber_collisions": 0, "unmapped_rows": 0, "needs_review_rows": 0}])
    write_csv(root / "Checks" / "panel_qc" / "panel_qa_summary.csv", [{"raw_rows": 2, "clean_rows": 2, "suspicious_flags": 0}])
    write_csv(root / "Checks" / "panel_qc" / "panel_qa_coverage_matrix.csv", [{"flag": "PRCH_F", "status": "cleaned"}])

    rows = audit_mod.collect_acceptance_rows(root, years)
    dup_row = next(row for row in rows if row["check_name"] == "panel_clean:duplicate_unitid_year")
    assert dup_row["passed"] is False
    assert dup_row["value"] == 1
