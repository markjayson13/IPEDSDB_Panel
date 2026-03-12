"""
Tests for Stage 05 stitched-long validation.

Focus:
- successful year stitching
- missing-year failure
- null or blank key-field failure
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from helpers import run_script, write_parquet, load_script_module


stitch_mod = load_script_module("stitch_long_stage", "Scripts/05_stitch_long.py")


def stitched_output_path(root: Path, years: str) -> Path:
    start, end = years.split(":", 1)
    return root / "Panels" / f"{start}-{end}" / f"panel_long_varnum_{start}_{end}.parquet"


def test_stitch_long_success_writes_summary(tmp_path: Path) -> None:
    layout = stitch_mod.ensure_data_layout(tmp_path)
    write_parquet(
        layout.cross_sections / "panel_long_varnum_2022.parquet",
        [
            {
                "year": 2022,
                "UNITID": 100654,
                "varnumber": "00000001",
                "source_file": "HD",
                "varname": "INSTNM",
                "value": "Example A",
            }
        ],
    )
    write_parquet(
        layout.cross_sections / "panel_long_varnum_2023.parquet",
        [
            {
                "year": 2023,
                "UNITID": 100663,
                "varnumber": "00000001",
                "source_file": "HD",
                "varname": "INSTNM",
                "value": "Example B",
            }
        ],
    )

    output = stitched_output_path(layout.root, "2022:2023")
    result = run_script(
        "Scripts/05_stitch_long.py",
        "--root",
        layout.root,
        "--years",
        "2022:2023",
        "--cross-sections-dir",
        layout.cross_sections,
        "--output",
        output,
    )

    assert result.returncode == 0, result.stdout
    stitched = pd.read_parquet(output)
    assert len(stitched) == 2
    assert sorted(stitched["year"].tolist()) == [2022, 2023]

    summary_path = layout.checks / "harmonize_qc" / "stitch_summary.csv"
    summary = pd.read_csv(summary_path)
    assert summary.loc[0, "years_min"] == 2022
    assert summary.loc[0, "years_max"] == 2023
    assert summary.loc[0, "years_count"] == 2


def test_stitch_long_fails_when_requested_year_is_missing(tmp_path: Path) -> None:
    layout = stitch_mod.ensure_data_layout(tmp_path)
    write_parquet(
        layout.cross_sections / "panel_long_varnum_2022.parquet",
        [
            {
                "year": 2022,
                "UNITID": 100654,
                "varnumber": "00000001",
                "source_file": "HD",
                "varname": "INSTNM",
                "value": "Example A",
            }
        ],
    )

    output = stitched_output_path(layout.root, "2022:2023")
    result = run_script(
        "Scripts/05_stitch_long.py",
        "--root",
        layout.root,
        "--years",
        "2022:2023",
        "--cross-sections-dir",
        layout.cross_sections,
        "--output",
        output,
    )

    assert result.returncode != 0
    assert "Missing per-year long parquet" in result.stdout


def test_stitch_long_fails_on_blank_key_fields(tmp_path: Path) -> None:
    layout = stitch_mod.ensure_data_layout(tmp_path)
    write_parquet(
        layout.cross_sections / "panel_long_varnum_2022.parquet",
        [
            {
                "year": 2022,
                "UNITID": 100654,
                "varnumber": "00000001",
                "source_file": "HD",
                "varname": "INSTNM",
                "value": "Example A",
            }
        ],
    )
    write_parquet(
        layout.cross_sections / "panel_long_varnum_2023.parquet",
        [
            {
                "year": 2023,
                "UNITID": 100663,
                "varnumber": "00000001",
                "source_file": "",
                "varname": "INSTNM",
                "value": "Example B",
            }
        ],
    )

    output = stitched_output_path(layout.root, "2022:2023")
    result = run_script(
        "Scripts/05_stitch_long.py",
        "--root",
        layout.root,
        "--years",
        "2022:2023",
        "--cross-sections-dir",
        layout.cross_sections,
        "--output",
        output,
    )

    assert result.returncode != 0
    assert "Stitched panel contains null/blank key fields" in result.stdout
