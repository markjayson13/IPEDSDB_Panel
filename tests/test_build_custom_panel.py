"""
Integration tests for Stage 08 custom-panel exports.

Focus:
- retaining UNITID and year automatically
- variable selection from files
- year filtering
- strict missing-variable failure
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from helpers import run_script, write_parquet


def test_build_custom_panel_retains_keys_and_filters_years(tmp_path: Path) -> None:
    root = tmp_path / "data_root"
    input_path = root / "Panels" / "panel_clean_analysis_2022_2023.parquet"
    output_path = root / "Panels" / "custom_subset.parquet"
    vars_file = root / "Customize_Panel" / "vars.txt"

    write_parquet(
        input_path,
        [
            {"year": 2022, "UNITID": 100654, "INSTNM": "Example A", "CONTROL": 1, "EXTRA": "drop"},
            {"year": 2023, "UNITID": 100663, "INSTNM": "Example B", "CONTROL": 2, "EXTRA": "drop"},
        ],
    )
    vars_file.parent.mkdir(parents=True, exist_ok=True)
    vars_file.write_text("INSTNM\nCONTROL\nINSTNM\n", encoding="utf-8")

    result = run_script(
        "Scripts/08_build_custom_panel.py",
        "--input",
        input_path,
        "--output",
        output_path,
        "--vars-file",
        vars_file,
        "--years",
        "2023:2023",
        env={"IPEDSDB_ROOT": str(root)},
    )

    assert result.returncode == 0, result.stdout
    out = pd.read_parquet(output_path)
    assert out.columns.tolist() == ["year", "UNITID", "INSTNM", "CONTROL"]
    assert len(out) == 1
    assert out.iloc[0]["year"] == 2023
    assert out.iloc[0]["UNITID"] == 100663
    assert out.iloc[0]["INSTNM"] == "Example B"


def test_build_custom_panel_strict_missing_fails(tmp_path: Path) -> None:
    root = tmp_path / "data_root"
    input_path = root / "Panels" / "panel_clean_analysis_2022_2023.parquet"
    output_path = root / "Panels" / "custom_subset.parquet"

    write_parquet(
        input_path,
        [
            {"year": 2023, "UNITID": 100663, "INSTNM": "Example B", "CONTROL": 2},
        ],
    )

    result = run_script(
        "Scripts/08_build_custom_panel.py",
        "--input",
        input_path,
        "--output",
        output_path,
        "--vars",
        "INSTNM,MISSING_VAR",
        "--strict",
        env={"IPEDSDB_ROOT": str(root)},
    )

    assert result.returncode != 0
    assert "Missing 1 vars: MISSING_VAR" in result.stdout
