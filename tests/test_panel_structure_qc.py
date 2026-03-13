"""
Integration tests for literature-guided panel-structure QA outputs.

Focus:
- generating readable structure diagnostics from a synthetic clean panel
- graceful handling when OPEID is absent
- stable summary artifacts for entry/exit, timing, finance, and classification
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from helpers import run_script, write_parquet


def test_panel_structure_qc_writes_expected_outputs(tmp_path: Path) -> None:
    root = tmp_path / "data_root"
    input_path = root / "Panels" / "panel_clean_analysis_2022_2024.parquet"
    dictionary_path = root / "Dictionary" / "dictionary_lake.parquet"
    out_dir = root / "Checks" / "panel_qc"

    write_parquet(
        input_path,
        [
            {"year": 2022, "UNITID": 100654, "INSTNM": "A", "CONTROL": 1, "SECTOR": 1, "ICLEVEL": 1, "FINVAL": 100.0},
            {"year": 2023, "UNITID": 100654, "INSTNM": "A", "CONTROL": 1, "SECTOR": 1, "ICLEVEL": 1, "FINVAL": 110.0},
            {"year": 2024, "UNITID": 100654, "INSTNM": "A", "CONTROL": 2, "SECTOR": 1, "ICLEVEL": 1, "FINVAL": 120.0},
            {"year": 2022, "UNITID": 100663, "INSTNM": "B", "CONTROL": 2, "SECTOR": 2, "ICLEVEL": 2, "FINVAL": 90.0},
            {"year": 2024, "UNITID": 100663, "INSTNM": "B", "CONTROL": 2, "SECTOR": 2, "ICLEVEL": 2, "FINVAL": 95.0},
        ],
    )
    write_parquet(
        dictionary_path,
        [
            {
                "year": 2022,
                "varnumber": "0001",
                "varname": "FINVAL",
                "varTitle": "Depreciation expense",
                "longDescription": "Depreciation expense reported in finance statements.",
                "source_file": "F_F",
            },
            {
                "year": 2022,
                "varnumber": "0002",
                "varname": "CONTROL",
                "varTitle": "Control",
                "longDescription": "Institutional control sector.",
                "source_file": "IC",
            },
        ],
    )

    result = run_script(
        "Scripts/QA_QC/09_panel_structure_qc.py",
        "--root",
        root,
        "--years",
        "2022:2024",
        "--input",
        input_path,
        "--dictionary",
        dictionary_path,
        "--out-dir",
        out_dir,
    )
    assert result.returncode == 0, result.stdout

    panel_structure = pd.read_csv(out_dir / "panel_structure_summary.csv")
    entry_exit = pd.read_csv(out_dir / "entry_exit_gap_summary.csv")
    linkage = pd.read_csv(out_dir / "identifier_linkage_summary.csv")
    timing = pd.read_csv(out_dir / "component_timing_reference.csv")
    finance = pd.read_csv(out_dir / "finance_comparability_summary.csv")
    classification = pd.read_csv(out_dir / "classification_stability_summary.csv")
    flags = pd.read_csv(out_dir / "institution_pattern_flags.csv")

    assert not panel_structure.empty
    assert not entry_exit.empty
    assert not linkage.empty
    assert not timing.empty
    assert not finance.empty
    assert not classification.empty
    assert not flags.empty

    intermittent_metric = panel_structure.loc[panel_structure["metric"] == "intermittent_gap_unitids", "value"].iloc[0]
    assert int(intermittent_metric) == 1
    assert linkage.iloc[0]["record_type"] == "unavailable"
    assert "gracefully" in linkage.iloc[0]["notes"]
    assert "accounting_standard_sensitive" in finance["comparability_flag"].fillna("").tolist()
