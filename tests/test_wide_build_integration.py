"""
Integration tests for Stage 06 wide-build fail gates and disc QC outputs.

Focus:
- scalar-conflict failure and QC emission
- anti-garbage failure and QC emission
- expected discrete conflicts downgraded in summary output
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from helpers import run_script, write_parquet


def build_common_args(root: Path, input_path: Path, out_dir: Path) -> list[str]:
    return [
        "--input",
        input_path,
        "--out_dir",
        out_dir,
        "--years",
        "2023:2023",
        "--qc-dir",
        root / "Checks" / "wide_qc",
        "--disc-qc-dir",
        root / "Checks" / "disc_qc",
        "--duckdb-path",
        root / "build" / "test_build.duckdb",
        "--duckdb-temp-dir",
        root / "build" / "duckdb_tmp",
        "--no-persist-duckdb",
        "--no-legacy-analysis-schema",
    ]


def test_wide_build_scalar_conflict_gate_fails_and_writes_qc(tmp_path: Path) -> None:
    root = tmp_path / "data_root"
    input_path = root / "Panels" / "2023-2023" / "panel_long_varnum_2023_2023.parquet"
    out_dir = root / "Panels" / "wide_parts"

    write_parquet(
        input_path,
        [
            {
                "UNITID": 100654,
                "year": 2023,
                "varname": "INSTNM",
                "value": "Example A",
                "source_file": "HD",
                "varnumber": "00000001",
            },
            {
                "UNITID": 100654,
                "year": 2023,
                "varname": "INSTNM",
                "value": "Example B",
                "source_file": "HD",
                "varnumber": "00000001",
            },
        ],
    )

    result = run_script(
        "Scripts/06_build_wide_panel.py",
        *build_common_args(root, input_path, out_dir),
        "--lane-split",
        "--scalar-conflict-buckets",
        "1",
        env={"IPEDSDB_ROOT": str(root)},
        timeout=60,
    )

    assert result.returncode != 0
    assert "scalar conflict gate failed" in result.stdout

    qc_path = root / "Checks" / "wide_qc" / "qc_scalar_conflicts.csv"
    assert qc_path.exists()
    qc = pd.read_csv(qc_path)
    assert len(qc) == 2
    assert set(qc["value"]) == {"Example A", "Example B"}
    assert set(qc["distinct_values"]) == {2}


def test_wide_build_anti_garbage_gate_fails_and_writes_qc(tmp_path: Path) -> None:
    root = tmp_path / "data_root"
    input_path = root / "Panels" / "2023-2023" / "panel_long_varnum_2023_2023.parquet"
    out_dir = root / "Panels" / "wide_parts"

    write_parquet(
        input_path,
        [
            {
                "UNITID": 100654,
                "year": 2023,
                "varname": "CIPCODE",
                "value": "52.0201",
                "source_file": "HD",
                "varnumber": "00000002",
            }
        ],
    )

    result = run_script(
        "Scripts/06_build_wide_panel.py",
        *build_common_args(root, input_path, out_dir),
        "--lane-split",
        "--no-drop-anti-garbage-cols",
        env={"IPEDSDB_ROOT": str(root)},
        timeout=60,
    )

    assert result.returncode != 0
    assert "anti-garbage gate failed" in result.stdout

    qc_path = root / "Checks" / "wide_qc" / "qc_anti_garbage_failures.csv"
    assert qc_path.exists()
    qc = pd.read_csv(qc_path)
    assert qc["blocked_identifier_column"].tolist() == ["CIPCODE"]


def test_wide_build_disc_summary_downgrades_expected_patterns(tmp_path: Path) -> None:
    root = tmp_path / "data_root"
    input_path = root / "Panels" / "2023-2023" / "panel_long_varnum_2023_2023.parquet"
    out_dir = root / "Panels" / "wide_parts"
    dictionary_path = root / "Dictionary" / "dictionary_lake.parquet"

    write_parquet(
        input_path,
        [
            {
                "UNITID": 100654,
                "year": 2023,
                "varname": "LEVEL1",
                "value": "1",
                "source_file": "IC",
                "varnumber": "00000011",
            },
            {
                "UNITID": 100654,
                "year": 2023,
                "varname": "LEVEL2",
                "value": "1",
                "source_file": "IC",
                "varnumber": "00000012",
            },
            {
                "UNITID": 100654,
                "year": 2023,
                "varname": "INSTNM",
                "value": "Example U",
                "source_file": "HD",
                "varnumber": "00000001",
            },
        ],
    )
    write_parquet(
        dictionary_path,
        [
            {"varname": "LEVEL1", "DataType": "disc", "format": "disc", "source_file": "IC"},
            {"varname": "LEVEL2", "DataType": "disc", "format": "disc", "source_file": "IC"},
            {"varname": "INSTNM", "DataType": "char", "format": "char", "source_file": "HD"},
        ],
    )

    result = run_script(
        "Scripts/06_build_wide_panel.py",
        *build_common_args(root, input_path, out_dir),
        "--dictionary",
        dictionary_path,
        "--collapse-disc",
        env={"IPEDSDB_ROOT": str(root)},
        timeout=60,
    )

    assert result.returncode == 0, result.stdout

    summary_path = root / "Checks" / "disc_qc" / "disc_conflicts_summary_all_years.csv"
    assert summary_path.exists()
    summary = pd.read_csv(summary_path)
    level_row = summary[(summary["source_file"] == "IC") & (summary["variable_family"] == "LEVEL")].iloc[0]
    assert bool(level_row["expected_pattern"]) is True
    assert level_row["signal_level"] == "expected"
    assert bool(level_row["high_signal"]) is False
