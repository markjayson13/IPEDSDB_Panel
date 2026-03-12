"""
Tiny synthetic-root smoke test for the core stage chain.

Focus:
- harmonize from exported Access-style CSV tables
- stitch long output
- wide build
- PRCH cleaning
- panel QA over the generated artifacts
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from helpers import run_script, write_parquet


DICT_COLUMNS = [
    "year",
    "varnumber",
    "varname",
    "varTitle",
    "longDescription",
    "DataType",
    "format",
    "Fieldwidth",
    "imputationvar",
    "source_file",
    "source_file_label",
    "access_table_name",
    "metadata_table_name",
    "academic_year_label",
    "release_type",
    "metadata_source",
]


def write_year_inputs(root: Path, year: int, unitid: int, prch_f: int, finval: float) -> None:
    year_dir = root / "Raw_Access_Databases" / str(year)
    tables_dir = year_dir / "tables_csv"
    metadata_dir = year_dir / "metadata"
    tables_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {"year": str(year), "academic_year_label": f"{year}-{(year + 1) % 100:02d}", "release_type": "Final"},
        ]
    ).to_csv(year_dir / "manifest.csv", index=False)

    pd.DataFrame(
        [
            {
                "table_role": "data",
                "has_unitid": "true",
                "table_name": f"FIN_{year}",
                "normalized_table_name": "F_F",
                "csv_path": f"tables_csv/FIN_{year}.csv",
                "row_count_csv": "1",
            },
            {
                "table_role": "data",
                "has_unitid": "true",
                "table_name": f"FLAGS_{year}",
                "normalized_table_name": "FLAGS",
                "csv_path": f"tables_csv/FLAGS_{year}.csv",
                "row_count_csv": "1",
            },
        ]
    ).to_csv(metadata_dir / "table_inventory.csv", index=False)

    pd.DataFrame([{"UNITID": unitid, "FINVAL": finval}]).to_csv(tables_dir / f"FIN_{year}.csv", index=False)
    pd.DataFrame([{"UNITID": unitid, "PRCH_F": prch_f}]).to_csv(tables_dir / f"FLAGS_{year}.csv", index=False)


def test_synthetic_root_core_pipeline_smoke(tmp_path: Path) -> None:
    root = tmp_path / "data_root"
    dictionary_path = root / "Dictionary" / "dictionary_lake.parquet"

    write_parquet(
        dictionary_path,
        [
            {
                "year": 2022,
                "varnumber": "00000001",
                "varname": "FINVAL",
                "varTitle": "Finance value",
                "longDescription": "Finance value to be cleaned when the row is reported with a parent.",
                "DataType": "cont",
                "format": "cont",
                "Fieldwidth": "",
                "imputationvar": "",
                "source_file": "F_F",
                "source_file_label": "Finance",
                "access_table_name": "FIN_2022",
                "metadata_table_name": "FIN_META_2022",
                "academic_year_label": "2022-23",
                "release_type": "Final",
                "metadata_source": "synthetic_test",
            },
            {
                "year": 2022,
                "varnumber": "00000002",
                "varname": "PRCH_F",
                "varTitle": "Finance parent-child flag",
                "longDescription": "Finance parent-child reporting code.",
                "DataType": "disc",
                "format": "disc",
                "Fieldwidth": "",
                "imputationvar": "",
                "source_file": "FLAGS",
                "source_file_label": "Flags",
                "access_table_name": "FLAGS_2022",
                "metadata_table_name": "FLAGS_META_2022",
                "academic_year_label": "2022-23",
                "release_type": "Final",
                "metadata_source": "synthetic_test",
            },
            {
                "year": 2023,
                "varnumber": "00000001",
                "varname": "FINVAL",
                "varTitle": "Finance value",
                "longDescription": "Finance value to be cleaned when the row is reported with a parent.",
                "DataType": "cont",
                "format": "cont",
                "Fieldwidth": "",
                "imputationvar": "",
                "source_file": "F_F",
                "source_file_label": "Finance",
                "access_table_name": "FIN_2023",
                "metadata_table_name": "FIN_META_2023",
                "academic_year_label": "2023-24",
                "release_type": "Final",
                "metadata_source": "synthetic_test",
            },
            {
                "year": 2023,
                "varnumber": "00000002",
                "varname": "PRCH_F",
                "varTitle": "Finance parent-child flag",
                "longDescription": "Finance parent-child reporting code.",
                "DataType": "disc",
                "format": "disc",
                "Fieldwidth": "",
                "imputationvar": "",
                "source_file": "FLAGS",
                "source_file_label": "Flags",
                "access_table_name": "FLAGS_2023",
                "metadata_table_name": "FLAGS_META_2023",
                "academic_year_label": "2023-24",
                "release_type": "Final",
                "metadata_source": "synthetic_test",
            },
        ],
    )

    write_year_inputs(root, 2022, 100654, 4, 100.0)
    write_year_inputs(root, 2023, 100663, 6, 200.0)

    env = {"IPEDSDB_ROOT": str(root)}

    harmonize = run_script(
        "Scripts/04_harmonize.py",
        "--root",
        root,
        "--years",
        "2022:2023",
        "--output-dir",
        root / "Cross_sections",
        "--parts-dir-base",
        root / "Cross_sections",
        "--no-dedupe",
        env=env,
        timeout=60,
    )
    assert harmonize.returncode == 0, harmonize.stdout

    stitch = run_script(
        "Scripts/05_stitch_long.py",
        "--root",
        root,
        "--years",
        "2022:2023",
        "--cross-sections-dir",
        root / "Cross_sections",
        "--output",
        root / "Panels" / "2022-2023" / "panel_long_varnum_2022_2023.parquet",
        env=env,
        timeout=60,
    )
    assert stitch.returncode == 0, stitch.stdout

    wide = run_script(
        "Scripts/06_build_wide_panel.py",
        "--input",
        root / "Panels" / "2022-2023" / "panel_long_varnum_2022_2023.parquet",
        "--out_dir",
        root / "Panels" / "wide_parts",
        "--years",
        "2022:2023",
        "--dictionary",
        dictionary_path,
        "--write_single",
        root / "Panels" / "panel_wide_analysis_2022_2023.parquet",
        "--qc-dir",
        root / "Checks" / "wide_qc",
        "--disc-qc-dir",
        root / "Checks" / "disc_qc",
        "--duckdb-path",
        root / "build" / "smoke_build.duckdb",
        "--duckdb-temp-dir",
        root / "build" / "duckdb_tmp",
        "--no-persist-duckdb",
        "--no-legacy-analysis-schema",
        env=env,
        timeout=60,
    )
    assert wide.returncode == 0, wide.stdout

    clean = run_script(
        "Scripts/07_clean_panel.py",
        "--input",
        root / "Panels" / "panel_wide_analysis_2022_2023.parquet",
        "--output",
        root / "Panels" / "panel_clean_analysis_2022_2023.parquet",
        "--dictionary",
        dictionary_path,
        "--qc-dir",
        root / "Checks" / "prch_qc",
        env=env,
        timeout=60,
    )
    assert clean.returncode == 0, clean.stdout

    qa = run_script(
        "Scripts/QA_QC/01_panel_qa.py",
        "--raw",
        root / "Panels" / "panel_wide_analysis_2022_2023.parquet",
        "--clean",
        root / "Panels" / "panel_clean_analysis_2022_2023.parquet",
        "--out-dir",
        root / "Checks" / "panel_qc",
        "--prch-qc-dir",
        root / "Checks" / "prch_qc",
        env=env,
        timeout=60,
    )
    assert qa.returncode == 0, qa.stdout

    raw_panel = pd.read_parquet(root / "Panels" / "panel_wide_analysis_2022_2023.parquet")
    clean_panel = pd.read_parquet(root / "Panels" / "panel_clean_analysis_2022_2023.parquet")
    assert len(raw_panel) == 2
    assert len(clean_panel) == 2

    child_row = clean_panel.loc[clean_panel["UNITID"] == 100654].iloc[0]
    review_row = clean_panel.loc[clean_panel["UNITID"] == 100663].iloc[0]
    assert pd.isna(child_row["FINVAL"])
    assert float(review_row["FINVAL"]) == 200.0

    coverage = pd.read_csv(root / "Checks" / "panel_qc" / "panel_qa_coverage_matrix.csv")
    finance_row = coverage[coverage["flag"] == "PRCH_F"].iloc[0]
    assert finance_row["status"] == "cleaned"
    assert finance_row["child_rows_raw"] == 1
    assert finance_row["review_rows_raw"] == 1
