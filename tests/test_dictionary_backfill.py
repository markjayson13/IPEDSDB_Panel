"""
Tests for synthetic metadata backfills used to bridge sparse Access metadata years.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from helpers import load_script_module


ingest_mod = load_script_module("dictionary_ingest_stage", "Scripts/03_dictionary_ingest.py")
access_utils_mod = load_script_module("access_build_utils_for_backfill", "Scripts/access_build_utils.py")


def test_salary_lt9_metadata_backfills_from_nearest_year(tmp_path: Path) -> None:
    layout = access_utils_mod.ensure_data_layout(tmp_path)
    year_2010 = layout.raw_access / "2010"
    (year_2010 / "metadata").mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "table_role": "data",
                "normalized_table_name": "SAL_A_LT",
                "table_name": "SAL2010_A_LT9",
                "row_count_csv": "854",
            }
        ]
    ).to_csv(year_2010 / "metadata" / "table_inventory.csv", index=False)
    pd.DataFrame(
        [
            {
                "year": "2010",
                "academic_year_label": "2010-11",
                "release_type": "Final",
            }
        ]
    ).to_csv(year_2010 / "manifest.csv", index=False)

    lake = pd.DataFrame(
        [
            {
                "year": 2009,
                "varnumber": "00000010",
                "varname": "ARANK",
                "varTitle": "Academic rank",
                "longDescription": "Academic rank bucket.",
                "DataType": "disc",
                "format": "",
                "Fieldwidth": "",
                "imputationvar": "",
                "source_file": "SAL_A_LT",
                "source_file_label": "Salary rank under 9/10 month contract",
                "access_table_name": "SAL2009_A_LT9",
                "metadata_table_name": "SAL2009_VARS",
                "academic_year_label": "2009-10",
                "release_type": "Final",
                "metadata_source": "access_varlist",
            }
        ]
    )

    out, count = ingest_mod.append_nearest_year_source_backfill_rows(
        lake,
        layout=layout,
        years=[2009, 2010],
        source_file="SAL_A_LT",
        metadata_source="synthetic_backfill_salary_lt9",
    )

    backfilled = out[(out["year"] == 2010) & (out["source_file"] == "SAL_A_LT")]
    assert count == 1
    assert len(backfilled) == 1
    assert backfilled.iloc[0]["access_table_name"] == "SAL2010_A_LT9"
    assert backfilled.iloc[0]["metadata_source"] == "synthetic_backfill_salary_lt9"
    assert backfilled.iloc[0]["source_file_label"] == "SAL2010_A_LT9"
