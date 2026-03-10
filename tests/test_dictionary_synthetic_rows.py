from __future__ import annotations

import pandas as pd

from helpers import load_script_module


dictionary_mod = load_script_module("dictionary_ingest", "Scripts/03_dictionary_ingest.py")


def test_append_synthetic_imputation_rows_adds_missing_flag() -> None:
    frame = pd.DataFrame(
        [
            {
                "year": 2023,
                "varnumber": "00000042",
                "varname": "PELL_RECP",
                "varTitle": "Pell recipients",
                "longDescription": "",
                "DataType": "integer",
                "format": "",
                "Fieldwidth": "",
                "imputationvar": "XPELL_RECP",
                "source_file": "SFA",
                "source_file_label": "Student aid",
                "access_table_name": "SFA2023",
                "metadata_table_name": "SFA_VARLIST",
                "academic_year_label": "2023-24",
                "release_type": "Final",
                "metadata_source": "access_varlist",
            }
        ]
    )
    out, added = dictionary_mod.append_synthetic_imputation_rows(frame)
    assert added == 1
    assert "XPELL_RECP" in set(out["varname"])


def test_append_unitid_metadata_rows_adds_key_row() -> None:
    frame = pd.DataFrame(
        [
            {
                "year": 2023,
                "varnumber": "00000042",
                "varname": "PELL_RECP",
                "varTitle": "Pell recipients",
                "longDescription": "",
                "DataType": "integer",
                "format": "",
                "Fieldwidth": "",
                "imputationvar": "",
                "source_file": "SFA",
                "source_file_label": "Student aid",
                "access_table_name": "SFA2023",
                "metadata_table_name": "SFA_VARLIST",
                "academic_year_label": "2023-24",
                "release_type": "Final",
                "metadata_source": "access_varlist",
            }
        ]
    )
    out, added = dictionary_mod.append_unitid_metadata_rows(frame)
    assert added == 1
    assert ((out["source_file"] == "KEYS") & (out["varname"] == "UNITID")).any()
