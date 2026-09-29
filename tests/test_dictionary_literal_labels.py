"""Labels that resemble pandas missing-value tokens are still source text."""
from __future__ import annotations

import csv

import pyarrow.parquet as pq

from helpers import run_script


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_dictionary_ingest_preserves_literal_none_and_na_labels(tmp_path):
    root = tmp_path / "data"
    year = root / "Raw_Access_Databases" / "2008"
    write_csv(year / "manifest.csv", [{"year": "2008", "academic_year_label": "2008-09", "release_type": "Final"}])
    write_csv(year / "metadata/table_inventory.csv", [
        {"table_name": "vartable08", "table_role": "metadata_varlist", "csv_path": "tables_csv/vartable08.csv",
         "row_count_csv": "1", "has_varnumber": "true", "has_varname": "true", "has_vartitle": "true",
         "has_longdesc": "true", "has_codevalue": "false", "has_valuelabel": "false"},
        {"table_name": "valuesets08", "table_role": "metadata_codes", "csv_path": "tables_csv/valuesets08.csv",
         "row_count_csv": "3", "has_varnumber": "true", "has_varname": "true", "has_vartitle": "true",
         "has_longdesc": "false", "has_codevalue": "true", "has_valuelabel": "true"},
    ])
    definition = {"TableName": "FLAGS2008", "varNumber": "60056", "varName": "CUFASB",
                  "varTitle": "Number of component units", "longDescription": "None", "DataType": "N", "format": "Disc"}
    write_csv(year / "tables_csv/vartable08.csv", [definition])
    write_csv(year / "tables_csv/valuesets08.csv", [
        {"TableName": "FLAGS2008", "varNumber": "60056", "varName": "CUFASB", "Codevalue": value, "valueLabel": label}
        for value, label in [("0", "None"), ("-2", "N/A"), ("38", "")]
    ])
    run = run_script("Scripts/03_dictionary_ingest.py", "--root", root, "--years", "2008")
    assert run.returncode == 0, run.stdout
    codes = pq.read_table(root / "Dictionary/dictionary_codes.parquet").to_pylist()
    assert {r["codevalue"]: r["valuelabel"] for r in codes} == {"0": "None", "-2": "N/A", "38": ""}
    definitions = pq.read_table(root / "Dictionary/dictionary_lake.parquet").to_pylist()
    assert next(r["longDescription"] for r in definitions if r["varname"] == "CUFASB") == "None"
