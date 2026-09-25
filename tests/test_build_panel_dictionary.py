"""
Integration tests for Stage 09 panel-dictionary generation.

Focus:
- schema-driven row order
- dictionary text merge
- controlled YEAR metadata when not present in dictionary_lake
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd
import pytest
from openpyxl import load_workbook

from helpers import run_script, write_parquet


def test_build_panel_dictionary_merges_schema_and_dictionary_text(tmp_path: Path) -> None:
    root = tmp_path / "data_root"
    input_path = root / "Panels" / "panel_clean_analysis_2022_2023.parquet"
    dictionary_path = root / "Dictionary" / "dictionary_lake.parquet"
    output_path = root / "Panels" / "panel_dictionary.csv"

    write_parquet(
        input_path,
        [
            {"year": 2023, "UNITID": 100663, "INSTNM": "Example B", "CONTROL": 2},
        ],
    )
    write_parquet(
        dictionary_path,
        [
            {
                "year": 2023, "source_file": "HD", "source_table": "HD2023",
                "varname": "INSTNM",
                "varTitle": "Institution name",
                "longDescription": "Institution name used in reporting and public lookup outputs.",
                "DataType": "char",
            },
            {
                "year": 2023, "source_file": "HD", "source_table": "HD2023",
                "varname": "CONTROL",
                "varTitle": "Control",
                "longDescription": "Institutional control sector.",
                "DataType": "disc",
            },
        ],
    )

    result = run_script(
        "Scripts/09_build_panel_dictionary.py",
        "--input",
        input_path,
        "--dictionary",
        dictionary_path,
        "--output",
        output_path,
    )

    assert result.returncode == 0, result.stdout
    out = pd.read_csv(output_path)
    assert out["varname"].tolist() == ["YEAR", "UNITID", "INSTNM", "CONTROL"]

    year_row = out[out["varname"] == "YEAR"].iloc[0]
    assert year_row["varTitle"] == "IPEDS reporting year"
    assert "institution-year panel" in year_row["longDescription"]

    instnm_row = out[out["varname"] == "INSTNM"].iloc[0]
    assert instnm_row["varTitle"] == "Institution name"
    assert instnm_row["dictionaryDataType"] == "char"

    control_row = out[out["varname"] == "CONTROL"].iloc[0]
    assert control_row["panelDataType"] in {"int64", "int64[pyarrow]", "int32", "int32[pyarrow]"}


def test_build_panel_dictionary_writes_formatted_excel_workbook(tmp_path: Path) -> None:
    root = tmp_path / "data_root"
    input_path = root / "Panels" / "panel_clean_analysis_2022_2023.parquet"
    dictionary_path = root / "Dictionary" / "dictionary_lake.parquet"
    output_path = root / "Panels" / "panel_dictionary.xlsx"

    write_parquet(
        input_path,
        [
            {"year": 2023, "UNITID": 100663, "INSTNM": "Example B", "CONTROL": 2},
        ],
    )
    write_parquet(
        dictionary_path,
        [
            {
                "year": 2023, "source_file": "HD", "source_table": "HD2023",
                "varname": "INSTNM",
                "varTitle": "Institution name",
                "longDescription": "Institution name used in reporting and public lookup outputs.",
                "DataType": "char",
            },
            {
                "year": 2023, "source_file": "HD", "source_table": "HD2023",
                "varname": "CONTROL",
                "varTitle": "Control",
                "longDescription": "Institutional control sector.",
                "DataType": "disc",
            },
        ],
    )

    result = run_script(
        "Scripts/09_build_panel_dictionary.py",
        "--input",
        input_path,
        "--dictionary",
        dictionary_path,
        "--output",
        output_path,
    )

    assert result.returncode == 0, result.stdout
    wb = load_workbook(output_path)
    assert wb.sheetnames == ["panel_dictionary", "about", "value_labels", "issues"]

    ws = wb["panel_dictionary"]
    assert ws.freeze_panes == "A2"
    headers = [cell.value for cell in ws[1]]
    assert headers == ["column_order", "varname", "varTitle", "longDescription", "panelDataType", "dictionaryDataType", "metadata_status", "comparability_status"]
    assert ws["B2"].value == "YEAR"
    assert ws["C2"].value == "IPEDS reporting year"

    about = wb["about"]
    assert about["A1"].value == "Panel dictionary export"
    assert str(input_path) == about["B6"].value


@pytest.mark.parametrize("versioned", [False, True])
def test_dictionary_uses_observed_year_and_lineage_instead_of_longest_label(tmp_path: Path, versioned: bool) -> None:
    root = tmp_path / "data_root"
    version = Path("v2") if versioned else Path()
    panel = root / "Panels" / version / "panel.parquet"
    dictionary = root / "Dictionary" / version / "dictionary_lake.parquet"
    codes = root / "Dictionary" / version / "dictionary_codes.parquet"
    lineage = root / "Checks" / version / "wide_qc/qc_column_lineage.csv"
    output = root / "Panels" / version / "dictionary.csv"
    write_parquet(panel, [{"year": 2023, "UNITID": 100654, "CONTROL": 1}])
    write_parquet(dictionary, [
        {"year": 2023, "source_file": "HD", "source_table": "HD2023", "varname": "CONTROL", "varnumber": "1",
         "varTitle": "Current control", "longDescription": "Current meaning.", "DataType": "disc"},
        {"year": 2022, "source_file": "HD", "source_table": "HD2022", "varname": "CONTROL", "varnumber": "1",
         "varTitle": "Much longer but obsolete control label", "longDescription": "An obsolete definition with more characters.", "DataType": "disc"},
        {"year": 2023, "source_file": "FIN", "source_table": "FIN2023", "varname": "CONTROL", "varnumber": "1",
         "varTitle": "Another very long label from the wrong source", "longDescription": "Unrelated definition with more characters.", "DataType": "disc"},
    ])
    write_parquet(codes, [{"year": 2023, "source_file": "HD", "source_table": "HD2023", "varname": "CONTROL",
                           "varnumber": "1", "codevalue": "1", "valuelabel": "Public"}])
    lineage.parent.mkdir(parents=True)
    with lineage.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["output_column", "source_varnames", "source_files"])
        writer.writeheader()
        writer.writerow({"output_column": "CONTROL", "source_varnames": "CONTROL", "source_files": "HD"})
    result = run_script("Scripts/09_build_panel_dictionary.py", "--input", panel, "--dictionary", dictionary,
                        "--output", output, "--require-ready")
    assert result.returncode == 0, result.stdout
    row = pd.read_csv(output).set_index("varname").loc["CONTROL"]
    assert row["varTitle"] == "Current control"
    assert row["longDescription"] == "Current meaning."
    metadata = json.loads(Path(str(output) + ".metadata.json").read_text())
    variable = next(v for v in metadata["variables"] if v["name"] == "CONTROL")
    assert len(variable["source_metadata"]) == 1
    assert variable["value_labels"] == [{"value": "1", "label": "Public"}]
    assert metadata["source_fingerprints"]["panel"]["sha256"]
    assert Path(str(output) + ".value_labels.csv").is_file()


def test_dictionary_without_year_is_diagnosed_without_applying_label(tmp_path: Path) -> None:
    panel, dictionary, output = (tmp_path / name for name in ("panel.parquet", "dictionary.parquet", "out.csv"))
    write_parquet(panel, [{"year": 2023, "UNITID": 100654, "CONTROL": 1}])
    write_parquet(dictionary, [{"varname": "CONTROL", "varTitle": "Unscoped control", "longDescription": "No known year."}])
    result = run_script("Scripts/09_build_panel_dictionary.py", "--input", panel, "--dictionary", dictionary, "--output", output)
    assert result.returncode == 0, result.stdout
    metadata = json.loads(Path(str(output) + ".metadata.json").read_text())
    variable = next(v for v in metadata["variables"] if v["name"] == "CONTROL")
    assert variable["label"] == "CONTROL"
    assert variable["source_metadata"] == []
    assert metadata["metadata_status"] == "incomplete"
