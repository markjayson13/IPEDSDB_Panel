"""
Integration tests for the optional variable-browser utility.

Focus:
- only variables present in the panel schema are exported
- component groups are inferred from dictionary source_file metadata
- multi-source history chooses a stable primary source for grouping
"""
from __future__ import annotations

import json
import re
from pathlib import Path

from helpers import run_script, write_parquet


def extract_payload(html_text: str) -> dict:
    match = re.search(
        r'<script id="variable-browser-data" type="application/json">(.*?)</script>',
        html_text,
        flags=re.DOTALL,
    )
    assert match is not None, "variable browser payload script not found"
    return json.loads(match.group(1))


def test_build_variable_browser_writes_static_html_with_grouped_vars(tmp_path: Path) -> None:
    root = tmp_path / "data_root"
    input_path = root / "Panels" / "panel_clean_analysis_2022_2023.parquet"
    dictionary_path = root / "Dictionary" / "dictionary_lake.parquet"
    output_path = root / "Customize_Panel" / "variable_browser.html"

    write_parquet(
        input_path,
        [
            {
                "year": 2023,
                "UNITID": 100663,
                "INSTNM": "Example U",
                "TUITION1": 12345,
                "TUITION2": 23456,
                "ANYAIDN": 850,
                "GRN4A2": 64.2,
            },
        ],
    )
    write_parquet(
        dictionary_path,
        [
            {
                "year": 2022,
                "varname": "INSTNM",
                "varTitle": "Institution name",
                "longDescription": "Institution name used in reporting outputs.",
                "DataType": "char",
                "format": "alpha",
                "source_file": "IC",
            },
            {
                "year": 2023,
                "varname": "TUITION1",
                "varTitle": "Published in-state tuition",
                "longDescription": "Published tuition and required fees for in-state students.",
                "DataType": "num",
                "format": "numeric",
                "source_file": "DRVIC",
            },
            {
                "year": 2022,
                "varname": "ANYAIDN",
                "varTitle": "Students receiving any aid",
                "longDescription": "Number of students receiving any financial aid.",
                "DataType": "num",
                "format": "numeric",
                "source_file": "SFA_P",
            },
            {
                "year": 2023,
                "varname": "ANYAIDN",
                "varTitle": "Students receiving any aid",
                "longDescription": "Number of students receiving any financial aid in the fall cohort.",
                "DataType": "num",
                "format": "numeric",
                "source_file": "SFA_P",
            },
            {
                "year": 2021,
                "varname": "ANYAIDN",
                "varTitle": "Students receiving any aid",
                "longDescription": "Historical source variant for financial aid recipients.",
                "DataType": "num",
                "format": "numeric",
                "source_file": "SFA",
            },
            {
                "year": 2023,
                "varname": "GRN4A2",
                "varTitle": "Graduation rate within 4 years",
                "longDescription": "Graduation rate for the adjusted cohort within four years.",
                "DataType": "num",
                "format": "pct",
                "source_file": "GR200",
            },
            {
                "year": 2023,
                "varname": "TUITION2",
                "varTitle": "Published in-state tuition",
                "longDescription": "Published tuition and required fees for in-state students from the in-state perspective.",
                "DataType": "num",
                "format": "numeric",
                "source_file": "DRVIC",
            },
        ],
    )

    result = run_script(
        "Scripts/10_build_variable_browser.py",
        "--input",
        input_path,
        "--dictionary",
        dictionary_path,
        "--output",
        output_path,
    )

    assert result.returncode == 0, result.stdout
    html_text = output_path.read_text(encoding="utf-8")
    assert "Variable Browser" in html_text
    assert "Reset filters" in html_text
    assert "Replace with import" in html_text
    assert "Add import" in html_text
    assert "Save current set" in html_text
    assert "Duplicate" in html_text
    assert "Rename" in html_text
    assert "Delete" in html_text
    assert "Select group (" in html_text
    assert "Select family (" in html_text

    payload = extract_payload(html_text)
    assert payload["panelName"] == input_path.name
    assert payload["totalVariables"] == 5
    assert payload["panelRows"] == 1
    assert payload["panelYears"] == {"min": 2023, "max": 2023, "count": 1}
    assert isinstance(payload["schemaHash"], str)
    assert len(payload["schemaHash"]) == 64
    assert "Costs / Price" in payload["groupNames"]
    assert "Student Financial Aid" in payload["groupNames"]

    rows = {row["varname"]: row for row in payload["variables"]}
    assert set(rows) == {"INSTNM", "TUITION1", "TUITION2", "ANYAIDN", "GRN4A2"}

    anyaid = rows["ANYAIDN"]
    assert anyaid["primarySource"] == "SFA_P"
    assert anyaid["sourceCount"] == 2
    assert anyaid["componentGroup"] == "Student Financial Aid"
    assert anyaid["semanticFamily"] == "Aid recipients and take-up"
    assert anyaid["variableForm"] == "count"
    assert anyaid["yearMin"] == 2021
    assert anyaid["yearMax"] == 2023
    assert anyaid["coverageBucket"] == "full-window"
    assert anyaid["completenessPct"] == 100.0

    tuition = rows["TUITION1"]
    assert tuition["componentGroup"] == "Costs / Price"
    assert tuition["semanticFamily"] == "Tuition and fees"
    assert tuition["variableForm"] == "amount"
    assert tuition["coverageBucket"] == "full-window"
    assert "TUITION2" in tuition["relatedVariables"]
    assert tuition["panelDataType"] in {"int64", "int64[pyarrow]", "int32", "int32[pyarrow]"}

    instnm = rows["INSTNM"]
    assert instnm["semanticFamily"] == "Institution identity"
    assert instnm["variableForm"] == "text"

    grn4a2 = rows["GRN4A2"]
    assert grn4a2["componentGroup"] == "Graduation / Outcomes"
    assert grn4a2["semanticFamily"] == "Graduation rates"
    assert grn4a2["variableForm"] == "rate"

    presets = {preset["id"]: preset for preset in payload["presets"]}
    assert "core-baseline-controls" in presets
    assert "costs-price" in presets
    assert "aid-packaging" in presets
    assert "enrollment-outcomes" in presets
    assert "INSTNM" in presets["core-baseline-controls"]["variables"]
    assert "TUITION1" in presets["costs-price"]["variables"]
    assert "ANYAIDN" in presets["aid-packaging"]["variables"]
    assert "GRN4A2" in presets["enrollment-outcomes"]["variables"]
