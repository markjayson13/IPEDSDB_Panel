"""Dictionary and SQL exports must preserve a self-contained panel's source metadata."""
from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from openpyxl import load_workbook

from export_metadata import build_export_metadata
from export_metadata_supplement import DEFAULT_REGISTRY, apply_export_metadata_supplement
from helpers import run_script
from panel_export import annotated_schema
from scoped_export_metadata import apply_year_scoped_metadata


def annotated_fixture(tmp_path: Path, *, stale: bool) -> tuple[Path, Path, dict]:
    root = tmp_path / "data"
    final = root / "Final"
    final.mkdir(parents=True)
    (root / "layout.json").write_text(json.dumps({"schema_version": 1, "current_release": "fixture"}))
    registry = json.loads(DEFAULT_REGISTRY.read_text())
    supplement = next(row for row in registry["definitions"] if row["varname"] == "APPDATE" and row["year"] == 2010)
    definitions = []
    for year in [2010, 2011]:
        definitions.append({
            "year": year, "source_file": "IC", "access_table_name": f"IC{year}",
            "varnumber": supplement["varnumber"], "varname": "APPDATE",
            "varTitle": supplement["varTitle"] if year == 2010 else "Later reporting period",
            "longDescription": f"Reporting period description for {year}.",
            "DataType": "N", "format": "Disc", "original_note": "Verbatim source note. " * 1000,
        })
    codes = [{"year": 2011, "source_file": "IC", "access_table_name": "IC2011", "varname": "APPDATE",
              "varnumber": supplement["varnumber"], "codevalue": "1", "valuelabel": "Later fall period"}]
    table = pa.table({"year": [2010, 2011], "UNITID": [100654, 100654], "APPDATE": ["1", "1"]})
    seed_dictionary, seed_codes = tmp_path / "seed_dictionary.parquet", tmp_path / "seed_codes.parquet"
    pq.write_table(pa.Table.from_pylist(definitions), seed_dictionary)
    pq.write_table(pa.Table.from_pylist(codes), seed_codes)
    metadata = build_export_metadata(table.schema, [2010, 2011], seed_dictionary, seed_codes,
                                     observed_years_by_variable={name: [2010, 2011] for name in table.column_names})
    apply_export_metadata_supplement(metadata)
    apply_year_scoped_metadata(metadata)
    expected = next(v for v in metadata["variables"] if v["name"] == "APPDATE")
    assert expected["metadata_status"] == "complete"
    assert any(row.get("metadata_supplement_id") for row in expected["value_label_records"])
    schema = annotated_schema(table.schema, metadata)
    assert b"ipeds:variable:zlib" in schema.field("APPDATE").metadata
    panel = final / "panel_clean_prch_2010_2011.parquet"
    pq.write_table(table.cast(schema), panel)
    seed_dictionary.unlink()
    seed_codes.unlink()
    if stale:
        companions = final / "Metadata"
        companions.mkdir()
        stale_definitions = [{**row, "varTitle": "Explicit older definition", "longDescription": "Older meaning."}
                             for row in definitions]
        stale_codes = [{**codes[0], "year": year, "access_table_name": f"IC{year}", "valuelabel": "Older category"}
                       for year in [2010, 2011]]
        pq.write_table(pa.Table.from_pylist(stale_definitions), companions / "dictionary_lake.parquet")
        pq.write_table(pa.Table.from_pylist(stale_codes), companions / "dictionary_codes.parquet")
    return root, panel, expected


def assert_embedded_variable(metadata: dict, expected: dict) -> None:
    variable = next(v for v in metadata["variables"] if v["name"] == "APPDATE")
    assert variable["label"] == expected["label"] == "[varies by year] Later reporting period"
    assert variable["value_labels"] == expected["value_labels"]
    assert variable["source_metadata"] == expected["source_metadata"]
    assert variable["metadata_origin"] == "embedded_dictionary"
    assert any(row.get("metadata_supplement_id") for row in variable["value_label_records"])
    assert metadata["metadata_scope_policy"] == "explicit_year_scopes"
    assert metadata["readiness_status"] == "complete"


@pytest.mark.parametrize("stale", [False, True])
def test_stage09_prefers_embedded_scoped_supplemented_metadata(tmp_path, stale):
    root, panel, expected = annotated_fixture(tmp_path, stale=stale)
    output = tmp_path / "dictionary.csv"
    result = run_script("Scripts/09_build_panel_dictionary.py", "--root", root, "--input", panel,
                        "--output", output, "--require-ready")
    assert result.returncode == 0, result.stdout
    metadata = json.loads(Path(str(output) + ".metadata.json").read_text())
    assert_embedded_variable(metadata, expected)
    assert metadata["metadata_sources"] == {"dictionary": "embedded_parquet", "codes": "embedded_parquet"}
    assert set(metadata["source_fingerprints"]) == {"panel"}


def test_stage09_offline_workbook_identifies_embedded_metadata(tmp_path):
    root, panel, _ = annotated_fixture(tmp_path, stale=False)
    output = tmp_path / "dictionary.xlsx"
    result = run_script("Scripts/09_build_panel_dictionary.py", "--root", root, "--input", panel,
                        "--output", output, "--require-ready")
    assert result.returncode == 0, result.stdout
    workbook = load_workbook(output)
    assert workbook["about"]["B7"].value == "Embedded definitions in the source panel"
    workbook.close()


@pytest.mark.parametrize("argument,filename", [("--dictionary", "dictionary_lake.parquet"), ("--codes", "dictionary_codes.parquet")])
def test_stage09_explicit_metadata_request_keeps_precedence(tmp_path, argument, filename):
    root, panel, _ = annotated_fixture(tmp_path, stale=True)
    output = tmp_path / "explicit.csv"
    result = run_script("Scripts/09_build_panel_dictionary.py", "--root", root, "--input", panel,
                        argument, root / "Final/Metadata" / filename, "--output", output, "--require-ready")
    assert result.returncode == 0, result.stdout
    metadata = json.loads(Path(str(output) + ".metadata.json").read_text())
    variable = next(v for v in metadata["variables"] if v["name"] == "APPDATE")
    assert variable["label"] == "Explicit older definition"
    assert variable["value_labels"] == [{"value": "1", "label": "Older category"}]
    assert variable["metadata_origin"] == "source_dictionary"


@pytest.mark.parametrize("stale", [False, True])
def test_sql_projection_uses_embedded_scoped_supplemented_metadata(tmp_path, stale):
    root, _, expected = annotated_fixture(tmp_path, stale=stale)
    query = tmp_path / "projection.sql"
    query.write_text("SELECT year, UNITID, APPDATE FROM inspect.panel_clean")
    output = tmp_path / "query_results"
    result = run_script("Scripts/run_saved_query.py", query, "--root", root, "--years", "2010:2011",
                        "--output-dir", output, "--format", "parquet", "--export-mode", "panel")
    assert result.returncode == 0, result.stdout
    metadata = json.loads(next(output.glob("*/result.parquet.metadata.json")).read_text())
    assert_embedded_variable(metadata, expected)
    assert metadata["metadata_source_modes"] == {"dictionary": "embedded_parquet", "codes": "embedded_parquet"}
    assert metadata["metadata_sources"] == {"dictionary": None, "codes": None, "lineage": None}


@pytest.mark.parametrize("expression,name", [("CAST(APPDATE AS INTEGER) + 1 AS APPDATE", "APPDATE"),
                                            ("APPDATE AS PERIOD", "PERIOD")])
def test_sql_computation_or_alias_cannot_inherit_embedded_meaning(tmp_path, expression, name):
    root, _, _ = annotated_fixture(tmp_path, stale=True)
    query = tmp_path / "computed.sql"
    query.write_text(f"SELECT year, UNITID, {expression} FROM inspect.panel_clean")
    output = tmp_path / "query_results"
    result = run_script("Scripts/run_saved_query.py", query, "--root", root, "--years", "2010:2011",
                        "--output-dir", output)
    assert result.returncode == 0, result.stdout
    metadata = json.loads(next(output.glob("*/result.csv.metadata.json")).read_text())
    variable = next(v for v in metadata["variables"] if v["name"] == name)
    assert variable["label"] == name
    assert variable["source_metadata"] == variable["value_labels"] == []
    assert variable["metadata_origin"] == "unverified_sql_expression"
    assert metadata["readiness_status"] == "incomplete"
