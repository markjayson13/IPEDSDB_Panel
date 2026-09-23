"""Metadata exports must retain provenance without inventing universal meanings."""
from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from export_metadata import build_export_metadata


def write_records(path: Path, rows: list[dict]) -> Path:
    columns = sorted(set().union(*(row.keys() for row in rows)))
    pq.write_table(pa.table({column: [row.get(column) for row in rows] for column in columns}), path)
    return path


def definition(year=2023, source="HD", name="CONTROL", **kwargs) -> dict:
    return {
        "year": year, "source_file": source, "varnumber": "00000012", "varname": name,
        "varTitle": "Institutional control", "longDescription": "Institutional control sector.",
        **kwargs,
    }


def code(year=2023, source="HD", **kwargs) -> dict:
    return {
        "year": year, "source_file": source, "varnumber": "00000012", "varname": "",
        "codevalue": "1", "valuelabel": "Public", **kwargs,
    }


def test_missing_metadata_keeps_schema_and_marks_unknown_definitions() -> None:
    result = build_export_metadata(pa.schema([("UNITID", pa.int64()), ("year", pa.int32()), ("UNKNOWN", pa.float64())]), [2023], None, None)
    unitid, year, unknown = result["variables"]
    assert unitid["label"] == "IPEDS institution identifier"
    assert year["label"] == "IPEDS reporting year"
    assert unitid["metadata_origin"] == year["metadata_origin"] == "controlled_panel_key"
    assert unknown["label"] == "UNKNOWN"
    assert unknown["description"] == ""
    assert unknown["source_metadata"] == []
    assert unknown["metadata_status"] == result["metadata_status"] == "incomplete"
    assert {row["code"] for row in result["issues"]} >= {"dictionary_unavailable", "codes_unavailable", "variable_metadata_missing"}
    json.dumps(result, allow_nan=False)


def test_only_selected_years_define_labels_and_preserve_source_fields(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [
        definition(2022, varTitle="Old control", units="Old units"),
        definition(2023, units="Provided units", currency="USD", release_type="Final"),
    ])
    codes = write_records(tmp_path / "codes.parquet", [code(2022, valuelabel="Old meaning"), code(2023)])
    result = build_export_metadata(pa.schema([("CONTROL", pa.int64())]), [2023], dictionary, codes)
    variable = result["variables"][0]
    assert variable["label"] == "Institutional control"
    assert variable["value_labels"] == [{"value": "1", "label": "Public"}]
    assert variable["metadata_status"] == "complete"
    assert len(variable["source_metadata"]) == 1
    assert variable["source_metadata"][0]["units"] == "Provided units"
    assert variable["source_metadata"][0]["currency"] == "USD"
    assert all(row["year"] == 2023 for row in variable["value_label_records"])


def test_conflicting_definitions_remain_in_sidecars_without_arbitrary_choice(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [
        definition(2022, varTitle="Old control", longDescription="Old definition"),
        definition(2023, varTitle="New control", longDescription="New definition"),
    ])
    codes = write_records(tmp_path / "codes.parquet", [code(2022), code(2023)])
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32())]), [2023, 2022], dictionary, codes)
    variable = result["variables"][0]
    assert result["years"] == [2022, 2023]
    assert variable["label"] == "CONTROL"
    assert variable["description"] == ""
    assert len(variable["source_metadata"]) == 2
    assert variable["metadata_status"] == "incomplete"
    assert {row["code"] for row in result["issues"]} >= {"variable_label_conflict", "variable_description_conflict"}


def test_number_only_codes_require_year_source_and_table_scope(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition(source_table="HD2023")])
    codes = write_records(tmp_path / "codes.parquet", [
        code(varnumber="12.0", source_table="HD2023"),
        code(2022, source_table="HD2023", valuelabel="Wrong year"),
        code(source="FIN", source_table="HD2023", valuelabel="Wrong source"),
        code(source_table="OTHER", valuelabel="Wrong source table"),
        code(source=None, source_table=None, valuelabel="Unscoped"),
        code(varname="OTHER_VARIABLE", source_table="HD2023", valuelabel="Wrong named variable"),
    ])
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32())]), [2023], dictionary, codes)
    variable = result["variables"][0]
    assert variable["value_labels"] == [{"value": "1", "label": "Public"}]
    assert len(variable["value_label_records"]) == 1


def test_code_meaning_conflicts_disable_universal_mapping(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition(2022), definition(2023)])
    codes = write_records(tmp_path / "codes.parquet", [code(2022), code(2023, valuelabel="Private")])
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32())]), [2022, 2023], dictionary, codes)
    variable = result["variables"][0]
    assert variable["value_labels"] == []
    assert len(variable["value_label_records"]) == 2
    assert variable["metadata_status"] == "incomplete"
    assert "value_label_conflict" in {row["code"] for row in result["issues"]}


def test_nonnumeric_and_literal_na_codes_are_preserved(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition()])
    codes = write_records(tmp_path / "codes.parquet", [code(codevalue="NA", valuelabel="Not applicable"), code(codevalue="R", valuelabel="Reported")])
    variable = build_export_metadata(pa.schema([("CONTROL", pa.string())]), [2023], dictionary, codes)["variables"][0]
    assert variable["value_labels"] == [{"value": "NA", "label": "Not applicable"}, {"value": "R", "label": "Reported"}]
    assert {row["codevalue"] for row in variable["value_label_records"]} == {"NA", "R"}


def test_lineage_resolves_alias_and_restricts_sources(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition(), definition(source="OTHER", varTitle="Unrelated")])
    codes = write_records(tmp_path / "codes.parquet", [code(), code(source="OTHER", valuelabel="Unrelated")])
    lineage = tmp_path / "lineage.csv"
    lineage.write_text("output_column,source_varnames,source_files,primary_source_file\nCONTROL_ALIAS,CONTROL,HD,HD\n", encoding="utf-8")
    result = build_export_metadata(pa.schema([("CONTROL_ALIAS", pa.int32())]), [2023], dictionary, codes, lineage)
    variable = result["variables"][0]
    assert variable["name"] == "CONTROL_ALIAS"
    assert variable["label"] == "Institutional control"
    assert variable["value_labels"] == [{"value": "1", "label": "Public"}]
    assert {record["source_file"] for record in variable["source_metadata"]} == {"HD"}
    assert variable["lineage_records"][0]["source_varnames"] == "CONTROL"


def test_generic_imputation_codes_use_explicit_parent_mapping_only(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [
        definition(imputationvar="XCONTROL"),
        definition(name="XCONTROL", varnumber="00000013", varTitle="Control imputation flag"),
        definition(source="FIN", name="OTHER", imputationvar="XOTHER"),
    ])
    codes = write_records(tmp_path / "codes.parquet", [
        code(varnumber="", codevalue="R", valuelabel="Reported", label_scope="imputation_variable"),
        code(source="FIN", varnumber="", codevalue="R", valuelabel="Wrong source flag", label_scope="imputation_variable"),
        code(2022, varnumber="", codevalue="R", valuelabel="Wrong year flag", label_scope="imputation_variable"),
    ])
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32()), ("XCONTROL", pa.string())]), [2023], dictionary, codes)
    control, flag = result["variables"]
    assert control["value_labels"] == []
    assert flag["value_labels"] == [{"value": "R", "label": "Reported"}]
    assert len(flag["value_label_records"]) == 1
    assert flag["imputation_parent_metadata"][0]["varname"] == "CONTROL"


def test_metadata_without_year_is_not_silently_applied(tmp_path: Path) -> None:
    dictionary = tmp_path / "dictionary.csv"
    dictionary.write_text("varname,varTitle,longDescription\nCONTROL,Control,Description\n", encoding="utf-8")
    codes = write_records(tmp_path / "codes.parquet", [code()])
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32())]), [2023], dictionary, codes)
    assert result["variables"][0]["source_metadata"] == []
    assert result["variables"][0]["metadata_status"] == "incomplete"
    assert "dictionary_year_missing" in {row["code"] for row in result["issues"]}


def test_missing_year_coverage_is_explicit(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition(2023)])
    codes = write_records(tmp_path / "codes.parquet", [code(2023)])
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32())]), [2022, 2023], dictionary, codes)
    assert result["variables"][0]["metadata_status"] == "incomplete"
    issue = next(row for row in result["issues"] if row["code"] == "variable_year_coverage_incomplete")
    assert "2022" in issue["message"]


def test_partial_definitions_and_missing_categorical_codes_are_incomplete(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [
        definition(2022, DataType="disc", longDescription=None),
        definition(2023, DataType="disc"),
    ])
    codes = write_records(tmp_path / "codes.parquet", [code(varnumber="99999999")])
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32())]), [2022, 2023], dictionary, codes)
    variable = result["variables"][0]
    assert variable["metadata_status"] == "incomplete"
    assert variable["value_labels"] == []
    assert {row["code"] for row in result["issues"]} >= {"variable_description_coverage_incomplete", "value_labels_missing"}


def test_named_codes_without_matching_year_source_are_preserved_only(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition(2023)])
    codes = write_records(tmp_path / "codes.parquet", [code(2022, varname="CONTROL"), code(2023)])
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32())]), [2022, 2023], dictionary, codes)
    variable = result["variables"][0]
    assert variable["value_labels"] == []
    assert len(variable["value_label_records"]) == 2
    assert "value_label_source_unmatched" in {row["code"] for row in result["issues"]}


def test_partial_code_year_coverage_does_not_create_universal_labels(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition(2022), definition(2023)])
    codes = write_records(tmp_path / "codes.parquet", [code(2022)])
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32())]), [2022, 2023], dictionary, codes)
    variable = result["variables"][0]
    assert variable["value_labels"] == []
    assert variable["value_label_records"][0]["year"] == 2022
    assert variable["metadata_status"] == "incomplete"
    issue = next(row for row in result["issues"] if row["code"] == "value_label_coverage_incomplete")
    assert "2023/HD" in issue["message"]


def embedded_schema(schema: pa.Schema, metadata: dict) -> pa.Schema:
    return pa.schema([
        field.with_metadata({b"ipeds:variable": json.dumps(variable).encode("utf-8")})
        for field, variable in zip(schema, metadata["variables"])
    ])


def test_parquet_embedded_metadata_can_travel_without_dictionary_files(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition()])
    codes = write_records(tmp_path / "codes.parquet", [code()])
    schema = pa.schema([("CONTROL", pa.int32())])
    initial = build_export_metadata(schema, [2023], dictionary, codes)
    labeled_path = tmp_path / "labeled.parquet"
    pq.write_table(pa.Table.from_arrays([pa.array([1], type=pa.int32())], schema=embedded_schema(schema, initial)), labeled_path)
    dictionary.unlink()
    codes.unlink()
    offline = build_export_metadata(pq.read_schema(labeled_path), [2023], None, None)
    variable = offline["variables"][0]
    assert variable["label"] == "Institutional control"
    assert variable["value_labels"] == [{"value": "1", "label": "Public"}]
    assert variable["metadata_status"] == "complete"
    assert variable["metadata_origin"] == "embedded_dictionary"
    assert offline["metadata_sources"] == {"dictionary": "embedded_parquet", "codes": "embedded_parquet"}


def test_embedded_metadata_is_refiltered_and_explicit_sources_are_not_replaced(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition(2022), definition(2023)])
    codes = write_records(tmp_path / "codes.parquet", [code(2022, valuelabel="Old control"), code(2023)])
    schema = pa.schema([("CONTROL", pa.int32())])
    initial = build_export_metadata(schema, [2022, 2023], dictionary, codes)
    labeled_schema = embedded_schema(schema, initial)
    subset = build_export_metadata(labeled_schema, [2023], None, None)
    assert subset["variables"][0]["value_labels"] == [{"value": "1", "label": "Public"}]
    assert subset["variables"][0]["metadata_status"] == "complete"
    assert len(subset["variables"][0]["source_metadata"]) == 1
    unavailable = build_export_metadata(labeled_schema, [2023], tmp_path / "missing.parquet", codes)
    assert unavailable["variables"][0]["source_metadata"] == []
    assert unavailable["variables"][0]["metadata_status"] == "incomplete"
    assert "dictionary_unavailable" in {row["code"] for row in unavailable["issues"]}


def test_missing_original_codes_do_not_become_complete_offline(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition()])
    schema = pa.schema([("CONTROL", pa.int32())])
    initial = build_export_metadata(schema, [2023], dictionary, None)
    offline = build_export_metadata(embedded_schema(schema, initial), [2023], None, None)
    assert offline["variables"][0]["metadata_status"] == "incomplete"
    assert "embedded_codes_incomplete" in {row["code"] for row in offline["issues"]}


def test_external_sources_do_not_borrow_missing_embedded_companion(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition()])
    codes = write_records(tmp_path / "codes.parquet", [code()])
    schema = pa.schema([("CONTROL", pa.int32())])
    initial = build_export_metadata(schema, [2023], dictionary, codes)
    labeled_schema = embedded_schema(schema, initial)
    dictionary_only = build_export_metadata(labeled_schema, [2023], dictionary, None)
    assert dictionary_only["variables"][0]["value_labels"] == []
    assert dictionary_only["variables"][0]["metadata_status"] == "incomplete"
    assert "codes_unavailable" in {row["code"] for row in dictionary_only["issues"]}
    codes_only = build_export_metadata(labeled_schema, [2023], None, codes)
    assert codes_only["variables"][0]["source_metadata"] == []
    assert codes_only["variables"][0]["metadata_status"] == "incomplete"
    assert "dictionary_unavailable" in {row["code"] for row in codes_only["issues"]}
