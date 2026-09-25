"""Metadata exports must retain provenance without inventing universal meanings."""
from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from export_metadata import build_export_metadata, discover_export_metadata


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


def test_unnamed_code_ambiguity_is_checked_against_unexported_variables_and_survives_offline(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [
        definition(name="A", source_table="TABLE_A"),
        definition(name="B", source_table="TABLE_B"),
    ])
    codes = write_records(tmp_path / "codes.parquet", [code()])
    schema = pa.schema([("A", pa.int32())])
    initial = build_export_metadata(schema, [2023], dictionary, codes)
    for result in (initial, build_export_metadata(embedded_schema(schema, initial), [2023], None, None)):
        variable = result["variables"][0]
        assert variable["value_labels"] == []
        assert variable["resolved_value_label_records"] == []
        assert len(variable["value_label_records"]) == 1
        assert {row["varname"] for row in variable["code_identity_candidates"]} == {"A", "B"}
        assert variable["metadata_status"] == "incomplete"
        assert "value_label_identity_ambiguous" in {row["code"] for row in result["issues"]}


def test_explicit_table_identity_disambiguates_unnamed_codes(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [
        definition(name="A", source_table="TABLE_A"), definition(name="B", source_table="TABLE_B"),
    ])
    codes = write_records(tmp_path / "codes.parquet", [code(source_table="TABLE_A")])
    result = build_export_metadata(pa.schema([("A", pa.int32())]), [2023], dictionary, codes)
    assert result["variables"][0]["value_labels"] == [{"value": "1", "label": "Public"}]
    assert result["metadata_status"] == "complete"


@pytest.mark.parametrize("lineage", [
    "output_column,source_varnames,source_files\nCONTROL,A|B,HD\n",
    "output_column,source_varnames,source_files\nCONTROL,A,HD|MISSING\n",
])
def test_every_declared_lineage_component_requires_a_definition(tmp_path: Path, lineage: str) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition(name="A")])
    codes = write_records(tmp_path / "codes.parquet", [code()])
    lineage_path = tmp_path / "lineage.csv"
    lineage_path.write_text(lineage, encoding="utf-8")
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32())]), [2023], dictionary, codes, lineage_path)
    variable = result["variables"][0]
    assert variable["label"] == "CONTROL"
    assert variable["description"] == ""
    assert variable["value_labels"] == variable["resolved_value_label_records"] == []
    assert variable["source_metadata"][0]["varname"] == "A"
    assert variable["value_label_records"][0]["valuelabel"] == "Public"
    assert result["metadata_status"] == "incomplete"
    assert "lineage_metadata_incomplete" in {row["code"] for row in result["issues"]}


def test_malformed_code_schema_cannot_certify_a_numeric_variable(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition(DataType="numeric")])
    codes = write_records(tmp_path / "codes.parquet", [{"year": 2023}])
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32())]), [2023], dictionary, codes)
    assert result["metadata_status"] == result["variables"][0]["metadata_status"] == "incomplete"
    assert "codes_schema_invalid" in {row["code"] for row in result["issues"]}
    assert result["unapplied_metadata_records"]["codes"] == [{"year": 2023}]


@pytest.mark.parametrize("missing_column", ["varname", "varTitle", "longDescription", "source_file"])
def test_dictionary_requires_definition_and_source_columns(tmp_path: Path, missing_column: str) -> None:
    record = definition()
    del record[missing_column]
    dictionary = write_records(tmp_path / "dictionary.parquet", [record])
    codes = write_records(tmp_path / "codes.parquet", [code()])
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32())]), [2023], dictionary, codes)
    assert result["metadata_status"] == "incomplete"
    assert "dictionary_schema_invalid" in {row["code"] for row in result["issues"]}
    assert result["unapplied_metadata_records"]["dictionary"] == [record]


def test_valid_empty_codebook_retains_verified_schema_offline(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition(DataType="numeric")])
    codes = tmp_path / "codes.parquet"
    pq.write_table(pa.table({"year": pa.array([], pa.int32()), "varname": pa.array([], pa.string()),
                            "source_file": pa.array([], pa.string()), "codevalue": pa.array([], pa.string()),
                            "valuelabel": pa.array([], pa.string())}), codes)
    schema = pa.schema([("CONTROL", pa.int32())])
    initial = build_export_metadata(schema, [2023], dictionary, codes)
    offline = build_export_metadata(embedded_schema(schema, initial), [2023], None, None)
    assert initial["metadata_status"] == offline["metadata_status"] == "complete"
    assert offline["variables"][0]["value_labels"] == []


def test_empty_malformed_codebook_and_unidentified_rows_are_not_complete(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition(DataType="numeric")])
    codes = tmp_path / "codes.parquet"
    pq.write_table(pa.table({"year": pa.array([], pa.int32())}), codes)
    schema = pa.schema([("CONTROL", pa.int32())])
    assert build_export_metadata(schema, [2023], dictionary, codes)["metadata_status"] == "incomplete"
    write_records(codes, [code(varnumber="", varname="")])
    result = build_export_metadata(schema, [2023], dictionary, codes)
    assert result["metadata_status"] == "incomplete"
    assert "codes_record_identity_missing" in {row["code"] for row in result["issues"]}


def test_measurement_conflicts_and_unknown_fields_are_explicit(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [
        definition(2022, units="dollars", currency="USD"),
        definition(2023, units="thousands of dollars", currency="EUR"),
    ])
    codes = write_records(tmp_path / "codes.parquet", [code(2022), code(2023)])
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32())]), [2022, 2023], dictionary, codes)
    variable = result["variables"][0]
    assert variable["comparability_status"] == result["comparability_status"] == "conflicting"
    assert variable["semantic_metadata"]["units"]["value"] is None
    assert variable["semantic_metadata"]["units"]["values"] == ["dollars", "thousands of dollars"]
    assert variable["unknown_semantic_fields"] == ["price_basis", "reference_period"]
    assert variable["semantic_metadata"]["reference_period"]["status"] == "unknown"
    assert variable["metadata_status"] == "incomplete"
    assert "semantic_metadata_conflict" in {row["code"] for row in result["issues"]}
    assert variable["source_metadata"][1]["currency"] == "EUR"


def test_missing_semantics_do_not_invent_comparability_or_make_known_labels_incomplete(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition()])
    codes = write_records(tmp_path / "codes.parquet", [code()])
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32())]), [2023], dictionary, codes)
    variable = result["variables"][0]
    assert variable["metadata_status"] == "complete"
    assert variable["comparability_status"] == "unknown"
    assert len(variable["unknown_semantic_fields"]) == 4
    assert all(details["value"] is None for details in variable["semantic_metadata"].values())


def test_fully_declared_semantics_can_agree_without_inferring_missing_years(tmp_path: Path) -> None:
    supplied = dict(units="dollars", currency="USD", price_basis="current", reference_period="academic year")
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition(2022, **supplied), definition(2023, **supplied)])
    codes = write_records(tmp_path / "codes.parquet", [code(2022), code(2023)])
    schema = pa.schema([("CONTROL", pa.int32())])
    result = build_export_metadata(schema, [2022, 2023], dictionary, codes)
    assert result["comparability_status"] == "consistent"
    assert result["variables"][0]["unknown_semantic_fields"] == []
    write_records(dictionary, [definition(2022, **supplied), definition(2023)])
    partial = build_export_metadata(schema, [2022, 2023], dictionary, codes)
    assert partial["comparability_status"] == "unknown"
    assert partial["variables"][0]["semantic_metadata"]["currency"]["missing_source_records"] == 1


def test_resolved_physical_identity_overrides_original_but_preserves_it(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [
        definition(access_table_name="ORIGINAL_WRONG", resolved_physical_table="ACTUAL_TABLE"),
    ])
    codes = write_records(tmp_path / "codes.parquet", [code(access_table_name="ACTUAL_TABLE")])
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32())]), [2023], dictionary, codes)
    variable = result["variables"][0]
    assert variable["value_labels"] == [{"value": "1", "label": "Public"}]
    assert variable["source_metadata"][0]["access_table_name"] == "ORIGINAL_WRONG"
    assert variable["source_metadata"][0]["resolved_physical_table"] == "ACTUAL_TABLE"


def test_canonical_value_lineage_is_projected_deduplicated_and_year_scoped(tmp_path: Path, monkeypatch) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [
        definition(name="RAW", access_table_name="ORIGINAL", resolved_physical_table="TABLE_A"),
        definition(name="RAW", access_table_name="TABLE_B", varTitle="Other table"),
    ])
    codes = write_records(tmp_path / "codes.parquet", [code(access_table_name="TABLE_A")])
    lineage = tmp_path / "canonical.parquet"
    write_records(lineage, [
        {"analysis_column": "CONTROL", "year": 2023, "varname": "RAW", "source_file": "HD",
         "source_varnumber": "12", "access_table_name": "TABLE_A", "UNITID": i, "value": i}
        for i in range(50)
    ] + [{"analysis_column": "CONTROL", "year": 2022, "varname": "OTHER", "source_file": "OTHER",
          "source_varnumber": "99", "access_table_name": "OTHER", "UNITID": 1, "value": 9}])
    original_read = pq.read_table
    def no_full_lineage_read(path, *args, **kwargs):
        assert Path(path) != lineage, "Canonical lineage must not be materialized through pq.read_table"
        return original_read(path, *args, **kwargs)
    monkeypatch.setattr(pq, "read_table", no_full_lineage_read)
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32())]), [2023], dictionary, codes, lineage)
    variable = result["variables"][0]
    assert result["metadata_status"] == "complete"
    assert len(variable["lineage_records"]) == 1
    assert "UNITID" not in variable["lineage_records"][0]
    assert variable["source_metadata"][0]["resolved_physical_table"] == "TABLE_A"
    assert variable["value_labels"] == [{"value": "1", "label": "Public"}]


def test_unrecognized_lineage_schema_is_reported(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition()])
    codes = write_records(tmp_path / "codes.parquet", [code()])
    lineage = write_records(tmp_path / "lineage.parquet", [{"something_else": "CONTROL"}])
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32())]), [2023], dictionary, codes, lineage)
    assert result["metadata_status"] == "incomplete"
    assert "lineage_schema_invalid" in {row["code"] for row in result["issues"]}


def test_every_canonical_physical_scope_requires_its_own_definition(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition(name="RAW", access_table_name="TABLE_A")])
    codes = write_records(tmp_path / "codes.parquet", [code(access_table_name="TABLE_A")])
    lineage = write_records(tmp_path / "lineage.parquet", [
        {"analysis_column": "CONTROL", "year": 2023, "varname": "RAW", "source_file": "HD", "access_table_name": table}
        for table in ("TABLE_A", "TABLE_B")
    ])
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32())]), [2023], dictionary, codes, lineage)
    assert result["metadata_status"] == "incomplete"
    assert result["variables"][0]["value_labels"] == []
    assert "TABLE_B" in next(row["message"] for row in result["issues"] if row["code"] == "lineage_metadata_incomplete")


def test_missing_definition_year_prevents_false_consistent_comparability(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition(
        units="dollars", currency="USD", price_basis="current", reference_period="academic year")])
    codes = write_records(tmp_path / "codes.parquet", [code()])
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32())]), [2022, 2023], dictionary, codes)
    assert result["metadata_status"] == "incomplete"
    assert result["comparability_status"] == "unknown"


def test_numeric_storage_with_discrete_format_requires_a_codebook(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition(DataType="N", format="Disc")])
    codes = tmp_path / "codes.parquet"
    pq.write_table(pa.table({"year": pa.array([], pa.int32()), "varname": pa.array([], pa.string()),
                            "source_file": pa.array([], pa.string()), "codevalue": pa.array([], pa.string()),
                            "valuelabel": pa.array([], pa.string())}), codes)
    result = build_export_metadata(pa.schema([("CONTROL", pa.int32())]), [2023], dictionary, codes)
    assert result["metadata_status"] == "incomplete"
    assert "value_labels_missing" in {row["code"] for row in result["issues"]}


def test_v2_panel_discovers_own_versioned_dictionary_and_canonical_lineage(tmp_path: Path) -> None:
    own = tmp_path / "own"
    other = tmp_path / "configured"
    panel = own / "Panels/v2/panel_clean_prch_2004_2023.parquet"
    dictionary = own / "Dictionary/v2/dictionary_lake.parquet"
    lineage = own / "Panels/v2/wide_release/current/qc/wide_qc/qc_value_lineage.parquet"
    for path in (panel, dictionary, lineage, own / "Dictionary/dictionary_lake.parquet", other / "Dictionary/v2/dictionary_lake.parquet"):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    assert discover_export_metadata(None, "Dictionary/dictionary_lake.parquet", panel, other) == dictionary
    assert discover_export_metadata(None, "Checks/wide_qc/qc_column_lineage.csv", panel, other) == lineage
    package_lineage = own / "Checks/v2/wide_qc/qc_value_lineage.parquet"
    package_lineage.parent.mkdir(parents=True)
    package_lineage.touch()
    assert discover_export_metadata(None, "Checks/wide_qc/qc_column_lineage.csv", panel, other) == package_lineage


def test_v2_discovery_does_not_borrow_legacy_metadata_when_versioned_file_is_missing(tmp_path: Path) -> None:
    panel = tmp_path / "Panels/v2/panel.parquet"
    legacy = tmp_path / "Dictionary/dictionary_lake.parquet"
    legacy.parent.mkdir()
    legacy.touch()
    assert discover_export_metadata(None, "Dictionary/dictionary_lake.parquet", panel, tmp_path) is None
    assert discover_export_metadata(legacy, "Dictionary/dictionary_lake.parquet", panel, tmp_path) == legacy
    with pytest.raises(ValueError, match="does not exist"):
        discover_export_metadata(tmp_path / "missing", "Dictionary/dictionary_lake.parquet", panel, tmp_path)


def test_offline_reexport_retains_original_source_and_code_provenance(tmp_path: Path) -> None:
    dictionary = write_records(tmp_path / "dictionary.parquet", [definition()])
    codes = write_records(tmp_path / "codes.parquet", [code()])
    schema = pa.schema([("CONTROL", pa.int32())])
    initial = build_export_metadata(schema, [2023], dictionary, codes)
    provenance = {"source_panel": "/original/panel.parquet", "source_panel_sha256": "original-data-hash",
                  "exporter_provenance": {"git_commit": "original-code-sha"},
                  "metadata_source_sha256": {"dictionary": "original-dictionary-hash"},
                  "format": "parquet", "row_count": 1}
    annotated = embedded_schema(schema, initial).with_metadata({b"ipeds:export": json.dumps(provenance).encode()})
    offline = build_export_metadata(annotated, [2023], None, None)
    assert offline["upstream_export_provenance"] == [provenance]
    second_parent = {"source_panel": "/local/intermediate.parquet", "source_panel_sha256": "intermediate-hash",
                     "upstream_export_provenance": offline["upstream_export_provenance"]}
    second = embedded_schema(schema, offline).with_metadata({b"ipeds:export": json.dumps(second_parent).encode()})
    again = build_export_metadata(second, [2023], None, None)
    assert len(again["upstream_export_provenance"]) == 2
    assert again["upstream_export_provenance"][-1] == provenance
