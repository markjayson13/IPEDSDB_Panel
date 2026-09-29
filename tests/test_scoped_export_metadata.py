"""Year-varying meanings may be rendered explicitly, never harmonized by guess."""
from __future__ import annotations

import copy
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from export_metadata import build_export_metadata
from scoped_export_metadata import apply_year_scoped_metadata, year_scope_enabled


def definition(year: int, **extra) -> dict:
    return {"year": year, "varname": "CAT", "varnumber": "1", "source_file": "HD",
            "varTitle": "Category", "longDescription": "Category definition", **extra}


def code(year: int, **extra) -> dict:
    return {"year": year, "varname": "CAT", "source_file": "HD", "varnumber": "1",
            "codevalue": "1", "valuelabel": "First category", **extra}


def metadata(tmp_path, definitions, codes, years, observed=None):
    dictionary = tmp_path / "dictionary.parquet"
    codebook = tmp_path / "codebook.parquet"
    pq.write_table(pa.Table.from_pylist(definitions), dictionary)
    pq.write_table(pa.Table.from_pylist(codes), codebook)
    return build_export_metadata(pa.schema([("CAT", pa.string())]), years, dictionary, codebook,
                                 observed_years_by_variable=observed)


def test_explicit_scopes_render_all_original_year_meanings_without_claiming_comparability(tmp_path):
    definitions = [definition(y, varTitle="Earlier category", longDescription="Earlier meaning") for y in [2004, 2005]]
    definitions += [definition(2007, varTitle="Later category", longDescription="Later meaning")]
    codes = [code(y, valuelabel="Earlier code") for y in [2004, 2005]] + [code(2007, valuelabel="Later code")]
    result = metadata(tmp_path, definitions, codes, [2004, 2005, 2007], {"CAT": [2004, 2005, 2007]})
    assert result["metadata_status"] == "incomplete"
    assert result["variables"][0]["label"] == "CAT"
    original_sources = copy.deepcopy(result["variables"][0]["source_metadata"])
    original_codes = copy.deepcopy(result["variables"][0]["value_label_records"])
    original_issues = copy.deepcopy(result["issues"])
    apply_year_scoped_metadata(result)
    variable = result["variables"][0]
    assert variable["label"] == "[varies by year] Later category"
    assert variable["label_reference_year"] == 2007
    assert variable["label_scope"] == "year_specific"
    assert variable["description"] == "2004-2005: Earlier meaning; 2007: Later meaning"
    assert variable["value_labels"] == [{"value": "1", "label": "2004-2005: Earlier code; 2007: Later code"}]
    assert variable["source_metadata"] == original_sources
    assert variable["value_label_records"] == original_codes
    assert [issue["original_issue"] for issue in result["scoped_issues"]] == original_issues
    assert all(issue["severity"] == "info" for issue in result["scoped_issues"])
    assert result["metadata_status"] == "complete"
    assert result["comparability_status"] == variable["comparability_status"] == "conflicting"
    snapshot = copy.deepcopy(result)
    apply_year_scoped_metadata(result)
    assert result == snapshot


def test_latest_title_uses_observed_year_and_requires_observation_evidence(tmp_path):
    definitions = [definition(2022, varTitle="Observed title"), definition(2023, varTitle="Unobserved title")]
    result = metadata(tmp_path, definitions, [code(2022), code(2023)], [2022, 2023], {"CAT": [2022]})
    apply_year_scoped_metadata(result)
    assert result["variables"][0]["label"] == "[varies by year] Observed title"
    assert len(result["variables"][0]["year_scoped_definitions"]) == 2
    unassessed = metadata(tmp_path, definitions, [code(2022), code(2023)], [2022, 2023])
    apply_year_scoped_metadata(unassessed)
    assert unassessed["variables"][0]["label"] == "CAT"
    assert unassessed["metadata_status"] == "incomplete"


def test_same_year_disagreements_remain_unresolved(tmp_path):
    result = metadata(tmp_path, [definition(2023), definition(2023, varTitle="Other title", longDescription="Other meaning")],
                      [code(2023), code(2023, valuelabel="Other code meaning")], [2023], {"CAT": [2023]})
    apply_year_scoped_metadata(result)
    assert result["metadata_status"] == "incomplete"
    assert result["variables"][0]["value_labels"] == []
    assert result["scoped_issues"] == []
    assert {issue["code"] for issue in result["issues"]} >= {
        "variable_label_conflict", "variable_description_conflict", "value_label_conflict",
    }


def test_missing_code_coverage_is_not_cured_by_year_scoped_labels(tmp_path):
    result = metadata(tmp_path, [definition(2021), definition(2022), definition(2023)],
                      [code(2021), code(2022, valuelabel="Later code")], [2021, 2022, 2023], {"CAT": [2021, 2022, 2023]})
    apply_year_scoped_metadata(result)
    assert result["metadata_status"] == "incomplete"
    assert result["variables"][0]["value_labels"] == []
    assert {issue["code"] for issue in result["issues"]} >= {"value_label_coverage_incomplete", "value_label_conflict"}


def test_whitespace_comparison_normalizes_display_only(tmp_path):
    definitions = [definition(2022, varTitle="A  category"), definition(2023, varTitle="A\ncategory")]
    result = metadata(tmp_path, definitions, [code(2022), code(2023)], [2022, 2023], {"CAT": [2022, 2023]})
    apply_year_scoped_metadata(result)
    assert result["metadata_status"] == "complete"
    assert result["variables"][0]["label"] == "A category"
    assert {row["varTitle"] for row in result["variables"][0]["source_metadata"]} == {"A  category", "A\ncategory"}


def test_stable_code_missing_an_observed_year_is_explicitly_scoped(tmp_path):
    rows = [code(2022), code(2023), code(2023, codevalue="2", valuelabel="Later category")]
    result = metadata(tmp_path, [definition(2022), definition(2023)], rows,
                      [2022, 2023], {"CAT": [2022, 2023]})
    assert result["variables"][0]["value_labels"][-1] == {"value": "2", "label": "Later category"}
    apply_year_scoped_metadata(result)
    variable = result["variables"][0]
    assert variable["value_labels"] == [
        {"value": "1", "label": "First category"},
        {"value": "2", "label": "2023: Later category"},
    ]
    assert variable["value_label_scope"] == "year_specific"
    assert variable["resolved_value_label_records"] == rows
    # A value 2 observed in 2022 still fails its own year's code validation;
    # the display scope is not a made-up 2022 definition.
    import pyarrow.dataset as ds
    from export_integrity import validate_observed_codes
    observed = ds.dataset(pa.table({"year": [2022, 2023], "CAT": ["2", "2"]}))
    checked = validate_observed_codes(observed, result)
    assert checked["unknown_code_count"] == 1
    assert checked["issues"][0]["examples"] == [{"year": 2022, "value": "2"}]
    snapshot = copy.deepcopy(result)
    apply_year_scoped_metadata(result)
    assert result == snapshot


def test_inactive_unmatched_codes_remain_auditable_without_blocking_observed_years(tmp_path):
    rows = [code(2022, valuelabel="Unmatched inactive year"), code(2023)]
    result = metadata(tmp_path, [definition(2023)], rows, [2022, 2023], {"CAT": [2023]})
    variable = result["variables"][0]
    assert result["metadata_status"] == "complete"
    assert variable["value_label_records"] == rows
    assert variable["resolved_value_label_records"] == [rows[1]]
    assert variable["inactive_unmatched_value_label_records"] == [rows[0]]
    assert variable["value_labels"] == [{"value": "1", "label": "First category"}]
    observed = metadata(tmp_path, [definition(2023)], rows, [2022, 2023], {"CAT": [2022, 2023]})
    assert observed["metadata_status"] == "incomplete"
    assert "value_label_source_unmatched" in {issue["code"] for issue in observed["issues"]}


def test_resolved_definitions_do_not_clear_observation_or_availability_failures(tmp_path):
    result = metadata(tmp_path, [definition(2022, varTitle="Old"), definition(2023)],
                      [code(2022), code(2023)], [2022, 2023], {"CAT": [2022, 2023]})
    result["issues"].append({"severity": "warning", "code": "unknown_observed_code", "variable": "CAT", "message": "Unknown code."})
    result["readiness_status"] = "incomplete"
    result["observation_validation"] = {"status": "incomplete"}
    apply_year_scoped_metadata(result)
    assert result["readiness_status"] == result["metadata_status"] == "incomplete"
    assert [issue["code"] for issue in result["issues"]] == ["unknown_observed_code"]


def test_scope_policy_survives_stored_panel_reexport_and_provenance(tmp_path):
    schema = pa.schema([("CAT", pa.string())])
    assert not year_scope_enabled(schema)
    assert year_scope_enabled(schema, requested=True)
    scoped_schema = schema.with_metadata({b"ipeds:export": json.dumps({
        "format": "parquet", "metadata_scope_policy": "explicit_year_scopes",
    }).encode()})
    assert year_scope_enabled(scoped_schema)
    dictionary = tmp_path / "dictionary.parquet"
    codes = tmp_path / "codes.parquet"
    pq.write_table(pa.Table.from_pylist([definition(2023)]), dictionary)
    pq.write_table(pa.Table.from_pylist([code(2023)]), codes)
    result = build_export_metadata(scoped_schema, [2023], dictionary, codes)
    assert result["upstream_export_provenance"][0]["metadata_scope_policy"] == "explicit_year_scopes"


@pytest.mark.parametrize("embedded", [b"broken json", b"[]", b"null", b"\xff"])
def test_malformed_embedded_export_scope_fails_clearly(embedded):
    schema = pa.schema([("CAT", pa.string())]).with_metadata({b"ipeds:export": embedded})
    with pytest.raises(ValueError, match="embedded export scope policy"):
        year_scope_enabled(schema)
