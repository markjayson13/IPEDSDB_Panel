"""Year-varying meanings may be rendered explicitly, never harmonized by guess."""
from __future__ import annotations

import copy
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from export_metadata import build_export_metadata, decode_embedded_variable_metadata
from scoped_export_metadata import apply_year_scoped_metadata, year_scope_enabled
from helpers import run_script


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


def test_returning_sfa_identity_cannot_relabel_retired_historical_column(tmp_path):
    old, current = "AGRNT_A__SFA__OLD", "AGRNT_A__SFA_P__CURRENT"
    records = [definition(year, varname="AGRNT_A", varnumber="70321", source_file=source,
                          access_table_name=table, varTitle=title, longDescription=title, DataType="N")
               for year, source, table, title in [
                   (2008, "SFA", "SFA0708", "Historical definition"),
                   (2023, "SFA_P", "SFA2223_P1", "Recent definition"),
                   (2024, "SFA", "SFA2324", "New collection definition")]]
    dictionary, codes, lineage = [tmp_path / name for name in ("dictionary.parquet", "codes.parquet", "lineage.parquet")]
    pq.write_table(pa.Table.from_pylist(records), dictionary)
    pq.write_table(pa.table({"year": pa.array([], pa.int64()), "varname": pa.array([], pa.string()),
                            "source_file": pa.array([], pa.string()), "varnumber": pa.array([], pa.string()),
                            "codevalue": pa.array([], pa.string()), "valuelabel": pa.array([], pa.string())}), codes)
    pq.write_table(pa.Table.from_pylist([
        {"analysis_column": old if r["year"] == 2008 else current, "year": r["year"],
         "varname": r["varname"], "source_file": r["source_file"],
         "access_table_name": r["access_table_name"], "source_varnumber": r["varnumber"]}
        for r in records]), lineage)
    result = build_export_metadata(pa.schema([(old, pa.float64()), (current, pa.float64())]),
                                   [2008, 2023, 2024], dictionary, codes, lineage,
                                   observed_years_by_variable={old: [2008], current: [2023, 2024]})
    apply_year_scoped_metadata(result)
    variables = {v["name"]: v for v in result["variables"]}
    assert {r["year"] for r in variables[old]["source_metadata"]} == {2008}
    assert variables[old]["label"] == "Historical definition"
    assert {r["year"] for r in variables[current]["source_metadata"]} == {2023, 2024}
    assert variables[current]["label"] == "[varies by year] New collection definition"
    assert result["metadata_status"] == "complete"


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


def test_consolidation_survives_real_export_and_embedded_reexport_with_year_meanings(tmp_path):
    panel = tmp_path / "source.parquet"
    dictionary = tmp_path / "dictionary.parquet"
    codes = tmp_path / "codes.parquet"
    lineage = tmp_path / "lineage.parquet"
    rule = {
        "canonical_name": "CAT", "rationale": "Verified source table relocation; no value recoding.",
        "members": [{"column": "CAT__OLD__00000001__A", "years": [2004]},
                    {"column": "CAT__NEW__00000001__B", "years": [2006],
                     "source_metadata_years": [2006, 2024]}],
        "caveats": ["Meaning changes in 2006; preserve annual labels.",
                    "The documented 2024 field is all null."],
    }
    consolidation = {"policy_id": "source-family-consolidation-v1", "groups": [rule]}
    table = pa.table({"UNITID": [100001, 100001, 100001], "year": [2004, 2006, 2024],
                      "CAT": pa.array(["1", "1", None], pa.string())})
    table = table.replace_schema_metadata({b"ipeds:column_consolidation": json.dumps(consolidation).encode()})
    pq.write_table(table, panel)
    records = [definition(y, source_file=source, access_table_name=f"{source}{y}",
                          varTitle=title, longDescription=f"{title} definition", DataType="N")
               for y, source, title in [(2004, "OLD", "Earlier category"),
                                         (2006, "NEW", "Later category"),
                                         (2024, "NEW", "Documented all-null category")]]
    pq.write_table(pa.Table.from_pylist(records), dictionary)
    pq.write_table(pa.Table.from_pylist([
        code(r["year"], source_file=r["source_file"], valuelabel=r["varTitle"])
        for r in records]), codes)
    pq.write_table(pa.Table.from_pylist([
        {"analysis_column": "CAT", "year": r["year"], "varname": "CAT", "source_file": r["source_file"],
         "source_varnumber": "1", "access_table_name": r["access_table_name"]}
        for r in records]), lineage)
    portable = tmp_path / "portable.parquet"
    result = run_script("Scripts/08_build_custom_panel.py", "--input", panel, "--output", portable,
                        "--all-vars", "--year-scoped-labels", "--require-ready", "--log-file", "",
                        "--dictionary", dictionary, "--codes", codes, "--column-lineage", lineage,
                        env={"IPEDSDB_ROOT": str(tmp_path)})
    assert result.returncode == 0, result.stdout
    assert pq.read_table(portable).to_pydict() == table.to_pydict()
    first = json.loads(portable.with_name(portable.name + ".metadata.json").read_text())
    assert first["column_consolidation"] == consolidation
    variables = {v["name"]: v for v in first["variables"]}
    cat = variables["CAT"]
    assert cat["column_consolidation"] == rule
    assert "column_consolidation" not in variables["UNITID"]
    assert cat["observed_years"] == [2004, 2006] and cat["null_count"] == 1
    assert {r["year"] for r in cat["source_metadata"]} == {2004, 2006, 2024}
    assert cat["value_labels"] == [{"value": "1", "label": (
        "2004: Earlier category; 2006: Later category; 2024: Documented all-null category")}]
    stored = pq.read_schema(portable)
    assert json.loads(stored.metadata[b"ipeds:column_consolidation"]) == consolidation
    assert json.loads(stored.metadata[b"ipeds:export"])["column_consolidation"] == consolidation
    assert decode_embedded_variable_metadata(stored.field("CAT"))["column_consolidation"] == rule

    # Prove that the annotated file alone carries the original columns and
    # year-scoped meanings into another actual export.
    for path in (dictionary, codes, lineage):
        path.unlink()
    output = tmp_path / "reexport.csv"
    result = run_script("Scripts/08_build_custom_panel.py", "--input", portable, "--output", output,
                        "--vars", "CAT", "--require-ready", "--log-file", "",
                        env={"IPEDSDB_ROOT": str(tmp_path)})
    assert result.returncode == 0, result.stdout
    second = json.loads(output.with_name(output.name + ".metadata.json").read_text())
    reexported = next(v for v in second["variables"] if v["name"] == "CAT")
    assert second["column_consolidation"] == consolidation
    assert reexported["column_consolidation"] == rule
    assert reexported["source_metadata"] == cat["source_metadata"]
    assert reexported["value_labels"] == cat["value_labels"]
    import pyarrow.csv as pcsv
    readback = pcsv.read_csv(output, convert_options=pcsv.ConvertOptions(column_types={"CAT": pa.string()},
                                                                    strings_can_be_null=True))
    assert readback["CAT"].to_pylist() == ["1", "1", None]


@pytest.mark.parametrize("payload", [b"[]", b"null", b"true"])
def test_export_refuses_nonobject_consolidation_before_publishing_files(tmp_path, payload):
    panel, dictionary, codes, output = [tmp_path / name for name in (
        "source.parquet", "dictionary.parquet", "codes.parquet", "export.parquet")]
    table = pa.table({"UNITID": [100001], "year": [2023], "CAT": ["1"]})
    pq.write_table(table.replace_schema_metadata({b"ipeds:column_consolidation": payload}), panel)
    pq.write_table(pa.Table.from_pylist([definition(2023)]), dictionary)
    pq.write_table(pa.Table.from_pylist([code(2023)]), codes)
    result = run_script("Scripts/08_build_custom_panel.py", "--input", panel, "--output", output,
                        "--all-vars", "--require-ready", "--log-file", "", "--dictionary", dictionary,
                        "--codes", codes, env={"IPEDSDB_ROOT": str(tmp_path)})
    assert result.returncode != 0
    assert "Source column consolidation must be a JSON object" in result.stdout
    assert not output.exists()
    assert not list(tmp_path.glob("export.parquet.*"))


@pytest.mark.parametrize("embedded", [b"broken json", b"[]", b"null", b"\xff"])
def test_malformed_embedded_export_scope_fails_clearly(embedded):
    schema = pa.schema([("CAT", pa.string())]).with_metadata({b"ipeds:export": embedded})
    with pytest.raises(ValueError, match="embedded export scope policy"):
        year_scope_enabled(schema)
