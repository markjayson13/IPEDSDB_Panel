from __future__ import annotations

import copy
import csv
import json

import pyarrow as pa
import pyarrow.dataset as ds
import pytest

from export_integrity import validate_observed_codes
from export_metadata import build_export_metadata
from export_metadata_supplement import DEFAULT_REGISTRY, SOURCE, apply_export_metadata_supplement, load_supplement


def definition(name="APPDATE", year=2010, **changes):
    registry, _, _ = load_supplement()
    row = next(r for r in registry["definitions"] if r["varname"] == name and r["year"] == year)
    return {**{k: v for k, v in row.items() if k not in {"codes", "evidence_id", "varlist_row"}},
            "longDescription": "Source description", "DataType": "N", "format": "Disc", **changes}


def build(tmp_path, source, codes):
    dictionary = tmp_path / "dictionary.csv"
    codebook = tmp_path / "codes.csv"
    for path, rows, columns in [(dictionary, [source], list(source)),
                                (codebook, codes, ["year", "source_file", "access_table_name", "varname",
                                                   "varnumber", "varTitle", "codevalue", "valuelabel"])]:
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
    return build_export_metadata(pa.schema([(source["varname"], pa.string())]), [source["year"]], dictionary, codebook)


def test_registry_contains_only_verified_nonblank_labels():
    registry, evidence, checksum = load_supplement()
    assert len(registry["definitions"]) == 36
    assert sum(len(r["codes"]) for r in registry["definitions"]) == 1553
    assert len(evidence["sources"]) == 17 and len(checksum) == 64
    assert all(c["valuelabel"] for r in registry["definitions"] for c in r["codes"])


def test_missing_codes_receive_exact_year_meanings_and_readiness_recomputed(tmp_path):
    result = build(tmp_path, definition(), [])
    original = copy.deepcopy(result["variables"][0]["source_metadata"])
    apply_export_metadata_supplement(result)
    variable = result["variables"][0]
    assert variable["source_metadata"] == original
    assert variable["metadata_status"] == result["metadata_status"] == "complete"
    assert variable["value_labels"] == [
        {"value": "-1", "label": "Not reported"}, {"value": "-2", "label": "Not applicable"},
        {"value": "1", "label": "Fall 2009"}, {"value": "2", "label": "Fall 2010"},
    ]
    assert all(r["source"] == SOURCE and r["source_row"] > 1 for r in variable["resolved_value_label_records"])
    assert all(r["metadata_supplement_sha256"] == result["metadata_supplement"]["sha256"]
               for r in variable["resolved_value_label_records"])
    snapshot = copy.deepcopy(result)
    apply_export_metadata_supplement(result)
    assert result == snapshot


@pytest.mark.parametrize("change", [
    {"year": 2009}, {"access_table_name": "IC2010_OTHER"}, {"source_file": "HD"},
    {"varnumber": "999"}, {"varname": "OTHER"}, {"varTitle": "Different meaning"},
])
def test_no_cross_year_table_source_variable_number_or_title_borrowing(tmp_path, change):
    result = build(tmp_path, {**definition(), **change}, [])
    original = copy.deepcopy(result)
    apply_export_metadata_supplement(result)
    assert result == original


def test_nonblank_existing_conflict_is_refused(tmp_path):
    source = definition()
    existing = {**source, "codevalue": "1", "valuelabel": "Existing authoritative meaning"}
    result = build(tmp_path, source, [existing])
    before = copy.deepcopy(result["variables"][0]["value_label_records"])
    apply_export_metadata_supplement(result)
    variable = result["variables"][0]
    assert variable["value_label_records"][:1] == before
    assert [r["valuelabel"] for r in variable["resolved_value_label_records"] if r["codevalue"] == "1"] == [existing["valuelabel"]]
    assert "metadata_supplement_conflict" in {i["code"] for i in result["issues"]}
    assert variable["value_labels"] == []
    assert result["metadata_status"] == "incomplete"


def test_literal_none_restored_original_retained_and_unknown_38_stays_unknown(tmp_path):
    source = definition("CUFASB", 2008)
    result = build(tmp_path, source, [{**source, "codevalue": "0", "valuelabel": ""}])
    original = copy.deepcopy(result["variables"][0]["value_label_records"])
    apply_export_metadata_supplement(result)
    variable = result["variables"][0]
    assert variable["value_label_records"][:1] == original
    assert variable["value_labels"] == [{"value": "0", "label": "None"}]
    assert variable["resolved_value_label_records"][0]["valuelabel"] == "None"
    dataset = ds.dataset(pa.table({"year": [2008, 2008], "CUFASB": ["0", "38"]}))
    check = validate_observed_codes(dataset, result)
    assert check["unknown_code_count"] == 1
    assert check["issues"][0]["examples"] == [{"year": 2008, "value": "38"}]


@pytest.mark.parametrize("original_label", ["", "Conflicting existing meaning"])
def test_unnamed_original_code_requires_exact_unique_number_and_keeps_nonblank_meaning(tmp_path, original_label):
    source = definition("CUFASB", 2008)
    result = build(tmp_path, source, [{**source, "varname": "", "codevalue": "0", "valuelabel": original_label}])
    original = copy.deepcopy(result["variables"][0]["value_label_records"])
    apply_export_metadata_supplement(result)
    variable = result["variables"][0]
    assert variable["value_label_records"][:1] == original
    if original_label:
        assert variable["resolved_value_label_records"][0]["valuelabel"] == original_label
        assert "metadata_supplement_conflict" in {r["code"] for r in result["issues"]}
    else:
        assert variable["value_labels"] == [{"value": "0", "label": "None"}]
        assert variable["resolved_value_label_records"][0]["varname"] == "CUFASB"


def test_ambiguous_number_candidates_cannot_be_cured_by_named_supplement(tmp_path):
    source = definition("CUFASB", 2008)
    result = build(tmp_path, source, [{**source, "varname": "", "codevalue": "0", "valuelabel": ""}])
    result["variables"][0]["code_identity_candidates"] = [source, {**source, "varname": "OTHER"}]
    issue = {"code": "value_label_identity_ambiguous", "variable": "CUFASB", "severity": "warning", "message": "Ambiguous."}
    result["issues"].append(issue)
    before = copy.deepcopy(result)
    apply_export_metadata_supplement(result)
    assert result == before


def test_unrelated_description_and_observation_failures_remain(tmp_path):
    result = build(tmp_path, definition(longDescription=""), [])
    observation = {"severity": "error", "code": "unknown_observed_code", "variable": "APPDATE", "message": "Not rechecked"}
    result["issues"].append(observation)
    result["readiness_status"] = "incomplete"
    apply_export_metadata_supplement(result)
    assert observation in result["issues"]
    assert "variable_description_missing" in {i["code"] for i in result["issues"]}
    assert result["variables"][0]["description"] == ""
    assert result["metadata_status"] == result["readiness_status"] == "incomplete"


def test_embedded_roundtrip_reapplies_only_versioned_evidence(tmp_path):
    source = definition("CUFASB", 2008)
    result = build(tmp_path, source, [{**source, "codevalue": "0", "valuelabel": ""}])
    apply_export_metadata_supplement(result)
    variable = result["variables"][0]
    schema = pa.schema([pa.field("CUFASB", pa.string(), metadata={b"ipeds:variable": json.dumps(variable).encode()})])
    again = build_export_metadata(schema, [2008], None, None)
    apply_export_metadata_supplement(again)
    assert again["variables"][0]["value_label_records"] == variable["value_label_records"]
    assert again["variables"][0]["resolved_value_label_records"] == variable["resolved_value_label_records"]
    assert again["metadata_status"] == "complete"
    again["variables"][0]["value_label_records"][-1]["valuelabel"] = "Invented replacement"
    with pytest.raises(ValueError, match="versioned proof"):
        apply_export_metadata_supplement(again)


def test_embedded_missing_code_supplement_retains_top_level_proof(tmp_path):
    result = build(tmp_path, definition(), [])
    apply_export_metadata_supplement(result)
    variable = result["variables"][0]
    schema = pa.schema([pa.field("APPDATE", pa.string(), metadata={b"ipeds:variable": json.dumps(variable).encode()})])
    again = build_export_metadata(schema, [2010], None, None)
    apply_export_metadata_supplement(again)
    assert again["metadata_supplement"] == result["metadata_supplement"]
    assert again["metadata_status"] == "complete"


def test_evidence_manifest_checksum_is_bound(tmp_path):
    registry = json.loads(DEFAULT_REGISTRY.read_text())
    registry_path = tmp_path / "registry.json"
    registry_path.write_text(json.dumps(registry))
    (tmp_path / registry["evidence_manifest"]).write_text("{}")
    with pytest.raises(ValueError, match="checksum"):
        load_supplement(registry_path)
