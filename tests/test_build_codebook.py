from __future__ import annotations

import copy
import csv
import gzip
import json

import pytest

import build_codebook
from build_codebook import (PANEL, add_stata, build, code_records, grouped, make_detail, parquet_codebook,
                            public_issue, sha256, verify_hash, write_assets)


def variable(**changes):
    return {"name": "V", "label": "Panel label", "description": "Panel description",
            "storage_type": "string", "observed_years": [2004, 2006], "null_count": 2,
            "metadata_status": "complete", "source_metadata": [], "semantic_metadata": {}, **changes}


def metadata(v):
    return {"variables": [v], "row_count": 3, "column_count": 1, "years": [2004, 2006],
            "panel_keys": ["UNITID", "year"], "issues": []}


def test_year_groups_preserve_gaps_blanks_and_different_meanings():
    records = [{"year": 2004, "label": "A", "description": "Meaning"},
               {"year": 2006, "label": "A", "description": "Meaning"},
               {"year": 2005, "label": "A", "description": ""},
               {"year": 2007, "label": "A", "description": "Other meaning"}]
    result = grouped(records)
    assert result[0]["years"] == [2004, 2006]
    assert result[1]["description"] == "" and result[1]["years"] == [2005]
    assert result[2]["description"] == "Other meaning"


def test_table_correction_preserves_original_reference_and_exact_evidence():
    source = {"year": 2023, "source_file": "SFA_P", "varname": "UPGRNTN", "varnumber": "00070306",
              "access_table_name": "SFA2223_P1", "resolved_physical_table": "SFA2223_P1",
              "original_access_table_name": "SFA2223_P2", "varTitle": "Pell recipients",
              "longDescription": "Pell grant recipients", "metadata_correction_id": "2023-sfa-v1:70306:UPGRNTN",
              "metadata_correction_reason": "Verified physical column", "imputationvar": "XUPGRNTN",
              "reference_period_start": "2022-07-01", "reference_period_end": "2023-06-30",
              "source_table_reference_period": "July 1, 2022 - June 30, 2023",
              "imputation_flag_availability": "not included in this Access release; dictionary association only",
              "source_database_sha256": "a" * 64}
    original = copy.deepcopy(source)
    result = make_detail(variable(source_metadata=[source]), [])
    row = result["source_records"][0]
    assert row["table"] == "SFA2223_P1" and row["original_table"] == "SFA2223_P2"
    assert row["correction_id"] == source["metadata_correction_id"]
    assert row["source_database_sha256"] == "a" * 64
    assert row["reference_period_start"] == "2022-07-01" and row["years"] == [2023]
    assert result["definitions"][row["definition_index"]]["description"] == "Pell grant recipients"
    assert "title" not in row and "description" not in row
    assert source == original


def test_source_definition_references_normalize_only_whitespace_and_keep_blanks():
    v = variable(year_scoped_definitions=[{"year": 2004, "label": "A label", "description": "The meaning"}],
                 source_metadata=[
                     {"year": 2004, "varTitle": "A label", "longDescription": "The\n\n meaning", "source_file": "S"},
                     {"year": 2006, "varTitle": "A label", "longDescription": "", "source_file": "S"},
                 ])
    detail = make_detail(v, [])
    assert len(detail["definitions"]) == 2
    assert detail["description"] == ""
    rows = detail["source_records"]
    first = detail["definitions"][rows[0]["definition_index"]]
    second = detail["definitions"][rows[1]["definition_index"]]
    assert first == {"label": "A label", "description": "The meaning", "years": [2004]}
    assert second == {"label": "A label", "description": "", "years": [2006]}


def test_codes_group_sources_within_year_before_grouping_years():
    rows = [{"year": 2004, "source_file": "IC", "codevalue": "1", "valuelabel": "Required"},
            {"year": 2006, "source_file": "ADM", "codevalue": "1", "valuelabel": "Required"},
            {"year": 2007, "source_file": "ADM", "codevalue": "1", "valuelabel": "Required"}]
    codes = code_records(rows)
    assert codes == [{"code": "1", "label": "Required", "sources": ["IC"], "years": [2004]},
                     {"code": "1", "label": "Required", "sources": ["ADM"], "years": [2006, 2007]}]


def test_unknown_observation_is_not_given_a_meaning_and_blank_labels_survive():
    issue = {"variable": "V", "code": "unknown_observed_code", "count": 1,
             "message": "No meaning is available", "examples": [{"year": 2006, "value": "38", "UNITID": 999}]}
    result = make_detail(variable(resolved_value_label_records=[
        {"year": 2004, "codevalue": "0", "valuelabel": "None", "source_file": "IC"},
        {"year": 2006, "codevalue": "0", "valuelabel": "", "source_file": "IC"},
    ]), [public_issue(issue)])
    assert [c["label"] for c in result["codes"]] == ["None", ""]
    assert not any(c["code"] == "38" for c in result["codes"])
    assert result["issues"][0]["examples"] == [{"year": 2006, "value": "38"}]
    assert result["has_issues"]


def test_supplement_preserves_original_blank_label_and_evidence():
    v = variable(value_label_records=[{"year": 2004, "codevalue": "0", "valuelabel": "", "source_file": "IC"}],
                 resolved_value_label_records=[{"year": 2004, "codevalue": "0", "valuelabel": "None", "source_file": "IC",
                    "metadata_supplement_id": "exact-year-v1", "metadata_supplement_sha256": "b" * 64,
                    "evidence_url": "https://nces.ed.gov/example.zip"}])
    result = make_detail(v, [])
    assert result["codes"][0]["label"] == "None"
    assert result["original_codes"][0]["label"] == ""
    assert result["code_provenance"][0]["metadata_supplement_sha256"] == "b" * 64


def test_native_stata_mapping_preserved_without_recoding_source_codebook():
    v = variable(resolved_value_label_records=[{"year": 2004, "codevalue": "A", "valuelabel": "Active"}])
    index, details = parquet_codebook(metadata(v))
    stata = metadata({**v, "export_name": "V_stata", "export_label": "Actual truncated Stata label",
                      "stata_storage_conversion": "string_categories_to_numeric",
                      "stata_source_code_map": [{"source_code": "A", "export_code": 1, "label": "Active"}],
                      "stata_value_labels": {"1": "A: Active"}})
    add_stata(index, details, stata)
    assert details["V"]["codes"][0]["code"] == "A"
    assert details["V"]["stata"]["source_code_map"][0] == {"source_code": "A", "export_code": 1, "label": "Active"}
    assert details["V"]["stata"]["value_labels"] == {"1": "A: Active"}
    assert details["V"]["stata_name"] == "V_stata"
    assert details["V"]["stata"]["label"] == "Actual truncated Stata label"


def test_hash_mismatch_stops_generation(tmp_path):
    path = tmp_path / "panel.parquet"
    path.write_bytes(b"changed data")
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        verify_hash(path, "a" * 64)


def test_published_manifest_binds_metadata_to_data_before_writing(tmp_path):
    final = tmp_path / "Final"
    final.mkdir()
    data = final / f"{PANEL}.parquet"
    data.write_bytes(b"published panel")
    companion = final / f"{PANEL}.parquet.metadata.json"
    companion.write_text(json.dumps({**metadata(variable()), "data_sha256": "a" * 64}))
    artifacts = [{"path": f"Final/{p.name}", "sha256": sha256(p)} for p in (data, companion)]
    (final / "manifest.json").write_text(json.dumps({"artifacts": artifacts}))
    output = tmp_path / "public"
    with pytest.raises(ValueError, match="data_sha256 differs from published manifest"):
        build(tmp_path, output)
    assert not output.exists()


def test_stata_identity_and_coverage_mismatch_rejected():
    index, details = parquet_codebook(metadata(variable()))
    with pytest.raises(ValueError, match="identities differ"):
        add_stata(index, details, metadata(variable(name="OTHER")))
    with pytest.raises(ValueError, match="observation coverage"):
        add_stata(index, details, metadata(variable(null_count=0)))


def test_downloads_and_shards_are_complete_and_deterministic(tmp_path):
    index, details = parquet_codebook(metadata(variable()))
    add_stata(index, details, metadata(variable()))
    write_assets(index, details, tmp_path)
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    write_assets(index, details, tmp_path)
    assert before == {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    saved = json.loads((tmp_path / "index.json").read_text())
    item = saved["variables"][0]
    detail = json.loads((tmp_path / item["detail_file"]).read_text())["V"]
    assert detail["definitions"][0]["years"] == [2004, 2006]
    assert all((tmp_path / name).exists() for key, name in saved["downloads"].items() if key != "pdf")


def test_dictionary_csv_uses_latest_scoped_description_and_history_remains_separate(tmp_path):
    v = variable(year_scoped_definitions=[
        {"year": 2004, "label": "Earlier", "description": "Earlier meaning"},
        {"year": 2006, "label": "Later", "description": "Latest meaning"},
        {"year": 2008, "label": "Later", "description": "Latest meaning"},
    ])
    index, details = parquet_codebook(metadata(v))
    add_stata(index, details, metadata(v))
    write_assets(index, details, tmp_path)
    with (tmp_path / "codebook.csv").open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["description"] == "Latest meaning"
    assert rows[0]["description_years"] == "2006; 2008"
    assert "source_records" not in rows[0] and "definitions" not in rows[0]
    with (tmp_path / "definitions.csv").open(encoding="utf-8-sig", newline="") as handle:
        definitions = list(csv.DictReader(handle))
    assert definitions == [
        {"variable": "V", "definition_index": "0", "years": "2004", "label": "Earlier", "description": "Earlier meaning"},
        {"variable": "V", "definition_index": "1", "years": "2006; 2008", "label": "Later", "description": "Latest meaning"},
    ]


def test_dictionary_csv_does_not_fill_missing_latest_description_from_older_year():
    detail = make_detail(variable(year_scoped_definitions=[
        {"year": 2004, "label": "A", "description": "Old meaning"},
        {"year": 2006, "label": "A", "description": ""},
    ]), [])
    row = build_codebook.dictionary_csv_row(detail)
    assert row["description"] == "" and row["description_years"] == "2006"


def test_shard_boundaries_keep_all_variables_and_gzip_csv_is_lossless(tmp_path, monkeypatch):
    monkeypatch.setattr(build_codebook, "SHARD_BYTES", 1500)
    details = {}
    index = None
    for name in ("A", "B", "C"):
        one_index, one_detail = parquet_codebook(metadata(variable(name=name)))
        add_stata(one_index, one_detail, metadata(variable(name=name)))
        details.update(one_detail)
        index = one_index
    write_assets(index, details, tmp_path)
    shards = list(tmp_path.glob("variables-*.json"))
    assert len(shards) > 1
    assert {name for shard in shards for name in json.loads(shard.read_text())} == {"A", "B", "C"}
    assert all(shard.stat().st_size < 1500 for shard in shards)
    monkeypatch.setattr(build_codebook, "MAX_ASSET_BYTES", 1000)
    name = build_codebook.write_csv(tmp_path, "large.csv", ["description"], [{"description": "x" * 2000}])
    assert name == "large.csv.gz"
    assert gzip.decompress((tmp_path / name).read_bytes()).decode("utf-8-sig") == "description\n" + "x" * 2000 + "\n"
