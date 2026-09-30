"""Check that downloadable reference text preserves authoritative metadata."""
import csv
import json
from pathlib import Path

import pytest


@pytest.fixture(params=["codebook", "provisional/codebook"])
def release(request):
    root = Path(__file__).parents[1] / "docs" / request.param
    index = json.loads((root / "index.json").read_text())
    details = {}
    for filename in {v["detail_file"] for v in index["variables"]}:
        details.update(json.loads((root / filename).read_text()))
    return root, index, details


def test_published_search_summaries_and_year_scopes_agree_with_full_records(release):
    _, index, details = release
    release_years = set(index["years"])
    for summary in index["variables"]:
        name = summary["name"]
        detail = details[name]
        for field in ("label", "storage_type", "stata_name", "observed_years", "sources",
                      "metadata_status", "has_codes", "has_issues", "null_count"):
            assert summary[field] == detail[field], (name, field)
        assert 0 <= detail["null_count"] <= index["row_count"], name
        definition_years = {year for record in detail["definitions"] for year in record["years"]}
        assert set(detail["observed_years"]) <= definition_years <= release_years, name
        for kind in ("definitions", "source_records", "codes"):
            for record in detail[kind]:
                assert set(record["years"]) <= release_years, (name, kind)
        meanings = {}
        for record in detail["codes"]:
            for year in record["years"]:
                key = (year, record["code"])
                assert meanings.setdefault(key, record["label"]) == record["label"], (name, key)


def test_dictionary_keeps_each_definition_when_latest_year_has_multiple_sources(release):
    root, index, details = release
    with (root / index["downloads"]["dictionary"]).open(encoding="utf-8-sig", newline="") as stream:
        rows = {row["name"]: row for row in csv.DictReader(stream)}
    for name in ("LINE_55", "PCF_F_RV", "REV_IC", "SFTETOTL"):
        definitions = details[name]["definitions"]
        latest = max(year for record in definitions for year in record["years"])
        variants = [record for record in definitions if latest in record["years"]]
        assert len(variants) > 1
        assert rows[name]["description_years"] == str(latest)
        assert f"Multiple source definitions for {latest}" in rows[name]["description"]
        for variant in variants:
            assert (variant["description"] or "Description not supplied") in rows[name]["description"]


@pytest.mark.parametrize("name,source_code,expected", [
    ("FYBEG", "012003", "012003: 2004: January 2003"),
    ("STABBR", "AK", "AK: Alaska"),
])
def test_pdf_preserves_source_token_in_actual_native_stata_label(release, name, source_code, expected):
    pypdf = pytest.importorskip("pypdf")
    root, index, details = release
    variable = details[name]
    mapping = next(row for row in variable["stata"]["source_code_map"] if row["source_code"] == source_code)
    native = variable["stata"]["value_labels"][str(mapping["export_code"])]
    assert native == expected
    reader = pypdf.PdfReader(root / index["downloads"]["pdf"])
    destinations = {item.title: reader.get_destination_page_number(item) for item in reader.outline}
    start = destinations[name]
    end = min(min(page for page in destinations.values() if page > start) + 1, len(reader.pages))
    text = " ".join("\n".join(reader.pages[page].extract_text() for page in range(start, end)).split())
    assert native in text
